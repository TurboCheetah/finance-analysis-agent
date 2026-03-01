"""Service-layer recurring detection and scheduling workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from calendar import monthrange
from statistics import median
import math
from numbers import Number
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Recurring, RecurringEvent, ReviewItem, Transaction
from finance_analysis_agent.recurring.types import (
    RecurringDetectCause,
    RecurringDetectRequest,
    RecurringDetectResult,
    RecurringEventWarning,
    RecurringScheduleSnapshot,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow

_POSTED_STATUS = "posted"
_ACTIVE_REVIEW_STATUSES = (ReviewItemStatus.TO_REVIEW.value, ReviewItemStatus.IN_PROGRESS.value)
_RECURRING_EVENT_STATUS_OBSERVED = "observed"
_RECURRING_EVENT_STATUS_MISSED = "missed"
_REVIEW_ITEM_TYPE_MISSED = "recurring_missed_event"
_REVIEW_REASON_CODE_MISSED = "recurring.missed_event"
_REVIEW_REF_TABLE_RECURRING_EVENTS = "recurring_events"
_ACTIVE_RECURRING_MISSED_REVIEW_FILTER_SQL = (
    "ref_table = 'recurring_events' "
    "AND item_type = 'recurring_missed_event' "
    "AND source = 'recurring' "
    "AND status IN ('to_review', 'in_progress')"
)


@dataclass(slots=True)
class _ValidatedRecurringRequest:
    as_of_date: date
    actor: str
    reason: str
    lookback_days: int
    minimum_occurrences: int
    tolerance_days_default: int
    max_expected_iterations: int
    create_review_items: bool


@dataclass(slots=True)
class _InferredSchedule:
    schedule_type: str
    interval_n: int
    anchor_date: date
    tolerance_days: int
    dates: list[date]


@dataclass(slots=True)
class _TxnPoint:
    transaction_id: str
    posted_date: date


def _parse_non_empty(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _parse_bool(value: object, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise ValueError(f"{field_name} must be a boolean")


def _parse_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise ValueError(f"{field_name} must be an integer")
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{field_name} must be an integer")
        if normalized[0] in {"+", "-"}:
            digits = normalized[1:]
        else:
            digits = normalized
        if not digits or not digits.isdigit():
            raise ValueError(f"{field_name} must be an integer")
        return int(normalized)
    if isinstance(value, Number):
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{field_name} must be an integer") from exc
        if value != parsed:
            raise ValueError(f"{field_name} must be an integer")
        return parsed
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc


def _validate_request(request: RecurringDetectRequest) -> _ValidatedRecurringRequest:
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    if not isinstance(request.as_of_date, date) or isinstance(request.as_of_date, datetime):
        raise ValueError("as_of_date must be a date")
    lookback_days = _parse_int(request.lookback_days, field_name="lookback_days")
    minimum_occurrences = _parse_int(request.minimum_occurrences, field_name="minimum_occurrences")
    tolerance_days_default = _parse_int(
        request.tolerance_days_default,
        field_name="tolerance_days_default",
    )
    max_expected_iterations = _parse_int(
        request.max_expected_iterations,
        field_name="max_expected_iterations",
    )
    create_review_items = _parse_bool(
        request.create_review_items,
        field_name="create_review_items",
    )

    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    if minimum_occurrences < 3:
        raise ValueError("minimum_occurrences must be >= 3")
    if tolerance_days_default < 0:
        raise ValueError("tolerance_days_default must be >= 0")
    if max_expected_iterations <= 0:
        raise ValueError("max_expected_iterations must be > 0")

    return _ValidatedRecurringRequest(
        as_of_date=request.as_of_date,
        actor=actor,
        reason=reason,
        lookback_days=lookback_days,
        minimum_occurrences=minimum_occurrences,
        tolerance_days_default=tolerance_days_default,
        max_expected_iterations=max_expected_iterations,
        create_review_items=create_review_items,
    )


def _add_months(value: date, *, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    if year > date.max.year:
        return date.max
    if year < date.min.year:
        return date.min
    month = (month_index % 12) + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def _group_transactions(
    *,
    as_of_date: date,
    lookback_days: int,
    session: Session,
) -> tuple[dict[tuple[str, str], list[_TxnPoint]], int]:
    from_date = as_of_date - timedelta(days=lookback_days)
    rows = session.scalars(
        select(Transaction)
        .where(
            Transaction.posted_date >= from_date,
            Transaction.posted_date <= as_of_date,
            Transaction.pending_status == _POSTED_STATUS,
            Transaction.excluded.is_(False),
        )
        .order_by(Transaction.posted_date.asc(), Transaction.id.asc())
    ).all()

    grouped: dict[tuple[str, str], list[_TxnPoint]] = {}
    skipped = 0
    for row in rows:
        grouping_key: tuple[str, str] | None = None
        if row.merchant_id is not None:
            grouping_key = ("merchant", row.merchant_id)
        elif row.category_id is not None:
            grouping_key = ("category", row.category_id)

        if grouping_key is None:
            skipped += 1
            continue

        grouped.setdefault(grouping_key, []).append(
            _TxnPoint(transaction_id=row.id, posted_date=row.posted_date)
        )

    return grouped, skipped


def _dedupe_sorted_dates(txns: list[_TxnPoint]) -> list[date]:
    unique_dates: list[date] = []
    for point in txns:
        if unique_dates and unique_dates[-1] == point.posted_date:
            continue
        unique_dates.append(point.posted_date)
    return unique_dates


def _infer_schedule(
    *,
    dates: list[date],
    minimum_occurrences: int,
    tolerance_days_default: int,
) -> _InferredSchedule | None:
    if len(dates) < minimum_occurrences:
        return None

    intervals = [
        (dates[index] - dates[index - 1]).days
        for index in range(1, len(dates))
        if (dates[index] - dates[index - 1]).days > 0
    ]
    if len(intervals) < (minimum_occurrences - 1):
        return None

    median_interval = round(median(intervals))
    max_deviation = max(abs(value - median_interval) for value in intervals)
    tolerance_days = max(tolerance_days_default, max_deviation)

    if 6 <= median_interval <= 8 and max_deviation <= 2:
        schedule_type = "weekly"
        interval_n = 1
    elif 12 <= median_interval <= 16 and max_deviation <= 3:
        schedule_type = "biweekly"
        interval_n = 1
    elif 26 <= median_interval <= 33 and max_deviation <= 5:
        schedule_type = "monthly"
        interval_n = 1
    elif median_interval >= 34 and max_deviation <= max(7, math.ceil(median_interval * 0.20)):
        schedule_type = "non_monthly"
        interval_n = max(1, median_interval // 30)
    else:
        return None

    return _InferredSchedule(
        schedule_type=schedule_type,
        interval_n=interval_n,
        anchor_date=dates[0],
        tolerance_days=tolerance_days,
        dates=dates,
    )


def _advance_expected_date(*, schedule_type: str, _interval_n: int, current: date) -> date:
    if schedule_type == "weekly":
        return current + timedelta(days=7)
    if schedule_type == "biweekly":
        return current + timedelta(days=14)
    raise ValueError(
        "Unsupported schedule_type for iterative advance: "
        f"{schedule_type}; use anchor-based monthly expected-date generation"
    )


def _expected_dates(
    *,
    inferred: _InferredSchedule,
    as_of_date: date,
    max_iterations: int,
) -> list[date]:
    expected: list[date] = []
    guard = 0
    current = inferred.anchor_date

    if inferred.schedule_type in {"monthly", "non_monthly"}:
        months_step = 1 if inferred.schedule_type == "monthly" else max(1, inferred.interval_n)
        while current <= as_of_date:
            if guard >= max_iterations:
                raise ValueError(
                    "max_expected_iterations exceeded while generating recurring expected dates: "
                    f"schedule_type={inferred.schedule_type}, "
                    f"interval_n={inferred.interval_n}, "
                    f"as_of_date={as_of_date}, "
                    f"current={current}, "
                    f"guard={guard}, "
                    f"max_iterations={max_iterations}"
                )
            expected.append(current)
            guard += 1
            next_current = _add_months(inferred.anchor_date, months=guard * months_step)
            if next_current <= current:
                break
            current = next_current
        return expected

    while current <= as_of_date:
        if guard >= max_iterations:
            raise ValueError(
                "max_expected_iterations exceeded while generating recurring expected dates: "
                f"schedule_type={inferred.schedule_type}, "
                f"interval_n={inferred.interval_n}, "
                f"as_of_date={as_of_date}, "
                f"current={current}, "
                f"guard={guard}, "
                f"max_iterations={max_iterations}"
            )
        expected.append(current)
        current = _advance_expected_date(
            schedule_type=inferred.schedule_type,
            _interval_n=inferred.interval_n,
            current=current,
        )
        guard += 1
    return expected


def _find_best_observed_transaction(
    *,
    expected_date: date,
    tolerance_days: int,
    txns: list[_TxnPoint],
    used_transaction_ids: set[str],
) -> _TxnPoint | None:
    best: _TxnPoint | None = None
    best_distance: int | None = None
    for point in txns:
        if point.transaction_id in used_transaction_ids:
            continue
        distance = abs((point.posted_date - expected_date).days)
        if distance > tolerance_days:
            continue
        if best is None or best_distance is None or distance < best_distance:
            best = point
            best_distance = distance
            continue
        if distance == best_distance and point.posted_date < best.posted_date:
            best = point
            best_distance = distance
            continue
        if distance == best_distance and point.posted_date == best.posted_date and point.transaction_id < best.transaction_id:
            best = point
            best_distance = distance
    return best


def _upsert_recurring(
    *,
    group_key: tuple[str, str],
    inferred: _InferredSchedule,
    actor: str,
    reason: str,
    session: Session,
) -> Recurring:
    key_type, key_value = group_key
    if key_type == "merchant":
        merchant_id = key_value
        category_id = None
        conflict_elements = [Recurring.merchant_id]
        conflict_where = text("active = 1 AND merchant_id IS NOT NULL AND category_id IS NULL")
    elif key_type == "category":
        merchant_id = None
        category_id = key_value
        conflict_elements = [Recurring.category_id]
        conflict_where = text("active = 1 AND category_id IS NOT NULL AND merchant_id IS NULL")
    else:
        raise ValueError(f"Unsupported recurring group key type: {key_type}")

    metadata = {
        "detected_by": "recurring_detect_and_schedule",
        "actor": actor,
        "reason": reason,
    }

    stmt = sqlite_insert(Recurring).values(
        id=str(uuid4()),
        merchant_id=merchant_id,
        category_id=category_id,
        schedule_type=inferred.schedule_type,
        interval_n=inferred.interval_n,
        anchor_date=inferred.anchor_date,
        tolerance_days=inferred.tolerance_days,
        active=True,
        metadata_json=metadata,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_elements,
        index_where=conflict_where,
        set_={
            "schedule_type": inferred.schedule_type,
            "interval_n": inferred.interval_n,
            "anchor_date": inferred.anchor_date,
            "tolerance_days": inferred.tolerance_days,
            "active": True,
            "metadata_json": metadata,
        },
    )
    session.execute(stmt)
    recurring = session.scalar(
        select(Recurring)
        .execution_options(populate_existing=True)
        .where(
            Recurring.active.is_(True),
            Recurring.merchant_id == merchant_id,
            Recurring.category_id == category_id,
        )
        .order_by(Recurring.id.asc())
        .limit(1)
    )
    if recurring is None:
        raise RuntimeError("Recurring upsert did not return an active recurring row")
    return recurring


def _upsert_recurring_event(
    *,
    recurring_id: str,
    expected_date: date,
    observed_transaction_id: str | None,
    status: str,
    session: Session,
) -> RecurringEvent:
    stmt = sqlite_insert(RecurringEvent).values(
        id=str(uuid4()),
        recurring_id=recurring_id,
        expected_date=expected_date,
        observed_transaction_id=observed_transaction_id,
        status=status,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[RecurringEvent.recurring_id, RecurringEvent.expected_date],
        set_={
            "status": status,
            "observed_transaction_id": observed_transaction_id,
        },
    )
    session.execute(stmt)
    event = session.scalar(
        select(RecurringEvent)
        .execution_options(populate_existing=True)
        .where(
            RecurringEvent.recurring_id == recurring_id,
            RecurringEvent.expected_date == expected_date,
        )
        .order_by(RecurringEvent.id.asc())
        .limit(1)
    )
    if event is None:
        raise RuntimeError("RecurringEvent upsert did not return a row")
    return event


def _ensure_active_missed_review_item(
    *,
    recurring_event: RecurringEvent,
    recurring: Recurring,
    actor: str,
    reason: str,
    session: Session,
) -> str:
    created_at = utcnow()
    stmt = sqlite_insert(ReviewItem).values(
        id=str(uuid4()),
        item_type=_REVIEW_ITEM_TYPE_MISSED,
        ref_table=_REVIEW_REF_TABLE_RECURRING_EVENTS,
        ref_id=recurring_event.id,
        reason_code=_REVIEW_REASON_CODE_MISSED,
        confidence=None,
        status=ReviewItemStatus.TO_REVIEW.value,
        source=ReviewSource.RECURRING.value,
        assigned_to=None,
        payload_json={
            "recurring_id": recurring.id,
            "expected_date": recurring_event.expected_date.isoformat(),
            "schedule_type": recurring.schedule_type,
            "interval_n": recurring.interval_n,
            "merchant_id": recurring.merchant_id,
            "category_id": recurring.category_id,
            "actor": actor,
            "reason": reason,
        },
        created_at=created_at,
        resolved_at=None,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=[
            ReviewItem.ref_table,
            ReviewItem.ref_id,
            ReviewItem.item_type,
            ReviewItem.source,
        ],
        index_where=text(_ACTIVE_RECURRING_MISSED_REVIEW_FILTER_SQL),
    )
    session.execute(stmt)

    persisted = session.scalar(
        select(ReviewItem)
        .where(
            ReviewItem.ref_table == _REVIEW_REF_TABLE_RECURRING_EVENTS,
            ReviewItem.ref_id == recurring_event.id,
            ReviewItem.item_type == _REVIEW_ITEM_TYPE_MISSED,
            ReviewItem.reason_code == _REVIEW_REASON_CODE_MISSED,
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_(_ACTIVE_REVIEW_STATUSES),
        )
        .order_by(ReviewItem.created_at.asc(), ReviewItem.id.asc())
        .limit(1)
    )
    if persisted is None:
        raise RuntimeError("Recurring missed-event review upsert did not return a row")
    return persisted.id


def _resolve_active_missed_review_items(
    *,
    recurring_event_id: str,
    session: Session,
) -> None:
    active_items = session.scalars(
        select(ReviewItem).where(
            ReviewItem.ref_table == _REVIEW_REF_TABLE_RECURRING_EVENTS,
            ReviewItem.ref_id == recurring_event_id,
            ReviewItem.item_type == _REVIEW_ITEM_TYPE_MISSED,
            ReviewItem.reason_code == _REVIEW_REASON_CODE_MISSED,
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_(_ACTIVE_REVIEW_STATUSES),
        )
    ).all()
    if not active_items:
        return
    resolved_at = utcnow()
    for item in active_items:
        item.status = ReviewItemStatus.RESOLVED.value
        item.resolved_at = resolved_at


def recurring_detect_and_schedule(
    request: RecurringDetectRequest,
    session: Session,
) -> RecurringDetectResult:
    """Detect recurring patterns and persist expected/missed recurring events."""

    validated = _validate_request(request)
    grouped_txns, skipped_without_key = _group_transactions(
        as_of_date=validated.as_of_date,
        lookback_days=validated.lookback_days,
        session=session,
    )

    causes: list[RecurringDetectCause] = []
    if skipped_without_key > 0:
        causes.append(
            RecurringDetectCause(
                code="transactions_skipped_without_group_key",
                message=(
                    f"Skipped {skipped_without_key} transaction(s) with no merchant/category key "
                    "for recurring detection"
                ),
                severity="info",
            )
        )

    schedule_snapshots: list[RecurringScheduleSnapshot] = []
    warnings: list[RecurringEventWarning] = []

    for group_key in sorted(grouped_txns):
        points = grouped_txns[group_key]
        deduped_dates = _dedupe_sorted_dates(points)
        inferred = _infer_schedule(
            dates=deduped_dates,
            minimum_occurrences=validated.minimum_occurrences,
            tolerance_days_default=validated.tolerance_days_default,
        )
        if inferred is None:
            if len(deduped_dates) >= validated.minimum_occurrences:
                causes.append(
                    RecurringDetectCause(
                        code="recurring_pattern_low_confidence",
                        message=(
                            f"Group {group_key[0]}:{group_key[1]} has sufficient history "
                            "but no stable recurring interval"
                        ),
                        severity="info",
                    )
                )
            continue

        expected_dates = _expected_dates(
            inferred=inferred,
            as_of_date=validated.as_of_date,
            max_iterations=validated.max_expected_iterations,
        )
        recurring = _upsert_recurring(
            group_key=group_key,
            inferred=inferred,
            actor=validated.actor,
            reason=validated.reason,
            session=session,
        )
        used_txn_ids: set[str] = set()
        observed_count = 0
        missed_count = 0
        last_observed_date: date | None = None

        for expected_date in expected_dates:
            matched = _find_best_observed_transaction(
                expected_date=expected_date,
                tolerance_days=recurring.tolerance_days,
                txns=points,
                used_transaction_ids=used_txn_ids,
            )

            status = _RECURRING_EVENT_STATUS_MISSED
            observed_transaction_id: str | None = None
            if matched is not None:
                status = _RECURRING_EVENT_STATUS_OBSERVED
                observed_transaction_id = matched.transaction_id
                used_txn_ids.add(matched.transaction_id)
                observed_count += 1
                if last_observed_date is None or matched.posted_date > last_observed_date:
                    last_observed_date = matched.posted_date
            else:
                missed_count += 1

            event = _upsert_recurring_event(
                recurring_id=recurring.id,
                expected_date=expected_date,
                observed_transaction_id=observed_transaction_id,
                status=status,
                session=session,
            )
            if status == _RECURRING_EVENT_STATUS_OBSERVED:
                _resolve_active_missed_review_items(
                    recurring_event_id=event.id,
                    session=session,
                )

            if status == _RECURRING_EVENT_STATUS_MISSED:
                review_item_id: str | None = None
                if validated.create_review_items:
                    review_item_id = _ensure_active_missed_review_item(
                        recurring_event=event,
                        recurring=recurring,
                        actor=validated.actor,
                        reason=validated.reason,
                        session=session,
                    )
                warnings.append(
                    RecurringEventWarning(
                        recurring_event_id=event.id,
                        recurring_id=recurring.id,
                        expected_date=expected_date,
                        tolerance_days=recurring.tolerance_days,
                        reason_code=_REVIEW_REASON_CODE_MISSED,
                        review_item_id=review_item_id,
                    )
                )

        schedule_snapshots.append(
            RecurringScheduleSnapshot(
                recurring_id=recurring.id,
                merchant_id=recurring.merchant_id,
                category_id=recurring.category_id,
                schedule_type=recurring.schedule_type,
                interval_n=recurring.interval_n,
                anchor_date=recurring.anchor_date,
                tolerance_days=recurring.tolerance_days,
                observed_count=observed_count,
                expected_count=len(expected_dates),
                missed_count=missed_count,
                last_observed_date=last_observed_date,
            )
        )

    if not schedule_snapshots:
        causes.append(
            RecurringDetectCause(
                code="no_recurring_schedules_detected",
                message="No recurring schedules were detected for the requested window",
                severity="warning",
            )
        )

    session.flush()

    return RecurringDetectResult(
        as_of_date=validated.as_of_date,
        schedules=sorted(schedule_snapshots, key=lambda item: item.recurring_id),
        warnings=sorted(warnings, key=lambda item: (item.expected_date, item.recurring_id, item.recurring_event_id)),
        causes=causes,
    )
