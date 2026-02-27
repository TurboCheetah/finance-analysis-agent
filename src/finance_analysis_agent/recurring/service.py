"""Service-layer recurring detection and scheduling workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from calendar import monthrange
from statistics import median
import math
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


def _parse_non_empty(value: str, *, field_name: str) -> str:
    """
    Trim whitespace from `value` and ensure the result is not empty.
    
    Parameters:
        value (str): Input string to normalize.
        field_name (str): Field name to include in the error message if the normalized value is empty.
    
    Returns:
        str: The trimmed input string.
    
    Raises:
        ValueError: If the trimmed string is empty; message will be "`{field_name} is required`".
    """
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _validate_request(request: RecurringDetectRequest) -> _ValidatedRecurringRequest:
    """
    Validate and coerce fields from a RecurringDetectRequest into a _ValidatedRecurringRequest.
    
    Trims and requires non-empty `actor` and `reason`, converts numeric fields to ints, and enforces validation rules for lookback and iteration limits.
    
    Parameters:
        request (RecurringDetectRequest): Incoming request with raw fields to validate and coerce.
    
    Returns:
        _ValidatedRecurringRequest: Validated and normalized request data ready for downstream processing.
    
    Raises:
        ValueError: If any of the following constraints are violated:
            - `lookback_days` is not greater than 0
            - `minimum_occurrences` is less than 3
            - `tolerance_days_default` is negative
            - `max_expected_iterations` is not greater than 0
            - `actor` or `reason` is empty after trimming
    """
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    lookback_days = int(request.lookback_days)
    minimum_occurrences = int(request.minimum_occurrences)
    tolerance_days_default = int(request.tolerance_days_default)
    max_expected_iterations = int(request.max_expected_iterations)

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
        create_review_items=bool(request.create_review_items),
    )


def _add_months(value: date, *, months: int) -> date:
    """
    Advance a date by a given number of months, adjusting the day to fit the destination month.
    
    Parameters:
        value (date): The starting date.
        months (int): Number of months to add (may be zero or negative).
    
    Returns:
        date: The resulting date after adding `months`; if the destination month has fewer days than `value.day`, the result uses the last valid day of that month.
    """
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def _group_transactions(
    *,
    as_of_date: date,
    lookback_days: int,
    session: Session,
) -> tuple[dict[tuple[str, str], list[_TxnPoint]], int]:
    """
    Group posted, non-excluded transactions within a lookback window by merchant or category.
    
    Parameters:
        as_of_date (date): Inclusive end date for the lookback window.
        lookback_days (int): Number of days to look back from as_of_date (start = as_of_date - lookback_days).
    
    Returns:
        grouped (dict[tuple[str, str], list[_TxnPoint]]): Mapping where keys are ("merchant", merchant_id) or ("category", category_id)
            and values are lists of _TxnPoint ordered by posted_date then transaction id.
        skipped (int): Count of transactions within the window that had neither merchant_id nor category_id and were therefore ignored.
    """
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
    """
    Deduplicate consecutive transactions by `posted_date`, preserving order.
    
    Parameters:
        txns (list[_TxnPoint]): Ordered list of transaction points to process.
    
    Returns:
        list[date]: List of `posted_date` values with consecutive duplicates removed, keeping the first occurrence of each date.
    """
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
    """
    Infer a recurring schedule (weekly, biweekly, monthly, or non_monthly) from an ordered sequence of observed dates.
    
    Analyzes day-to-day intervals between successive distinct posted dates, computes a representative interval and deviation, and classifies the pattern into one of the supported schedule types when confidence thresholds are met. The function also determines a tolerance in days to allow matching expected occurrences to observed transactions.
    
    Parameters:
        dates (list[date]): Observed occurrence dates in chronological order (duplicates by date should be removed before calling).
        minimum_occurrences (int): Minimum number of observed dates required to consider inference.
        tolerance_days_default (int): Baseline tolerance in days; the returned tolerance is the greater of this value and the observed maximal deviation.
    
    Returns:
        _InferredSchedule | None: An _InferredSchedule with fields (schedule_type, interval_n, anchor_date, tolerance_days, dates) when a schedule can be inferred with sufficient confidence; otherwise `None`.
    """
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
        interval_n = max(2, round(median_interval / 30))
    else:
        return None

    return _InferredSchedule(
        schedule_type=schedule_type,
        interval_n=interval_n,
        anchor_date=dates[0],
        tolerance_days=tolerance_days,
        dates=dates,
    )


def _advance_expected_date(*, schedule_type: str, interval_n: int, current: date) -> date:
    """
    Advance a date to the next expected occurrence according to the given recurring schedule.
    
    Parameters:
        schedule_type (str): One of "weekly", "biweekly", "monthly", or "non_monthly" indicating the recurrence pattern.
        interval_n (int): Interval count used for "non_monthly" schedules (ignored for other types); treated as at least 1.
        current (date): The current occurrence date to advance from.
    
    Returns:
        date: The next expected occurrence date.
    
    Raises:
        ValueError: If `schedule_type` is not one of the supported values.
    """
    if schedule_type == "weekly":
        return current + timedelta(days=7)
    if schedule_type == "biweekly":
        return current + timedelta(days=14)
    if schedule_type == "monthly":
        return _add_months(current, months=1)
    if schedule_type == "non_monthly":
        return _add_months(current, months=max(1, interval_n))
    raise ValueError(f"Unsupported schedule_type: {schedule_type}")


def _expected_dates(
    *,
    inferred: _InferredSchedule,
    as_of_date: date,
    max_iterations: int,
) -> list[date]:
    """
    Generate the sequence of expected occurrence dates from the schedule's anchor up to the given as_of_date.
    
    Parameters:
        inferred (_InferredSchedule): Inferred schedule containing `schedule_type`, `interval_n`, and `anchor_date` that determine the recurrence cadence and starting date.
        as_of_date (date): Inclusive upper bound for generated expected dates.
        max_iterations (int): Maximum number of iterations allowed when generating dates; used to prevent infinite loops.
    
    Returns:
        list[date]: Ordered list of expected occurrence dates starting at `inferred.anchor_date` and not after `as_of_date`.
    
    Raises:
        ValueError: If the number of generated iterations reaches `max_iterations` before `as_of_date` is reached.
    """
    expected: list[date] = []
    current = inferred.anchor_date
    guard = 0
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
            interval_n=inferred.interval_n,
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
    """
    Select the best matching observed transaction for an expected date within a tolerance window.
    
    Parameters:
        expected_date (date): The target date to match.
        tolerance_days (int): Maximum allowed absolute difference in days between an observed transaction's posted_date and the expected_date.
        txns (list[_TxnPoint]): Candidate transactions to consider.
        used_transaction_ids (set[str]): Transaction IDs to exclude from matching.
    
    Returns:
        _TxnPoint | None: The chosen transaction whose posted_date is within `tolerance_days` of `expected_date`, or `None` if no candidate matches. When multiple candidates are within tolerance, preference is given to the smallest absolute day difference; ties are broken by earlier posted_date, then by smaller transaction_id.
    """
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
    """
    Create or update a Recurring record for a merchant or category based on an inferred schedule.
    
    Parameters:
        group_key (tuple[str, str]): A two-element tuple where the first element is the key type
            ("merchant" or "category") and the second element is the corresponding id.
        inferred (_InferredSchedule): Inferred schedule describing schedule_type, interval_n,
            anchor_date, tolerance_days, and observed dates.
        actor (str): Identifier of the actor that triggered detection (recorded in metadata).
        reason (str): Reason for the detection (recorded in metadata).
        session (Session): SQLAlchemy session used to execute the upsert and fetch the resulting row.
    
    Returns:
        Recurring: The active Recurring row after insert or update.
    
    Raises:
        ValueError: If group_key has an unsupported key type.
        RuntimeError: If the upsert completes but no active Recurring row can be retrieved.
    """
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
    """
    Insert or update a RecurringEvent for a specific recurring schedule and return the resulting row.
    
    Parameters:
    	recurring_id (str): Identifier of the Recurring row this event belongs to.
    	expected_date (date): The expected occurrence date for the recurring event.
    	observed_transaction_id (str | None): ID of the transaction that matches this expected date, or None if not observed.
    	status (str): Event status to set (e.g., "OBSERVED" or "MISSED").
    	session (Session): Database session used for the upsert and retrieval.
    
    Returns:
    	RecurringEvent: The inserted or updated RecurringEvent row.
    
    Raises:
    	RuntimeError: If the upsert did not produce or return a RecurringEvent row.
    """
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
    """
    Ensure there is an active "missed" ReviewItem for the given recurring event and return its id.
    
    Parameters:
    	recurring_event (RecurringEvent): The recurring event that was missed.
    	recurring (Recurring): The associated recurring definition containing schedule and grouping metadata.
    	actor (str): Identifier of the actor that triggered creation of the review item.
    	reason (str): Human-readable reason or context for creating the review item.
    
    Returns:
    	review_item_id (str): The id of the active missed ReviewItem.
    
    Raises:
    	RuntimeError: If an active missed ReviewItem cannot be retrieved after attempting to create or preserve one.
    """
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
    """
    Resolve any active missed review items associated with a recurring event.
    
    Finds active ReviewItem rows that correspond to a missed recurring event and updates each to status RESOLVED while setting `resolved_at` to the current UTC time.
    
    Parameters:
        recurring_event_id (str): ID of the recurring event whose missed review items should be resolved.
    """
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
    """
    Detect recurring spending patterns from transactions, persist recurring schedules and events, and produce a detection result with schedules, warnings, and causes.
    
    Processes the validated request window to group transactions by merchant or category, infer recurring schedules when there is sufficient history, upsert Recurring and RecurringEvent records for expected dates, resolve or create missed-event review items as appropriate, and accumulate warnings and informational causes encountered during processing.
    
    Parameters:
        request (RecurringDetectRequest): Detection parameters and options (lookback window, minimum occurrences, actor, reason, flags).
        session (Session): Database session used for queries and persistence.
    
    Returns:
        RecurringDetectResult: Result object containing the as_of_date, list of RecurringScheduleSnapshot entries (one per detected recurring), a list of RecurringEventWarning entries for missed events, and any informational or warning causes collected during detection.
    """

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

        recurring = _upsert_recurring(
            group_key=group_key,
            inferred=inferred,
            actor=validated.actor,
            reason=validated.reason,
            session=session,
        )

        expected_dates = _expected_dates(
            inferred=inferred,
            as_of_date=validated.as_of_date,
            max_iterations=validated.max_expected_iterations,
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
