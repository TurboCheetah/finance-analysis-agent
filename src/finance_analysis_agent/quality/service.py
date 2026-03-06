"""Service-layer quality metrics persistence and query workflows."""

from __future__ import annotations

from calendar import monthrange
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import hashlib
from importlib.resources import files
import json
import logging
from pathlib import Path
from statistics import median
from uuid import uuid4

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from finance_analysis_agent.db.base import Base
from finance_analysis_agent.db.models import (
    Account,
    MetricObservation,
    Reconciliation,
    ReviewItem,
    ReviewItemEvent,
    Transaction,
)
from finance_analysis_agent.dedupe import TxnDedupeMatchRequest, txn_dedupe_match
from finance_analysis_agent.pdf_contract.types import PdfExtractedRow, PdfSubagentRequest
from finance_analysis_agent.pdf_extract.pipeline import run_layered_extraction
from finance_analysis_agent.pdf_extract.thresholds import resolve_quality_floors
from finance_analysis_agent.provenance.audit_writers import finish_run_metadata, start_run_metadata
from finance_analysis_agent.provenance.types import RunMetadataFinishRequest, RunMetadataStartRequest
from finance_analysis_agent.quality.types import (
    MetricAlertStatus,
    MetricObservationQueryRequest,
    MetricObservationQueryResult,
    MetricObservationRecord,
    QualityMetricsGenerateRequest,
    QualityMetricsGenerateResult,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus
from finance_analysis_agent.utils.time import utcnow

_PIPELINE_NAME = "quality_metrics_generate"
_SERVICE_VERSION = "quality-metrics-v1"
_SCHEMA_VERSION = "1.0.0"
_POSTED_STATUS = "posted"
_LOW_CONFIDENCE_REASONS = {
    "low_confidence_row",
    "low_confidence_page",
    "categorize.low_confidence",
}
_ACTIVE_REVIEW_STATUSES = {
    ReviewItemStatus.TO_REVIEW.value,
    ReviewItemStatus.IN_PROGRESS.value,
}
_FIXTURES_PACKAGE = "finance_analysis_agent.fixtures"
_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _ValidatedRequest:
    actor: str
    reason: str
    period_start: date
    period_end: date
    account_ids: list[str]


@dataclass(slots=True)
class _Threshold:
    value: float | None
    operator: str | None


def _normalize_for_hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_non_empty(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _period_month_bounds(period_month: str) -> tuple[date, date]:
    if len(period_month) != 7 or period_month[4] != "-":
        raise ValueError("period_month must be in YYYY-MM format")
    start = date.fromisoformat(f"{period_month}-01")
    end = date(start.year, start.month, monthrange(start.year, start.month)[1])
    return start, end


def _resolve_period(request: QualityMetricsGenerateRequest) -> tuple[date, date]:
    if request.period_month is not None:
        if request.period_start is not None or request.period_end is not None:
            raise ValueError("period_month cannot be combined with period_start/period_end")
        return _period_month_bounds(request.period_month)
    if request.period_start is None or request.period_end is None:
        raise ValueError("Either period_month or both period_start and period_end are required")
    if request.period_end < request.period_start:
        raise ValueError("period_end must be >= period_start")
    return request.period_start, request.period_end


def _normalize_account_ids(values: list[str]) -> list[str]:
    normalized: set[str] = set()
    for index, value in enumerate(values):
        normalized.add(_parse_non_empty(value, field_name=f"account_ids[{index}]"))
    return sorted(normalized)


def _normalize_string_list(values: list[str], *, field_name: str) -> list[str]:
    normalized: set[str] = set()
    for index, value in enumerate(values):
        normalized.add(_parse_non_empty(value, field_name=f"{field_name}[{index}]"))
    return sorted(normalized)


def _validate_request(request: QualityMetricsGenerateRequest) -> _ValidatedRequest:
    period_start, period_end = _resolve_period(request)
    return _ValidatedRequest(
        actor=_parse_non_empty(request.actor, field_name="actor"),
        reason=_parse_non_empty(request.reason, field_name="reason"),
        period_start=period_start,
        period_end=period_end,
        account_ids=_normalize_account_ids(request.account_ids),
    )


def _start_run(validated: _ValidatedRequest, session: Session) -> str:
    run = start_run_metadata(
        RunMetadataStartRequest(
            pipeline_name=_PIPELINE_NAME,
            code_version=_SERVICE_VERSION,
            schema_version=_SCHEMA_VERSION,
            config_hash=_normalize_for_hash(
                {
                    "period_start": validated.period_start.isoformat(),
                    "period_end": validated.period_end.isoformat(),
                    "account_ids": validated.account_ids,
                }
            ),
            status="running",
            diagnostics_json={
                "phase": "start",
                "period_start": validated.period_start.isoformat(),
                "period_end": validated.period_end.isoformat(),
                "account_ids": validated.account_ids,
            },
        ),
        session,
    )
    return run.id


def _finish_run(*, run_metadata_id: str, status: str, diagnostics_json: dict[str, object], session: Session) -> None:
    finish_run_metadata(
        RunMetadataFinishRequest(
            run_metadata_id=run_metadata_id,
            status=status,
            diagnostics_json=diagnostics_json,
        ),
        session,
    )


def _metric_float(value: float | int | Decimal | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _date_bounds(validated: _ValidatedRequest) -> tuple[datetime, datetime]:
    return (
        datetime.combine(validated.period_start, time.min),
        datetime.combine(validated.period_end, time.max),
    )


def _scoped_account_ids(validated: _ValidatedRequest, session: Session) -> list[str]:
    if validated.account_ids:
        return list(validated.account_ids)

    transaction_ids = session.scalars(
        select(Transaction.account_id)
        .where(
            Transaction.posted_date >= validated.period_start,
            Transaction.posted_date <= validated.period_end,
        )
        .distinct()
        .order_by(Transaction.account_id.asc())
    ).all()
    reconciliation_ids = session.scalars(
        select(Reconciliation.account_id)
        .where(
            Reconciliation.period_end >= validated.period_start,
            Reconciliation.period_end <= validated.period_end,
        )
        .distinct()
        .order_by(Reconciliation.account_id.asc())
    ).all()
    return sorted({*transaction_ids, *reconciliation_ids})


def _make_observation(
    *,
    metric_group: str,
    metric_key: str,
    period_start: date,
    period_end: date,
    alert_status: MetricAlertStatus,
    metric_value: float | int | Decimal | None = None,
    account_id: str | None = None,
    template_key: str | None = None,
    numerator: float | int | Decimal | None = None,
    denominator: float | int | Decimal | None = None,
    threshold: _Threshold | None = None,
    dimensions: dict[str, object] | None = None,
    run_id: str | None = None,
) -> MetricObservationRecord:
    return MetricObservationRecord(
        metric_group=metric_group,
        metric_key=metric_key,
        period_start=period_start,
        period_end=period_end,
        alert_status=alert_status,
        metric_value=_metric_float(metric_value),
        account_id=account_id,
        template_key=template_key,
        numerator=_metric_float(numerator),
        denominator=_metric_float(denominator),
        threshold_value=_metric_float(threshold.value if threshold is not None else None),
        threshold_operator=threshold.operator if threshold is not None else None,
        dimensions=dict(sorted((dimensions or {}).items())),
        run_id=run_id,
    )


def _status_for_threshold(value: float | None, threshold: _Threshold) -> MetricAlertStatus:
    if value is None:
        return MetricAlertStatus.NO_DATA
    if threshold.value is None:
        return MetricAlertStatus.NO_DATA
    if threshold.operator == "<":
        return MetricAlertStatus.ALERT if value < float(threshold.value) else MetricAlertStatus.OK
    if threshold.operator == ">":
        return MetricAlertStatus.ALERT if value > float(threshold.value) else MetricAlertStatus.OK
    raise ValueError(f"Unsupported threshold operator: {threshold.operator}")


def _serialize_record(record: MetricObservationRecord) -> dict[str, object]:
    return {
        "metric_group": record.metric_group,
        "metric_key": record.metric_key,
        "key": f"{record.metric_group}.{record.metric_key}",
        "period_start": record.period_start.isoformat(),
        "period_end": record.period_end.isoformat(),
        "metric_value": record.metric_value,
        "numerator": record.numerator,
        "denominator": record.denominator,
        "threshold_value": record.threshold_value,
        "threshold_operator": record.threshold_operator,
        "alert_status": record.alert_status.value,
        "account_id": record.account_id,
        "template_key": record.template_key,
        "dimensions": record.dimensions,
        "run_id": record.run_id,
    }


def _to_record(row: MetricObservation) -> MetricObservationRecord:
    return MetricObservationRecord(
        metric_group=row.metric_group,
        metric_key=row.metric_key,
        period_start=row.period_start,
        period_end=row.period_end,
        alert_status=MetricAlertStatus(row.alert_status),
        metric_value=_metric_float(row.metric_value),
        account_id=row.account_id,
        template_key=row.template_key,
        numerator=_metric_float(row.numerator),
        denominator=_metric_float(row.denominator),
        threshold_value=_metric_float(row.threshold_value),
        threshold_operator=row.threshold_operator,
        dimensions=dict(sorted((row.dimensions_json or {}).items())),
        run_id=row.run_id,
    )


def _persist_observations(observations: list[MetricObservationRecord], *, run_id: str, session: Session) -> None:
    deletion_keys = {
        (
            observation.metric_key,
            observation.period_start,
            observation.period_end,
            observation.account_id,
            observation.template_key,
        )
        for observation in observations
    }

    for metric_key, period_start, period_end, account_id, template_key in deletion_keys:
        conditions = [
            MetricObservation.metric_key == metric_key,
            MetricObservation.period_start == period_start,
            MetricObservation.period_end == period_end,
        ]
        conditions.append(
            MetricObservation.account_id.is_(None) if account_id is None else MetricObservation.account_id == account_id
        )
        conditions.append(
            MetricObservation.template_key.is_(None)
            if template_key is None
            else MetricObservation.template_key == template_key
        )
        session.execute(delete(MetricObservation).where(*conditions))

    created_at = utcnow()
    for observation in observations:
        session.add(
            MetricObservation(
                id=str(uuid4()),
                run_id=run_id,
                metric_group=observation.metric_group,
                metric_key=observation.metric_key,
                period_start=observation.period_start,
                period_end=observation.period_end,
                account_id=observation.account_id,
                template_key=observation.template_key,
                metric_value=observation.metric_value,
                numerator=observation.numerator,
                denominator=observation.denominator,
                threshold_value=observation.threshold_value,
                threshold_operator=observation.threshold_operator,
                alert_status=observation.alert_status.value,
                dimensions_json=observation.dimensions,
                created_at=created_at,
            )
        )


def _transaction_scope_filters(validated: _ValidatedRequest) -> list[object]:
    filters: list[object] = [
        Transaction.posted_date >= validated.period_start,
        Transaction.posted_date <= validated.period_end,
        Transaction.pending_status == _POSTED_STATUS,
        Transaction.excluded.is_(False),
    ]
    if validated.account_ids:
        filters.append(Transaction.account_id.in_(validated.account_ids))
    return filters


def _reconciliation_pass_rate_metrics(
    validated: _ValidatedRequest,
    *,
    scoped_account_ids: list[str],
    run_id: str,
    session: Session,
) -> list[MetricObservationRecord]:
    rows = session.execute(
        select(Reconciliation.account_id, Reconciliation.status).where(
            Reconciliation.period_end >= validated.period_start,
            Reconciliation.period_end <= validated.period_end,
            *( [Reconciliation.account_id.in_(validated.account_ids)] if validated.account_ids else [] ),
        )
    ).all()
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for account_id, status in rows:
        counts[str(account_id)][str(status)] += 1

    threshold = _Threshold(0.95, "<")
    observations: list[MetricObservationRecord] = []
    for account_id in scoped_account_ids:
        total = sum(counts[account_id].values())
        passing = counts[account_id].get("pass", 0)
        value = None if total == 0 else passing / total
        observations.append(
            _make_observation(
                metric_group="correctness",
                metric_key="reconciliation_pass_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                account_id=account_id,
                numerator=passing,
                denominator=total,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )

    if not validated.account_ids:
        total = len(rows)
        passing = sum(1 for _, status in rows if status == "pass")
        value = None if total == 0 else passing / total
        observations.append(
            _make_observation(
                metric_group="correctness",
                metric_key="reconciliation_pass_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=passing,
                denominator=total,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )
    return observations


def _suggestion_acceptance_metrics(
    validated: _ValidatedRequest,
    *,
    run_id: str,
    session: Session,
) -> list[MetricObservationRecord]:
    period_start_dt, period_end_dt = _date_bounds(validated)
    stmt = (
        select(Transaction.account_id, ReviewItemEvent.action)
        .join(ReviewItem, ReviewItem.id == ReviewItemEvent.review_item_id)
        .join(
            Transaction,
            and_(
                ReviewItem.ref_table == "transactions",
                ReviewItem.ref_id == Transaction.id,
            ),
        )
        .where(
            ReviewItem.source == "categorize",
            ReviewItemEvent.event_type == "bulk_action_applied",
            ReviewItemEvent.action.in_(["approve_suggestion", "reject_suggestion"]),
            ReviewItemEvent.created_at >= period_start_dt,
            ReviewItemEvent.created_at <= period_end_dt,
        )
        .order_by(ReviewItemEvent.created_at.asc(), ReviewItemEvent.id.asc())
    )
    if validated.account_ids:
        stmt = stmt.where(Transaction.account_id.in_(validated.account_ids))

    rows = session.execute(stmt).all()
    threshold = _Threshold(0.70, "<")
    if not validated.account_ids:
        approved = sum(1 for _, action in rows if action == "approve_suggestion")
        rejected = sum(1 for _, action in rows if action == "reject_suggestion")
        total = approved + rejected
        value = None if total == 0 else approved / total
        return [
            _make_observation(
                metric_group="automation_quality",
                metric_key="suggestion_acceptance_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=approved,
                denominator=total,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        ]

    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for account_id, action in rows:
        counts[str(account_id)][str(action)] += 1

    observations: list[MetricObservationRecord] = []
    for account_id in validated.account_ids:
        approved = counts[account_id]["approve_suggestion"]
        rejected = counts[account_id]["reject_suggestion"]
        total = approved + rejected
        value = None if total == 0 else approved / total
        observations.append(
            _make_observation(
                metric_group="automation_quality",
                metric_key="suggestion_acceptance_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                account_id=account_id,
                numerator=approved,
                denominator=total,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )
    return observations


def _review_time_to_inbox_zero_metrics(
    validated: _ValidatedRequest,
    *,
    run_id: str,
    session: Session,
) -> list[MetricObservationRecord]:
    period_start_dt, period_end_dt = _date_bounds(validated)
    stmt = select(Transaction.account_id, ReviewItem.created_at, ReviewItem.resolved_at).join(
        Transaction,
        and_(
            ReviewItem.ref_table == "transactions",
            ReviewItem.ref_id == Transaction.id,
        ),
    ).where(
        ReviewItem.status.in_([ReviewItemStatus.RESOLVED.value, ReviewItemStatus.REJECTED.value]),
        ReviewItem.resolved_at.is_not(None),
        ReviewItem.created_at >= period_start_dt,
        ReviewItem.created_at <= period_end_dt,
    )
    if validated.account_ids:
        stmt = stmt.where(Transaction.account_id.in_(validated.account_ids))

    rows = session.execute(stmt).all()
    threshold = _Threshold(72.0, ">")
    if not validated.account_ids:
        durations = [
            round(((resolved_at - created_at).total_seconds() / 3600), 6)
            for _, created_at, resolved_at in rows
            if created_at is not None and resolved_at is not None
        ]
        value = None if not durations else float(round(median(durations), 6))
        return [
            _make_observation(
                metric_group="automation_quality",
                metric_key="review_time_to_inbox_zero_hours",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=len(durations),
                denominator=len(durations),
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        ]

    durations_by_account: dict[str, list[float]] = defaultdict(list)
    for account_id, created_at, resolved_at in rows:
        if created_at is None or resolved_at is None:
            continue
        durations_by_account[str(account_id)].append(
            round(((resolved_at - created_at).total_seconds() / 3600), 6)
        )

    observations: list[MetricObservationRecord] = []
    for account_id in validated.account_ids:
        durations = durations_by_account.get(account_id, [])
        value = None if not durations else float(round(median(durations), 6))
        observations.append(
            _make_observation(
                metric_group="automation_quality",
                metric_key="review_time_to_inbox_zero_hours",
                period_start=validated.period_start,
                period_end=validated.period_end,
                account_id=account_id,
                numerator=len(durations),
                denominator=len(durations),
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )
    return observations


def _unknown_merchant_rate_metrics(
    validated: _ValidatedRequest,
    *,
    scoped_account_ids: list[str],
    run_id: str,
    session: Session,
) -> list[MetricObservationRecord]:
    rows = session.execute(
        select(Transaction.account_id, Transaction.merchant_id).where(*_transaction_scope_filters(validated))
    ).all()
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for account_id, merchant_id in rows:
        counts[str(account_id)]["total"] += 1
        if merchant_id is None:
            counts[str(account_id)]["unknown"] += 1

    threshold = _Threshold(0.05, ">")
    observations: list[MetricObservationRecord] = []
    for account_id in scoped_account_ids:
        total = counts[account_id]["total"]
        unknown = counts[account_id]["unknown"]
        value = None if total == 0 else unknown / total
        observations.append(
            _make_observation(
                metric_group="trust_health",
                metric_key="unknown_merchant_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                account_id=account_id,
                numerator=unknown,
                denominator=total,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )

    if not validated.account_ids:
        total = len(rows)
        unknown = sum(1 for _, merchant_id in rows if merchant_id is None)
        value = None if total == 0 else unknown / total
        observations.append(
            _make_observation(
                metric_group="trust_health",
                metric_key="unknown_merchant_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=unknown,
                denominator=total,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )
    return observations


def _low_confidence_transaction_rate_metrics(
    validated: _ValidatedRequest,
    *,
    scoped_account_ids: list[str],
    run_id: str,
    session: Session,
) -> list[MetricObservationRecord]:
    period_start_dt, period_end_dt = _date_bounds(validated)
    base_scope = select(Transaction.id, Transaction.account_id).where(*_transaction_scope_filters(validated)).subquery()
    numerator_rows = session.execute(
        select(base_scope.c.account_id, func.count(func.distinct(base_scope.c.id)))
        .join(ReviewItem, and_(ReviewItem.ref_table == "transactions", ReviewItem.ref_id == base_scope.c.id))
        .where(
            ReviewItem.reason_code.in_(sorted(_LOW_CONFIDENCE_REASONS)),
            or_(
                ReviewItem.status.in_(sorted(_ACTIVE_REVIEW_STATUSES)),
                and_(ReviewItem.created_at >= period_start_dt, ReviewItem.created_at <= period_end_dt),
            ),
        )
        .group_by(base_scope.c.account_id)
    ).all()
    denominator_rows = session.execute(
        select(Transaction.account_id, func.count(Transaction.id))
        .where(*_transaction_scope_filters(validated))
        .group_by(Transaction.account_id)
    ).all()

    numerators = {str(account_id): int(count) for account_id, count in numerator_rows}
    denominators = {str(account_id): int(count) for account_id, count in denominator_rows}
    threshold = _Threshold(0.10, ">")
    observations: list[MetricObservationRecord] = []
    for account_id in scoped_account_ids:
        denominator = denominators.get(account_id, 0)
        numerator = numerators.get(account_id, 0)
        value = None if denominator == 0 else numerator / denominator
        observations.append(
            _make_observation(
                metric_group="trust_health",
                metric_key="low_confidence_transaction_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                account_id=account_id,
                numerator=numerator,
                denominator=denominator,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )

    if not validated.account_ids:
        denominator = sum(denominators.values())
        numerator = sum(numerators.values())
        value = None if denominator == 0 else numerator / denominator
        observations.append(
            _make_observation(
                metric_group="trust_health",
                metric_key="low_confidence_transaction_rate",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=numerator,
                denominator=denominator,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(value, threshold),
                run_id=run_id,
            )
        )
    return observations


def _unreconciled_days_metrics(
    validated: _ValidatedRequest,
    *,
    scoped_account_ids: list[str],
    run_id: str,
    session: Session,
) -> list[MetricObservationRecord]:
    threshold = _Threshold(30.0, ">")
    default_days = (validated.period_end - validated.period_start).days + 1
    rows = session.execute(
        select(Reconciliation.account_id, func.max(Reconciliation.period_end))
        .where(
            Reconciliation.status == "pass",
            Reconciliation.period_end <= validated.period_end,
            *( [Reconciliation.account_id.in_(validated.account_ids)] if validated.account_ids else [] ),
        )
        .group_by(Reconciliation.account_id)
    ).all()
    latest = {str(account_id): period_end for account_id, period_end in rows}
    observations: list[MetricObservationRecord] = []
    for account_id in scoped_account_ids:
        latest_pass = latest.get(account_id)
        value = default_days if latest_pass is None else max((validated.period_end - latest_pass).days, 0)
        observations.append(
            _make_observation(
                metric_group="trust_health",
                metric_key="unreconciled_days",
                period_start=validated.period_start,
                period_end=validated.period_end,
                account_id=account_id,
                numerator=value,
                denominator=default_days,
                metric_value=value,
                threshold=threshold,
                alert_status=_status_for_threshold(float(value), threshold),
                run_id=run_id,
            )
        )
    return observations


def _normalize_date(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _normalize_amount(value: Decimal | str | int | float | None) -> str | None:
    if value is None:
        return None
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, ValueError):
        return str(value)


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else None
    return str(value)


def _expected_row_key(expected: dict[str, object]) -> tuple[str | None, str | None, str | None, str | None, int | None]:
    return (
        _normalize_date(expected.get("posted_date")),
        _normalize_amount(expected.get("amount")),
        _normalize_text(expected.get("currency")),
        _normalize_text(expected.get("original_statement")),
        expected.get("page_no") if isinstance(expected.get("page_no"), int) else None,
    )


def _predicted_row_key(row: PdfExtractedRow) -> tuple[str | None, str | None, str | None, str | None, int | None]:
    return (
        _normalize_date(row.posted_date),
        _normalize_amount(row.amount),
        _normalize_text(row.currency),
        _normalize_text(row.original_statement),
        row.page_no,
    )


def _row_precision_recall(
    *,
    rows: list[PdfExtractedRow],
    expected_rows: list[dict[str, object]],
) -> tuple[int, int, int, float, float]:
    predicted_counts = Counter(_predicted_row_key(row) for row in rows if row.parse_status == "parsed")
    expected_counts = Counter(_expected_row_key(row) for row in expected_rows)
    all_keys = predicted_counts.keys() | expected_counts.keys()
    true_positives = sum(min(predicted_counts[key], expected_counts[key]) for key in all_keys)
    false_positives = sum(predicted_counts.values()) - true_positives
    false_negatives = sum(expected_counts.values()) - true_positives
    precision_denominator = true_positives + false_positives
    recall_denominator = true_positives + false_negatives
    precision = 0.0 if precision_denominator == 0 else true_positives / precision_denominator
    recall = 1.0 if recall_denominator == 0 else true_positives / recall_denominator
    return true_positives, false_positives, false_negatives, round(precision, 6), round(recall, 6)


def _build_fixture_request(fixture: dict[str, object], *, template_key: str) -> PdfSubagentRequest:
    metadata = fixture.get("metadata")
    return PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(Path("/") / "tmp" / f"{template_key}.pdf"),
        account_id="acct-fixture",
        schema_version="1.0.0",
        actor="quality-metrics",
        confidence_threshold=0.8,
        template_hint=template_key,
        metadata=metadata if isinstance(metadata, dict) else {},
        source_ref=f"fixtures/{template_key}.pdf",
    )


def _pdf_fixture_resources() -> list[object]:
    fixture_dir = files(_FIXTURES_PACKAGE).joinpath("pdf_quality")
    return sorted(
        (resource for resource in fixture_dir.iterdir() if resource.name.endswith(".json")),
        key=lambda resource: resource.name,
    )


def _dedupe_fixture_payload() -> dict[str, object]:
    return json.loads(
        files(_FIXTURES_PACKAGE).joinpath("dedupe").joinpath("labeled_pairs.json").read_text(encoding="utf-8")
    )


def _pdf_fixture_metrics(
    validated: _ValidatedRequest,
    *,
    run_id: str,
) -> list[MetricObservationRecord]:
    observations: list[MetricObservationRecord] = []
    for fixture_resource in _pdf_fixture_resources():
        fixture = json.loads(fixture_resource.read_text(encoding="utf-8"))
        raw_template = fixture.get("template_hint")
        template_key = (
            raw_template.strip()
            if isinstance(raw_template, str) and raw_template.strip()
            else Path(fixture_resource.name).stem
        )
        request = _build_fixture_request(fixture, template_key=template_key)
        text_pages = fixture.get("text_pages")
        expected_rows = fixture.get("expected_rows")
        if not isinstance(text_pages, list) or not isinstance(expected_rows, list):
            raise ValueError(f"Fixture {fixture_resource.name} is missing text_pages or expected_rows")

        response = run_layered_extraction(
            request,
            text_page_supplier=lambda _request, pages=text_pages: (pages, []),
        )
        tp, fp, fn, precision, recall = _row_precision_recall(
            rows=response.rows,
            expected_rows=[row for row in expected_rows if isinstance(row, dict)],
        )

        metadata = fixture.get("metadata")
        issuer = metadata.get("issuer") if isinstance(metadata, dict) else None
        floors = resolve_quality_floors(template_hint=template_key, issuer=issuer if isinstance(issuer, str) else None)
        precision_threshold = _Threshold(float(floors.precision_min), "<")
        recall_threshold = _Threshold(float(floors.recall_min), "<")
        precision_status = _status_for_threshold(precision, precision_threshold)
        recall_status = _status_for_threshold(recall, recall_threshold)

        observations.append(
            _make_observation(
                metric_group="parsing_quality",
                metric_key="pdf_row_precision",
                period_start=validated.period_start,
                period_end=validated.period_end,
                template_key=template_key,
                numerator=tp,
                denominator=tp + fp,
                metric_value=precision,
                threshold=precision_threshold,
                alert_status=precision_status,
                run_id=run_id,
            )
        )
        observations.append(
            _make_observation(
                metric_group="parsing_quality",
                metric_key="pdf_row_recall",
                period_start=validated.period_start,
                period_end=validated.period_end,
                template_key=template_key,
                numerator=tp,
                denominator=tp + fn,
                metric_value=recall,
                threshold=recall_threshold,
                alert_status=recall_status,
                run_id=run_id,
            )
        )

        error_counts = response.diagnostics.run_summary.get("error_counts", {})
        if isinstance(error_counts, dict) and error_counts:
            for error_code in sorted(error_counts):
                count = int(error_counts[error_code])
                error_status = (
                    MetricAlertStatus.ALERT
                    if count > 0
                    and precision_status is MetricAlertStatus.OK
                    and recall_status is MetricAlertStatus.OK
                    else MetricAlertStatus.OK
                )
                observations.append(
                    _make_observation(
                        metric_group="parsing_quality",
                        metric_key="pdf_error_count",
                        period_start=validated.period_start,
                        period_end=validated.period_end,
                        template_key=template_key,
                        numerator=count,
                        denominator=sum(error_counts.values()),
                        metric_value=count,
                        threshold=_Threshold(0.0, ">"),
                        alert_status=error_status,
                        dimensions={"error_code": str(error_code)},
                        run_id=run_id,
                    )
                )
        else:
            observations.append(
                _make_observation(
                    metric_group="parsing_quality",
                    metric_key="pdf_error_count",
                    period_start=validated.period_start,
                    period_end=validated.period_end,
                    template_key=template_key,
                    numerator=0,
                    denominator=0,
                    metric_value=0,
                    threshold=_Threshold(0.0, ">"),
                    alert_status=MetricAlertStatus.OK,
                    dimensions={"error_code": "_none"},
                    run_id=run_id,
                )
            )
    return observations


def _pair_key(txn_a_id: str, txn_b_id: str) -> tuple[str, str]:
    return (txn_a_id, txn_b_id) if txn_a_id <= txn_b_id else (txn_b_id, txn_a_id)


def _seed_dedupe_fixture_data(session: Session, fixture: dict[str, object]) -> None:
    session.add(Account(id="acct-1", name="Checking", type="checking", currency="USD"))
    now = utcnow()
    for row in fixture["transactions"]:
        session.add(
            Transaction(
                id=row["id"],
                account_id=row["account_id"],
                posted_date=date.fromisoformat(row["posted_date"]),
                effective_date=date.fromisoformat(row["posted_date"]),
                amount=Decimal(row["amount"]),
                currency=row["currency"],
                original_amount=Decimal(row["amount"]),
                original_currency=row["currency"],
                pending_status=row["pending_status"],
                original_statement=row["original_statement"],
                merchant_id=None,
                category_id=None,
                excluded=False,
                notes=None,
                source_kind=row["source_kind"],
                source_transaction_id=f"src-{row['id']}",
                import_batch_id=None,
                transfer_group_id=None,
                created_at=now,
                updated_at=now,
            )
        )
    session.flush()


def _dedupe_fixture_metrics(
    validated: _ValidatedRequest,
    *,
    run_id: str,
) -> list[MetricObservationRecord]:
    fixture = _dedupe_fixture_payload()
    engine = create_engine("sqlite:///:memory:")
    session_factory = sessionmaker(bind=engine, autoflush=False)
    Base.metadata.create_all(engine)
    fixture_session = session_factory()
    try:
        _seed_dedupe_fixture_data(fixture_session, fixture)
        result = txn_dedupe_match(
            TxnDedupeMatchRequest(
                actor="quality-metrics",
                reason="fixture metrics",
                include_pending=False,
                soft_review_threshold=0.75,
                soft_autolink_threshold=1.0,
            ),
            fixture_session,
        )

        expected_soft = {_pair_key(pair[0], pair[1]) for pair in fixture["expected_soft_duplicates"]}
        predicted_soft = {
            _pair_key(candidate.txn_a_id, candidate.txn_b_id)
            for candidate in result.candidates
            if candidate.classification == "soft" and candidate.score >= 0.75
        }
        true_positive = len(predicted_soft & expected_soft)
        precision = None if not predicted_soft else round(true_positive / len(predicted_soft), 6)
        recall = None if not expected_soft else round(true_positive / len(expected_soft), 6)

        return [
            _make_observation(
                metric_group="correctness",
                metric_key="dedupe_precision",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=true_positive,
                denominator=len(predicted_soft),
                metric_value=precision,
                threshold=_Threshold(0.95, "<"),
                alert_status=_status_for_threshold(precision, _Threshold(0.95, "<")),
                run_id=run_id,
            ),
            _make_observation(
                metric_group="correctness",
                metric_key="dedupe_recall",
                period_start=validated.period_start,
                period_end=validated.period_end,
                numerator=true_positive,
                denominator=len(expected_soft),
                metric_value=recall,
                threshold=_Threshold(0.90, "<"),
                alert_status=_status_for_threshold(recall, _Threshold(0.90, "<")),
                run_id=run_id,
            ),
        ]
    finally:
        fixture_session.close()
        engine.dispose()


def generate_quality_metrics(request: QualityMetricsGenerateRequest, session: Session) -> QualityMetricsGenerateResult:
    """Generate and persist quality metrics for the requested period."""

    validated = _validate_request(request)
    run_metadata_id = _start_run(validated, session)
    scoped_account_ids = _scoped_account_ids(validated, session)

    try:
        observations: list[MetricObservationRecord] = []
        observations.extend(
            _reconciliation_pass_rate_metrics(
                validated,
                scoped_account_ids=scoped_account_ids,
                run_id=run_metadata_id,
                session=session,
            )
        )
        observations.extend(_suggestion_acceptance_metrics(validated, run_id=run_metadata_id, session=session))
        observations.extend(_review_time_to_inbox_zero_metrics(validated, run_id=run_metadata_id, session=session))
        observations.extend(_pdf_fixture_metrics(validated, run_id=run_metadata_id))
        observations.extend(_dedupe_fixture_metrics(validated, run_id=run_metadata_id))
        observations.extend(
            _unreconciled_days_metrics(
                validated,
                scoped_account_ids=scoped_account_ids,
                run_id=run_metadata_id,
                session=session,
            )
        )
        observations.extend(
            _unknown_merchant_rate_metrics(
                validated,
                scoped_account_ids=scoped_account_ids,
                run_id=run_metadata_id,
                session=session,
            )
        )
        observations.extend(
            _low_confidence_transaction_rate_metrics(
                validated,
                scoped_account_ids=scoped_account_ids,
                run_id=run_metadata_id,
                session=session,
            )
        )

        observations.sort(
            key=lambda item: (
                item.metric_group,
                item.metric_key,
                item.account_id or "",
                item.template_key or "",
                json.dumps(item.dimensions, sort_keys=True, separators=(",", ":")),
            )
        )
        _persist_observations(observations, run_id=run_metadata_id, session=session)
        session.flush()

        alert_count = sum(1 for item in observations if item.alert_status is MetricAlertStatus.ALERT)
        _finish_run(
            run_metadata_id=run_metadata_id,
            status="success",
            diagnostics_json={
                "period_start": validated.period_start.isoformat(),
                "period_end": validated.period_end.isoformat(),
                "account_ids": validated.account_ids,
                "metric_count": len(observations),
                "alert_count": alert_count,
                "groups": dict(sorted(Counter(item.metric_group for item in observations).items())),
            },
            session=session,
        )
        return QualityMetricsGenerateResult(
            run_metadata_id=run_metadata_id,
            period_start=validated.period_start,
            period_end=validated.period_end,
            observations=observations,
            alert_count=alert_count,
        )
    except Exception as exc:
        try:
            _finish_run(
                run_metadata_id=run_metadata_id,
                status="failed",
                diagnostics_json={
                    "period_start": validated.period_start.isoformat(),
                    "period_end": validated.period_end.isoformat(),
                    "account_ids": validated.account_ids,
                    "error": str(exc),
                },
                session=session,
            )
        except Exception as finish_exc:  # noqa: BLE001
            _LOGGER.warning(
                "Failed to finalize run metadata %s for quality metrics: %s",
                run_metadata_id,
                finish_exc,
            )
        raise


def query_metric_observations(request: MetricObservationQueryRequest, session: Session) -> MetricObservationQueryResult:
    """Query persisted metric observations with deterministic ordering."""

    if (
        request.period_start is not None
        and request.period_end is not None
        and request.period_end < request.period_start
    ):
        raise ValueError("period_end must be >= period_start")

    conditions: list[object] = []
    if request.period_start is not None:
        conditions.append(MetricObservation.period_start >= request.period_start)
    if request.period_end is not None:
        conditions.append(MetricObservation.period_end <= request.period_end)
    if request.account_ids:
        conditions.append(MetricObservation.account_id.in_(_normalize_account_ids(request.account_ids)))
    if request.template_keys:
        conditions.append(
            MetricObservation.template_key.in_(
                _normalize_string_list(request.template_keys, field_name="template_keys")
            )
        )
    if request.metric_keys:
        conditions.append(
            MetricObservation.metric_key.in_(_normalize_string_list(request.metric_keys, field_name="metric_keys"))
        )
    if request.metric_groups:
        conditions.append(
            MetricObservation.metric_group.in_(
                _normalize_string_list(request.metric_groups, field_name="metric_groups")
            )
        )
    if request.alert_statuses:
        normalized_statuses = []
        for index, value in enumerate(request.alert_statuses):
            if isinstance(value, MetricAlertStatus):
                normalized_statuses.append(value.value)
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"alert_statuses[{index}] must be a MetricAlertStatus or non-empty string")
            normalized_statuses.append(MetricAlertStatus(value.strip()).value)
        conditions.append(MetricObservation.alert_status.in_(sorted(set(normalized_statuses))))

    rows = session.scalars(select(MetricObservation).where(*conditions)).all()
    observations = [_to_record(row) for row in rows]
    observations.sort(
        key=lambda item: (
            item.period_start.isoformat(),
            item.period_end.isoformat(),
            item.metric_group,
            item.metric_key,
            item.account_id or "",
            item.template_key or "",
            json.dumps(item.dimensions, sort_keys=True, separators=(",", ":")),
            "" if item.metric_value is None else f"{item.metric_value:.6f}",
            "" if item.numerator is None else f"{item.numerator:.6f}",
            "" if item.denominator is None else f"{item.denominator:.6f}",
            item.alert_status.value,
            item.threshold_operator or "",
            "" if item.threshold_value is None else f"{item.threshold_value:.6f}",
        )
    )
    return MetricObservationQueryResult(observations=observations)
