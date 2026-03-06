"""Service-layer reporting workflows for deterministic finance reports."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import logging
import re
from uuid import uuid4

from sqlalchemy import Date, case, func, literal, select, union_all
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    Account,
    BalanceSnapshot,
    Budget,
    BudgetAllocation,
    BudgetCategory,
    BudgetPeriod,
    Category,
    Goal,
    GoalAllocation,
    GoalEvent,
    Report,
    Transaction,
)
from finance_analysis_agent.provenance.audit_writers import finish_run_metadata, start_run_metadata
from finance_analysis_agent.provenance.types import RunMetadataFinishRequest, RunMetadataStartRequest
from finance_analysis_agent.quality import (
    MetricObservationRecord,
    QualityMetricsGenerateRequest,
    QualityMetricsGenerateResult,
    generate_quality_metrics,
)
from finance_analysis_agent.reporting.types import (
    GeneratedReport,
    ReportRunCause,
    ReportType,
    ReportingGenerateRequest,
    ReportingGenerateResult,
)
from finance_analysis_agent.utils.time import utcnow

_DECIMAL_2 = Decimal("0.01")
_PERIOD_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_POSTED_STATUS = "posted"
_PIPELINE_NAME = "reporting_generate"
_SERVICE_VERSION = "reporting-generate-v1"
_SCHEMA_VERSION = "1.0.0"
_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _ValidatedRequest:
    actor: str
    reason: str
    period_start: date
    period_end: date
    report_types: list[ReportType]
    account_ids: list[str]
    budget_id: str | None


@dataclass(slots=True)
class _MoneySummary:
    inflow: Decimal
    outflow: Decimal
    net: Decimal


def _money(value: Decimal) -> Decimal:
    return value.quantize(_DECIMAL_2, rounding=ROUND_HALF_UP)


def _format_money(value: Decimal) -> str:
    return format(_money(value), "f")


def _parse_non_empty(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _normalize_account_ids(values: list[str]) -> list[str]:
    result: set[str] = set()
    for index, value in enumerate(values):
        normalized = _parse_non_empty(value, field_name=f"account_ids[{index}]")
        result.add(normalized)
    return sorted(result)


def _month_key(value: date) -> str:
    return f"{value.year:04d}-{value.month:02d}"


def _period_month_bounds(period_month: str) -> tuple[date, date]:
    if not _PERIOD_MONTH_RE.fullmatch(period_month):
        raise ValueError("period_month must be in YYYY-MM format")
    start = date.fromisoformat(f"{period_month}-01")
    end_day = monthrange(start.year, start.month)[1]
    return start, date(start.year, start.month, end_day)


def _normalize_report_types(values: list[ReportType | str]) -> list[ReportType]:
    if not values:
        return list(ReportType)

    normalized: list[ReportType] = []
    seen: set[ReportType] = set()
    for index, value in enumerate(values):
        if isinstance(value, ReportType):
            report_type = value
        elif isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                raise ValueError(f"report_types[{index}] must be non-empty")
            try:
                report_type = ReportType(candidate)
            except ValueError as exc:
                raise ValueError(f"Unsupported report type: {candidate}") from exc
        else:
            raise ValueError(f"report_types[{index}] must be a ReportType or string")

        if report_type not in seen:
            seen.add(report_type)
            normalized.append(report_type)
    return normalized


def _resolve_period(request: ReportingGenerateRequest) -> tuple[date, date]:
    if request.period_month is not None:
        if request.period_start is not None or request.period_end is not None:
            raise ValueError("period_month cannot be combined with period_start/period_end")
        return _period_month_bounds(request.period_month)

    if request.period_start is None or request.period_end is None:
        raise ValueError("Either period_month or both period_start and period_end are required")
    if request.period_end < request.period_start:
        raise ValueError("period_end must be >= period_start")
    return request.period_start, request.period_end


def _validate_request(request: ReportingGenerateRequest) -> _ValidatedRequest:
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    period_start, period_end = _resolve_period(request)
    report_types = _normalize_report_types(request.report_types)
    account_ids = _normalize_account_ids(request.account_ids)
    budget_id = request.budget_id.strip() if isinstance(request.budget_id, str) else None
    if budget_id == "":
        budget_id = None

    if ReportType.BUDGET_VS_ACTUAL in report_types and budget_id is None:
        raise ValueError("budget_id is required when budget_vs_actual report is requested")

    return _ValidatedRequest(
        actor=actor,
        reason=reason,
        period_start=period_start,
        period_end=period_end,
        report_types=report_types,
        account_ids=account_ids,
        budget_id=budget_id,
    )


def _normalize_for_hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
                    "report_types": [item.value for item in validated.report_types],
                    "account_ids": validated.account_ids,
                    "budget_id": validated.budget_id,
                }
            ),
            status="running",
            diagnostics_json={
                "phase": "start",
                "period_start": validated.period_start.isoformat(),
                "period_end": validated.period_end.isoformat(),
                "report_types": [item.value for item in validated.report_types],
                "account_ids": validated.account_ids,
                "budget_id": validated.budget_id,
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


def _base_transaction_filters(validated: _ValidatedRequest) -> list[object]:
    filters: list[object] = [
        Transaction.posted_date >= validated.period_start,
        Transaction.posted_date <= validated.period_end,
        Transaction.pending_status == _POSTED_STATUS,
        Transaction.excluded.is_(False),
        Transaction.transfer_group_id.is_(None),
    ]
    if validated.account_ids:
        filters.append(Transaction.account_id.in_(validated.account_ids))
    return filters


def _month_sequence(period_start: date, period_end: date) -> list[str]:
    year = period_start.year
    month = period_start.month
    result: list[str] = []
    while (year, month) <= (period_end.year, period_end.month):
        result.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return result


def _month_end(period_month: str) -> date:
    start = date.fromisoformat(f"{period_month}-01")
    return date(start.year, start.month, monthrange(start.year, start.month)[1])


def _net_worth_points(period_start: date, period_end: date) -> list[date]:
    months = _month_sequence(period_start, period_end)
    points: list[date] = []
    for index, period_month in enumerate(months):
        if index == len(months) - 1:
            points.append(period_end)
        else:
            points.append(_month_end(period_month))
    return points


def _accumulate_money(summary: _MoneySummary, amount: Decimal) -> _MoneySummary:
    inflow = summary.inflow
    outflow = summary.outflow
    net = summary.net + amount
    if amount > 0:
        inflow += amount
    elif amount < 0:
        outflow += -amount
    return _MoneySummary(inflow=_money(inflow), outflow=_money(outflow), net=_money(net))


def _build_cash_flow_payload(validated: _ValidatedRequest, session: Session) -> dict[str, object]:
    rows = session.execute(
        select(
            Transaction.account_id,
            Transaction.posted_date,
            Transaction.amount,
        )
        .where(*_base_transaction_filters(validated))
        .order_by(Transaction.posted_date.asc(), Transaction.id.asc())
    ).all()

    summary = _MoneySummary(inflow=Decimal("0.00"), outflow=Decimal("0.00"), net=Decimal("0.00"))
    by_account: dict[str, _MoneySummary] = {}
    by_month: dict[str, _MoneySummary] = {}

    for account_id, posted_date, amount in rows:
        decimal_amount = _money(Decimal(amount))
        summary = _accumulate_money(summary, decimal_amount)
        by_account[account_id] = _accumulate_money(
            by_account.get(account_id, _MoneySummary(Decimal("0.00"), Decimal("0.00"), Decimal("0.00"))),
            decimal_amount,
        )
        month = _month_key(posted_date)
        by_month[month] = _accumulate_money(
            by_month.get(month, _MoneySummary(Decimal("0.00"), Decimal("0.00"), Decimal("0.00"))),
            decimal_amount,
        )

    account_items = [
        {
            "account_id": account_id,
            "inflow": _format_money(value.inflow),
            "outflow": _format_money(value.outflow),
            "net": _format_money(value.net),
        }
        for account_id, value in sorted(by_account.items())
    ]
    month_items = [
        {
            "month": month,
            "inflow": _format_money(value.inflow),
            "outflow": _format_money(value.outflow),
            "net": _format_money(value.net),
        }
        for month, value in sorted(by_month.items())
    ]

    return {
        "summary": {
            "inflow": _format_money(summary.inflow),
            "outflow": _format_money(summary.outflow),
            "net": _format_money(summary.net),
            "transaction_count": len(rows),
        },
        "by_account": account_items,
        "by_month": month_items,
    }


def _build_category_trends_payload(validated: _ValidatedRequest, session: Session) -> dict[str, object]:
    rows = session.execute(
        select(
            Transaction.posted_date,
            Transaction.category_id,
            Transaction.amount,
        )
        .where(*_base_transaction_filters(validated))
        .order_by(Transaction.posted_date.asc(), Transaction.id.asc())
    ).all()

    spending_by_month_category: dict[str, dict[str | None, Decimal]] = {}
    count_by_month_category: dict[str, dict[str | None, int]] = {}
    category_ids: set[str] = set()
    total_spend = Decimal("0.00")
    spend_txn_count = 0

    for posted_date, category_id, amount in rows:
        decimal_amount = _money(Decimal(amount))
        if decimal_amount >= 0:
            continue
        spend = _money(-decimal_amount)
        month = _month_key(posted_date)
        spending_by_month_category.setdefault(month, {})
        count_by_month_category.setdefault(month, {})
        spending_by_month_category[month][category_id] = _money(
            spending_by_month_category[month].get(category_id, Decimal("0.00")) + spend
        )
        count_by_month_category[month][category_id] = count_by_month_category[month].get(category_id, 0) + 1
        if category_id is not None:
            category_ids.add(category_id)
        total_spend = _money(total_spend + spend)
        spend_txn_count += 1

    category_name_rows = session.execute(
        select(Category.id, Category.name).where(Category.id.in_(sorted(category_ids))).order_by(Category.id.asc())
    ).all()
    category_names = dict(category_name_rows)

    month_items: list[dict[str, object]] = []
    for month in sorted(spending_by_month_category):
        categories: list[dict[str, object]] = []
        month_total = Decimal("0.00")
        category_rows = []
        for category_id, spend in spending_by_month_category[month].items():
            name = category_names.get(category_id, "Uncategorized") if category_id is not None else "Uncategorized"
            category_rows.append((name.lower(), category_id or "", category_id, name, spend))
        for _, _, category_id, name, spend in sorted(category_rows):
            count = count_by_month_category[month][category_id]
            categories.append(
                {
                    "category_id": category_id,
                    "category_name": name,
                    "spend": _format_money(spend),
                    "transaction_count": count,
                }
            )
            month_total = _money(month_total + spend)

        month_items.append(
            {
                "month": month,
                "month_total_spend": _format_money(month_total),
                "categories": categories,
            }
        )

    return {
        "summary": {
            "total_spend": _format_money(total_spend),
            "spending_transaction_count": spend_txn_count,
        },
        "months": month_items,
    }


def _build_net_worth_payload(validated: _ValidatedRequest, session: Session) -> dict[str, object]:
    account_stmt = select(Account.id, Account.name)
    if validated.account_ids:
        account_stmt = account_stmt.where(Account.id.in_(validated.account_ids))
    account_rows = session.execute(account_stmt.order_by(Account.id.asc())).all()

    source_priority = case(
        (BalanceSnapshot.source == "statement", 0),
        (BalanceSnapshot.source == "reconciliation", 1),
        else_=2,
    )

    points = _net_worth_points(validated.period_start, validated.period_end)
    account_ids = [account_id for account_id, _ in account_rows]

    snapshot_lookup: dict[tuple[date, str], tuple[Decimal, date, str]] = {}
    if account_ids and points:
        point_selects = [
            select(literal(point, type_=Date).label("report_point"))
            for point in points
        ]
        if len(point_selects) == 1:
            report_points_cte = point_selects[0].cte("report_points")
        else:
            report_points_cte = union_all(*point_selects).cte("report_points")

        ranked_snapshots = (
            select(
                report_points_cte.c.report_point.label("report_point"),
                BalanceSnapshot.account_id.label("account_id"),
                BalanceSnapshot.balance.label("balance"),
                BalanceSnapshot.snapshot_date.label("snapshot_date"),
                BalanceSnapshot.source.label("source"),
                func.row_number()
                .over(
                    partition_by=(
                        report_points_cte.c.report_point,
                        BalanceSnapshot.account_id,
                    ),
                    order_by=(
                        BalanceSnapshot.snapshot_date.desc(),
                        source_priority.asc(),
                        BalanceSnapshot.created_at.desc(),
                        BalanceSnapshot.id.desc(),
                    ),
                )
                .label("row_num"),
            )
            .join(
                BalanceSnapshot,
                (BalanceSnapshot.account_id.in_(account_ids))
                & (BalanceSnapshot.snapshot_date <= report_points_cte.c.report_point),
            )
            .cte("ranked_snapshots")
        )

        rows = session.execute(
            select(
                ranked_snapshots.c.report_point,
                ranked_snapshots.c.account_id,
                ranked_snapshots.c.balance,
                ranked_snapshots.c.snapshot_date,
                ranked_snapshots.c.source,
            )
            .where(ranked_snapshots.c.row_num == 1)
            .order_by(
                ranked_snapshots.c.report_point.asc(),
                ranked_snapshots.c.account_id.asc(),
            )
        ).all()
        snapshot_lookup = {
            (report_point, account_id): (Decimal(balance), snapshot_date, source)
            for report_point, account_id, balance, snapshot_date, source in rows
        }

    timeline: list[dict[str, object]] = []

    for point in points:
        point_accounts: list[dict[str, object]] = []
        total = Decimal("0.00")
        for account_id, account_name in account_rows:
            snapshot = snapshot_lookup.get((point, account_id))
            if snapshot is None:
                continue

            balance_value, snapshot_date, source = snapshot
            balance = _money(balance_value)
            total = _money(total + balance)
            point_accounts.append(
                {
                    "account_id": account_id,
                    "account_name": account_name,
                    "balance": _format_money(balance),
                    "snapshot_date": snapshot_date.isoformat(),
                    "source": source,
                }
            )

        timeline.append(
            {
                "as_of": point.isoformat(),
                "total_net_worth": _format_money(total),
                "accounts": point_accounts,
            }
        )

    latest = timeline[-1] if timeline else {
        "as_of": validated.period_end.isoformat(),
        "total_net_worth": _format_money(Decimal("0.00")),
        "accounts": [],
    }

    return {
        "timeline": timeline,
        "latest": latest,
    }


def _build_budget_vs_actual_payload(validated: _ValidatedRequest, session: Session) -> dict[str, object]:
    if validated.budget_id is None:
        raise ValueError("budget_id is required when budget_vs_actual report is requested")

    budget = session.get(Budget, validated.budget_id)
    if budget is None:
        raise ValueError(f"Budget not found: {validated.budget_id}")

    months = _month_sequence(validated.period_start, validated.period_end)

    budget_categories_rows = session.execute(
        select(BudgetCategory.id, BudgetCategory.category_id, Category.name)
        .outerjoin(Category, Category.id == BudgetCategory.category_id)
        .where(BudgetCategory.budget_id == validated.budget_id)
        .order_by(BudgetCategory.id.asc())
    ).all()
    budget_category_ids = {category_id for _, category_id, _ in budget_categories_rows}
    category_names = {
        category_id: (name if name is not None else "Unknown Category")
        for _, category_id, name in budget_categories_rows
    }

    allocation_rows = session.execute(
        select(
            BudgetPeriod.period_month,
            BudgetCategory.category_id,
            func.coalesce(func.sum(BudgetAllocation.assigned_amount), 0),
        )
        .join(BudgetAllocation, BudgetAllocation.budget_period_id == BudgetPeriod.id)
        .join(BudgetCategory, BudgetCategory.id == BudgetAllocation.budget_category_id)
        .where(
            BudgetPeriod.budget_id == validated.budget_id,
            BudgetPeriod.period_month.in_(months),
        )
        .group_by(BudgetPeriod.period_month, BudgetCategory.category_id)
        .order_by(BudgetPeriod.period_month.asc(), BudgetCategory.category_id.asc())
    ).all()

    assigned_by_month_category: dict[str, dict[str | None, Decimal]] = {}
    for period_month, category_id, assigned in allocation_rows:
        assigned_by_month_category.setdefault(period_month, {})
        assigned_by_month_category[period_month][category_id] = _money(Decimal(assigned))

    transaction_rows = session.execute(
        select(
            Transaction.posted_date,
            Transaction.category_id,
            Transaction.amount,
        )
        .where(*_base_transaction_filters(validated))
        .order_by(Transaction.posted_date.asc(), Transaction.id.asc())
    ).all()

    actual_by_month_category: dict[str, dict[str | None, Decimal]] = {}
    for posted_date, category_id, amount in transaction_rows:
        decimal_amount = _money(Decimal(amount))
        if decimal_amount >= 0:
            continue
        period_month = _month_key(posted_date)
        actual_by_month_category.setdefault(period_month, {})
        actual_by_month_category[period_month][category_id] = _money(
            actual_by_month_category[period_month].get(category_id, Decimal("0.00")) + (-decimal_amount)
        )

    month_items: list[dict[str, object]] = []
    total_assigned = Decimal("0.00")
    total_actual = Decimal("0.00")

    for period_month in months:
        assigned_for_month = assigned_by_month_category.get(period_month, {})
        actual_for_month = actual_by_month_category.get(period_month, {})
        category_ids = sorted(
            set(budget_category_ids)
            | set(assigned_for_month)
            | set(actual_for_month),
            key=lambda item: (category_names.get(item, "Uncategorized").lower(), item or ""),
        )

        categories: list[dict[str, object]] = []
        month_assigned = Decimal("0.00")
        month_actual = Decimal("0.00")
        for category_id in category_ids:
            assigned = _money(assigned_for_month.get(category_id, Decimal("0.00")))
            actual = _money(actual_for_month.get(category_id, Decimal("0.00")))
            variance = _money(assigned - actual)
            month_assigned = _money(month_assigned + assigned)
            month_actual = _money(month_actual + actual)
            categories.append(
                {
                    "category_id": category_id,
                    "category_name": category_names.get(category_id, "Uncategorized"),
                    "assigned": _format_money(assigned),
                    "actual_spend": _format_money(actual),
                    "variance": _format_money(variance),
                }
            )

        total_assigned = _money(total_assigned + month_assigned)
        total_actual = _money(total_actual + month_actual)
        month_items.append(
            {
                "month": period_month,
                "assigned_total": _format_money(month_assigned),
                "actual_total": _format_money(month_actual),
                "variance_total": _format_money(_money(month_assigned - month_actual)),
                "categories": categories,
            }
        )

    return {
        "budget_id": validated.budget_id,
        "budget_name": budget.name,
        "summary": {
            "assigned_total": _format_money(total_assigned),
            "actual_total": _format_money(total_actual),
            "variance_total": _format_money(_money(total_assigned - total_actual)),
            "months": len(months),
        },
        "months": month_items,
    }


def _parse_decimal(value: object, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal-compatible value") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be a decimal-compatible value")
    return _money(parsed)


def _build_goal_progress_payload(validated: _ValidatedRequest, session: Session) -> dict[str, object]:
    goals = session.scalars(select(Goal).order_by(Goal.id.asc())).all()
    if not goals:
        return {
            "summary": {
                "goal_count": 0,
                "target_total": _format_money(Decimal("0.00")),
                "progress_total": _format_money(Decimal("0.00")),
                "remaining_total": _format_money(Decimal("0.00")),
            },
            "goals": [],
        }

    period_month = _month_key(validated.period_end)
    goal_ids = [goal.id for goal in goals]

    allocation_stmt = (
        select(
            GoalAllocation.goal_id,
            func.coalesce(func.sum(GoalAllocation.amount), 0),
        )
        .where(
            GoalAllocation.goal_id.in_(goal_ids),
            GoalAllocation.period_month <= period_month,
        )
        .group_by(GoalAllocation.goal_id)
    )
    if validated.account_ids:
        allocation_stmt = allocation_stmt.where(GoalAllocation.account_id.in_(validated.account_ids))

    allocation_rows = session.execute(allocation_stmt).all()
    allocated_by_goal = {
        goal_id: _parse_decimal(total, field_name="allocated_total")
        for goal_id, total in allocation_rows
    }

    spending_stmt = (
        select(
            GoalEvent.goal_id,
            GoalEvent.amount,
            Transaction.amount,
        )
        .outerjoin(Transaction, Transaction.id == GoalEvent.related_transaction_id)
        .where(
            GoalEvent.goal_id.in_(goal_ids),
            GoalEvent.event_date <= validated.period_end,
        )
        .order_by(GoalEvent.goal_id.asc(), GoalEvent.event_date.asc(), GoalEvent.id.asc())
    )
    if validated.account_ids:
        spending_stmt = spending_stmt.where(Transaction.account_id.in_(validated.account_ids))

    spending_rows = session.execute(spending_stmt).all()
    spending_by_goal: dict[str, Decimal] = {}
    for goal_id, goal_event_amount, transaction_amount in spending_rows:
        effective_amount = transaction_amount if transaction_amount is not None else goal_event_amount
        decimal_amount = _parse_decimal(effective_amount, field_name="goal_event_amount")
        if decimal_amount >= 0:
            continue
        spending_by_goal[goal_id] = _money(spending_by_goal.get(goal_id, Decimal("0.00")) + (-decimal_amount))

    goal_items: list[dict[str, object]] = []
    target_total = Decimal("0.00")
    progress_total = Decimal("0.00")

    for goal in goals:
        target_amount = _parse_decimal(goal.target_amount, field_name=f"goals[{goal.id}].target_amount")
        allocated_total = allocated_by_goal.get(goal.id, Decimal("0.00"))
        spending_total = spending_by_goal.get(goal.id, Decimal("0.00")) if goal.spending_reduces_progress else Decimal("0.00")
        progress_amount = _money(max(allocated_total - spending_total, Decimal("0.00")))
        remaining_amount = _money(max(target_amount - progress_amount, Decimal("0.00")))
        computed_status = goal.status
        if remaining_amount == Decimal("0.00"):
            computed_status = "completed"

        target_total = _money(target_total + target_amount)
        progress_total = _money(progress_total + progress_amount)

        goal_items.append(
            {
                "goal_id": goal.id,
                "goal_name": goal.name,
                "status": computed_status,
                "target_amount": _format_money(target_amount),
                "allocated_total": _format_money(allocated_total),
                "spending_total": _format_money(spending_total),
                "progress_amount": _format_money(progress_amount),
                "remaining_amount": _format_money(remaining_amount),
                "target_date": goal.target_date.isoformat() if goal.target_date is not None else None,
                "spending_reduces_progress": goal.spending_reduces_progress,
            }
        )

    remaining_total = _money(max(target_total - progress_total, Decimal("0.00")))

    return {
        "summary": {
            "goal_count": len(goal_items),
            "target_total": _format_money(target_total),
            "progress_total": _format_money(progress_total),
            "remaining_total": _format_money(remaining_total),
        },
        "goals": goal_items,
    }


def _serialize_metric_observation(record: MetricObservationRecord) -> dict[str, object]:
    item = record
    return {
        "key": f"{item.metric_group}.{item.metric_key}",
        "metric_group": item.metric_group,
        "metric_key": item.metric_key,
        "metric_value": item.metric_value,
        "numerator": item.numerator,
        "denominator": item.denominator,
        "threshold_value": item.threshold_value,
        "threshold_operator": item.threshold_operator,
        "alert_status": item.alert_status.value,
        "account_id": item.account_id,
        "template_key": item.template_key,
        "dimensions": item.dimensions,
        "period_start": item.period_start.isoformat(),
        "period_end": item.period_end.isoformat(),
    }


def _build_quality_trust_dashboard_payload(
    validated: _ValidatedRequest,
    metric_result: QualityMetricsGenerateResult,
) -> dict[str, object]:
    groups: dict[str, list[dict[str, object]]] = {
        "correctness": [],
        "automation_quality": [],
        "parsing_quality": [],
        "trust_health": [],
    }
    alerts: list[dict[str, object]] = []
    no_data_count = 0

    for observation in metric_result.observations:
        serialized = _serialize_metric_observation(observation)
        groups.setdefault(observation.metric_group, []).append(serialized)
        if observation.alert_status.value == "alert":
            alerts.append(serialized)
        elif observation.alert_status.value == "no_data":
            no_data_count += 1

    metric_snapshot_id = _payload_hash(
        {
            "period_start": validated.period_start.isoformat(),
            "period_end": validated.period_end.isoformat(),
            "account_ids": validated.account_ids,
            "observations": [item for values in groups.values() for item in values],
        }
    )

    return {
        "summary": {
            "metric_count": len(metric_result.observations),
            "alert_count": len(alerts),
            "no_data_count": no_data_count,
            "account_scope": validated.account_ids,
        },
        "alerts": alerts,
        "metric_run_id": metric_result.run_metadata_id,
        "metric_snapshot_id": metric_snapshot_id,
        "groups": {
            group_name: {"observations": observations}
            for group_name, observations in groups.items()
        },
    }


def _build_report_payload(report_type: ReportType, validated: _ValidatedRequest, session: Session) -> tuple[dict[str, object], list[ReportRunCause]]:
    causes: list[ReportRunCause] = []
    if report_type is ReportType.CASH_FLOW:
        return _build_cash_flow_payload(validated, session), causes
    if report_type is ReportType.CATEGORY_TRENDS:
        return _build_category_trends_payload(validated, session), causes
    if report_type is ReportType.NET_WORTH:
        return _build_net_worth_payload(validated, session), causes
    if report_type is ReportType.BUDGET_VS_ACTUAL:
        return _build_budget_vs_actual_payload(validated, session), causes
    if report_type is ReportType.GOAL_PROGRESS:
        return _build_goal_progress_payload(validated, session), causes
    # QUALITY_TRUST_DASHBOARD is handled separately in reporting_generate().
    raise ValueError(f"Unhandled report type: {report_type}")


def _payload_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _payload_for_hash(report_type: ReportType, payload: dict[str, object]) -> dict[str, object]:
    if report_type is not ReportType.QUALITY_TRUST_DASHBOARD:
        return payload
    normalized = dict(payload)
    normalized.pop("metric_run_id", None)
    return normalized


def reporting_generate(request: ReportingGenerateRequest, session: Session) -> ReportingGenerateResult:
    """Generate deterministic reporting payloads and persist run/report metadata."""

    validated = _validate_request(request)
    run_metadata_id = _start_run(validated, session)

    try:
        generated_payloads: list[tuple[ReportType, dict[str, object], list[ReportRunCause]]] = []
        deferred_quality_dashboard = ReportType.QUALITY_TRUST_DASHBOARD in validated.report_types
        for report_type in validated.report_types:
            if report_type is ReportType.QUALITY_TRUST_DASHBOARD:
                continue
            payload, causes = _build_report_payload(report_type, validated, session)
            generated_payloads.append((report_type, payload, causes))

        if deferred_quality_dashboard:
            metric_result = generate_quality_metrics(
                QualityMetricsGenerateRequest(
                    actor=validated.actor,
                    reason=validated.reason,
                    period_start=validated.period_start,
                    period_end=validated.period_end,
                    account_ids=list(validated.account_ids),
                ),
                session,
            )
            generated_payloads.append(
                (
                    ReportType.QUALITY_TRUST_DASHBOARD,
                    _build_quality_trust_dashboard_payload(validated, metric_result),
                    [],
                )
            )

        generated_at = utcnow()
        generated_reports: list[GeneratedReport] = []
        all_causes: list[ReportRunCause] = []
        report_hashes: dict[str, str] = {}

        for report_type, payload, causes in generated_payloads:
            payload_hash = _payload_hash(_payload_for_hash(report_type, payload))
            report = Report(
                id=str(uuid4()),
                report_type=report_type.value,
                period_start=validated.period_start,
                period_end=validated.period_end,
                generated_at=generated_at,
                payload_json=payload,
                run_id=run_metadata_id,
            )
            session.add(report)
            generated_reports.append(
                GeneratedReport(
                    report_id=report.id,
                    report_type=report_type,
                    payload_hash=payload_hash,
                    payload_json=payload,
                )
            )
            report_hashes[report_type.value] = payload_hash
            all_causes.extend(causes)

        session.flush()

        diagnostics_json = {
            "period_start": validated.period_start.isoformat(),
            "period_end": validated.period_end.isoformat(),
            "report_types": [item.value for item in validated.report_types],
            "account_ids": validated.account_ids,
            "budget_id": validated.budget_id,
            "report_hashes": report_hashes,
            "report_count": len(generated_reports),
            "cause_count": len(all_causes),
        }
        _finish_run(
            run_metadata_id=run_metadata_id,
            status="success",
            diagnostics_json=diagnostics_json,
            session=session,
        )

        return ReportingGenerateResult(
            run_metadata_id=run_metadata_id,
            period_start=validated.period_start,
            period_end=validated.period_end,
            report_types=list(validated.report_types),
            reports=generated_reports,
            causes=all_causes,
        )
    except Exception as exc:
        try:
            _finish_run(
                run_metadata_id=run_metadata_id,
                status="failed",
                diagnostics_json={
                    "period_start": validated.period_start.isoformat(),
                    "period_end": validated.period_end.isoformat(),
                    "report_types": [item.value for item in validated.report_types],
                    "account_ids": validated.account_ids,
                    "budget_id": validated.budget_id,
                    "error": str(exc),
                },
                session=session,
            )
        except Exception as finish_exc:  # noqa: BLE001
            _LOGGER.warning(
                "Failed to finalize run metadata %s for reporting: %s",
                run_metadata_id,
                finish_exc,
            )
        raise
