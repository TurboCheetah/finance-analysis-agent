"""Service-layer budgeting workflows."""

from __future__ import annotations

from dataclasses import dataclass
import re
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from calendar import monthrange
from uuid import uuid4

from sqlalchemy import and_, case, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from finance_analysis_agent.budget.types import (
    BudgetBucketPlanInput,
    BudgetBucketSnapshot,
    BudgetCategoryAllocationInput,
    BudgetCategoryPlanInput,
    BudgetCategoryRolloverSnapshot,
    BudgetCategorySnapshot,
    BudgetComputeFlexRequest,
    BudgetComputeFlexResult,
    BudgetComputeZeroBasedRequest,
    BudgetComputeZeroBasedResult,
    BudgetRunCause,
    BudgetTargetPolicyInput,
)
from finance_analysis_agent.db.models import (
    Budget,
    BudgetAllocation,
    BudgetBucket,
    BudgetBucketCategoryMapping,
    BudgetBucketDefinition,
    BudgetCategory,
    BudgetPeriod,
    BudgetRollover,
    BudgetTarget,
    Transaction,
)

_DECIMAL_2 = Decimal("0.01")
_BUDGET_METHOD_ZERO_BASED = "zero_based"
_BUDGET_METHOD_FLEX = "flex"
_POSTED_STATUS = "posted"
_PERIOD_STATUS_OPEN = "open"
_PERIOD_STATUS_CLOSED = "closed"
_ALLOWED_PERIOD_STATUSES = {_PERIOD_STATUS_OPEN, _PERIOD_STATUS_CLOSED}
_ALLOWED_CADENCES = {"monthly", "every_n_months"}
_ALLOWED_FLEX_BUCKET_KEYS = ("fixed", "non_monthly", "flex")
_ALLOWED_ROLLOVER_POLICIES = {"none", "carry_positive", "carry_negative", "carry_both"}
_DEFAULT_EVERY_N_MONTHS_ANCHOR = "2000-01"
_ROLLOVER_DIMENSION_TYPE = "budget_period_overspent"
_ROLLOVER_POLICY_REDUCE_TO_ASSIGN = "reduce_to_assign"
_ALLOCATION_SOURCE_ENGINE = "budget_compute_zero_based"
_ALLOCATION_SOURCE_FLEX_ENGINE = "budget_compute_flex"
_ROLLOVER_DIMENSION_TYPE_BUCKET = "budget_bucket"
_ROLLOVER_DIMENSION_TYPE_CATEGORY = "budget_category"
_PERIOD_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


@dataclass(slots=True)
class _ValidatedAllocation:
    budget_category_id: str
    assigned_amount: Decimal
    source: str


@dataclass(slots=True)
class _ValidatedTargetPolicy:
    budget_category_id: str
    target_type: str
    amount: Decimal | None
    cadence: str | None
    top_up: bool | None
    snoozed_until: date | None
    metadata_json: dict[str, object] | None


@dataclass(slots=True)
class _ValidatedRequest:
    budget_id: str
    period_month: str
    period_start: date
    period_end: date
    previous_period_month: str
    available_cash: Decimal
    actor: str
    reason: str
    status: str
    category_allocations: dict[str, _ValidatedAllocation]
    target_policies: list[_ValidatedTargetPolicy]


@dataclass(slots=True)
class _ValidatedBucketPlan:
    bucket_key: str
    planned_amount: Decimal
    rollover_policy: str | None


@dataclass(slots=True)
class _ValidatedCategoryPlan:
    budget_category_id: str
    bucket_key: str
    planned_amount: Decimal
    rollover_policy: str | None


@dataclass(slots=True)
class _ValidatedFlexRequest:
    budget_id: str
    period_month: str
    period_start: date
    period_end: date
    previous_period_month: str
    available_cash: Decimal
    actor: str
    reason: str
    status: str
    bucket_plans: dict[str, _ValidatedBucketPlan]
    category_plans: dict[str, _ValidatedCategoryPlan]


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_DECIMAL_2, rounding=ROUND_HALF_UP)


def _parse_non_empty(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _parse_decimal(value: object, *, field_name: str, non_negative: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{field_name} must be a decimal-compatible value") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be a decimal-compatible value")
    if non_negative and parsed < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return _quantize_money(parsed)


def _period_bounds(period_month: str) -> tuple[date, date]:
    if not _PERIOD_MONTH_RE.fullmatch(period_month):
        raise ValueError("period_month must be in YYYY-MM format")
    try:
        start = date.fromisoformat(f"{period_month}-01")
    except ValueError as exc:
        raise ValueError("period_month must be in YYYY-MM format") from exc
    end_day = monthrange(start.year, start.month)[1]
    return start, date(start.year, start.month, end_day)


def _previous_period_month(period_month: str) -> str:
    start, _ = _period_bounds(period_month)
    year = start.year
    month = start.month - 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"


def _months_between(anchor_period_month: str, period_month: str) -> int:
    anchor, _ = _period_bounds(anchor_period_month)
    current, _ = _period_bounds(period_month)
    return ((current.year - anchor.year) * 12) + (current.month - anchor.month)


def _validate_allocations(values: list[BudgetCategoryAllocationInput]) -> dict[str, _ValidatedAllocation]:
    parsed: dict[str, _ValidatedAllocation] = {}
    for index, allocation in enumerate(values):
        category_id = _parse_non_empty(
            allocation.budget_category_id,
            field_name=f"category_allocations[{index}].budget_category_id",
        )
        if category_id in parsed:
            raise ValueError(f"Duplicate budget_category_id in category_allocations: {category_id}")
        parsed[category_id] = _ValidatedAllocation(
            budget_category_id=category_id,
            assigned_amount=_parse_decimal(
                allocation.assigned_amount,
                field_name=f"category_allocations[{index}].assigned_amount",
                non_negative=True,
            ),
            source=_parse_non_empty(
                allocation.source,
                field_name=f"category_allocations[{index}].source",
            ),
        )
    return parsed


def _validate_target_policies(values: list[BudgetTargetPolicyInput]) -> list[_ValidatedTargetPolicy]:
    parsed: list[_ValidatedTargetPolicy] = []
    seen_budget_category_ids: set[str] = set()
    for index, policy in enumerate(values):
        category_id = _parse_non_empty(
            policy.budget_category_id,
            field_name=f"target_policies[{index}].budget_category_id",
        )
        if category_id in seen_budget_category_ids:
            raise ValueError(f"target_policies[{index}].budget_category_id is duplicated")
        seen_budget_category_ids.add(category_id)
        target_type = _parse_non_empty(
            policy.target_type,
            field_name=f"target_policies[{index}].target_type",
        )
        cadence: str | None = None
        if policy.cadence is not None:
            cadence = _parse_non_empty(
                policy.cadence,
                field_name=f"target_policies[{index}].cadence",
            )
            if cadence not in _ALLOWED_CADENCES:
                raise ValueError(
                    f"target_policies[{index}].cadence must be one of {sorted(_ALLOWED_CADENCES)}"
                )
        metadata_json = dict(policy.metadata_json) if policy.metadata_json is not None else None
        if cadence == "every_n_months":
            if metadata_json is None:
                raise ValueError(
                    f"target_policies[{index}].metadata_json must include one of: "
                    "months_interval, interval_months, every_n_months"
                )
            try:
                _parse_interval_months(metadata_json)
                _parse_anchor_month("2000-01", metadata_json)
            except ValueError as exc:
                raise ValueError(f"target_policies[{index}].metadata_json {exc}") from exc
        amount = (
            _parse_decimal(
                policy.amount,
                field_name=f"target_policies[{index}].amount",
                non_negative=True,
            )
            if policy.amount is not None
            else None
        )
        parsed.append(
            _ValidatedTargetPolicy(
                budget_category_id=category_id,
                target_type=target_type,
                amount=amount,
                cadence=cadence,
                top_up=policy.top_up,
                snoozed_until=policy.snoozed_until,
                metadata_json=metadata_json,
            )
        )
    return parsed


def _validate_request(request: BudgetComputeZeroBasedRequest) -> _ValidatedRequest:
    budget_id = _parse_non_empty(request.budget_id, field_name="budget_id")
    period_month = _parse_non_empty(request.period_month, field_name="period_month")
    period_start, period_end = _period_bounds(period_month)
    available_cash = _parse_decimal(request.available_cash, field_name="available_cash")
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    status = _parse_non_empty(request.status, field_name="status").lower()
    if status not in _ALLOWED_PERIOD_STATUSES:
        raise ValueError(f"status must be one of {sorted(_ALLOWED_PERIOD_STATUSES)}")

    return _ValidatedRequest(
        budget_id=budget_id,
        period_month=period_month,
        period_start=period_start,
        period_end=period_end,
        previous_period_month=_previous_period_month(period_month),
        available_cash=available_cash,
        actor=actor,
        reason=reason,
        status=status,
        category_allocations=_validate_allocations(request.category_allocations),
        target_policies=_validate_target_policies(request.target_policies),
    )


def _normalize_bucket_key(value: object, *, field_name: str) -> str:
    normalized = _parse_non_empty(value, field_name=field_name).lower()
    normalized = normalized.replace("-", "_").replace(" ", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    if normalized not in _ALLOWED_FLEX_BUCKET_KEYS:
        raise ValueError(f"{field_name} must be one of {list(_ALLOWED_FLEX_BUCKET_KEYS)}")
    return normalized


def _parse_rollover_policy(value: object, *, field_name: str) -> str:
    policy = _parse_non_empty(value, field_name=field_name).lower()
    if policy not in _ALLOWED_ROLLOVER_POLICIES:
        raise ValueError(f"{field_name} must be one of {sorted(_ALLOWED_ROLLOVER_POLICIES)}")
    return policy


def _validate_bucket_plans(values: list[BudgetBucketPlanInput]) -> dict[str, _ValidatedBucketPlan]:
    parsed: dict[str, _ValidatedBucketPlan] = {}
    for index, bucket_plan in enumerate(values):
        bucket_key = _normalize_bucket_key(
            bucket_plan.bucket_key,
            field_name=f"bucket_plans[{index}].bucket_key",
        )
        if bucket_key in parsed:
            raise ValueError(f"Duplicate bucket_key in bucket_plans: {bucket_key}")
        rollover_policy = None
        if bucket_plan.rollover_policy is not None:
            rollover_policy = _parse_rollover_policy(
                bucket_plan.rollover_policy,
                field_name=f"bucket_plans[{index}].rollover_policy",
            )
        parsed[bucket_key] = _ValidatedBucketPlan(
            bucket_key=bucket_key,
            planned_amount=_parse_decimal(
                bucket_plan.planned_amount,
                field_name=f"bucket_plans[{index}].planned_amount",
                non_negative=True,
            ),
            rollover_policy=rollover_policy,
        )
    missing = sorted(set(_ALLOWED_FLEX_BUCKET_KEYS) - set(parsed))
    if missing:
        raise ValueError(f"bucket_plans must include all bucket keys: {list(_ALLOWED_FLEX_BUCKET_KEYS)}")
    return parsed


def _validate_category_plans(values: list[BudgetCategoryPlanInput]) -> dict[str, _ValidatedCategoryPlan]:
    parsed: dict[str, _ValidatedCategoryPlan] = {}
    for index, category_plan in enumerate(values):
        budget_category_id = _parse_non_empty(
            category_plan.budget_category_id,
            field_name=f"category_plans[{index}].budget_category_id",
        )
        if budget_category_id in parsed:
            raise ValueError(f"Duplicate budget_category_id in category_plans: {budget_category_id}")
        rollover_policy = None
        if category_plan.rollover_policy is not None:
            rollover_policy = _parse_rollover_policy(
                category_plan.rollover_policy,
                field_name=f"category_plans[{index}].rollover_policy",
            )
        parsed[budget_category_id] = _ValidatedCategoryPlan(
            budget_category_id=budget_category_id,
            bucket_key=_normalize_bucket_key(
                category_plan.bucket_key,
                field_name=f"category_plans[{index}].bucket_key",
            ),
            planned_amount=_parse_decimal(
                category_plan.planned_amount,
                field_name=f"category_plans[{index}].planned_amount",
                non_negative=True,
            ),
            rollover_policy=rollover_policy,
        )
    return parsed


def _validate_flex_request(request: BudgetComputeFlexRequest) -> _ValidatedFlexRequest:
    budget_id = _parse_non_empty(request.budget_id, field_name="budget_id")
    period_month = _parse_non_empty(request.period_month, field_name="period_month")
    period_start, period_end = _period_bounds(period_month)
    available_cash = _parse_decimal(request.available_cash, field_name="available_cash")
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    status = _parse_non_empty(request.status, field_name="status").lower()
    if status not in _ALLOWED_PERIOD_STATUSES:
        raise ValueError(f"status must be one of {sorted(_ALLOWED_PERIOD_STATUSES)}")
    return _ValidatedFlexRequest(
        budget_id=budget_id,
        period_month=period_month,
        period_start=period_start,
        period_end=period_end,
        previous_period_month=_previous_period_month(period_month),
        available_cash=available_cash,
        actor=actor,
        reason=reason,
        status=status,
        bucket_plans=_validate_bucket_plans(request.bucket_plans),
        category_plans=_validate_category_plans(request.category_plans),
    )


def _apply_rollover_policy(*, delta: Decimal, policy: str) -> Decimal:
    if policy == "none":
        return Decimal("0.00")
    if policy == "carry_positive":
        return _quantize_money(max(delta, Decimal("0.00")))
    if policy == "carry_negative":
        return _quantize_money(min(delta, Decimal("0.00")))
    if policy == "carry_both":
        return _quantize_money(delta)
    raise ValueError(f"Unsupported rollover policy: {policy}")


def _bucket_display_name(bucket_key: str) -> str:
    if bucket_key == "non_monthly":
        return "Non-monthly"
    return bucket_key.replace("_", " ").title()


def _resolve_active_budget_for_method(*, budget_id: str, expected_method: str, session: Session) -> Budget:
    budget = session.get(Budget, budget_id)
    if budget is None:
        raise ValueError(f"Budget not found: {budget_id}")
    if not budget.active:
        raise ValueError(f"Budget is not active: {budget_id}")
    if budget.method != expected_method:
        raise ValueError(
            f"Budget method must be {expected_method!r} for compute; got {budget.method!r}"
        )
    return budget


def _resolve_active_budget(*, budget_id: str, session: Session) -> Budget:
    return _resolve_active_budget_for_method(
        budget_id=budget_id,
        expected_method=_BUDGET_METHOD_ZERO_BASED,
        session=session,
    )


def _resolve_budget_categories(*, budget_id: str, session: Session) -> list[BudgetCategory]:
    return session.scalars(
        select(BudgetCategory)
        .where(BudgetCategory.budget_id == budget_id)
        .order_by(BudgetCategory.id.asc(), BudgetCategory.category_id.asc())
    ).all()


def _upsert_target_policies(
    *,
    policies: list[_ValidatedTargetPolicy],
    categories_by_id: dict[str, BudgetCategory],
    session: Session,
) -> None:
    if not policies:
        return

    for policy in policies:
        if policy.budget_category_id not in categories_by_id:
            raise ValueError(f"Unknown budget_category_id in target_policies: {policy.budget_category_id}")

        existing = session.scalars(
            select(BudgetTarget)
            .where(BudgetTarget.budget_category_id == policy.budget_category_id)
            .order_by(BudgetTarget.id.asc())
        ).all()
        target = existing[0] if existing else None
        if len(existing) > 1:
            raise ValueError(
                "Expected at most one BudgetTarget per budget category; "
                f"found {len(existing)} for {policy.budget_category_id}"
            )

        target_id = target.id if target is not None else f"target:{policy.budget_category_id}"
        stmt = sqlite_insert(BudgetTarget).values(
            id=target_id,
            budget_category_id=policy.budget_category_id,
            target_type=policy.target_type,
            amount=policy.amount,
            cadence=policy.cadence,
            top_up=policy.top_up,
            snoozed_until=policy.snoozed_until,
            metadata_json=policy.metadata_json,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetTarget.id],
            set_={
                "budget_category_id": policy.budget_category_id,
                "target_type": policy.target_type,
                "amount": policy.amount,
                "cadence": policy.cadence,
                "top_up": policy.top_up,
                "snoozed_until": policy.snoozed_until,
                "metadata_json": policy.metadata_json,
            },
        )
        session.execute(stmt)
        if target is not None:
            # Core upserts bypass ORM state synchronization; ensure the loaded target
            # instance is refreshed before later reads in this session.
            session.expire(target)


def _resolve_targets_by_budget_category(
    *,
    budget_category_ids: list[str],
    session: Session,
) -> dict[str, BudgetTarget]:
    if not budget_category_ids:
        return {}

    targets = session.scalars(
        select(BudgetTarget)
        .where(BudgetTarget.budget_category_id.in_(budget_category_ids))
        .order_by(BudgetTarget.budget_category_id.asc(), BudgetTarget.id.asc())
    ).all()
    by_category: dict[str, BudgetTarget] = {}
    for target in targets:
        if target.budget_category_id in by_category:
            raise ValueError(
                "Expected at most one BudgetTarget per budget category; "
                f"found multiple for {target.budget_category_id}"
            )
        by_category[target.budget_category_id] = target
    return by_category


def _ledger_spend_by_category(
    *,
    category_ids: list[str],
    period_start: date,
    period_end: date,
    session: Session,
) -> dict[str, Decimal]:
    if not category_ids:
        return {}
    spend_expr = case(
        (Transaction.amount < 0, -Transaction.amount),
        else_=Decimal("0.00"),
    )
    rows = session.execute(
        select(
            Transaction.category_id,
            func.coalesce(func.sum(spend_expr), Decimal("0.00")),
        )
        .where(
            and_(
                Transaction.category_id.in_(category_ids),
                Transaction.posted_date >= period_start,
                Transaction.posted_date <= period_end,
                Transaction.pending_status == _POSTED_STATUS,
                Transaction.excluded.is_(False),
            )
        )
        .group_by(Transaction.category_id)
    ).all()

    by_category: dict[str, Decimal] = {}
    for category_id, total in rows:
        if category_id is None:
            continue
        by_category[category_id] = _parse_decimal(total, field_name="spent_amount", non_negative=True)
    return by_category


def _resolve_period(
    *,
    budget_id: str,
    period_month: str,
    session: Session,
) -> BudgetPeriod | None:
    return session.scalar(
        select(BudgetPeriod)
        .where(
            BudgetPeriod.budget_id == budget_id,
            BudgetPeriod.period_month == period_month,
        )
        .limit(1)
    )


def _allocation_map_for_period(*, budget_period_id: str, session: Session) -> dict[str, BudgetAllocation]:
    allocations = session.scalars(
        select(BudgetAllocation)
        .where(BudgetAllocation.budget_period_id == budget_period_id)
        .order_by(BudgetAllocation.budget_category_id.asc(), BudgetAllocation.id.asc())
    ).all()
    by_budget_category_id: dict[str, BudgetAllocation] = {}
    for allocation in allocations:
        if allocation.budget_category_id in by_budget_category_id:
            raise ValueError(
                "Expected at most one BudgetAllocation per budget category in period; "
                f"found duplicate for period {budget_period_id} and budget_category_id "
                f"{allocation.budget_category_id}"
            )
        by_budget_category_id[allocation.budget_category_id] = allocation
    return by_budget_category_id


def _parse_interval_months(metadata_json: dict[str, object] | None) -> int:
    if metadata_json is None:
        raise ValueError(
            "every_n_months target metadata must include one of: "
            "months_interval, interval_months, every_n_months"
        )
    interval_raw: object = 1
    if "months_interval" in metadata_json:
        interval_raw = metadata_json["months_interval"]
    elif "interval_months" in metadata_json:
        interval_raw = metadata_json["interval_months"]
    elif "every_n_months" in metadata_json:
        interval_raw = metadata_json["every_n_months"]
    else:
        raise ValueError(
            "every_n_months target metadata must include one of: "
            "months_interval, interval_months, every_n_months"
        )
    try:
        interval = int(str(interval_raw))
    except (TypeError, ValueError) as exc:
        raise ValueError("every_n_months target metadata must include integer interval") from exc
    if interval <= 0:
        raise ValueError("every_n_months interval must be > 0")
    return interval


def _parse_anchor_month(period_month: str, metadata_json: dict[str, object] | None) -> str:
    if metadata_json is None or "anchor_month" not in metadata_json:
        return period_month
    anchor_raw = metadata_json.get("anchor_month")
    if anchor_raw is None:
        return period_month
    anchor = str(anchor_raw).strip()
    if not anchor:
        return period_month
    _period_bounds(anchor)
    return anchor


def _target_cadence_active(
    *,
    cadence: str,
    period_month: str,
    metadata_json: dict[str, object] | None,
) -> bool:
    if cadence == "monthly":
        return True
    if cadence != "every_n_months":
        raise ValueError(f"Unsupported target cadence: {cadence}")
    interval = _parse_interval_months(metadata_json)
    anchor = _parse_anchor_month(_DEFAULT_EVERY_N_MONTHS_ANCHOR, metadata_json)
    delta = _months_between(anchor, period_month)
    if delta < 0:
        return False
    return delta % interval == 0


def _upsert_budget_period_values(
    *,
    budget_id: str,
    period_month: str,
    status: str,
    assigned_total: Decimal,
    spent_total: Decimal,
    rollover_total: Decimal,
    to_assign: Decimal,
    session: Session,
) -> BudgetPeriod:
    existing_budget_period = _resolve_period(
        budget_id=budget_id,
        period_month=period_month,
        session=session,
    )
    stmt = sqlite_insert(BudgetPeriod).values(
        id=str(uuid4()),
        budget_id=budget_id,
        period_month=period_month,
        to_assign=to_assign,
        assigned_total=assigned_total,
        spent_total=spent_total,
        rollover_total=rollover_total,
        status=status,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[BudgetPeriod.budget_id, BudgetPeriod.period_month],
        set_={
            "to_assign": to_assign,
            "assigned_total": assigned_total,
            "spent_total": spent_total,
            "rollover_total": rollover_total,
            "status": status,
        },
    )
    session.execute(stmt)
    if existing_budget_period is not None:
        session.expire(existing_budget_period)

    budget_period = _resolve_period(
        budget_id=budget_id,
        period_month=period_month,
        session=session,
    )
    if budget_period is None:
        raise RuntimeError(
            f"Failed to resolve BudgetPeriod after upsert for {budget_id}/{period_month}"
        )
    return budget_period


def _upsert_budget_period(
    *,
    validated: _ValidatedRequest,
    assigned_total: Decimal,
    spent_total: Decimal,
    rollover_total: Decimal,
    to_assign: Decimal,
    session: Session,
) -> BudgetPeriod:
    return _upsert_budget_period_values(
        budget_id=validated.budget_id,
        period_month=validated.period_month,
        status=validated.status,
        assigned_total=assigned_total,
        spent_total=spent_total,
        rollover_total=rollover_total,
        to_assign=to_assign,
        session=session,
    )


def _sync_budget_allocations(
    *,
    budget_period_id: str,
    snapshots: list[BudgetCategorySnapshot],
    allocation_inputs: dict[str, _ValidatedAllocation],
    session: Session,
) -> None:
    canonical_ids: dict[str, str] = {}

    for snapshot in snapshots:
        allocation_input = allocation_inputs.get(snapshot.budget_category_id)
        source = allocation_input.source if allocation_input is not None else _ALLOCATION_SOURCE_ENGINE
        allocation_id = f"alloc:{budget_period_id}:{snapshot.budget_category_id}"
        canonical_ids[snapshot.budget_category_id] = allocation_id
        stmt = sqlite_insert(BudgetAllocation).values(
            id=allocation_id,
            budget_period_id=budget_period_id,
            budget_category_id=snapshot.budget_category_id,
            assigned_amount=snapshot.assigned_amount,
            source=source,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetAllocation.id],
            set_={
                "budget_period_id": budget_period_id,
                "budget_category_id": snapshot.budget_category_id,
                "assigned_amount": snapshot.assigned_amount,
                "source": source,
            },
        )
        session.execute(stmt)

    existing = session.scalars(
        select(BudgetAllocation).where(BudgetAllocation.budget_period_id == budget_period_id)
    ).all()
    for allocation in existing:
        canonical_id = canonical_ids.get(allocation.budget_category_id)
        if canonical_id is None or allocation.id != canonical_id:
            session.delete(allocation)


def _sync_rollover_row(
    *,
    budget_id: str,
    previous_period_month: str,
    period_month: str,
    carry_amount: Decimal,
    session: Session,
) -> None:
    rollover_id = f"rollover:budget:{budget_id}:{previous_period_month}->{period_month}:overspent"
    if carry_amount <= 0:
        existing = session.get(BudgetRollover, rollover_id)
        if existing is not None:
            session.delete(existing)
        return

    stmt = sqlite_insert(BudgetRollover).values(
        id=rollover_id,
        budget_id=budget_id,
        dimension_type=_ROLLOVER_DIMENSION_TYPE,
        dimension_id=budget_id,
        from_period=previous_period_month,
        to_period=period_month,
        carry_amount=carry_amount,
        policy_applied=_ROLLOVER_POLICY_REDUCE_TO_ASSIGN,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[BudgetRollover.id],
        set_={
            "budget_id": budget_id,
            "dimension_type": _ROLLOVER_DIMENSION_TYPE,
            "dimension_id": budget_id,
            "from_period": previous_period_month,
            "to_period": period_month,
            "carry_amount": carry_amount,
            "policy_applied": _ROLLOVER_POLICY_REDUCE_TO_ASSIGN,
        },
    )
    session.execute(stmt)


def _resolve_budget_bucket_definitions(
    *,
    budget_id: str,
    session: Session,
) -> dict[str, BudgetBucketDefinition]:
    definitions = session.scalars(
        select(BudgetBucketDefinition)
        .where(BudgetBucketDefinition.budget_id == budget_id)
        .order_by(BudgetBucketDefinition.bucket_key.asc(), BudgetBucketDefinition.id.asc())
    ).all()
    by_key: dict[str, BudgetBucketDefinition] = {}
    for definition in definitions:
        if definition.bucket_key in by_key:
            raise ValueError(
                "Expected at most one BudgetBucketDefinition per budget/bucket_key; "
                f"found duplicate for {budget_id}/{definition.bucket_key}"
            )
        by_key[definition.bucket_key] = definition
    return by_key


def _upsert_budget_bucket_definitions(
    *,
    budget_id: str,
    plans_by_key: dict[str, _ValidatedBucketPlan],
    session: Session,
) -> dict[str, BudgetBucketDefinition]:
    existing_by_key = _resolve_budget_bucket_definitions(budget_id=budget_id, session=session)
    for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS:
        plan = plans_by_key[bucket_key]
        existing = existing_by_key.get(bucket_key)
        definition_id = existing.id if existing is not None else f"bucketdef:{budget_id}:{bucket_key}"
        rollover_policy = (
            plan.rollover_policy
            if plan.rollover_policy is not None
            else (existing.rollover_policy if existing is not None else "none")
        )
        stmt = sqlite_insert(BudgetBucketDefinition).values(
            id=definition_id,
            budget_id=budget_id,
            bucket_key=bucket_key,
            name=_bucket_display_name(bucket_key),
            rollover_policy=rollover_policy,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetBucketDefinition.id],
            set_={
                "budget_id": budget_id,
                "bucket_key": bucket_key,
                "name": _bucket_display_name(bucket_key),
                "rollover_policy": rollover_policy,
            },
        )
        session.execute(stmt)
        if existing is not None:
            session.expire(existing)
    session.flush()
    return _resolve_budget_bucket_definitions(budget_id=budget_id, session=session)


def _sync_budget_bucket_category_mappings(
    *,
    budget_id: str,
    category_plans: dict[str, _ValidatedCategoryPlan],
    categories_by_id: dict[str, BudgetCategory],
    bucket_definitions_by_key: dict[str, BudgetBucketDefinition],
    session: Session,
) -> None:
    canonical_budget_category_ids: set[str] = set()
    for category_id, plan in category_plans.items():
        category = categories_by_id.get(category_id)
        if category is None:
            raise ValueError(f"Unknown budget_category_id in category_plans: {category_id}")
        canonical_budget_category_ids.add(category_id)
        bucket_definition = bucket_definitions_by_key[plan.bucket_key]
        if plan.rollover_policy is not None:
            category.rollover_policy = plan.rollover_policy

        mapping_id = f"bucketmap:{category_id}"
        stmt = sqlite_insert(BudgetBucketCategoryMapping).values(
            id=mapping_id,
            bucket_definition_id=bucket_definition.id,
            budget_category_id=category_id,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetBucketCategoryMapping.budget_category_id],
            set_={
                "bucket_definition_id": bucket_definition.id,
                "budget_category_id": category_id,
            },
        )
        session.execute(stmt)

    existing = session.scalars(
        select(BudgetBucketCategoryMapping)
        .join(
            BudgetCategory,
            BudgetCategory.id == BudgetBucketCategoryMapping.budget_category_id,
        )
        .where(BudgetCategory.budget_id == budget_id)
    ).all()
    for mapping in existing:
        if mapping.budget_category_id not in canonical_budget_category_ids:
            session.delete(mapping)


def _resolve_bucket_key_for_bucket_row(
    *,
    bucket: BudgetBucket,
    definitions_by_id: dict[str, BudgetBucketDefinition],
) -> str | None:
    if bucket.bucket_definition_id is not None:
        definition = definitions_by_id.get(bucket.bucket_definition_id)
        if definition is not None:
            return definition.bucket_key
    try:
        return _normalize_bucket_key(bucket.bucket_name, field_name="bucket_name")
    except ValueError:
        return None


def _resolve_bucket_mappings(
    *,
    budget_category_ids: list[str],
    bucket_definitions_by_id: dict[str, BudgetBucketDefinition],
    session: Session,
) -> dict[str, str]:
    if not budget_category_ids:
        return {}
    mappings = session.scalars(
        select(BudgetBucketCategoryMapping)
        .where(BudgetBucketCategoryMapping.budget_category_id.in_(budget_category_ids))
        .order_by(BudgetBucketCategoryMapping.budget_category_id.asc(), BudgetBucketCategoryMapping.id.asc())
    ).all()
    mapped_bucket_by_budget_category: dict[str, str] = {}
    for mapping in mappings:
        if mapping.budget_category_id in mapped_bucket_by_budget_category:
            raise ValueError(
                "Expected at most one bucket mapping per budget category; "
                f"found duplicate for {mapping.budget_category_id}"
            )
        definition = bucket_definitions_by_id.get(mapping.bucket_definition_id)
        if definition is None:
            continue
        mapped_bucket_by_budget_category[mapping.budget_category_id] = definition.bucket_key
    return mapped_bucket_by_budget_category


def _sync_budget_allocations_from_plan(
    *,
    budget_period_id: str,
    planned_amounts_by_budget_category_id: dict[str, Decimal],
    source: str,
    session: Session,
) -> None:
    canonical_ids: dict[str, str] = {}
    for budget_category_id, planned_amount in planned_amounts_by_budget_category_id.items():
        allocation_id = f"alloc:{budget_period_id}:{budget_category_id}"
        canonical_ids[budget_category_id] = allocation_id
        stmt = sqlite_insert(BudgetAllocation).values(
            id=allocation_id,
            budget_period_id=budget_period_id,
            budget_category_id=budget_category_id,
            assigned_amount=planned_amount,
            source=source,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetAllocation.id],
            set_={
                "budget_period_id": budget_period_id,
                "budget_category_id": budget_category_id,
                "assigned_amount": planned_amount,
                "source": source,
            },
        )
        session.execute(stmt)

    existing = session.scalars(
        select(BudgetAllocation).where(BudgetAllocation.budget_period_id == budget_period_id)
    ).all()
    for allocation in existing:
        canonical_id = canonical_ids.get(allocation.budget_category_id)
        if canonical_id is None or allocation.id != canonical_id:
            session.delete(allocation)


def _sync_budget_bucket_rows(
    *,
    budget_id: str,
    period_month: str,
    bucket_definitions_by_key: dict[str, BudgetBucketDefinition],
    bucket_planned_by_key: dict[str, Decimal],
    bucket_actual_by_key: dict[str, Decimal],
    session: Session,
) -> None:
    canonical_ids: set[str] = set()
    for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS:
        definition = bucket_definitions_by_key[bucket_key]
        bucket_id = f"bucket:{budget_id}:{period_month}:{bucket_key}"
        canonical_ids.add(bucket_id)
        stmt = sqlite_insert(BudgetBucket).values(
            id=bucket_id,
            budget_id=budget_id,
            bucket_definition_id=definition.id,
            period_month=period_month,
            bucket_name=bucket_key,
            planned_amount=bucket_planned_by_key.get(bucket_key, Decimal("0.00")),
            actual_amount=bucket_actual_by_key.get(bucket_key, Decimal("0.00")),
            rollover_policy=definition.rollover_policy,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetBucket.id],
            set_={
                "budget_id": budget_id,
                "bucket_definition_id": definition.id,
                "period_month": period_month,
                "bucket_name": bucket_key,
                "planned_amount": bucket_planned_by_key.get(bucket_key, Decimal("0.00")),
                "actual_amount": bucket_actual_by_key.get(bucket_key, Decimal("0.00")),
                "rollover_policy": definition.rollover_policy,
            },
        )
        session.execute(stmt)

    existing_rows = session.scalars(
        select(BudgetBucket).where(
            BudgetBucket.budget_id == budget_id,
            BudgetBucket.period_month == period_month,
        )
    ).all()
    for row in existing_rows:
        if row.id not in canonical_ids:
            session.delete(row)


def _sync_flex_rollover_rows(
    *,
    budget_id: str,
    previous_period_month: str,
    period_month: str,
    bucket_definitions_by_key: dict[str, BudgetBucketDefinition],
    bucket_carry_by_key: dict[str, Decimal],
    bucket_policy_by_key: dict[str, str],
    category_carry_by_id: dict[str, Decimal],
    category_policy_by_id: dict[str, str],
    session: Session,
) -> None:
    canonical_rows: dict[str, tuple[str, str, Decimal, str]] = {}

    for bucket_key, carry_amount in bucket_carry_by_key.items():
        if carry_amount == Decimal("0.00"):
            continue
        definition = bucket_definitions_by_key[bucket_key]
        rollover_id = (
            f"rollover:flex:bucket:{budget_id}:{bucket_key}:{previous_period_month}->{period_month}"
        )
        canonical_rows[rollover_id] = (
            _ROLLOVER_DIMENSION_TYPE_BUCKET,
            definition.id,
            carry_amount,
            bucket_policy_by_key.get(bucket_key, "none"),
        )

    for budget_category_id, carry_amount in category_carry_by_id.items():
        if carry_amount == Decimal("0.00"):
            continue
        rollover_id = (
            "rollover:flex:category:"
            f"{budget_id}:{budget_category_id}:{previous_period_month}->{period_month}"
        )
        canonical_rows[rollover_id] = (
            _ROLLOVER_DIMENSION_TYPE_CATEGORY,
            budget_category_id,
            carry_amount,
            category_policy_by_id.get(budget_category_id, "none"),
        )

    existing_rows = session.scalars(
        select(BudgetRollover).where(
            BudgetRollover.budget_id == budget_id,
            BudgetRollover.from_period == previous_period_month,
            BudgetRollover.to_period == period_month,
            BudgetRollover.dimension_type.in_(
                [_ROLLOVER_DIMENSION_TYPE_BUCKET, _ROLLOVER_DIMENSION_TYPE_CATEGORY]
            ),
        )
    ).all()
    for existing in existing_rows:
        if existing.id not in canonical_rows:
            session.delete(existing)

    for rollover_id, (dimension_type, dimension_id, carry_amount, policy_applied) in canonical_rows.items():
        stmt = sqlite_insert(BudgetRollover).values(
            id=rollover_id,
            budget_id=budget_id,
            dimension_type=dimension_type,
            dimension_id=dimension_id,
            from_period=previous_period_month,
            to_period=period_month,
            carry_amount=carry_amount,
            policy_applied=policy_applied,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[BudgetRollover.id],
            set_={
                "budget_id": budget_id,
                "dimension_type": dimension_type,
                "dimension_id": dimension_id,
                "from_period": previous_period_month,
                "to_period": period_month,
                "carry_amount": carry_amount,
                "policy_applied": policy_applied,
            },
        )
        session.execute(stmt)


def budget_compute_zero_based(
    request: BudgetComputeZeroBasedRequest,
    session: Session,
) -> BudgetComputeZeroBasedResult:
    """Compute and persist a deterministic zero-based budget snapshot."""

    validated = _validate_request(request)
    _resolve_active_budget(budget_id=validated.budget_id, session=session)

    budget_categories = _resolve_budget_categories(budget_id=validated.budget_id, session=session)
    category_owner_by_id: dict[str, str] = {}
    duplicate_category_ids: set[str] = set()
    for budget_category in budget_categories:
        owner = category_owner_by_id.setdefault(budget_category.category_id, budget_category.id)
        if owner != budget_category.id:
            duplicate_category_ids.add(budget_category.category_id)
    if duplicate_category_ids:
        raise ValueError(
            "Duplicate category_id across budget_categories is not supported: "
            + ", ".join(sorted(duplicate_category_ids))
        )
    categories_by_id = {budget_category.id: budget_category for budget_category in budget_categories}

    unknown_allocation_ids = sorted(set(validated.category_allocations) - set(categories_by_id))
    if unknown_allocation_ids:
        raise ValueError(
            "Unknown budget_category_id in category_allocations: "
            + ", ".join(unknown_allocation_ids)
        )

    _upsert_target_policies(
        policies=validated.target_policies,
        categories_by_id=categories_by_id,
        session=session,
    )
    session.flush()

    budget_category_ids = [category.id for category in budget_categories]
    targets_by_budget_category = _resolve_targets_by_budget_category(
        budget_category_ids=budget_category_ids,
        session=session,
    )

    category_ids = sorted(category_owner_by_id)
    current_spend_by_category = _ledger_spend_by_category(
        category_ids=category_ids,
        period_start=validated.period_start,
        period_end=validated.period_end,
        session=session,
    )

    previous_period = _resolve_period(
        budget_id=validated.budget_id,
        period_month=validated.previous_period_month,
        session=session,
    )
    carry_in_overspent = Decimal("0.00")
    causes: list[BudgetRunCause] = []
    previous_available_by_budget_category: dict[str, Decimal] = {}

    if previous_period is not None:
        previous_start, previous_end = _period_bounds(validated.previous_period_month)
        previous_spend_by_category = _ledger_spend_by_category(
            category_ids=category_ids,
            period_start=previous_start,
            period_end=previous_end,
            session=session,
        )
        previous_allocations = _allocation_map_for_period(
            budget_period_id=previous_period.id,
            session=session,
        )
        for budget_category in budget_categories:
            previous_assigned = Decimal("0.00")
            if budget_category.id in previous_allocations:
                previous_assigned = _parse_decimal(
                    previous_allocations[budget_category.id].assigned_amount,
                    field_name="previous_assigned_amount",
                    non_negative=True,
                )
            previous_spent = previous_spend_by_category.get(
                budget_category.category_id,
                Decimal("0.00"),
            )
            previous_available_by_budget_category[budget_category.id] = _quantize_money(
                max(previous_assigned - previous_spent, Decimal("0.00"))
            )

        if previous_period.status == _PERIOD_STATUS_CLOSED:
            carry_in_overspent = _quantize_money(
                max(
                    _parse_decimal(previous_period.spent_total, field_name="previous_spent_total")
                    - _parse_decimal(previous_period.assigned_total, field_name="previous_assigned_total"),
                    Decimal("0.00"),
                )
            )
            if carry_in_overspent > 0:
                causes.append(
                    BudgetRunCause(
                        code="overspent_carry_applied",
                        message=(
                            "Previous closed period overspending reduced current period To Assign "
                            f"by {carry_in_overspent}"
                        ),
                        severity="info",
                    )
                )

    snapshots: list[BudgetCategorySnapshot] = []
    for budget_category in budget_categories:
        assigned_amount = validated.category_allocations.get(
            budget_category.id,
            _ValidatedAllocation(
                budget_category_id=budget_category.id,
                assigned_amount=Decimal("0.00"),
                source=_ALLOCATION_SOURCE_ENGINE,
            ),
        ).assigned_amount
        spent_amount = current_spend_by_category.get(budget_category.category_id, Decimal("0.00"))

        available_before_assignment = previous_available_by_budget_category.get(
            budget_category.id,
            Decimal("0.00"),
        )

        target = targets_by_budget_category.get(budget_category.id)
        target_required = Decimal("0.00")
        target_top_up: bool | None = None
        target_cadence: str | None = None
        target_type: str | None = None
        target_id: str | None = None
        snoozed = False

        if target is not None:
            target_id = target.id
            target_type = target.target_type
            target_top_up = target.top_up
            target_cadence = target.cadence or "monthly"
            if target_cadence not in _ALLOWED_CADENCES:
                raise ValueError(f"Unsupported target cadence: {target_cadence}")

            if target.snoozed_until is not None and target.snoozed_until >= validated.period_start:
                snoozed = True
            elif _target_cadence_active(
                cadence=target_cadence,
                period_month=validated.period_month,
                metadata_json=target.metadata_json,
            ):
                amount = (
                    _parse_decimal(target.amount, field_name="target.amount", non_negative=True)
                    if target.amount is not None
                    else Decimal("0.00")
                )
                if target.top_up:
                    target_required = _quantize_money(max(amount - available_before_assignment, Decimal("0.00")))
                else:
                    target_required = amount

        underfunded = _quantize_money(max(target_required - assigned_amount, Decimal("0.00")))
        overspent = _quantize_money(max(spent_amount - assigned_amount, Decimal("0.00")))
        snapshots.append(
            BudgetCategorySnapshot(
                budget_category_id=budget_category.id,
                category_id=budget_category.category_id,
                assigned_amount=assigned_amount,
                spent_amount=spent_amount,
                available_before_assignment=available_before_assignment,
                target_required=target_required,
                underfunded=underfunded,
                overspent=overspent,
                target_id=target_id,
                target_type=target_type,
                target_cadence=target_cadence,
                target_top_up=target_top_up,
                snoozed=snoozed,
            )
        )

    assigned_total = _quantize_money(sum((snapshot.assigned_amount for snapshot in snapshots), Decimal("0.00")))
    spent_total = _quantize_money(sum((snapshot.spent_amount for snapshot in snapshots), Decimal("0.00")))
    underfunded_total = _quantize_money(sum((snapshot.underfunded for snapshot in snapshots), Decimal("0.00")))
    overspent_total = _quantize_money(sum((snapshot.overspent for snapshot in snapshots), Decimal("0.00")))
    rollover_total = carry_in_overspent
    to_assign = _quantize_money(validated.available_cash - assigned_total - rollover_total)
    if to_assign < 0:
        causes.append(
            BudgetRunCause(
                code="to_assign_negative",
                message=(
                    "Current period is over-assigned: "
                    f"available_cash={validated.available_cash}, "
                    f"assigned_total={assigned_total}, "
                    f"rollover_total={rollover_total}, "
                    f"to_assign={to_assign}"
                ),
                severity="warning",
            )
        )

    budget_period = _upsert_budget_period(
        validated=validated,
        assigned_total=assigned_total,
        spent_total=spent_total,
        rollover_total=rollover_total,
        to_assign=to_assign,
        session=session,
    )
    session.flush()

    _sync_budget_allocations(
        budget_period_id=budget_period.id,
        snapshots=snapshots,
        allocation_inputs=validated.category_allocations,
        session=session,
    )
    _sync_rollover_row(
        budget_id=validated.budget_id,
        previous_period_month=validated.previous_period_month,
        period_month=validated.period_month,
        carry_amount=carry_in_overspent,
        session=session,
    )

    if not snapshots:
        causes.append(
            BudgetRunCause(
                code="no_budget_categories",
                message=f"Budget {validated.budget_id} has no configured budget_categories",
                severity="warning",
            )
        )

    return BudgetComputeZeroBasedResult(
        budget_period_id=budget_period.id,
        budget_id=validated.budget_id,
        period_month=validated.period_month,
        status=validated.status,
        available_cash=validated.available_cash,
        carry_in_overspent=carry_in_overspent,
        assigned_total=assigned_total,
        spent_total=spent_total,
        rollover_total=rollover_total,
        to_assign=to_assign,
        underfunded_total=underfunded_total,
        overspent_total=overspent_total,
        categories=snapshots,
        causes=causes,
    )


def budget_compute_flex(
    request: BudgetComputeFlexRequest,
    session: Session,
) -> BudgetComputeFlexResult:
    """Compute and persist a deterministic flex budget snapshot."""

    validated = _validate_flex_request(request)
    _resolve_active_budget_for_method(
        budget_id=validated.budget_id,
        expected_method=_BUDGET_METHOD_FLEX,
        session=session,
    )

    budget_categories = _resolve_budget_categories(budget_id=validated.budget_id, session=session)
    category_owner_by_id: dict[str, str] = {}
    duplicate_category_ids: set[str] = set()
    for budget_category in budget_categories:
        owner = category_owner_by_id.setdefault(budget_category.category_id, budget_category.id)
        if owner != budget_category.id:
            duplicate_category_ids.add(budget_category.category_id)
    if duplicate_category_ids:
        raise ValueError(
            "Duplicate category_id across budget_categories is not supported: "
            + ", ".join(sorted(duplicate_category_ids))
        )
    categories_by_id = {budget_category.id: budget_category for budget_category in budget_categories}
    unknown_plan_ids = sorted(set(validated.category_plans) - set(categories_by_id))
    if unknown_plan_ids:
        raise ValueError("Unknown budget_category_id in category_plans: " + ", ".join(unknown_plan_ids))

    bucket_definitions_by_key = _upsert_budget_bucket_definitions(
        budget_id=validated.budget_id,
        plans_by_key=validated.bucket_plans,
        session=session,
    )
    _sync_budget_bucket_category_mappings(
        budget_id=validated.budget_id,
        category_plans=validated.category_plans,
        categories_by_id=categories_by_id,
        bucket_definitions_by_key=bucket_definitions_by_key,
        session=session,
    )
    session.flush()

    bucket_definitions_by_key = _resolve_budget_bucket_definitions(
        budget_id=validated.budget_id,
        session=session,
    )
    bucket_definitions_by_id = {definition.id: definition for definition in bucket_definitions_by_key.values()}

    budget_category_ids = [category.id for category in budget_categories]
    mapped_bucket_by_budget_category = _resolve_bucket_mappings(
        budget_category_ids=budget_category_ids,
        bucket_definitions_by_id=bucket_definitions_by_id,
        session=session,
    )
    unmapped_budget_category_ids = sorted(
        budget_category.id
        for budget_category in budget_categories
        if budget_category.id not in mapped_bucket_by_budget_category
    )
    if unmapped_budget_category_ids:
        raise ValueError(
            "Missing bucket mapping for budget_category_id(s): " + ", ".join(unmapped_budget_category_ids)
        )

    category_ids = sorted(category_owner_by_id)
    current_spend_by_category = _ledger_spend_by_category(
        category_ids=category_ids,
        period_start=validated.period_start,
        period_end=validated.period_end,
        session=session,
    )

    category_planned_by_budget_category: dict[str, Decimal] = {
        budget_category_id: plan.planned_amount
        for budget_category_id, plan in validated.category_plans.items()
    }
    for budget_category_id in mapped_bucket_by_budget_category:
        category_planned_by_budget_category.setdefault(budget_category_id, Decimal("0.00"))

    category_policy_by_budget_category: dict[str, str] = {}
    for budget_category_id in mapped_bucket_by_budget_category:
        rollover_policy = categories_by_id[budget_category_id].rollover_policy or "none"
        category_policy_by_budget_category[budget_category_id] = _parse_rollover_policy(
            rollover_policy,
            field_name=f"budget_categories[{budget_category_id}].rollover_policy",
        )

    bucket_planned_by_key: dict[str, Decimal] = {
        bucket_key: validated.bucket_plans[bucket_key].planned_amount for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
    }
    bucket_actual_by_key: dict[str, Decimal] = {
        bucket_key: Decimal("0.00") for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
    }
    bucket_category_ids_by_key: dict[str, list[str]] = {
        bucket_key: [] for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
    }
    for budget_category in budget_categories:
        bucket_key = mapped_bucket_by_budget_category.get(budget_category.id)
        if bucket_key is None:
            continue
        bucket_category_ids_by_key[bucket_key].append(budget_category.id)
        bucket_actual_by_key[bucket_key] += current_spend_by_category.get(
            budget_category.category_id,
            Decimal("0.00"),
        )
    for bucket_key in bucket_actual_by_key:
        bucket_actual_by_key[bucket_key] = _quantize_money(bucket_actual_by_key[bucket_key])

    previous_period = _resolve_period(
        budget_id=validated.budget_id,
        period_month=validated.previous_period_month,
        session=session,
    )
    bucket_carry_by_key: dict[str, Decimal] = {
        bucket_key: Decimal("0.00") for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
    }
    category_carry_by_budget_category: dict[str, Decimal] = {}
    bucket_rollover_policy_by_key: dict[str, str] = {
        bucket_key: bucket_definitions_by_key[bucket_key].rollover_policy for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
    }
    rollover_total = Decimal("0.00")
    causes: list[BudgetRunCause] = []

    if previous_period is not None and previous_period.status == _PERIOD_STATUS_CLOSED:
        previous_start, previous_end = _period_bounds(validated.previous_period_month)
        previous_spend_by_category = _ledger_spend_by_category(
            category_ids=category_ids,
            period_start=previous_start,
            period_end=previous_end,
            session=session,
        )
        previous_allocations = _allocation_map_for_period(
            budget_period_id=previous_period.id,
            session=session,
        )

        previous_bucket_rows = session.scalars(
            select(BudgetBucket).where(
                BudgetBucket.budget_id == validated.budget_id,
                BudgetBucket.period_month == validated.previous_period_month,
            )
        ).all()
        previous_bucket_planned_by_key: dict[str, Decimal] = {
            bucket_key: Decimal("0.00") for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
        }
        previous_bucket_actual_by_key: dict[str, Decimal] = {
            bucket_key: Decimal("0.00") for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS
        }
        for previous_bucket_row in previous_bucket_rows:
            bucket_key = _resolve_bucket_key_for_bucket_row(
                bucket=previous_bucket_row,
                definitions_by_id=bucket_definitions_by_id,
            )
            if bucket_key is None:
                continue
            previous_bucket_planned_by_key[bucket_key] += _parse_decimal(
                previous_bucket_row.planned_amount,
                field_name="previous_bucket.planned_amount",
                non_negative=True,
            )
            previous_bucket_actual_by_key[bucket_key] += _parse_decimal(
                previous_bucket_row.actual_amount,
                field_name="previous_bucket.actual_amount",
                non_negative=True,
            )
            if previous_bucket_row.rollover_policy is not None:
                bucket_rollover_policy_by_key[bucket_key] = _parse_rollover_policy(
                    previous_bucket_row.rollover_policy,
                    field_name="previous_bucket.rollover_policy",
                )

        for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS:
            delta = _quantize_money(
                previous_bucket_planned_by_key[bucket_key] - previous_bucket_actual_by_key[bucket_key]
            )
            bucket_carry_by_key[bucket_key] = _apply_rollover_policy(
                delta=delta,
                policy=bucket_rollover_policy_by_key[bucket_key],
            )
            rollover_total += bucket_carry_by_key[bucket_key]

        for budget_category in budget_categories:
            if budget_category.id not in mapped_bucket_by_budget_category:
                continue
            previous_planned = Decimal("0.00")
            previous_allocation = previous_allocations.get(budget_category.id)
            if previous_allocation is not None:
                previous_planned = _parse_decimal(
                    previous_allocation.assigned_amount,
                    field_name="previous_category.planned_amount",
                    non_negative=True,
                )
            previous_actual = previous_spend_by_category.get(budget_category.category_id, Decimal("0.00"))
            policy = category_policy_by_budget_category[budget_category.id]
            carry = _apply_rollover_policy(
                delta=_quantize_money(previous_planned - previous_actual),
                policy=policy,
            )
            category_carry_by_budget_category[budget_category.id] = carry

    rollover_total = _quantize_money(rollover_total)
    fixed_planned = bucket_planned_by_key["fixed"]
    non_monthly_planned = bucket_planned_by_key["non_monthly"]
    flex_planned = bucket_planned_by_key["flex"]
    assigned_total = _quantize_money(sum(bucket_planned_by_key.values(), Decimal("0.00")))
    spent_total = _quantize_money(sum(bucket_actual_by_key.values(), Decimal("0.00")))
    flex_available = _quantize_money(validated.available_cash - fixed_planned - non_monthly_planned + rollover_total)
    if flex_available < 0:
        causes.append(
            BudgetRunCause(
                code="flex_available_negative",
                message=(
                    "Current period flex available is negative: "
                    f"available_cash={validated.available_cash}, "
                    f"fixed_planned={fixed_planned}, "
                    f"non_monthly_planned={non_monthly_planned}, "
                    f"rollover_total={rollover_total}, "
                    f"flex_available={flex_available}"
                ),
                severity="warning",
            )
        )

    budget_period = _upsert_budget_period_values(
        budget_id=validated.budget_id,
        period_month=validated.period_month,
        status=validated.status,
        assigned_total=assigned_total,
        spent_total=spent_total,
        rollover_total=rollover_total,
        to_assign=flex_available,
        session=session,
    )
    session.flush()

    _sync_budget_allocations_from_plan(
        budget_period_id=budget_period.id,
        planned_amounts_by_budget_category_id=category_planned_by_budget_category,
        source=_ALLOCATION_SOURCE_FLEX_ENGINE,
        session=session,
    )
    _sync_budget_bucket_rows(
        budget_id=validated.budget_id,
        period_month=validated.period_month,
        bucket_definitions_by_key=bucket_definitions_by_key,
        bucket_planned_by_key=bucket_planned_by_key,
        bucket_actual_by_key=bucket_actual_by_key,
        session=session,
    )
    if previous_period is not None:
        _sync_flex_rollover_rows(
            budget_id=validated.budget_id,
            previous_period_month=validated.previous_period_month,
            period_month=validated.period_month,
            bucket_definitions_by_key=bucket_definitions_by_key,
            bucket_carry_by_key=bucket_carry_by_key,
            bucket_policy_by_key=bucket_rollover_policy_by_key,
            category_carry_by_id=category_carry_by_budget_category,
            category_policy_by_id=category_policy_by_budget_category,
            session=session,
        )

    if not mapped_bucket_by_budget_category and not budget_categories:
        causes.append(
            BudgetRunCause(
                code="no_bucket_mappings",
                message=f"Budget {validated.budget_id} has no configured bucket/category mappings",
                severity="warning",
            )
        )

    bucket_snapshots: list[BudgetBucketSnapshot] = []
    for bucket_key in _ALLOWED_FLEX_BUCKET_KEYS:
        definition = bucket_definitions_by_key[bucket_key]
        bucket_snapshots.append(
            BudgetBucketSnapshot(
                bucket_definition_id=definition.id,
                bucket_key=bucket_key,
                bucket_name=definition.name,
                planned_amount=bucket_planned_by_key[bucket_key],
                actual_amount=bucket_actual_by_key[bucket_key],
                rollover_policy=definition.rollover_policy,
                rollover_carry=bucket_carry_by_key[bucket_key],
                category_ids=sorted(bucket_category_ids_by_key[bucket_key]),
            )
        )

    category_snapshots: list[BudgetCategoryRolloverSnapshot] = []
    for budget_category_id in sorted(mapped_bucket_by_budget_category):
        budget_category = categories_by_id[budget_category_id]
        category_snapshots.append(
            BudgetCategoryRolloverSnapshot(
                budget_category_id=budget_category_id,
                category_id=budget_category.category_id,
                bucket_key=mapped_bucket_by_budget_category[budget_category_id],
                planned_amount=category_planned_by_budget_category[budget_category_id],
                actual_amount=current_spend_by_category.get(budget_category.category_id, Decimal("0.00")),
                rollover_policy=category_policy_by_budget_category[budget_category_id],
                rollover_carry=category_carry_by_budget_category.get(
                    budget_category_id,
                    Decimal("0.00"),
                ),
            )
        )

    return BudgetComputeFlexResult(
        budget_period_id=budget_period.id,
        budget_id=validated.budget_id,
        period_month=validated.period_month,
        status=validated.status,
        available_cash=validated.available_cash,
        fixed_planned=fixed_planned,
        non_monthly_planned=non_monthly_planned,
        flex_planned=flex_planned,
        assigned_total=assigned_total,
        spent_total=spent_total,
        rollover_total=rollover_total,
        flex_available=flex_available,
        buckets=bucket_snapshots,
        categories=category_snapshots,
        causes=causes,
    )
