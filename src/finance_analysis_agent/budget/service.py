"""Service-layer zero-based budgeting workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from calendar import monthrange
from uuid import uuid4

from sqlalchemy import and_, case, func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.budget.types import (
    BudgetCategoryAllocationInput,
    BudgetCategorySnapshot,
    BudgetComputeZeroBasedRequest,
    BudgetComputeZeroBasedResult,
    BudgetRunCause,
    BudgetTargetPolicyInput,
)
from finance_analysis_agent.db.models import (
    Budget,
    BudgetAllocation,
    BudgetCategory,
    BudgetPeriod,
    BudgetRollover,
    BudgetTarget,
    Transaction,
)

_DECIMAL_2 = Decimal("0.01")
_BUDGET_METHOD_ZERO_BASED = "zero_based"
_POSTED_STATUS = "posted"
_PERIOD_STATUS_OPEN = "open"
_PERIOD_STATUS_CLOSED = "closed"
_ALLOWED_PERIOD_STATUSES = {_PERIOD_STATUS_OPEN, _PERIOD_STATUS_CLOSED}
_ALLOWED_CADENCES = {"monthly", "every_n_months"}
_ROLLOVER_DIMENSION_TYPE = "budget_period_overspent"
_ROLLOVER_POLICY_REDUCE_TO_ASSIGN = "reduce_to_assign"
_ALLOCATION_SOURCE_ENGINE = "budget_compute_zero_based"


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


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_DECIMAL_2, rounding=ROUND_HALF_UP)


def _parse_non_empty(value: str, *, field_name: str) -> str:
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
    try:
        year_text, month_text = period_month.split("-")
        year = int(year_text)
        month = int(month_text)
        start = date(year, month, 1)
    except (TypeError, ValueError) as exc:
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
    for index, policy in enumerate(values):
        category_id = _parse_non_empty(
            policy.budget_category_id,
            field_name=f"target_policies[{index}].budget_category_id",
        )
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
                metadata_json=dict(policy.metadata_json) if policy.metadata_json is not None else None,
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


def _resolve_active_budget(*, budget_id: str, session: Session) -> Budget:
    budget = session.get(Budget, budget_id)
    if budget is None:
        raise ValueError(f"Budget not found: {budget_id}")
    if not budget.active:
        raise ValueError(f"Budget is not active: {budget_id}")
    if budget.method != _BUDGET_METHOD_ZERO_BASED:
        raise ValueError(
            f"Budget method must be {_BUDGET_METHOD_ZERO_BASED!r} for zero-based compute; got {budget.method!r}"
        )
    return budget


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

        if target is None:
            target = BudgetTarget(
                id=str(uuid4()),
                budget_category_id=policy.budget_category_id,
                target_type=policy.target_type,
                amount=policy.amount,
                cadence=policy.cadence,
                top_up=policy.top_up,
                snoozed_until=policy.snoozed_until,
                metadata_json=policy.metadata_json,
            )
            session.add(target)
            continue

        target.target_type = policy.target_type
        target.amount = policy.amount
        target.cadence = policy.cadence
        target.top_up = policy.top_up
        target.snoozed_until = policy.snoozed_until
        target.metadata_json = policy.metadata_json


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
    return {allocation.budget_category_id: allocation for allocation in allocations}


def _parse_interval_months(metadata_json: dict[str, object] | None) -> int:
    if metadata_json is None:
        return 1
    interval_raw = (
        metadata_json.get("months_interval")
        or metadata_json.get("interval_months")
        or metadata_json.get("every_n_months")
        or 1
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
    anchor = _parse_anchor_month(period_month, metadata_json)
    delta = _months_between(anchor, period_month)
    if delta < 0:
        return False
    return delta % interval == 0


def _upsert_budget_period(
    *,
    validated: _ValidatedRequest,
    assigned_total: Decimal,
    spent_total: Decimal,
    rollover_total: Decimal,
    to_assign: Decimal,
    session: Session,
) -> BudgetPeriod:
    budget_period = _resolve_period(
        budget_id=validated.budget_id,
        period_month=validated.period_month,
        session=session,
    )
    if budget_period is None:
        budget_period = BudgetPeriod(
            id=str(uuid4()),
            budget_id=validated.budget_id,
            period_month=validated.period_month,
            to_assign=to_assign,
            assigned_total=assigned_total,
            spent_total=spent_total,
            rollover_total=rollover_total,
            status=validated.status,
        )
        session.add(budget_period)
        return budget_period

    budget_period.to_assign = to_assign
    budget_period.assigned_total = assigned_total
    budget_period.spent_total = spent_total
    budget_period.rollover_total = rollover_total
    budget_period.status = validated.status
    return budget_period


def _sync_budget_allocations(
    *,
    budget_period_id: str,
    snapshots: list[BudgetCategorySnapshot],
    allocation_inputs: dict[str, _ValidatedAllocation],
    session: Session,
) -> None:
    existing = _allocation_map_for_period(budget_period_id=budget_period_id, session=session)
    expected_ids = {snapshot.budget_category_id for snapshot in snapshots}

    for snapshot in snapshots:
        allocation_input = allocation_inputs.get(snapshot.budget_category_id)
        source = allocation_input.source if allocation_input is not None else _ALLOCATION_SOURCE_ENGINE
        allocation = existing.get(snapshot.budget_category_id)
        if allocation is None:
            allocation = BudgetAllocation(
                id=f"alloc:{budget_period_id}:{snapshot.budget_category_id}",
                budget_period_id=budget_period_id,
                budget_category_id=snapshot.budget_category_id,
                assigned_amount=snapshot.assigned_amount,
                source=source,
            )
            session.add(allocation)
            continue
        allocation.assigned_amount = snapshot.assigned_amount
        allocation.source = source

    for budget_category_id, allocation in existing.items():
        if budget_category_id not in expected_ids:
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
    existing = session.get(BudgetRollover, rollover_id)
    if carry_amount <= 0:
        if existing is not None:
            session.delete(existing)
        return

    if existing is None:
        session.add(
            BudgetRollover(
                id=rollover_id,
                budget_id=budget_id,
                dimension_type=_ROLLOVER_DIMENSION_TYPE,
                dimension_id=budget_id,
                from_period=previous_period_month,
                to_period=period_month,
                carry_amount=carry_amount,
                policy_applied=_ROLLOVER_POLICY_REDUCE_TO_ASSIGN,
            )
        )
        return

    existing.dimension_type = _ROLLOVER_DIMENSION_TYPE
    existing.dimension_id = budget_id
    existing.from_period = previous_period_month
    existing.to_period = period_month
    existing.carry_amount = carry_amount
    existing.policy_applied = _ROLLOVER_POLICY_REDUCE_TO_ASSIGN


def budget_compute_zero_based(
    request: BudgetComputeZeroBasedRequest,
    session: Session,
) -> BudgetComputeZeroBasedResult:
    """Compute and persist a deterministic zero-based budget snapshot."""

    validated = _validate_request(request)
    _resolve_active_budget(budget_id=validated.budget_id, session=session)

    budget_categories = _resolve_budget_categories(budget_id=validated.budget_id, session=session)
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

    category_ids = sorted({category.category_id for category in budget_categories})
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
