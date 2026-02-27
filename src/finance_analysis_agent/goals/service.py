"""Service-layer goal ledger workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from calendar import monthrange
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
import re
from uuid import uuid4

from sqlalchemy import case, func, literal, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Goal, GoalAllocation, GoalEvent, Transaction
from finance_analysis_agent.goals.types import (
    GoalAllocationInput,
    GoalLedgerCause,
    GoalLedgerComputeRequest,
    GoalLedgerComputeResult,
    GoalProgressSnapshot,
)
from finance_analysis_agent.utils.time import utcnow

_DECIMAL_2 = Decimal("0.01")
_PERIOD_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


@dataclass(slots=True)
class _ValidatedAllocation:
    goal_id: str
    account_id: str
    amount: Decimal
    allocation_type: str


@dataclass(slots=True)
class _ValidatedGoalRequest:
    period_month: str
    period_start: date
    period_end: date
    available_funds: Decimal
    actor: str
    reason: str
    allocations: list[_ValidatedAllocation]


def _quantize_money(value: Decimal) -> Decimal:
    """
    Quantize a monetary Decimal to two decimal places using HALF_UP rounding.
    
    Returns:
        Decimal: The value rounded to two decimal places with ROUND_HALF_UP.
    """
    return value.quantize(_DECIMAL_2, rounding=ROUND_HALF_UP)


def _parse_non_empty(value: object, *, field_name: str) -> str:
    """
    Validate that `value` is a non-empty string and return it trimmed.
    
    Parameters:
        value (object): The value to validate and trim.
        field_name (str): Field name used in the raised error message.
    
    Returns:
        str: The trimmed string.
    
    Raises:
        ValueError: If `value` is not a string or is empty after trimming.
    """
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _parse_decimal(value: object, *, field_name: str, non_negative: bool = False) -> Decimal:
    """
    Parse and validate a numeric value into a Decimal quantized to two decimal places.
    
    Parameters:
        value (object): The input value to convert to Decimal.
        field_name (str): Name used in error messages when validation fails.
        non_negative (bool): If True, require the resulting Decimal to be greater than or equal to zero.
    
    Returns:
        Decimal: The parsed and quantized Decimal (two decimal places, HALF_UP rounding).
    
    Raises:
        ValueError: If the value is not decimal-compatible, is not finite, or is negative when `non_negative` is True.
    """
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
    """
    Compute the start and end dates for the month specified by `period_month`.
    
    Parameters:
        period_month (str): Month identifier in "YYYY-MM" format.
    
    Returns:
        tuple[date, date]: (period_start, period_end) where `period_start` is the first day of the month and `period_end` is the last day of the month.
    
    Raises:
        ValueError: If `period_month` is not in the "YYYY-MM" format or does not represent a valid month.
    """
    if not _PERIOD_MONTH_RE.fullmatch(period_month):
        raise ValueError("period_month must be in YYYY-MM format")
    try:
        start = date.fromisoformat(f"{period_month}-01")
    except ValueError as exc:
        raise ValueError("period_month must be in YYYY-MM format") from exc
    end_day = monthrange(start.year, start.month)[1]
    return start, date(start.year, start.month, end_day)


def _add_months(value: date, *, months: int) -> date:
    """
    Add a number of months to a date, adjusting the day to the last valid day of the resulting month when necessary.
    
    Parameters:
        value (date): The starting date.
        months (int): Number of months to add (may be negative to subtract months).
    
    Returns:
        date: The resulting date with the month offset applied and day clipped to the month's length if needed.
    """
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def _validate_request(request: GoalLedgerComputeRequest) -> _ValidatedGoalRequest:
    """
    Validate a GoalLedgerComputeRequest and produce a normalized _ValidatedGoalRequest.
    
    Performs validation and normalization of the request's period, available funds, actor, reason, and each allocation. Ensures:
    - period_month is a valid YYYY-MM and computes period_start and period_end,
    - available_funds and each allocation.amount are finite, quantized to two decimals, and non-negative,
    - actor, reason, allocation.goal_id, allocation.account_id, and allocation.allocation_type are non-empty strings,
    - no duplicate allocation keys exist (duplicate defined by the tuple (goal_id, account_id, allocation_type)),
    - the sum of all allocation amounts does not exceed available_funds.
    
    Parameters:
        request (GoalLedgerComputeRequest): The input request to validate and normalize.
    
    Returns:
        _ValidatedGoalRequest: Dataclass containing period_month, period_start, period_end, parsed available_funds, actor, reason, and a list of validated allocations.
    
    Raises:
        ValueError: If any validation fails (invalid period format, empty required strings, non-finite or negative numeric values, duplicate allocations, or total allocations exceeding available funds).
    """
    period_start, period_end = _period_bounds(request.period_month)
    available_funds = _parse_decimal(request.available_funds, field_name="available_funds", non_negative=True)
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")

    allocations: list[_ValidatedAllocation] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for index, allocation in enumerate(request.allocations):
        goal_id = _parse_non_empty(
            allocation.goal_id,
            field_name=f"allocations[{index}].goal_id",
        )
        account_id = _parse_non_empty(
            allocation.account_id,
            field_name=f"allocations[{index}].account_id",
        )
        allocation_type = _parse_non_empty(
            allocation.allocation_type,
            field_name=f"allocations[{index}].allocation_type",
        )
        key = (goal_id, account_id, allocation_type)
        if key in seen_keys:
            raise ValueError(
                "Duplicate allocation key in allocations: "
                f"goal_id={goal_id}, account_id={account_id}, allocation_type={allocation_type}"
            )
        seen_keys.add(key)
        allocations.append(
            _ValidatedAllocation(
                goal_id=goal_id,
                account_id=account_id,
                amount=_parse_decimal(
                    allocation.amount,
                    field_name=f"allocations[{index}].amount",
                    non_negative=True,
                ),
                allocation_type=allocation_type,
            )
        )

    allocation_total = _quantize_money(sum((item.amount for item in allocations), Decimal("0.00")))
    if allocation_total > available_funds:
        raise ValueError(
            "Goal allocations exceed available funds: "
            f"allocated={allocation_total}, available_funds={available_funds}"
        )

    return _ValidatedGoalRequest(
        period_month=request.period_month,
        period_start=period_start,
        period_end=period_end,
        available_funds=available_funds,
        actor=actor,
        reason=reason,
        allocations=allocations,
    )


def _resolve_goals_for_allocations(*, allocations: list[_ValidatedAllocation], session: Session) -> None:
    """
    Ensure all allocations reference existing, active goals.
    
    Parameters:
        allocations (list[_ValidatedAllocation]): Validated allocation objects whose `goal_id` values must exist and be active.
        session (Session): Database session used to load Goal records.
    
    Raises:
        ValueError: If any referenced `goal_id` does not exist (message lists missing ids).
        ValueError: If any referenced goal exists but is not active (message lists inactive ids).
    """
    if not allocations:
        return
    goal_ids = sorted({item.goal_id for item in allocations})
    goals = session.scalars(select(Goal).where(Goal.id.in_(goal_ids))).all()
    goals_by_id = {goal.id: goal for goal in goals}

    missing_goal_ids = sorted(goal_id for goal_id in goal_ids if goal_id not in goals_by_id)
    if missing_goal_ids:
        raise ValueError("Unknown goal_id in allocations: " + ", ".join(missing_goal_ids))

    inactive_goal_ids = sorted(goal.id for goal in goals if goal.status != "active")
    if inactive_goal_ids:
        raise ValueError("allocations require active goals; inactive goal_id(s): " + ", ".join(inactive_goal_ids))


def _upsert_period_allocations(
    *,
    period_month: str,
    allocations: list[_ValidatedAllocation],
    event_time: datetime,
    session: Session,
) -> None:
    """
    Insert or update the provided goal allocations for a specific period into the database.
    
    Parameters:
        period_month (str): Period identifier in YYYY-MM format for which allocations apply.
        allocations (list[_ValidatedAllocation]): Validated allocation records to insert or update.
        event_time (datetime): Timestamp to record as the allocations' creation time.
    """
    if not allocations:
        return

    for allocation in allocations:
        stmt = sqlite_insert(GoalAllocation).values(
            id=str(uuid4()),
            goal_id=allocation.goal_id,
            account_id=allocation.account_id,
            period_month=period_month,
            amount=allocation.amount,
            allocation_type=allocation.allocation_type,
            created_at=event_time,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                GoalAllocation.period_month,
                GoalAllocation.goal_id,
                GoalAllocation.account_id,
                GoalAllocation.allocation_type,
            ],
            set_={
                "amount": allocation.amount,
            },
        )
        session.execute(stmt)


def _goal_allocations_sum_by_goal(
    *,
    goal_ids: list[str],
    through_period_month: str,
    session: Session,
) -> dict[str, Decimal]:
    """
    Compute the total allocated amount per goal up to and including the specified period month.
    
    Parameters:
        goal_ids (list[str]): Goal IDs to aggregate.
        through_period_month (str): Period month in `YYYY-MM` format; includes allocations whose period_month is less than or equal to this value.
    
    Returns:
        dict[str, Decimal]: Mapping from goal_id to the total allocated amount (quantized to two decimals and guaranteed >= 0). Goals with no allocations up to the given period are omitted from the mapping.
    """
    if not goal_ids:
        return {}

    rows = session.execute(
        select(
            GoalAllocation.goal_id,
            func.coalesce(func.sum(GoalAllocation.amount), 0),
        )
        .where(
            GoalAllocation.goal_id.in_(goal_ids),
            GoalAllocation.period_month <= through_period_month,
        )
        .group_by(GoalAllocation.goal_id)
    ).all()
    result: dict[str, Decimal] = {}
    for goal_id, total in rows:
        result[goal_id] = _parse_decimal(total, field_name="allocated_total", non_negative=True)
    return result


def _goal_period_allocations_sum_by_goal(
    *,
    goal_ids: list[str],
    period_month: str,
    session: Session,
) -> dict[str, Decimal]:
    """
    Compute the total allocated amount for each goal within a specific period month.
    
    Parameters:
        goal_ids (list[str]): Goal identifiers to include in the aggregation.
        period_month (str): Period in `YYYY-MM` format to filter allocations.
    
    Returns:
        dict[str, Decimal]: Mapping from `goal_id` to the summed allocation `Decimal` for the given period (zero if no allocations).
    """
    if not goal_ids:
        return {}

    rows = session.execute(
        select(
            GoalAllocation.goal_id,
            func.coalesce(func.sum(GoalAllocation.amount), 0),
        )
        .where(
            GoalAllocation.goal_id.in_(goal_ids),
            GoalAllocation.period_month == period_month,
        )
        .group_by(GoalAllocation.goal_id)
    ).all()
    result: dict[str, Decimal] = {}
    for goal_id, total in rows:
        result[goal_id] = _parse_decimal(total, field_name="period_allocation_total", non_negative=True)
    return result


def _goal_spending_sum_by_goal(
    *,
    goal_ids: list[str],
    through_date: date,
    session: Session,
) -> dict[str, Decimal]:
    """
    Compute total spending (outflows) per goal up to and including a given date.
    
    Parameters:
        goal_ids (list[str]): Goal identifiers to include in the aggregation.
        through_date (date): Inclusive upper bound for event dates to consider.
    
    Returns:
        dict[str, Decimal]: Mapping from goal_id to the sum of outflow amounts (negative amounts treated as positive), zero when there are no spending events.
    """
    if not goal_ids:
        return {}

    event_amount = case(
        (Transaction.id.is_not(None), Transaction.amount),
        else_=GoalEvent.amount,
    )
    outflow_amount = case(
        (event_amount < 0, -event_amount),
        else_=literal(Decimal("0.00")),
    )
    rows = session.execute(
        select(
            GoalEvent.goal_id,
            func.coalesce(func.sum(outflow_amount), 0),
        )
        .outerjoin(Transaction, Transaction.id == GoalEvent.related_transaction_id)
        .where(
            GoalEvent.goal_id.in_(goal_ids),
            GoalEvent.event_date <= through_date,
        )
        .group_by(GoalEvent.goal_id)
    ).all()

    result: dict[str, Decimal] = {}
    for goal_id, total in rows:
        result[goal_id] = _parse_decimal(total, field_name="spending_total", non_negative=True)
    return result


def _months_required(*, remaining: Decimal, pace: Decimal) -> int:
    """
    Compute the number of whole months required to cover a remaining monetary amount at a given monthly pace.
    
    Parameters:
        remaining (Decimal): Remaining amount to cover; treated as zero or positive.
        pace (Decimal): Monthly contribution amount; must be greater than zero.
    
    Returns:
        int: Number of months required (ceiling of remaining / pace). Returns 0 if `remaining` is less than or equal to 0.
    
    Raises:
        ValueError: If `pace` is less than or equal to 0.
    """
    if remaining <= 0:
        return 0
    if pace <= 0:
        raise ValueError("pace must be > 0")
    return int((remaining / pace).to_integral_value(rounding=ROUND_CEILING))


def goal_ledger_compute(
    request: GoalLedgerComputeRequest,
    session: Session,
) -> GoalLedgerComputeResult:
    """
    Compute and persist goal allocations and progress snapshots for a specified period.
    
    Validates the request, upserts the period allocations into storage, computes per-active-goal progress
    snapshots (allocated, spent, progress, remaining, projected completion and status), and returns a
    summary result that includes any warning causes produced during computation.
    
    Parameters:
        request (GoalLedgerComputeRequest): Request containing period_month, available funds, actor,
            reason, and allocations. The request will be validated before any persistence.
    
    Returns:
        GoalLedgerComputeResult: Result containing the period_month, available_funds,
        allocated_this_period_total, a list of GoalProgressSnapshot entries for each active goal,
        and any GoalLedgerCause items (warnings) produced during processing.
    
    Raises:
        ValueError: If the request is invalid (e.g., malformed period, duplicate or negative allocations),
            or if allocations reference unknown or inactive goals.
    """

    validated = _validate_request(request)
    _resolve_goals_for_allocations(allocations=validated.allocations, session=session)

    run_time = utcnow()
    _upsert_period_allocations(
        period_month=validated.period_month,
        allocations=validated.allocations,
        event_time=run_time,
        session=session,
    )
    session.flush()

    active_goals = session.scalars(
        select(Goal).where(Goal.status == "active").order_by(Goal.id.asc())
    ).all()
    if not active_goals:
        return GoalLedgerComputeResult(
            period_month=validated.period_month,
            available_funds=validated.available_funds,
            allocated_this_period_total=_quantize_money(
                sum((item.amount for item in validated.allocations), Decimal("0.00"))
            ),
            goals=[],
            causes=[
                GoalLedgerCause(
                    code="no_active_goals",
                    message="No active goals were found for goal ledger computation",
                    severity="warning",
                )
            ],
        )

    goal_ids = [goal.id for goal in active_goals]
    allocated_total_by_goal = _goal_allocations_sum_by_goal(
        goal_ids=goal_ids,
        through_period_month=validated.period_month,
        session=session,
    )
    period_allocated_total_by_goal = _goal_period_allocations_sum_by_goal(
        goal_ids=goal_ids,
        period_month=validated.period_month,
        session=session,
    )
    spending_total_by_goal = _goal_spending_sum_by_goal(
        goal_ids=goal_ids,
        through_date=validated.period_end,
        session=session,
    )

    causes: list[GoalLedgerCause] = []
    snapshots: list[GoalProgressSnapshot] = []
    for goal in active_goals:
        target_amount = _parse_decimal(goal.target_amount, field_name=f"goals[{goal.id}].target_amount", non_negative=True)
        monthly_contribution = (
            _parse_decimal(
                goal.monthly_contribution,
                field_name=f"goals[{goal.id}].monthly_contribution",
                non_negative=True,
            )
            if goal.monthly_contribution is not None
            else None
        )
        allocated_total = allocated_total_by_goal.get(goal.id, Decimal("0.00"))
        spending_total = (
            spending_total_by_goal.get(goal.id, Decimal("0.00")) if goal.spending_reduces_progress else Decimal("0.00")
        )
        progress_amount = _quantize_money(max(allocated_total - spending_total, Decimal("0.00")))
        remaining_amount = _quantize_money(max(target_amount - progress_amount, Decimal("0.00")))

        pace: Decimal | None = None
        if monthly_contribution is not None and monthly_contribution > 0:
            pace = monthly_contribution
        else:
            period_allocated = period_allocated_total_by_goal.get(goal.id, Decimal("0.00"))
            if period_allocated > 0:
                pace = period_allocated

        projected_completion_date: date | None = None
        months_to_completion: int | None = None
        computed_status = "active"

        if remaining_amount == 0:
            projected_completion_date = validated.period_end
            months_to_completion = 0
            computed_status = "completed"
        elif pace is not None and pace > 0:
            months_to_completion = _months_required(remaining=remaining_amount, pace=pace)
            projected_completion_date = _add_months(
                validated.period_start,
                months=max(months_to_completion - 1, 0),
            )
            computed_status = "on_track"
            if goal.target_date is not None and projected_completion_date > goal.target_date:
                computed_status = "at_risk"
                causes.append(
                    GoalLedgerCause(
                        code="goal_projection_after_target",
                        message=(
                            f"Goal {goal.id} projects completion on {projected_completion_date} "
                            f"after target date {goal.target_date}"
                        ),
                        severity="warning",
                    )
                )
        else:
            computed_status = "unfunded"

        snapshots.append(
            GoalProgressSnapshot(
                goal_id=goal.id,
                name=goal.name,
                status=computed_status,
                target_amount=target_amount,
                target_date=goal.target_date,
                spending_reduces_progress=goal.spending_reduces_progress,
                monthly_contribution=monthly_contribution,
                allocated_total=allocated_total,
                spending_total=spending_total,
                progress_amount=progress_amount,
                remaining_amount=remaining_amount,
                projected_completion_date=projected_completion_date,
                months_to_completion=months_to_completion,
            )
        )

    allocated_this_period_total = _quantize_money(sum((item.amount for item in validated.allocations), Decimal("0.00")))

    return GoalLedgerComputeResult(
        period_month=validated.period_month,
        available_funds=validated.available_funds,
        allocated_this_period_total=allocated_this_period_total,
        goals=snapshots,
        causes=causes,
    )
