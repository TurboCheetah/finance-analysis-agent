"""Typed contracts for goal ledger workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass(slots=True)
class GoalAllocationInput:
    goal_id: str
    account_id: str
    amount: Decimal | str
    allocation_type: str = "manual"


@dataclass(slots=True)
class GoalLedgerCause:
    code: str
    message: str
    severity: str


@dataclass(slots=True)
class GoalProgressSnapshot:
    goal_id: str
    name: str
    status: str
    target_amount: Decimal
    target_date: date | None
    spending_reduces_progress: bool
    monthly_contribution: Decimal | None
    allocated_total: Decimal
    spending_total: Decimal
    progress_amount: Decimal
    remaining_amount: Decimal
    projected_completion_date: date | None
    months_to_completion: int | None


@dataclass(slots=True)
class GoalLedgerComputeRequest:
    period_month: str
    available_funds: Decimal | str
    actor: str
    reason: str
    allocations: list[GoalAllocationInput] = field(default_factory=list)


@dataclass(slots=True)
class GoalLedgerComputeResult:
    period_month: str
    available_funds: Decimal
    allocated_this_period_total: Decimal
    goals: list[GoalProgressSnapshot] = field(default_factory=list)
    causes: list[GoalLedgerCause] = field(default_factory=list)
