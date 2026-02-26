"""Typed contracts for budgeting workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass(slots=True)
class BudgetCategoryAllocationInput:
    budget_category_id: str
    assigned_amount: Decimal | str
    source: str = "manual"


@dataclass(slots=True)
class BudgetTargetPolicyInput:
    budget_category_id: str
    target_type: str = "scheduled"
    amount: Decimal | str | None = None
    cadence: str | None = None
    top_up: bool | None = None
    snoozed_until: date | None = None
    metadata_json: dict[str, object] | None = None


@dataclass(slots=True)
class BudgetRunCause:
    code: str
    message: str
    severity: str


@dataclass(slots=True)
class BudgetCategorySnapshot:
    budget_category_id: str
    category_id: str
    assigned_amount: Decimal
    spent_amount: Decimal
    available_before_assignment: Decimal
    target_required: Decimal
    underfunded: Decimal
    overspent: Decimal
    target_id: str | None = None
    target_type: str | None = None
    target_cadence: str | None = None
    target_top_up: bool | None = None
    snoozed: bool = False


@dataclass(slots=True)
class BudgetComputeZeroBasedRequest:
    budget_id: str
    period_month: str
    available_cash: Decimal | str
    actor: str
    reason: str
    status: str = "open"
    category_allocations: list[BudgetCategoryAllocationInput] = field(default_factory=list)
    target_policies: list[BudgetTargetPolicyInput] = field(default_factory=list)


@dataclass(slots=True)
class BudgetComputeZeroBasedResult:
    budget_period_id: str
    budget_id: str
    period_month: str
    status: str
    available_cash: Decimal
    carry_in_overspent: Decimal
    assigned_total: Decimal
    spent_total: Decimal
    rollover_total: Decimal
    to_assign: Decimal
    underfunded_total: Decimal
    overspent_total: Decimal
    categories: list[BudgetCategorySnapshot] = field(default_factory=list)
    causes: list[BudgetRunCause] = field(default_factory=list)


@dataclass(slots=True)
class BudgetBucketPlanInput:
    bucket_key: str
    planned_amount: Decimal | str
    rollover_policy: str | None = None


@dataclass(slots=True)
class BudgetCategoryPlanInput:
    budget_category_id: str
    bucket_key: str
    planned_amount: Decimal | str
    rollover_policy: str | None = None


@dataclass(slots=True)
class BudgetBucketSnapshot:
    bucket_definition_id: str
    bucket_key: str
    bucket_name: str
    planned_amount: Decimal
    actual_amount: Decimal
    rollover_policy: str
    rollover_carry: Decimal
    category_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class BudgetCategoryRolloverSnapshot:
    budget_category_id: str
    category_id: str
    bucket_key: str
    planned_amount: Decimal
    actual_amount: Decimal
    rollover_policy: str
    rollover_carry: Decimal


@dataclass(slots=True)
class BudgetComputeFlexRequest:
    budget_id: str
    period_month: str
    available_cash: Decimal | str
    actor: str
    reason: str
    status: str = "open"
    bucket_plans: list[BudgetBucketPlanInput] = field(default_factory=list)
    category_plans: list[BudgetCategoryPlanInput] = field(default_factory=list)


@dataclass(slots=True)
class BudgetComputeFlexResult:
    budget_period_id: str
    budget_id: str
    period_month: str
    status: str
    available_cash: Decimal
    fixed_planned: Decimal
    non_monthly_planned: Decimal
    flex_planned: Decimal
    assigned_total: Decimal
    spent_total: Decimal
    rollover_total: Decimal
    flex_available: Decimal
    buckets: list[BudgetBucketSnapshot] = field(default_factory=list)
    categories: list[BudgetCategoryRolloverSnapshot] = field(default_factory=list)
    causes: list[BudgetRunCause] = field(default_factory=list)
