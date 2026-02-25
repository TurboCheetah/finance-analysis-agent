"""Typed contracts for reconciliation checkpoints and adjustment approval workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


@dataclass(slots=True)
class ReconciliationTrustWeights:
    match_rate_weight: float = 0.60
    unresolved_weight: float = 0.25
    adjustment_weight: float = 0.15


@dataclass(slots=True)
class ReconciliationThresholds:
    delta_tolerance: Decimal | str = "0.01"
    pass_threshold: float = 0.90


@dataclass(slots=True)
class ReconciliationRunCause:
    code: str
    message: str
    severity: str


@dataclass(slots=True)
class ReconciliationAdjustmentProposal:
    amount: Decimal
    currency: str
    rationale: str


@dataclass(slots=True)
class AccountReconcileRequest:
    account_id: str
    period_start: date
    period_end: date
    actor: str
    reason: str
    statement_id: str | None = None
    thresholds: ReconciliationThresholds = field(default_factory=ReconciliationThresholds)
    weights: ReconciliationTrustWeights = field(default_factory=ReconciliationTrustWeights)


@dataclass(slots=True)
class AccountReconcileResult:
    reconciliation_id: str
    account_id: str
    statement_id: str
    period_start: date
    period_end: date
    expected_balance: Decimal
    computed_balance: Decimal
    delta: Decimal
    match_rate: float
    trust_score: float
    status: str
    unresolved_count: int
    adjustment_magnitude: Decimal
    causes: list[ReconciliationRunCause] = field(default_factory=list)
    next_actions: list[str] = field(default_factory=list)
    adjustment_proposal: ReconciliationAdjustmentProposal | None = None


@dataclass(slots=True)
class ApproveReconciliationAdjustmentRequest:
    reconciliation_id: str
    actor: str
    reason: str
    delta_tolerance: Decimal | str = "0.01"


@dataclass(slots=True)
class ReconciliationAdjustmentResult:
    reconciliation_id: str
    adjustment_transaction_id: str
    approved_by: str
    approved_at: datetime
    status: str
