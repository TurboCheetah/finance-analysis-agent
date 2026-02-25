"""Typed contracts for transaction dedupe hard/soft matching workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(slots=True)
class TxnDedupeMatchRequest:
    actor: str
    reason: str
    scope_transaction_ids: list[str] = field(default_factory=list)
    include_pending: bool = False
    hard_date_window_days: int = 3
    soft_candidate_window_days: int = 7
    soft_review_threshold: float = 0.75
    soft_autolink_threshold: float = 1.0
    pending_posted_window_days: int = 5
    pending_amount_tolerance_pct: float = 0.01
    pending_amount_tolerance_abs: Decimal | str = "1.00"
    cross_source_review_only: bool = True
    limit: int = 1000


@dataclass(slots=True)
class DedupeScoreBreakdown:
    amount_factor: float
    date_factor: float
    merchant_payee_factor: float
    statement_factor: float
    source_kind_factor: float
    total_score: float
    details: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class DedupeCandidateResult:
    dedupe_candidate_id: str
    txn_a_id: str
    txn_b_id: str
    score: float
    classification: str
    decision: str | None
    queued_review_item_id: str | None = None
    score_breakdown: DedupeScoreBreakdown | None = None
    policy_flags: dict[str, bool] = field(default_factory=dict)


@dataclass(slots=True)
class TxnDedupeMatchResult:
    hard_auto_linked: int
    soft_queued: int
    soft_auto_linked: int
    skipped_existing: int
    candidates: list[DedupeCandidateResult] = field(default_factory=list)
