"""Service-layer reconciliation workflows with trust scoring and explicit adjustments."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, aliased

from finance_analysis_agent.db.models import (
    Account,
    BalanceSnapshot,
    DedupeCandidate,
    Reconciliation,
    ReviewItem,
    Statement,
    Transaction,
    TransactionEvent,
)
from finance_analysis_agent.reconcile.types import (
    AccountReconcileRequest,
    AccountReconcileResult,
    ApproveReconciliationAdjustmentRequest,
    ReconciliationAdjustmentProposal,
    ReconciliationAdjustmentResult,
    ReconciliationRunCause,
    ReconciliationThresholds,
    ReconciliationTrustWeights,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus
from finance_analysis_agent.utils.time import utcnow

_ACTIVE_REVIEW_STATUSES = (
    ReviewItemStatus.TO_REVIEW.value,
    ReviewItemStatus.IN_PROGRESS.value,
)
_POSTED_STATUS = "posted"
_RECONCILIATION_STATUS_PASS = "pass"
_RECONCILIATION_STATUS_FAIL = "fail"
_STATEMENT_BALANCE_SOURCE = "statement"
_RECONCILIATION_BALANCE_SOURCE = "reconciliation"
_ADJUSTMENT_SOURCE_KIND = "reconciliation_adjustment"
_ADJUSTMENT_EVENT_TYPE = "transaction.reconciliation_adjustment.created"
_ADJUSTMENT_PROVENANCE = "manual"


@dataclass(slots=True)
class _ValidatedThresholds:
    delta_tolerance: Decimal
    pass_threshold: float


@dataclass(slots=True)
class _ValidatedWeights:
    match_rate_weight: float
    unresolved_weight: float
    adjustment_weight: float


def _parse_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _parse_non_negative_decimal(value: object, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{field_name} must be a decimal-compatible value") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be a decimal-compatible value")
    if parsed < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return parsed


def _parse_thresholds(thresholds: ReconciliationThresholds) -> _ValidatedThresholds:
    delta_tolerance = _parse_non_negative_decimal(
        thresholds.delta_tolerance,
        field_name="delta_tolerance",
    )
    pass_threshold = float(thresholds.pass_threshold)
    if not math.isfinite(pass_threshold) or pass_threshold < 0 or pass_threshold > 1:
        raise ValueError("pass_threshold must be between 0 and 1")
    return _ValidatedThresholds(
        delta_tolerance=delta_tolerance,
        pass_threshold=pass_threshold,
    )


def _parse_weights(weights: ReconciliationTrustWeights) -> _ValidatedWeights:
    match_rate_weight = float(weights.match_rate_weight)
    unresolved_weight = float(weights.unresolved_weight)
    adjustment_weight = float(weights.adjustment_weight)
    parsed = (match_rate_weight, unresolved_weight, adjustment_weight)
    if any((not math.isfinite(value)) or value < 0 for value in parsed):
        raise ValueError("trust weights must be finite and >= 0")
    total = sum(parsed)
    if total <= 0:
        raise ValueError("trust weights must sum to > 0")
    return _ValidatedWeights(
        match_rate_weight=match_rate_weight / total,
        unresolved_weight=unresolved_weight / total,
        adjustment_weight=adjustment_weight / total,
    )


def _clamp01(value: float) -> float:
    if value <= 0:
        return 0.0
    if value >= 1:
        return 1.0
    return value


def _txn_scope_clause(
    transaction_model: type[Transaction],
    *,
    account_id: str,
    period_start: date,
    period_end: date,
) -> object:
    return and_(
        transaction_model.account_id == account_id,
        transaction_model.posted_date >= period_start,
        transaction_model.posted_date <= period_end,
        transaction_model.pending_status == _POSTED_STATUS,
        transaction_model.excluded.is_(False),
    )


def _resolve_statement(
    request: AccountReconcileRequest,
    *,
    session: Session,
) -> Statement:
    if request.statement_id is not None:
        statement = session.get(Statement, request.statement_id.strip())
        if statement is None:
            raise ValueError(f"Statement not found: {request.statement_id}")
    else:
        statement = session.scalar(
            select(Statement)
            .where(
                Statement.account_id == request.account_id,
                Statement.period_start == request.period_start,
                Statement.period_end == request.period_end,
            )
            .order_by(Statement.created_at.desc(), Statement.id.desc())
            .limit(1)
        )
        if statement is None:
            raise ValueError(
                "Statement not found for account and period "
                f"{request.account_id}: {request.period_start}..{request.period_end}"
            )

    if statement.account_id != request.account_id:
        raise ValueError("statement_id does not belong to account_id")
    if statement.period_start != request.period_start or statement.period_end != request.period_end:
        raise ValueError("statement period does not match reconcile request period")
    if statement.ending_balance is None:
        raise ValueError("statement ending_balance is required for reconciliation")
    return statement


def _resolve_opening_balance(
    *,
    account_id: str,
    period_start: date,
    session: Session,
) -> Decimal:
    snapshot = session.scalar(
        select(BalanceSnapshot)
        .where(
            BalanceSnapshot.account_id == account_id,
            BalanceSnapshot.snapshot_date <= period_start,
        )
        .order_by(BalanceSnapshot.snapshot_date.desc(), BalanceSnapshot.created_at.desc())
        .limit(1)
    )
    if snapshot is None:
        raise ValueError(
            "Opening balance snapshot is required at or before "
            f"{period_start} for account {account_id}"
        )
    return Decimal(snapshot.balance)


def _upsert_balance_snapshot(
    *,
    account_id: str,
    snapshot_date: date,
    balance: Decimal,
    source: str,
    statement_id: str | None,
    event_time,
    session: Session,
) -> None:
    existing = session.scalar(
        select(BalanceSnapshot).where(
            BalanceSnapshot.account_id == account_id,
            BalanceSnapshot.snapshot_date == snapshot_date,
            BalanceSnapshot.source == source,
        )
    )
    if existing is not None:
        existing.balance = balance
        existing.statement_id = statement_id
        return

    session.add(
        BalanceSnapshot(
            id=str(uuid4()),
            account_id=account_id,
            snapshot_date=snapshot_date,
            balance=balance,
            source=source,
            statement_id=statement_id,
            created_at=event_time,
        )
    )


def _count_unresolved_review_items(
    *,
    account_id: str,
    period_start: date,
    period_end: date,
    session: Session,
) -> tuple[int, int]:
    scope_ids = select(Transaction.id).where(
        _txn_scope_clause(
            Transaction,
            account_id=account_id,
            period_start=period_start,
            period_end=period_end,
        )
    )

    transaction_item_count = int(
        session.scalar(
            select(func.count())
            .select_from(ReviewItem)
            .where(
                ReviewItem.status.in_(_ACTIVE_REVIEW_STATUSES),
                ReviewItem.ref_table == "transactions",
                ReviewItem.ref_id.in_(scope_ids),
            )
        )
        or 0
    )

    txn_a = aliased(Transaction)
    txn_b = aliased(Transaction)
    dedupe_item_count = int(
        session.scalar(
            select(func.count(func.distinct(ReviewItem.id)))
            .select_from(ReviewItem)
            .join(DedupeCandidate, ReviewItem.ref_id == DedupeCandidate.id)
            .join(txn_a, DedupeCandidate.txn_a_id == txn_a.id)
            .join(txn_b, DedupeCandidate.txn_b_id == txn_b.id)
            .where(
                ReviewItem.status.in_(_ACTIVE_REVIEW_STATUSES),
                ReviewItem.ref_table == "dedupe_candidates",
                or_(
                    _txn_scope_clause(
                        txn_a,
                        account_id=account_id,
                        period_start=period_start,
                        period_end=period_end,
                    ),
                    _txn_scope_clause(
                        txn_b,
                        account_id=account_id,
                        period_start=period_start,
                        period_end=period_end,
                    ),
                ),
            )
        )
        or 0
    )

    return transaction_item_count, dedupe_item_count


def _build_causes_and_actions(
    *,
    delta: Decimal,
    delta_tolerance: Decimal,
    unresolved_count: int,
    trust_score: float,
    pass_threshold: float,
) -> tuple[list[ReconciliationRunCause], list[str]]:
    causes: list[ReconciliationRunCause] = []
    actions: list[str] = []

    if abs(delta) > delta_tolerance:
        causes.append(
            ReconciliationRunCause(
                code="balance_delta_exceeds_tolerance",
                message=(
                    f"Balance delta {format(delta, 'f')} exceeds tolerance "
                    f"{format(delta_tolerance, 'f')}"
                ),
                severity="high",
            )
        )
        actions.append(
            "Review in-period transactions and statement balances; approve adjustment only after validation."
        )

    if unresolved_count > 0:
        causes.append(
            ReconciliationRunCause(
                code="open_review_items",
                message=f"{unresolved_count} unresolved review item(s) in reconciliation scope",
                severity="medium",
            )
        )
        actions.append("Resolve review queue items in scope and rerun reconciliation.")

    if trust_score < pass_threshold:
        causes.append(
            ReconciliationRunCause(
                code="trust_below_threshold",
                message=f"Trust score {trust_score:.4f} is below threshold {pass_threshold:.4f}",
                severity="medium",
            )
        )
        actions.append("Investigate trust-score components and rerun reconciliation after remediation.")

    if not actions:
        actions.append("No action required.")
    return causes, actions


def account_reconcile(
    request: AccountReconcileRequest,
    session: Session,
) -> AccountReconcileResult:
    """Run reconciliation for an account-period and persist trust-scored checkpoint results."""

    account_id = _parse_non_empty(request.account_id, field_name="account_id")
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    if request.period_end < request.period_start:
        raise ValueError("period_end must be >= period_start")

    account = session.get(Account, account_id)
    if account is None:
        raise ValueError(f"Account not found: {account_id}")

    thresholds = _parse_thresholds(request.thresholds)
    weights = _parse_weights(request.weights)

    statement = _resolve_statement(
        AccountReconcileRequest(
            account_id=account_id,
            period_start=request.period_start,
            period_end=request.period_end,
            actor=actor,
            reason=reason,
            statement_id=request.statement_id,
            thresholds=request.thresholds,
            weights=request.weights,
        ),
        session=session,
    )
    expected_balance = Decimal(statement.ending_balance)
    opening_balance = _resolve_opening_balance(
        account_id=account_id,
        period_start=request.period_start,
        session=session,
    )

    transaction_scope = _txn_scope_clause(
        Transaction,
        account_id=account_id,
        period_start=request.period_start,
        period_end=request.period_end,
    )
    ledger_movement = Decimal(
        session.scalar(select(func.coalesce(func.sum(Transaction.amount), 0)).where(transaction_scope)) or 0
    )
    transaction_count = int(
        session.scalar(select(func.count()).select_from(Transaction).where(transaction_scope)) or 0
    )
    computed_balance = opening_balance + ledger_movement
    delta = expected_balance - computed_balance

    delta_abs = abs(delta)
    expected_abs = max(abs(expected_balance), Decimal("1"))
    if delta_abs <= thresholds.delta_tolerance:
        match_rate = 1.0
    else:
        match_rate = _clamp01(1.0 - float(delta_abs / expected_abs))

    transaction_unresolved_count, dedupe_unresolved_count = _count_unresolved_review_items(
        account_id=account_id,
        period_start=request.period_start,
        period_end=request.period_end,
        session=session,
    )
    unresolved_count = transaction_unresolved_count + dedupe_unresolved_count
    unresolved_ratio = _clamp01(unresolved_count / max(transaction_count, 1))
    adjustment_magnitude = delta_abs
    adjustment_ratio = _clamp01(float(adjustment_magnitude / expected_abs))

    trust_score = _clamp01(
        (weights.match_rate_weight * match_rate)
        + (weights.unresolved_weight * (1.0 - unresolved_ratio))
        + (weights.adjustment_weight * (1.0 - adjustment_ratio))
    )

    status = _RECONCILIATION_STATUS_PASS
    if (
        delta_abs > thresholds.delta_tolerance
        or unresolved_count > 0
        or trust_score < thresholds.pass_threshold
    ):
        status = _RECONCILIATION_STATUS_FAIL

    causes, next_actions = _build_causes_and_actions(
        delta=delta,
        delta_tolerance=thresholds.delta_tolerance,
        unresolved_count=unresolved_count,
        trust_score=trust_score,
        pass_threshold=thresholds.pass_threshold,
    )

    adjustment_proposal: ReconciliationAdjustmentProposal | None = None
    if adjustment_magnitude > 0:
        adjustment_proposal = ReconciliationAdjustmentProposal(
            amount=delta,
            currency=account.currency,
            rationale="Reconcile statement ending balance delta for period",
        )

    details_json = {
        "actor": actor,
        "reason": reason,
        "statement_id": statement.id,
        "thresholds": {
            "delta_tolerance": format(thresholds.delta_tolerance, "f"),
            "pass_threshold": thresholds.pass_threshold,
        },
        "weights": {
            "match_rate_weight": weights.match_rate_weight,
            "unresolved_weight": weights.unresolved_weight,
            "adjustment_weight": weights.adjustment_weight,
        },
        "components": {
            "transaction_count": transaction_count,
            "match_rate": match_rate,
            "unresolved_ratio": unresolved_ratio,
            "adjustment_ratio": adjustment_ratio,
            "unresolved_breakdown": {
                "transaction_review_items": transaction_unresolved_count,
                "dedupe_review_items": dedupe_unresolved_count,
            },
        },
        "causes": [
            {"code": cause.code, "message": cause.message, "severity": cause.severity} for cause in causes
        ],
        "next_actions": next_actions,
    }

    event_time = utcnow()
    reconciliation = Reconciliation(
        id=str(uuid4()),
        account_id=account_id,
        statement_id=statement.id,
        period_start=request.period_start,
        period_end=request.period_end,
        expected_balance=expected_balance,
        computed_balance=computed_balance,
        delta=delta,
        match_rate=match_rate,
        trust_score=trust_score,
        unresolved_count=unresolved_count,
        adjustment_magnitude=adjustment_magnitude,
        details_json=details_json,
        approved_adjustment_txn_id=None,
        approved_by=None,
        approved_at=None,
        status=status,
        created_at=event_time,
    )
    session.add(reconciliation)

    _upsert_balance_snapshot(
        account_id=account_id,
        snapshot_date=request.period_end,
        balance=expected_balance,
        source=_STATEMENT_BALANCE_SOURCE,
        statement_id=statement.id,
        event_time=event_time,
        session=session,
    )
    _upsert_balance_snapshot(
        account_id=account_id,
        snapshot_date=request.period_end,
        balance=computed_balance,
        source=_RECONCILIATION_BALANCE_SOURCE,
        statement_id=statement.id,
        event_time=event_time,
        session=session,
    )
    session.flush()

    return AccountReconcileResult(
        reconciliation_id=reconciliation.id,
        account_id=account_id,
        statement_id=statement.id,
        period_start=request.period_start,
        period_end=request.period_end,
        expected_balance=expected_balance,
        computed_balance=computed_balance,
        delta=delta,
        match_rate=match_rate,
        trust_score=trust_score,
        status=status,
        unresolved_count=unresolved_count,
        adjustment_magnitude=adjustment_magnitude,
        causes=causes,
        next_actions=next_actions,
        adjustment_proposal=adjustment_proposal,
    )


def approve_reconciliation_adjustment(
    request: ApproveReconciliationAdjustmentRequest,
    session: Session,
) -> ReconciliationAdjustmentResult:
    """Create an explicit reconciliation adjustment transaction for a reconciliation run."""

    reconciliation_id = _parse_non_empty(request.reconciliation_id, field_name="reconciliation_id")
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")
    delta_tolerance = _parse_non_negative_decimal(
        request.delta_tolerance,
        field_name="delta_tolerance",
    )

    reconciliation = session.get(Reconciliation, reconciliation_id)
    if reconciliation is None:
        raise ValueError(f"Reconciliation not found: {reconciliation_id}")
    if reconciliation.approved_adjustment_txn_id is not None:
        raise ValueError("Reconciliation adjustment already approved")
    if abs(Decimal(reconciliation.delta)) <= delta_tolerance:
        raise ValueError("Reconciliation delta is within tolerance; adjustment is not required")

    account = session.get(Account, reconciliation.account_id)
    if account is None:
        raise ValueError(f"Account not found: {reconciliation.account_id}")

    event_time = utcnow()
    transaction_id = str(uuid4())
    session.add(
        Transaction(
            id=transaction_id,
            account_id=account.id,
            posted_date=reconciliation.period_end,
            effective_date=reconciliation.period_end,
            amount=Decimal(reconciliation.delta),
            currency=account.currency,
            original_amount=Decimal(reconciliation.delta),
            original_currency=account.currency,
            pending_status=_POSTED_STATUS,
            original_statement="Reconciliation adjustment",
            merchant_id=None,
            category_id=None,
            excluded=False,
            notes=f"Reconciliation adjustment for {reconciliation.id}: {reason}",
            source_kind=_ADJUSTMENT_SOURCE_KIND,
            source_transaction_id=f"recon:{reconciliation.id}",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=event_time,
            updated_at=event_time,
        )
    )
    session.add(
        TransactionEvent(
            id=str(uuid4()),
            transaction_id=transaction_id,
            event_type=_ADJUSTMENT_EVENT_TYPE,
            old_value_json=None,
            new_value_json={
                "reconciliation_id": reconciliation.id,
                "amount": format(Decimal(reconciliation.delta), "f"),
                "currency": account.currency,
            },
            reason=reason,
            actor=actor,
            provenance=_ADJUSTMENT_PROVENANCE,
            created_at=event_time,
        )
    )

    details_json = dict(reconciliation.details_json or {})
    details_json["approved_adjustment"] = {
        "transaction_id": transaction_id,
        "approved_by": actor,
        "approved_at": event_time.isoformat(),
        "reason": reason,
    }
    reconciliation.details_json = details_json
    reconciliation.approved_adjustment_txn_id = transaction_id
    reconciliation.approved_by = actor
    reconciliation.approved_at = event_time
    session.flush()

    return ReconciliationAdjustmentResult(
        reconciliation_id=reconciliation.id,
        adjustment_transaction_id=transaction_id,
        approved_by=actor,
        approved_at=event_time,
        status=reconciliation.status,
    )
