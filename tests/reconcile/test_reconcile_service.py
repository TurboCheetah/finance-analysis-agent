from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    Account,
    BalanceSnapshot,
    Reconciliation,
    ReviewItem,
    Statement,
    Transaction,
    TransactionEvent,
)
from finance_analysis_agent.reconcile import (
    AccountReconcileRequest,
    ApproveReconciliationAdjustmentRequest,
    account_reconcile,
    approve_reconciliation_adjustment,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session) -> None:
    session.add(Account(id="acct-1", name="Checking", type="checking", currency="USD"))


def _seed_statement(
    session: Session,
    *,
    statement_id: str,
    period_start: date,
    period_end: date,
    ending_balance: Decimal,
) -> None:
    session.add(
        Statement(
            id=statement_id,
            account_id="acct-1",
            source_type="pdf",
            source_fingerprint=f"fp-{statement_id}",
            period_start=period_start,
            period_end=period_end,
            ending_balance=ending_balance,
            currency="USD",
            status="parsed",
            diagnostics_json={},
            created_at=utcnow(),
        )
    )


def _seed_balance_snapshot(
    session: Session,
    *,
    snapshot_id: str,
    snapshot_date: date,
    balance: Decimal,
    source: str = "statement",
) -> None:
    session.add(
        BalanceSnapshot(
            id=snapshot_id,
            account_id="acct-1",
            snapshot_date=snapshot_date,
            balance=balance,
            source=source,
            statement_id=None,
            created_at=utcnow(),
        )
    )


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    posted_date: date,
    amount: Decimal,
) -> None:
    timestamp = utcnow()
    session.add(
        Transaction(
            id=transaction_id,
            account_id="acct-1",
            posted_date=posted_date,
            effective_date=posted_date,
            amount=amount,
            currency="USD",
            original_amount=amount,
            original_currency="USD",
            pending_status="posted",
            original_statement="seed transaction",
            merchant_id=None,
            category_id=None,
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id=f"src-{transaction_id}",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=timestamp,
            updated_at=timestamp,
        )
    )


def _seed_review_item(
    session: Session,
    *,
    review_item_id: str,
    transaction_id: str,
) -> None:
    session.add(
        ReviewItem(
            id=review_item_id,
            item_type="transaction_review",
            ref_table="transactions",
            ref_id=transaction_id,
            reason_code="reconcile.needs_review",
            confidence=None,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.UNKNOWN.value,
            assigned_to=None,
            payload_json=None,
            created_at=utcnow(),
            resolved_at=None,
        )
    )


def test_account_reconcile_passes_and_persists_checkpoints(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_statement(
        db_session,
        statement_id="stmt-pass",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        ending_balance=Decimal("130.00"),
    )
    _seed_balance_snapshot(
        db_session,
        snapshot_id="snap-open",
        snapshot_date=date(2026, 1, 1),
        balance=Decimal("100.00"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-pass-1",
        posted_date=date(2026, 1, 10),
        amount=Decimal("20.00"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-pass-2",
        posted_date=date(2026, 1, 20),
        amount=Decimal("10.00"),
    )
    db_session.flush()

    result = account_reconcile(
        AccountReconcileRequest(
            account_id="acct-1",
            period_start=date(2026, 1, 1),
            period_end=date(2026, 1, 31),
            actor="reconciler",
            reason="monthly close",
        ),
        db_session,
    )

    reconciliation = db_session.get(Reconciliation, result.reconciliation_id)
    snapshots = db_session.scalars(
        select(BalanceSnapshot)
        .where(
            BalanceSnapshot.account_id == "acct-1",
            BalanceSnapshot.snapshot_date == date(2026, 1, 31),
        )
        .order_by(BalanceSnapshot.source.asc())
    ).all()

    assert result.status == "pass"
    assert result.delta == Decimal("0.00")
    assert result.unresolved_count == 0
    assert reconciliation is not None and reconciliation.status == "pass"
    assert len(snapshots) == 2
    assert {snapshot.source for snapshot in snapshots} == {"statement", "reconciliation"}


def test_account_reconcile_fails_with_unresolved_items_and_delta(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_statement(
        db_session,
        statement_id="stmt-fail",
        period_start=date(2026, 2, 1),
        period_end=date(2026, 2, 28),
        ending_balance=Decimal("150.00"),
    )
    _seed_balance_snapshot(
        db_session,
        snapshot_id="snap-open-fail",
        snapshot_date=date(2026, 2, 1),
        balance=Decimal("100.00"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-fail-1",
        posted_date=date(2026, 2, 5),
        amount=Decimal("20.00"),
    )
    _seed_review_item(db_session, review_item_id="ri-fail-1", transaction_id="txn-fail-1")
    db_session.flush()

    result = account_reconcile(
        AccountReconcileRequest(
            account_id="acct-1",
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            actor="reconciler",
            reason="monthly close",
        ),
        db_session,
    )

    cause_codes = {cause.code for cause in result.causes}
    assert result.status == "fail"
    assert result.delta == Decimal("30.00")
    assert result.unresolved_count == 1
    assert result.trust_score < 0.90
    assert result.adjustment_proposal is not None
    assert result.adjustment_proposal.amount == Decimal("30.00")
    assert "balance_delta_exceeds_tolerance" in cause_codes
    assert "open_review_items" in cause_codes


def test_approve_reconciliation_adjustment_creates_transaction_and_audit(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_statement(
        db_session,
        statement_id="stmt-adjust",
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        ending_balance=Decimal("125.00"),
    )
    _seed_balance_snapshot(
        db_session,
        snapshot_id="snap-open-adjust",
        snapshot_date=date(2026, 3, 1),
        balance=Decimal("100.00"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-adjust-1",
        posted_date=date(2026, 3, 2),
        amount=Decimal("20.00"),
    )
    db_session.flush()

    reconcile_result = account_reconcile(
        AccountReconcileRequest(
            account_id="acct-1",
            period_start=date(2026, 3, 1),
            period_end=date(2026, 3, 31),
            actor="reconciler",
            reason="monthly close",
        ),
        db_session,
    )
    assert reconcile_result.status == "fail"

    approval_result = approve_reconciliation_adjustment(
        ApproveReconciliationAdjustmentRequest(
            reconciliation_id=reconcile_result.reconciliation_id,
            actor="approver",
            reason="manual verified adjustment",
        ),
        db_session,
    )

    adjustment_txn = db_session.get(Transaction, approval_result.adjustment_transaction_id)
    reconciliation = db_session.get(Reconciliation, reconcile_result.reconciliation_id)
    event = db_session.scalar(
        select(TransactionEvent).where(TransactionEvent.transaction_id == approval_result.adjustment_transaction_id)
    )

    assert adjustment_txn is not None
    assert adjustment_txn.source_kind == "reconciliation_adjustment"
    assert adjustment_txn.source_transaction_id == f"recon:{reconcile_result.reconciliation_id}"
    assert adjustment_txn.amount == Decimal("5.00")
    assert event is not None and event.event_type == "transaction.reconciliation_adjustment.created"
    assert reconciliation is not None
    assert reconciliation.approved_adjustment_txn_id == adjustment_txn.id
    assert reconciliation.approved_by == "approver"
    assert reconciliation.approved_at is not None


def test_approve_reconciliation_adjustment_rejects_second_approval(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_statement(
        db_session,
        statement_id="stmt-adjust-repeat",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 30),
        ending_balance=Decimal("120.00"),
    )
    _seed_balance_snapshot(
        db_session,
        snapshot_id="snap-open-adjust-repeat",
        snapshot_date=date(2026, 4, 1),
        balance=Decimal("100.00"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-adjust-repeat-1",
        posted_date=date(2026, 4, 5),
        amount=Decimal("15.00"),
    )
    db_session.flush()

    reconcile_result = account_reconcile(
        AccountReconcileRequest(
            account_id="acct-1",
            period_start=date(2026, 4, 1),
            period_end=date(2026, 4, 30),
            actor="reconciler",
            reason="monthly close",
        ),
        db_session,
    )
    approve_reconciliation_adjustment(
        ApproveReconciliationAdjustmentRequest(
            reconciliation_id=reconcile_result.reconciliation_id,
            actor="approver",
            reason="manual verified adjustment",
        ),
        db_session,
    )

    with pytest.raises(ValueError, match="already approved"):
        approve_reconciliation_adjustment(
            ApproveReconciliationAdjustmentRequest(
                reconciliation_id=reconcile_result.reconciliation_id,
                actor="approver",
                reason="second attempt",
            ),
            db_session,
        )


def test_account_reconcile_requires_opening_snapshot(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_statement(
        db_session,
        statement_id="stmt-no-opening",
        period_start=date(2026, 5, 1),
        period_end=date(2026, 5, 31),
        ending_balance=Decimal("100.00"),
    )
    db_session.flush()

    with pytest.raises(ValueError, match="Opening balance snapshot is required"):
        account_reconcile(
            AccountReconcileRequest(
                account_id="acct-1",
                period_start=date(2026, 5, 1),
                period_end=date(2026, 5, 31),
                actor="reconciler",
                reason="monthly close",
            ),
            db_session,
        )
