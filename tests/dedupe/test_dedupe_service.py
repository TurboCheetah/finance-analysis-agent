from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.dedupe import TxnDedupeMatchRequest, txn_dedupe_match
from finance_analysis_agent.db.models import Account, DedupeCandidate, Merchant, ReviewItem, Transaction
from finance_analysis_agent.review_queue.types import ReviewSource
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session) -> None:
    session.add(Account(id="acct-1", name="Checking", type="checking", currency="USD"))


def _seed_merchant(session: Session, merchant_id: str, canonical_name: str) -> None:
    session.add(
        Merchant(
            id=merchant_id,
            canonical_name=canonical_name,
            confidence=1.0,
            created_at=utcnow(),
        )
    )


def _seed_transaction(
    session: Session,
    transaction_id: str,
    *,
    posted_date: date,
    amount: str,
    original_statement: str,
    source_kind: str = "manual",
    merchant_id: str | None = None,
    pending_status: str = "posted",
) -> None:
    now = utcnow()
    session.add(
        Transaction(
            id=transaction_id,
            account_id="acct-1",
            posted_date=posted_date,
            effective_date=posted_date,
            amount=Decimal(amount),
            currency="USD",
            original_amount=Decimal(amount),
            original_currency="USD",
            pending_status=pending_status,
            original_statement=original_statement,
            merchant_id=merchant_id,
            category_id=None,
            excluded=False,
            notes=None,
            source_kind=source_kind,
            source_transaction_id=f"src-{transaction_id}",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=now,
            updated_at=now,
        )
    )


def test_hard_match_autolinks_without_review_item(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, "mer-coffee", "Coffee Shop")
    _seed_transaction(
        db_session,
        "txn-hard-a",
        posted_date=date(2026, 1, 10),
        amount="12.34",
        original_statement="COFFEE SHOP #123",
        source_kind="csv",
        merchant_id="mer-coffee",
    )
    _seed_transaction(
        db_session,
        "txn-hard-b",
        posted_date=date(2026, 1, 11),
        amount="12.34",
        original_statement="coffee shop 123",
        source_kind="csv",
        merchant_id="mer-coffee",
    )
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="detect duplicate",
            scope_transaction_ids=["txn-hard-a", "txn-hard-b"],
        ),
        db_session,
    )
    db_session.flush()

    candidates = db_session.scalars(select(DedupeCandidate)).all()
    review_items = db_session.scalars(
        select(ReviewItem).where(ReviewItem.source == ReviewSource.DEDUPE.value)
    ).all()

    assert result.hard_auto_linked == 1
    assert result.soft_queued == 0
    assert result.soft_auto_linked == 0
    assert len(result.candidates) == 1
    assert result.candidates[0].classification == "hard"
    assert result.candidates[0].decision == "duplicate"

    assert len(candidates) == 1
    assert candidates[0].decision == "duplicate"
    assert candidates[0].reason_json is not None
    assert candidates[0].reason_json["match_type"] == "hard"
    assert review_items == []


def test_soft_match_creates_review_item_with_dedupe_payload(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(
        db_session,
        "txn-soft-a",
        posted_date=date(2026, 1, 15),
        amount="82.10",
        original_statement="GROCERY OUTLET WEST",
        source_kind="pdf",
    )
    _seed_transaction(
        db_session,
        "txn-soft-b",
        posted_date=date(2026, 1, 16),
        amount="82.50",
        original_statement="GROCERY OUTLET W",
        source_kind="pdf",
    )
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="score soft matches",
            soft_review_threshold=0.75,
            soft_autolink_threshold=1.0,
            scope_transaction_ids=["txn-soft-a", "txn-soft-b"],
        ),
        db_session,
    )
    db_session.flush()

    candidate = db_session.scalar(select(DedupeCandidate))
    review_item = db_session.scalar(
        select(ReviewItem).where(ReviewItem.source == ReviewSource.DEDUPE.value)
    )

    assert result.hard_auto_linked == 0
    assert result.soft_queued == 1
    assert result.soft_auto_linked == 0
    assert len(result.candidates) == 1
    assert result.candidates[0].classification == "soft"
    assert result.candidates[0].decision is None
    assert result.candidates[0].queued_review_item_id is not None

    assert candidate is not None
    assert candidate.decision is None
    assert review_item is not None
    assert review_item.reason_code == "dedupe.soft_match"
    assert review_item.ref_table == "dedupe_candidates"
    assert review_item.ref_id == candidate.id
    assert review_item.payload_json is not None
    assert review_item.payload_json["suggestion"]["kind"] == "dedupe_decision"
    assert review_item.payload_json["suggestion"]["dedupe_candidate_id"] == candidate.id


def test_pending_transactions_are_excluded_by_default(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(
        db_session,
        "txn-pend-a",
        posted_date=date(2026, 1, 20),
        amount="9.99",
        original_statement="PENDING COFFEE",
        pending_status="pending",
    )
    _seed_transaction(
        db_session,
        "txn-pend-b",
        posted_date=date(2026, 1, 21),
        amount="9.99",
        original_statement="pending coffee",
        pending_status="pending",
    )
    db_session.flush()

    default_result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="exclude pending by default",
            scope_transaction_ids=["txn-pend-a", "txn-pend-b"],
        ),
        db_session,
    )
    include_pending_result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="include pending explicitly",
            include_pending=True,
            scope_transaction_ids=["txn-pend-a", "txn-pend-b"],
        ),
        db_session,
    )

    assert default_result.candidates == []
    assert include_pending_result.hard_auto_linked == 1


def test_rerun_is_idempotent_for_existing_hard_candidate(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(
        db_session,
        "txn-idem-a",
        posted_date=date(2026, 1, 5),
        amount="30.00",
        original_statement="NETFLIX.COM",
    )
    _seed_transaction(
        db_session,
        "txn-idem-b",
        posted_date=date(2026, 1, 6),
        amount="30.00",
        original_statement="netflix com",
    )
    db_session.flush()

    first = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="first run",
            scope_transaction_ids=["txn-idem-a", "txn-idem-b"],
        ),
        db_session,
    )
    second = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="second run",
            scope_transaction_ids=["txn-idem-a", "txn-idem-b"],
        ),
        db_session,
    )
    db_session.flush()

    total_candidates = db_session.scalar(select(func.count()).select_from(DedupeCandidate))
    assert first.hard_auto_linked == 1
    assert second.hard_auto_linked == 1
    assert second.skipped_existing == 1
    assert total_candidates == 1
