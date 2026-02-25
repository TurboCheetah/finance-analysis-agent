from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.dedupe import TxnDedupeMatchRequest, txn_dedupe_match
from finance_analysis_agent.db.models import Account, DedupeCandidate, Merchant, ReviewItem, Transaction
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
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


def test_hard_match_still_runs_when_hard_window_exceeds_soft_window(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, "mer-hard-window", "Coffee Shop")
    _seed_transaction(
        db_session,
        "txn-hard-window-a",
        posted_date=date(2026, 1, 10),
        amount="12.34",
        original_statement="COFFEE SHOP #123",
        source_kind="csv",
        merchant_id="mer-hard-window",
    )
    _seed_transaction(
        db_session,
        "txn-hard-window-b",
        posted_date=date(2026, 1, 12),
        amount="12.34",
        original_statement="coffee shop 123",
        source_kind="csv",
        merchant_id="mer-hard-window",
    )
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="hard window larger than soft window",
            hard_date_window_days=3,
            soft_candidate_window_days=1,
            scope_transaction_ids=["txn-hard-window-a", "txn-hard-window-b"],
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


def test_soft_rerun_preserves_existing_duplicate_decision(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(
        db_session,
        "txn-preserve-a",
        posted_date=date(2026, 1, 15),
        amount="82.10",
        original_statement="GROCERY OUTLET WEST",
        source_kind="pdf",
    )
    _seed_transaction(
        db_session,
        "txn-preserve-b",
        posted_date=date(2026, 1, 16),
        amount="82.50",
        original_statement="GROCERY OUTLET W",
        source_kind="pdf",
    )
    decided_at_before = utcnow()
    db_session.add(
        DedupeCandidate(
            id="dc-preserve-accepted",
            txn_a_id="txn-preserve-a",
            txn_b_id="txn-preserve-b",
            score=0.0,
            decision="duplicate",
            reason_json={"seed": True},
            created_at=utcnow(),
            decided_at=decided_at_before,
        )
    )
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="preserve accepted decisions on rerun",
            soft_review_threshold=0.75,
            soft_autolink_threshold=1.0,
            scope_transaction_ids=["txn-preserve-a", "txn-preserve-b"],
        ),
        db_session,
    )
    db_session.flush()

    candidate = db_session.get(DedupeCandidate, "dc-preserve-accepted")
    review_items = db_session.scalars(
        select(ReviewItem).where(ReviewItem.source == ReviewSource.DEDUPE.value)
    ).all()

    assert result.hard_auto_linked == 0
    assert result.soft_auto_linked == 1
    assert result.soft_queued == 0
    assert len(result.candidates) == 1
    assert result.candidates[0].classification == "soft"
    assert result.candidates[0].decision == "duplicate"
    assert result.candidates[0].queued_review_item_id is None
    assert candidate is not None
    assert candidate.decision == "duplicate"
    assert candidate.decided_at == decided_at_before
    assert review_items == []


def test_autolink_resolves_existing_active_dedupe_review_item(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(
        db_session,
        "txn-resolve-a",
        posted_date=date(2026, 1, 2),
        amount="25.00",
        original_statement="STREAMING SERVICE",
    )
    _seed_transaction(
        db_session,
        "txn-resolve-b",
        posted_date=date(2026, 1, 3),
        amount="25.00",
        original_statement="streaming service",
    )
    db_session.add(
        DedupeCandidate(
            id="dc-existing-open",
            txn_a_id="txn-resolve-a",
            txn_b_id="txn-resolve-b",
            score=0.81,
            decision=None,
            reason_json={"seed": True},
            created_at=utcnow(),
            decided_at=None,
        )
    )
    db_session.add(
        ReviewItem(
            id="ri-existing-open",
            item_type="dedupe_candidate_suggestion",
            ref_table="dedupe_candidates",
            ref_id="dc-existing-open",
            reason_code="dedupe.soft_match",
            confidence=0.81,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.DEDUPE.value,
            assigned_to="triager",
            payload_json={"seed": True},
            created_at=utcnow(),
            resolved_at=None,
        )
    )
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="review-bot-fix",
            reason="auto-link now deterministic",
            scope_transaction_ids=["txn-resolve-a", "txn-resolve-b"],
        ),
        db_session,
    )
    db_session.flush()

    candidate = db_session.get(DedupeCandidate, "dc-existing-open")
    review_item = db_session.get(ReviewItem, "ri-existing-open")
    assert result.hard_auto_linked == 1
    assert candidate is not None and candidate.decision == "duplicate"
    assert review_item is not None
    assert review_item.status == ReviewItemStatus.RESOLVED.value
    assert review_item.resolved_at is not None
    assert isinstance(review_item.payload_json, dict)
    assert review_item.payload_json["resolution"]["actor"] == "review-bot-fix"
    assert review_item.payload_json["resolution"]["reason"] == "auto-link now deterministic"


def test_soft_score_details_align_with_canonical_txn_order(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(
        db_session,
        "txn-z-order",
        posted_date=date(2026, 1, 1),
        amount="42.10",
        original_statement="LEFT PAYEE STORE",
    )
    _seed_transaction(
        db_session,
        "txn-a-order",
        posted_date=date(2026, 1, 2),
        amount="42.50",
        original_statement="RIGHT PAYEE STORE",
    )
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="tester",
            reason="check detail ordering",
            soft_review_threshold=0.60,
            scope_transaction_ids=["txn-z-order", "txn-a-order"],
        ),
        db_session,
    )

    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.classification == "soft"
    score_breakdown = candidate.score_breakdown
    assert score_breakdown is not None
    assert score_breakdown.details["left_payee"] == "right payee store"
    assert score_breakdown.details["right_payee"] == "left payee store"


def test_active_dedupe_review_items_are_unique_for_candidate(db_session: Session) -> None:
    now = utcnow()
    db_session.add(
        ReviewItem(
            id="ri-dedupe-unique-a",
            item_type="dedupe_candidate_suggestion",
            ref_table="dedupe_candidates",
            ref_id="dc-unique",
            reason_code="dedupe.soft_match",
            confidence=0.75,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.DEDUPE.value,
            assigned_to=None,
            payload_json={"seed": "a"},
            created_at=now,
            resolved_at=None,
        )
    )
    db_session.add(
        ReviewItem(
            id="ri-dedupe-unique-b",
            item_type="dedupe_candidate_suggestion",
            ref_table="dedupe_candidates",
            ref_id="dc-unique",
            reason_code="dedupe.soft_match",
            confidence=0.76,
            status=ReviewItemStatus.IN_PROGRESS.value,
            source=ReviewSource.DEDUPE.value,
            assigned_to="triager",
            payload_json={"seed": "b"},
            created_at=now,
            resolved_at=None,
        )
    )

    with pytest.raises(IntegrityError):
        db_session.flush()
    db_session.rollback()
