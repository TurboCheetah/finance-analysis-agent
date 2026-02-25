from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    Account,
    Category,
    DedupeCandidate,
    ReviewItem,
    ReviewItemEvent,
    Transaction,
    TransactionEvent,
)
from finance_analysis_agent.review_queue import (
    BulkActionType,
    BulkTriageRequest,
    ReviewItemStatus,
    ReviewQueueListRequest,
    ReviewSource,
    bulk_triage,
    list_review_items,
)
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session) -> None:
    session.add(Account(id="acct-1", name="Checking", type="checking", currency="USD"))


def _seed_category(session: Session, category_id: str, name: str) -> None:
    session.add(
        Category(
            id=category_id,
            parent_id=None,
            name=name,
            system_flag=False,
            active=True,
            created_at=utcnow(),
        )
    )


def _seed_transaction(session: Session, transaction_id: str, *, category_id: str | None = None) -> None:
    timestamp = utcnow()
    session.add(
        Transaction(
            id=transaction_id,
            account_id="acct-1",
            posted_date=date(2026, 1, 20),
            effective_date=date(2026, 1, 20),
            amount=Decimal("12.34"),
            currency="USD",
            original_amount=Decimal("12.34"),
            original_currency="USD",
            pending_status="posted",
            original_statement="COFFEE SHOP",
            merchant_id=None,
            category_id=category_id,
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
    ref_table: str,
    ref_id: str,
    reason_code: str,
    source: str,
    status: str = "to_review",
    confidence: float | None = None,
    payload_json: dict[str, object] | None = None,
) -> None:
    session.add(
        ReviewItem(
            id=review_item_id,
            item_type="review",
            ref_table=ref_table,
            ref_id=ref_id,
            reason_code=reason_code,
            confidence=confidence,
            status=status,
            source=source,
            assigned_to=None,
            payload_json=payload_json,
            created_at=utcnow(),
            resolved_at=None,
        )
    )


def test_list_review_items_filters_by_confidence_reason_and_source(db_session: Session) -> None:
    _seed_review_item(
        db_session,
        review_item_id="ri-1",
        ref_table="run_metadata",
        ref_id="run-1",
        reason_code="low_confidence_row",
        source=ReviewSource.PDF_EXTRACT.value,
        confidence=0.42,
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-2",
        ref_table="run_metadata",
        ref_id="run-1",
        reason_code="low_confidence_row",
        source=ReviewSource.PDF_EXTRACT.value,
        confidence=0.91,
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-3",
        ref_table="transactions",
        ref_id="txn-1",
        reason_code="rule.needs_review",
        source=ReviewSource.RULES.value,
        confidence=0.95,
    )
    db_session.flush()

    result = list_review_items(
        ReviewQueueListRequest(
            confidence_min=0.8,
            reason_codes=["low_confidence_row"],
            sources=[ReviewSource.PDF_EXTRACT],
        ),
        db_session,
    )

    assert result.total_count == 1
    assert [item.id for item in result.items] == ["ri-2"]


def test_bulk_recategorize_updates_transaction_and_writes_audits(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, "cat-old", "Old")
    _seed_category(db_session, "cat-new", "New")
    _seed_transaction(db_session, "txn-1", category_id="cat-old")
    _seed_review_item(
        db_session,
        review_item_id="ri-rec-1",
        ref_table="transactions",
        ref_id="txn-1",
        reason_code="categorize.low_confidence",
        source=ReviewSource.CATEGORIZE.value,
    )
    db_session.flush()

    result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.RECATEGORIZE,
            review_item_ids=["ri-rec-1"],
            actor="reviewer",
            reason="bulk recategorize",
            category_id="cat-new",
        ),
        db_session,
    )

    item = db_session.get(ReviewItem, "ri-rec-1")
    transaction = db_session.get(Transaction, "txn-1")
    events = db_session.scalars(
        select(ReviewItemEvent)
        .where(ReviewItemEvent.review_item_id == "ri-rec-1")
        .order_by(ReviewItemEvent.created_at.asc())
    ).all()
    transaction_events = db_session.scalars(
        select(TransactionEvent).where(TransactionEvent.transaction_id == "txn-1")
    ).all()

    assert result.updated == 1
    assert result.failed == 0
    assert result.skipped == 0
    assert item is not None and item.status == ReviewItemStatus.RESOLVED.value
    assert item.resolved_at is not None
    assert transaction is not None and transaction.category_id == "cat-new"
    assert any(event.event_type == "bulk_action_applied" for event in events)
    assert any(event.event_type == "status_transition" for event in events)
    assert len(transaction_events) == 1
    assert transaction_events[0].event_type == "transaction.field_updated.category_id"


def test_bulk_mark_duplicate_updates_candidate_and_resolves_item(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(db_session, "txn-a")
    _seed_transaction(db_session, "txn-b")
    db_session.add(
        DedupeCandidate(
            id="dc-1",
            txn_a_id="txn-a",
            txn_b_id="txn-b",
            score=0.99,
            decision=None,
            reason_json={"seed": True},
            created_at=utcnow(),
            decided_at=None,
        )
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-dup-1",
        ref_table="dedupe_candidates",
        ref_id="dc-1",
        reason_code="dedupe.soft_match",
        source=ReviewSource.DEDUPE.value,
    )
    db_session.flush()

    result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.MARK_DUPLICATE,
            review_item_ids=["ri-dup-1"],
            actor="reviewer",
            reason="mark as duplicate",
        ),
        db_session,
    )

    candidate = db_session.get(DedupeCandidate, "dc-1")
    review_item = db_session.get(ReviewItem, "ri-dup-1")
    review_events = db_session.scalars(
        select(ReviewItemEvent).where(ReviewItemEvent.review_item_id == "ri-dup-1")
    ).all()

    assert result.updated == 1
    assert candidate is not None and candidate.decision == "duplicate"
    assert candidate.decided_at is not None
    assert candidate.reason_json is not None and candidate.reason_json["action"] == "mark_duplicate"
    assert review_item is not None and review_item.status == ReviewItemStatus.RESOLVED.value
    assert any(event.event_type == "bulk_action_applied" for event in review_events)


def test_bulk_approve_suggestion_applies_supported_payload(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, "cat-old", "Old")
    _seed_category(db_session, "cat-new", "New")
    _seed_transaction(db_session, "txn-approve-1", category_id="cat-old")
    _seed_review_item(
        db_session,
        review_item_id="ri-appr-1",
        ref_table="transactions",
        ref_id="txn-approve-1",
        reason_code="categorize.suggestion",
        source=ReviewSource.CATEGORIZE.value,
        payload_json={
            "suggestion": {
                "kind": "transaction_category",
                "transaction_id": "txn-approve-1",
                "category_id": "cat-new",
            }
        },
    )
    db_session.flush()

    result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.APPROVE_SUGGESTION,
            review_item_ids=["ri-appr-1"],
            actor="reviewer",
            reason="approve suggestion",
        ),
        db_session,
    )

    transaction = db_session.get(Transaction, "txn-approve-1")
    review_item = db_session.get(ReviewItem, "ri-appr-1")

    assert result.updated == 1
    assert transaction is not None and transaction.category_id == "cat-new"
    assert review_item is not None and review_item.status == ReviewItemStatus.RESOLVED.value


def test_bulk_approve_suggestion_fails_cleanly_for_unsupported_payload(db_session: Session) -> None:
    _seed_review_item(
        db_session,
        review_item_id="ri-appr-bad",
        ref_table="transactions",
        ref_id="txn-missing",
        reason_code="categorize.suggestion",
        source=ReviewSource.CATEGORIZE.value,
        payload_json={"unexpected": "shape"},
    )
    db_session.flush()

    result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.APPROVE_SUGGESTION,
            review_item_ids=["ri-appr-bad"],
            actor="reviewer",
            reason="approve suggestion",
        ),
        db_session,
    )

    review_item = db_session.get(ReviewItem, "ri-appr-bad")
    review_events = db_session.scalars(
        select(ReviewItemEvent).where(ReviewItemEvent.review_item_id == "ri-appr-bad")
    ).all()

    assert result.updated == 0
    assert result.failed == 1
    assert review_item is not None and review_item.status == ReviewItemStatus.TO_REVIEW.value
    assert any(event.event_type == "bulk_action_failed" for event in review_events)


def test_bulk_reject_suggestion_sets_rejected_without_transaction_mutation(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, "cat-old", "Old")
    _seed_transaction(db_session, "txn-rej-1", category_id="cat-old")
    _seed_review_item(
        db_session,
        review_item_id="ri-rej-1",
        ref_table="transactions",
        ref_id="txn-rej-1",
        reason_code="categorize.suggestion",
        source=ReviewSource.CATEGORIZE.value,
        payload_json={
            "suggestion": {
                "kind": "transaction_category",
                "transaction_id": "txn-rej-1",
                "category_id": "cat-never-applied",
            }
        },
    )
    db_session.flush()

    result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.REJECT_SUGGESTION,
            review_item_ids=["ri-rej-1"],
            actor="reviewer",
            reason="reject suggestion",
        ),
        db_session,
    )

    review_item = db_session.get(ReviewItem, "ri-rej-1")
    transaction = db_session.get(Transaction, "txn-rej-1")
    transaction_events = db_session.scalars(
        select(TransactionEvent).where(TransactionEvent.transaction_id == "txn-rej-1")
    ).all()

    assert result.updated == 1
    assert review_item is not None and review_item.status == ReviewItemStatus.REJECTED.value
    assert transaction is not None and transaction.category_id == "cat-old"
    assert transaction_events == []
