from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.categorize import CategorizeSuggestRequest, categorize_suggest
from finance_analysis_agent.db.models import ReviewItem, RunMetadata, Transaction
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from tests.categorize._helpers import _seed_account, _seed_category, _seed_merchant, _seed_transaction
from finance_analysis_agent.utils.time import utcnow


def _seed_open_rule_review(session: Session, *, review_item_id: str, transaction_id: str) -> None:
    session.add(
        ReviewItem(
            id=review_item_id,
            item_type="transaction_rule",
            ref_table="transactions",
            ref_id=transaction_id,
            reason_code="rule.needs_review",
            confidence=None,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.RULES.value,
            assigned_to=None,
            payload_json={"seed": True},
            created_at=utcnow(),
            resolved_at=None,
        )
    )


def _seed_existing_categorize_review(
    session: Session,
    *,
    review_item_id: str,
    transaction_id: str,
    category_id: str,
) -> None:
    session.add(
        ReviewItem(
            id=review_item_id,
            item_type="transaction_category_suggestion",
            ref_table="transactions",
            ref_id=transaction_id,
            reason_code="categorize.suggestion",
            confidence=0.9,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.CATEGORIZE.value,
            assigned_to=None,
            payload_json={
                "suggestion": {
                    "kind": "transaction_category",
                    "transaction_id": transaction_id,
                    "category_id": category_id,
                    "reason_codes": ["seed"],
                    "provider": "heuristic_v1",
                    "confidence": 0.9,
                }
            },
            created_at=utcnow(),
            resolved_at=None,
        )
    )


def _seed_baseline_data(session: Session) -> None:
    _seed_account(session)
    _seed_category(session, "cat-food", "Food")
    _seed_category(session, "cat-travel", "Travel")
    _seed_merchant(session, "mer-coffee", "Coffee Shop")
    _seed_merchant(session, "mer-market", "Market")

    _seed_transaction(
        session,
        transaction_id="txn-hist-coffee-1",
        posted_date=date(2026, 1, 10),
        original_statement="COFFEE SHOP DOWNTOWN",
        merchant_id="mer-coffee",
        category_id="cat-food",
    )
    _seed_transaction(
        session,
        transaction_id="txn-hist-coffee-2",
        posted_date=date(2026, 2, 1),
        original_statement="COFFEE SHOP 123",
        merchant_id="mer-coffee",
        category_id="cat-food",
    )
    _seed_transaction(
        session,
        transaction_id="txn-hist-market-1",
        posted_date=date(2026, 1, 11),
        original_statement="WHOLE MARKET STORE",
        merchant_id="mer-market",
        category_id="cat-food",
    )
    _seed_transaction(
        session,
        transaction_id="txn-hist-market-2",
        posted_date=date(2026, 1, 12),
        original_statement="TRAVEL MARKETPLACE FEE",
        merchant_id="mer-market",
        category_id="cat-travel",
    )

    _seed_transaction(
        session,
        transaction_id="txn-target-high",
        posted_date=date(2026, 2, 15),
        original_statement="COFFEE SHOP MAIN ST",
        merchant_id="mer-coffee",
        category_id=None,
    )
    _seed_transaction(
        session,
        transaction_id="txn-target-low",
        posted_date=date(2026, 2, 16),
        original_statement="MARKET PURCHASE LOCAL",
        merchant_id="mer-market",
        category_id=None,
    )
    _seed_transaction(
        session,
        transaction_id="txn-rule-open",
        posted_date=date(2026, 2, 17),
        original_statement="COFFEE SHOP RULE CONFLICT",
        merchant_id="mer-coffee",
        category_id=None,
    )
    _seed_transaction(
        session,
        transaction_id="txn-already-categorized",
        posted_date=date(2026, 2, 18),
        original_statement="ALREADY CATEGORIZED",
        merchant_id="mer-market",
        category_id="cat-travel",
    )

    _seed_open_rule_review(
        session,
        review_item_id="ri-rule-open",
        transaction_id="txn-rule-open",
    )


def test_categorize_suggest_queues_review_items_and_writes_run_metadata(db_session: Session) -> None:
    _seed_baseline_data(db_session)
    db_session.flush()

    result = categorize_suggest(
        CategorizeSuggestRequest(
            actor="tester",
            reason="generate category suggestions",
            confidence_threshold=0.8,
        ),
        db_session,
    )
    db_session.flush()

    suggestions_by_transaction = {item.transaction_id: item for item in result.suggestions}
    assert "txn-target-high" in suggestions_by_transaction
    assert "txn-target-low" in suggestions_by_transaction
    assert "txn-rule-open" not in suggestions_by_transaction

    high = suggestions_by_transaction["txn-target-high"]
    low = suggestions_by_transaction["txn-target-low"]
    assert high.confidence >= 0.8
    assert low.confidence < 0.8
    assert "categorize.history.merchant_majority" in high.reason_codes

    review_items = db_session.scalars(
        select(ReviewItem)
        .where(ReviewItem.source == ReviewSource.CATEGORIZE.value)
        .order_by(ReviewItem.ref_id.asc(), ReviewItem.id.asc())
    ).all()
    assert len(review_items) == 2
    assert {item.reason_code for item in review_items} == {"categorize.suggestion", "categorize.low_confidence"}
    assert all(item.status == ReviewItemStatus.TO_REVIEW.value for item in review_items)

    target_high = db_session.get(Transaction, "txn-target-high")
    target_low = db_session.get(Transaction, "txn-target-low")
    assert target_high is not None and target_high.category_id is None
    assert target_low is not None and target_low.category_id is None

    assert result.generated >= result.queued
    assert result.queued == 2
    assert result.low_confidence == 1
    assert result.high_confidence == 1
    assert result.skipped.get("rule_review_open") == 1

    run = db_session.get(RunMetadata, result.run_metadata_id)
    assert run is not None
    assert run.pipeline_name == "categorize_suggest"
    assert run.status == "success"
    assert run.diagnostics_json is not None
    assert run.diagnostics_json["generated"] == result.generated
    assert run.diagnostics_json["queued"] == 2


def test_categorize_suggest_is_idempotent_for_existing_active_review_item(db_session: Session) -> None:
    _seed_baseline_data(db_session)
    _seed_existing_categorize_review(
        db_session,
        review_item_id="ri-existing",
        transaction_id="txn-target-high",
        category_id="cat-food",
    )
    db_session.flush()

    first = categorize_suggest(
        CategorizeSuggestRequest(
            actor="tester",
            reason="first run",
            confidence_threshold=0.8,
            scope_transaction_ids=["txn-target-high"],
        ),
        db_session,
    )
    db_session.flush()
    second = categorize_suggest(
        CategorizeSuggestRequest(
            actor="tester",
            reason="second run",
            confidence_threshold=0.8,
            scope_transaction_ids=["txn-target-high"],
        ),
        db_session,
    )
    db_session.flush()

    assert len(first.suggestions) == 1
    assert len(second.suggestions) == 1
    assert first.suggestions[0].suggested_category_id == second.suggestions[0].suggested_category_id
    assert first.suggestions[0].confidence == second.suggestions[0].confidence
    assert first.suggestions[0].queued_review_item_id == "ri-existing"
    assert second.suggestions[0].queued_review_item_id == "ri-existing"

    active_count = db_session.scalar(
        select(func.count())
        .select_from(ReviewItem)
        .where(
            ReviewItem.ref_id == "txn-target-high",
            ReviewItem.source == ReviewSource.CATEGORIZE.value,
            ReviewItem.status.in_([ReviewItemStatus.TO_REVIEW.value, ReviewItemStatus.IN_PROGRESS.value]),
        )
    )
    assert active_count == 1


def test_categorize_suggest_fails_for_unknown_provider(db_session: Session) -> None:
    _seed_baseline_data(db_session)
    db_session.flush()

    with pytest.raises(ValueError, match="Unknown suggestion provider"):
        categorize_suggest(
            CategorizeSuggestRequest(
                actor="tester",
                reason="bad provider",
                provider="missing-provider",
            ),
            db_session,
        )
