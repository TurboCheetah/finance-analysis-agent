from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy.orm import Session

from finance_analysis_agent.categorize.providers import DEFAULT_PROVIDER_NAME, resolve_suggestion_provider
from finance_analysis_agent.categorize.types import CategorizeSuggestRequest
from tests.categorize._helpers import _seed_account, _seed_category, _seed_merchant, _seed_transaction


def test_heuristic_provider_reports_missing_history_as_skipped(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, "mer-x", "Merchant X")
    _seed_transaction(
        db_session,
        transaction_id="txn-target",
        posted_date=date(2026, 2, 10),
        original_statement="MERCHANT X",
        merchant_id="mer-x",
        category_id=None,
        amount=Decimal("9.99"),
    )
    db_session.flush()

    provider = resolve_suggestion_provider(DEFAULT_PROVIDER_NAME)
    result = provider.suggest(
        CategorizeSuggestRequest(actor="tester", reason="missing history"),
        db_session,
    )

    assert result.suggestions == []
    assert result.skipped == {"no_categorized_history": 1}


def test_heuristic_provider_is_deterministic_for_same_dataset(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, "cat-food", "Food")
    _seed_merchant(db_session, "mer-coffee", "Coffee Shop")
    _seed_transaction(
        db_session,
        transaction_id="txn-hist-1",
        posted_date=date(2026, 1, 1),
        original_statement="COFFEE SHOP DOWNTOWN",
        merchant_id="mer-coffee",
        category_id="cat-food",
        amount=Decimal("9.99"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-target-1",
        posted_date=date(2026, 2, 1),
        original_statement="COFFEE SHOP 123",
        merchant_id="mer-coffee",
        category_id=None,
        amount=Decimal("9.99"),
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-target-2",
        posted_date=date(2026, 2, 2),
        original_statement="COFFEE SHOP MAIN ST",
        merchant_id="mer-coffee",
        category_id=None,
        amount=Decimal("9.99"),
    )
    db_session.flush()

    provider = resolve_suggestion_provider(DEFAULT_PROVIDER_NAME)
    request = CategorizeSuggestRequest(actor="tester", reason="determinism")
    first = provider.suggest(request, db_session)
    second = provider.suggest(request, db_session)

    first_rows = [
        (item.transaction_id, item.suggested_category_id, item.confidence, tuple(item.reason_codes))
        for item in first.suggestions
    ]
    second_rows = [
        (item.transaction_id, item.suggested_category_id, item.confidence, tuple(item.reason_codes))
        for item in second.suggestions
    ]
    assert first_rows == second_rows
