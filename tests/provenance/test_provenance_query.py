from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Category, Merchant, Transaction
from finance_analysis_agent.provenance.provenance_query_service import (
    get_transaction_provenance,
)
from finance_analysis_agent.provenance.transaction_events_service import mutate_transaction_fields
from finance_analysis_agent.provenance.types import (
    ProvenanceSource,
    TransactionMutationRequest,
)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _seed(session: Session) -> str:
    session.add(Account(id="acct-a", name="A", type="checking", currency="USD"))
    session.add_all(
        [
            Category(id="cat-a", parent_id=None, name="A", system_flag=False, active=True, created_at=_utcnow()),
            Category(id="cat-b", parent_id=None, name="B", system_flag=False, active=True, created_at=_utcnow()),
            Merchant(id="mer-a", canonical_name="A", confidence=1.0, created_at=_utcnow()),
            Merchant(id="mer-b", canonical_name="B", confidence=1.0, created_at=_utcnow()),
        ]
    )
    session.add(
        Transaction(
            id="txn-provenance",
            account_id="acct-a",
            posted_date=date(2026, 3, 1),
            effective_date=date(2026, 3, 1),
            amount=Decimal("20.00"),
            currency="USD",
            original_amount=Decimal("20.00"),
            original_currency="USD",
            pending_status="posted",
            original_statement="seed",
            merchant_id="mer-a",
            category_id="cat-a",
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id="p-1",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
    )
    session.commit()
    return "txn-provenance"


def test_get_transaction_provenance_returns_latest_source_by_field(db_session: Session) -> None:
    txn_id = _seed(db_session)

    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="human",
            reason="manual fix",
            provenance=ProvenanceSource.MANUAL,
            changes={"category_id": "cat-b"},
        ),
        db_session,
    )
    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="rule-runner",
            reason="rule overwrite",
            provenance=ProvenanceSource.RULE,
            changes={"category_id": "cat-a", "merchant_id": "mer-b"},
        ),
        db_session,
    )
    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="heuristic-pass",
            reason="confidence adjustment",
            provenance=ProvenanceSource.HEURISTIC,
            changes={"amount": Decimal("19.50"), "excluded": True},
        ),
        db_session,
    )
    db_session.commit()

    result = get_transaction_provenance(txn_id, db_session)

    assert result.current_values["category_id"] == "cat-a"
    assert result.current_values["merchant_id"] == "mer-b"
    assert result.current_values["amount"] == "19.50"
    assert result.current_values["excluded"] is True

    assert result.latest_by_field["category_id"] is not None
    assert result.latest_by_field["category_id"].source == ProvenanceSource.RULE
    assert result.latest_by_field["merchant_id"] is not None
    assert result.latest_by_field["merchant_id"].source == ProvenanceSource.RULE
    assert result.latest_by_field["amount"] is not None
    assert result.latest_by_field["amount"].source == ProvenanceSource.HEURISTIC
    assert result.latest_by_field["excluded"] is not None
    assert result.latest_by_field["excluded"].source == ProvenanceSource.HEURISTIC

