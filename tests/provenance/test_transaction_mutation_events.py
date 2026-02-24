from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Category, Merchant, Transaction, TransactionEvent
from finance_analysis_agent.provenance.transaction_events_service import mutate_transaction_fields
from finance_analysis_agent.provenance.types import (
    ProvenanceSource,
    TransactionMutationRequest,
)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _seed_transaction_graph(session: Session) -> str:
    session.add(
        Account(
            id="acct-main",
            name="Checking",
            type="checking",
            currency="USD",
        )
    )
    session.add_all(
        [
            Category(id="cat-old", parent_id=None, name="Old", system_flag=False, active=True, created_at=_utcnow()),
            Category(id="cat-new", parent_id=None, name="New", system_flag=False, active=True, created_at=_utcnow()),
            Merchant(id="mer-old", canonical_name="Old Merchant", confidence=0.9, created_at=_utcnow()),
            Merchant(id="mer-new", canonical_name="New Merchant", confidence=0.9, created_at=_utcnow()),
        ]
    )
    session.add(
        Transaction(
            id="txn-1",
            account_id="acct-main",
            posted_date=date(2026, 2, 1),
            effective_date=date(2026, 2, 1),
            amount=Decimal("10.00"),
            currency="USD",
            original_amount=Decimal("10.00"),
            original_currency="USD",
            pending_status="posted",
            original_statement="OLD",
            merchant_id="mer-old",
            category_id="cat-old",
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id="seed-1",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
    )
    session.commit()
    return "txn-1"


def test_single_field_mutations_emit_before_after_events(db_session: Session) -> None:
    txn_id = _seed_transaction_graph(db_session)

    result = mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="tester",
            reason="manual correction",
            provenance=ProvenanceSource.MANUAL,
            changes={"category_id": "cat-new"},
        ),
        db_session,
    )
    db_session.commit()

    assert result.noop is False
    assert result.changed_fields == ["category_id"]
    assert len(result.event_ids) == 1

    event = db_session.scalar(select(TransactionEvent).where(TransactionEvent.id == result.event_ids[0]))
    assert event is not None
    assert event.event_type == "transaction.field_updated.category_id"
    assert event.old_value_json == {"field": "category_id", "value": "cat-old"}
    assert event.new_value_json == {"field": "category_id", "value": "cat-new"}
    assert event.provenance == ProvenanceSource.MANUAL.value

    transaction = db_session.get(Transaction, txn_id)
    assert transaction is not None
    assert transaction.category_id == "cat-new"


def test_multi_field_mutation_emits_one_event_per_changed_field(db_session: Session) -> None:
    txn_id = _seed_transaction_graph(db_session)

    result = mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="rule-engine",
            reason="rule normalization",
            provenance=ProvenanceSource.RULE,
            changes={
                "merchant_id": "mer-new",
                "amount": Decimal("12.34"),
                "excluded": True,
            },
        ),
        db_session,
    )
    db_session.commit()

    assert set(result.changed_fields) == {"merchant_id", "amount", "excluded"}
    assert len(result.event_ids) == 3

    events = db_session.scalars(
        select(TransactionEvent).where(TransactionEvent.transaction_id == txn_id)
    ).all()
    assert len(events) == 3
    assert {event.event_type for event in events} == {
        "transaction.field_updated.merchant_id",
        "transaction.field_updated.amount",
        "transaction.field_updated.excluded",
    }

    amount_event = next(event for event in events if event.event_type.endswith("amount"))
    assert amount_event.old_value_json == {"field": "amount", "value": "10.00"}
    assert amount_event.new_value_json == {"field": "amount", "value": "12.34"}

    transaction = db_session.get(Transaction, txn_id)
    assert transaction is not None
    assert transaction.merchant_id == "mer-new"
    assert transaction.amount == Decimal("12.34")
    assert transaction.excluded is True


def test_noop_mutation_writes_no_events(db_session: Session) -> None:
    txn_id = _seed_transaction_graph(db_session)

    result = mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="tester",
            reason="noop",
            provenance=ProvenanceSource.HEURISTIC,
            changes={"category_id": "cat-old", "excluded": False},
        ),
        db_session,
    )
    db_session.commit()

    assert result.noop is True
    assert result.event_ids == []
    assert result.changed_fields == []
    assert db_session.scalar(select(func.count()).select_from(TransactionEvent)) == 0


def test_validation_rejects_missing_reason_invalid_provenance_and_unsupported_fields(
    db_session: Session,
) -> None:
    txn_id = _seed_transaction_graph(db_session)

    with pytest.raises(ValueError, match="reason"):
        mutate_transaction_fields(
            TransactionMutationRequest(
                transaction_id=txn_id,
                actor="tester",
                reason=" ",
                provenance=ProvenanceSource.MANUAL,
                changes={"category_id": "cat-new"},
            ),
            db_session,
        )

    with pytest.raises(ValueError, match="Invalid provenance source"):
        mutate_transaction_fields(
            TransactionMutationRequest(
                transaction_id=txn_id,
                actor="tester",
                reason="x",
                provenance="system",
                changes={"category_id": "cat-new"},
            ),
            db_session,
        )

    with pytest.raises(ValueError, match="Unsupported mutation field"):
        mutate_transaction_fields(
            TransactionMutationRequest(
                transaction_id=txn_id,
                actor="tester",
                reason="x",
                provenance=ProvenanceSource.MODEL,
                changes={"notes": "bad"},
            ),
            db_session,
        )

