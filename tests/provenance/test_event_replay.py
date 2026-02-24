from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Category, Merchant, Transaction
from finance_analysis_agent.provenance.provenance_query_service import replay_transaction_field_history
from finance_analysis_agent.provenance.transaction_events_service import mutate_transaction_fields
from finance_analysis_agent.provenance.types import ProvenanceSource, TransactionMutationRequest


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _seed(session: Session) -> str:
    session.add(Account(id="acct-r", name="Replay", type="checking", currency="USD"))
    session.add_all(
        [
            Category(id="cat-r1", parent_id=None, name="R1", system_flag=False, active=True, created_at=_utcnow()),
            Category(id="cat-r2", parent_id=None, name="R2", system_flag=False, active=True, created_at=_utcnow()),
            Merchant(id="mer-r1", canonical_name="R1", confidence=1.0, created_at=_utcnow()),
            Merchant(id="mer-r2", canonical_name="R2", confidence=1.0, created_at=_utcnow()),
        ]
    )
    session.add(
        Transaction(
            id="txn-replay",
            account_id="acct-r",
            posted_date=date(2026, 3, 5),
            effective_date=date(2026, 3, 5),
            amount=Decimal("50.00"),
            currency="USD",
            original_amount=Decimal("50.00"),
            original_currency="USD",
            pending_status="posted",
            original_statement="seed",
            merchant_id="mer-r1",
            category_id="cat-r1",
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id="r-1",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
    )
    session.commit()
    return "txn-replay"


def test_replay_transaction_field_history_reconstructs_transitions(db_session: Session) -> None:
    txn_id = _seed(db_session)

    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="a1",
            reason="cat change",
            provenance=ProvenanceSource.MANUAL,
            changes={"category_id": "cat-r2"},
        ),
        db_session,
    )
    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="a2",
            reason="merchant + amount",
            provenance=ProvenanceSource.RULE,
            changes={"merchant_id": "mer-r2", "amount": Decimal("48.00")},
        ),
        db_session,
    )
    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=txn_id,
            actor="a3",
            reason="exclude",
            provenance=ProvenanceSource.MODEL,
            changes={"excluded": True},
        ),
        db_session,
    )
    db_session.commit()

    replay = replay_transaction_field_history(txn_id, db_session)

    assert len(replay.transitions) == 4
    assert replay.transitions[0].field == "category_id"
    assert replay.transitions[0].old_value == "cat-r1"
    assert replay.transitions[0].new_value == "cat-r2"

    assert replay.transitions[-1].field == "excluded"
    assert replay.transitions[-1].new_value is True
    assert replay.transitions[-1].source == ProvenanceSource.MODEL

    assert replay.final_state["category_id"] == "cat-r2"
    assert replay.final_state["merchant_id"] == "mer-r2"
    assert replay.final_state["amount"] == "48.00"
    assert replay.final_state["excluded"] is True

