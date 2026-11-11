from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from finance_analysis_agent.db.models import ImportBatch, Transaction, TransactionEvent
from finance_analysis_agent.ingest import (
    CanonicalTransactionInput,
    ConflictMode,
    ImportBatchStatus,
    IngestRequest,
    SourceType,
    ingest_transactions,
)
from finance_analysis_agent.provenance import (
    ProvenanceSource,
    TransactionMutationRequest,
    get_transaction_provenance,
    mutate_transaction_fields,
)
from tests.e2e.helpers import persist_artifact, seed_account, seed_category, write_json_artifact

pytestmark = pytest.mark.e2e


def test_journey_csv_manual_idempotency_and_provenance(db_session, tmp_path: Path) -> None:
    seed_account(db_session, account_id="acct-ingest", name="Ingest Checking")
    seed_category(db_session, category_id="cat-old", name="Old Category")
    seed_category(db_session, category_id="cat-new", name="New Category")
    db_session.flush()

    csv_request = IngestRequest(
        source_type=SourceType.CSV,
        schema_version="1.0.0",
        source_ref="fixtures/venmo-export.csv",
        payload_bytes=b"date,description,amount\n2026-02-01,Coffee,4.50\n",
        conflict_mode=ConflictMode.NORMAL,
        actor="e2e-csv",
        transactions=[
            CanonicalTransactionInput(
                account_id="acct-ingest",
                posted_date=date(2026, 2, 1),
                effective_date=date(2026, 2, 2),
                amount=Decimal("-4.50"),
                currency="USD",
                pending_status="posted",
                source_kind="csv",
                source_transaction_id="csv-transaction-1",
                original_amount=Decimal("4.50"),
                original_currency="USD",
                original_statement="COFFEE SHOP",
                category_id="cat-old",
            )
        ],
    )

    first_csv = ingest_transactions(csv_request, db_session)
    replay_csv = ingest_transactions(csv_request, db_session)
    db_session.flush()

    assert first_csv.created_new_batch is True
    assert first_csv.replayed is False
    assert first_csv.final_status == ImportBatchStatus.FINALIZED
    assert first_csv.status_history == [
        ImportBatchStatus.RECEIVED,
        ImportBatchStatus.PARSED,
        ImportBatchStatus.STAGED,
        ImportBatchStatus.NORMALIZED,
        ImportBatchStatus.DEDUPED,
        ImportBatchStatus.REVIEWED,
        ImportBatchStatus.FINALIZED,
    ]
    assert replay_csv.replayed is True
    assert replay_csv.batch_id == first_csv.batch_id

    with pytest.raises(ValueError, match="override_reason"):
        ingest_transactions(
            IngestRequest(
                source_type=SourceType.CSV,
                schema_version="1.0.0",
                source_ref=csv_request.source_ref,
                payload_bytes=csv_request.payload_bytes,
                conflict_mode=ConflictMode.FORCE,
                override_reason="",
                actor="e2e-csv",
                transactions=csv_request.transactions,
            ),
            db_session,
        )

    forced_csv = ingest_transactions(
        IngestRequest(
            source_type=SourceType.CSV,
            schema_version="1.0.0",
            source_ref=csv_request.source_ref,
            payload_bytes=csv_request.payload_bytes,
            conflict_mode=ConflictMode.FORCE,
            override_reason="parser fix replay",
            actor="e2e-csv",
            transactions=csv_request.transactions,
        ),
        db_session,
    )
    db_session.flush()

    assert forced_csv.created_new_batch is True
    assert forced_csv.replayed is False
    assert forced_csv.inserted_transactions_count == 0
    assert forced_csv.skipped_transactions_count == 1

    csv_transaction = db_session.scalar(
        select(Transaction).where(Transaction.source_transaction_id == "csv-transaction-1")
    )
    assert csv_transaction is not None
    assert csv_transaction.amount == Decimal("-4.50")
    assert csv_transaction.original_amount == Decimal("4.50")
    assert csv_transaction.effective_date == date(2026, 2, 2)
    assert csv_transaction.source_kind == "csv"

    manual_request = IngestRequest(
        source_type=SourceType.MANUAL,
        schema_version="1.0.0",
        actor="e2e-manual",
        manual_payload={"merchant": "Market", "amount": 15.0, "tags": ["food", "home"]},
        transactions=[
            CanonicalTransactionInput(
                account_id="acct-ingest",
                posted_date=date(2026, 2, 3),
                amount=Decimal("-15.00"),
                currency="USD",
                pending_status="posted",
                source_kind="manual",
                source_transaction_id="manual-transaction-1",
                original_statement="MANUAL MARKET",
                category_id="cat-old",
            )
        ],
    )
    manual_reordered_request = IngestRequest(
        source_type=SourceType.MANUAL,
        schema_version="1.0.0",
        actor="e2e-manual",
        manual_payload={"tags": ["food", "home"], "amount": 15.0, "merchant": "Market"},
        transactions=manual_request.transactions,
    )

    first_manual = ingest_transactions(manual_request, db_session)
    replay_manual = ingest_transactions(manual_reordered_request, db_session)
    db_session.flush()

    assert first_manual.created_new_batch is True
    assert replay_manual.replayed is True
    assert replay_manual.batch_id == first_manual.batch_id

    manual_transaction = db_session.scalar(
        select(Transaction).where(Transaction.source_transaction_id == "manual-transaction-1")
    )
    assert manual_transaction is not None
    assert manual_transaction.source_kind == "manual"

    mutation_result = mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=manual_transaction.id,
            actor="e2e-editor",
            reason="manual recategorization",
            provenance=ProvenanceSource.MANUAL,
            changes={"category_id": "cat-new", "excluded": True},
        ),
        db_session,
    )
    db_session.flush()

    assert mutation_result.noop is False
    assert set(mutation_result.changed_fields) == {"category_id", "excluded"}

    provenance = get_transaction_provenance(manual_transaction.id, db_session)
    assert provenance.current_values["category_id"] == "cat-new"
    assert provenance.current_values["excluded"] is True
    assert provenance.latest_by_field["category_id"] is not None
    assert provenance.latest_by_field["category_id"].source == ProvenanceSource.MANUAL
    assert provenance.latest_by_field["excluded"] is not None
    assert provenance.latest_by_field["excluded"].source == ProvenanceSource.MANUAL

    event_ids = db_session.execute(
        select(TransactionEvent.id).where(TransactionEvent.transaction_id == manual_transaction.id)
    ).scalars().all()
    assert len(event_ids) == 2

    batch_ids = db_session.execute(select(ImportBatch.id)).scalars().all()
    assert len(batch_ids) == 3

    summary_path = write_json_artifact(
        tmp_path / "journey-csv-manual-idempotency-and-provenance.json",
        {
            "first_csv_batch_id": first_csv.batch_id,
            "forced_csv_batch_id": forced_csv.batch_id,
            "first_manual_batch_id": first_manual.batch_id,
            "manual_transaction_id": manual_transaction.id,
            "manual_event_ids": mutation_result.event_ids,
        },
    )
    persist_artifact("journey-csv-manual-idempotency-and-provenance.json", summary_path)
