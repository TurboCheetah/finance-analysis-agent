from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, ImportBatch, RawTransaction, Transaction
from finance_analysis_agent.ingest.fingerprints import compute_source_fingerprint
from finance_analysis_agent.ingest.import_batch_service import ingest_transactions
from finance_analysis_agent.ingest.types import (
    CanonicalTransactionInput,
    ConflictMode,
    ImportBatchStatus,
    IngestRequest,
    SourceType,
)


def _seed_account(session: Session, account_id: str = "acct-checking") -> None:
    session.add(
        Account(
            id=account_id,
            name="Checking",
            type="checking",
            currency="USD",
        )
    )
    session.commit()


def _build_request(*, conflict_mode: ConflictMode = ConflictMode.NORMAL, override_reason: str | None = None) -> IngestRequest:
    return IngestRequest(
        source_type=SourceType.CSV,
        schema_version="1.0.0",
        source_ref="fixtures/statement.csv",
        payload_bytes=b"date,description,amount\n2026-01-01,Coffee,-4.50\n",
        conflict_mode=conflict_mode,
        override_reason=override_reason,
        transactions=[
            CanonicalTransactionInput(
                account_id="acct-checking",
                posted_date=date(2026, 1, 1),
                amount=Decimal("-4.50"),
                currency="USD",
                pending_status="posted",
                source_kind="csv",
                original_statement="COFFEE SHOP",
            )
        ],
        actor="test-suite",
    )


def test_first_ingest_creates_batch_raw_rows_transactions_and_status_events(db_session: Session) -> None:
    _seed_account(db_session)

    result = ingest_transactions(_build_request(), db_session)
    db_session.commit()

    assert result.created_new_batch is True
    assert result.replayed is False
    assert result.inserted_transactions_count == 1
    assert result.skipped_transactions_count == 0
    assert result.final_status == ImportBatchStatus.FINALIZED
    assert result.status_history == [
        ImportBatchStatus.RECEIVED,
        ImportBatchStatus.PARSED,
        ImportBatchStatus.STAGED,
        ImportBatchStatus.NORMALIZED,
        ImportBatchStatus.DEDUPED,
        ImportBatchStatus.REVIEWED,
        ImportBatchStatus.FINALIZED,
    ]

    assert db_session.scalar(select(func.count()).select_from(ImportBatch)) == 1
    assert db_session.scalar(select(func.count()).select_from(RawTransaction)) == 1
    assert db_session.scalar(select(func.count()).select_from(Transaction)) == 1

    persisted_batch = db_session.get(ImportBatch, result.batch_id)
    assert persisted_batch is not None
    assert persisted_batch.status == ImportBatchStatus.FINALIZED.value
    assert persisted_batch.finalized_at is not None
    assert persisted_batch.fingerprint_algo == "sha256"
    assert persisted_batch.conflict_mode == ConflictMode.NORMAL.value


def test_non_force_reimport_replays_existing_finalized_batch_without_new_writes(db_session: Session) -> None:
    _seed_account(db_session)
    initial_result = ingest_transactions(_build_request(), db_session)
    db_session.commit()

    replay_result = ingest_transactions(_build_request(), db_session)
    db_session.commit()

    assert replay_result.replayed is True
    assert replay_result.created_new_batch is False
    assert replay_result.batch_id == initial_result.batch_id
    assert replay_result.inserted_transactions_count == 0
    assert replay_result.skipped_transactions_count == 0

    assert db_session.scalar(select(func.count()).select_from(ImportBatch)) == 1
    assert db_session.scalar(select(func.count()).select_from(RawTransaction)) == 1
    assert db_session.scalar(select(func.count()).select_from(Transaction)) == 1


def test_force_reimport_creates_new_batch_with_reason_but_avoids_duplicate_transactions(
    db_session: Session,
) -> None:
    _seed_account(db_session)
    first_result = ingest_transactions(_build_request(), db_session)
    db_session.commit()

    forced_result = ingest_transactions(
        _build_request(conflict_mode=ConflictMode.FORCE, override_reason="rerun after parser fix"),
        db_session,
    )
    db_session.commit()

    assert forced_result.created_new_batch is True
    assert forced_result.replayed is False
    assert forced_result.inserted_transactions_count == 0
    assert forced_result.skipped_transactions_count == 1

    assert db_session.scalar(select(func.count()).select_from(ImportBatch)) == 2
    assert db_session.scalar(select(func.count()).select_from(RawTransaction)) == 2
    assert db_session.scalar(select(func.count()).select_from(Transaction)) == 1

    forced_batch = db_session.get(ImportBatch, forced_result.batch_id)
    assert forced_batch is not None
    assert forced_batch.conflict_mode == ConflictMode.FORCE.value
    assert forced_batch.override_reason == "rerun after parser fix"
    assert forced_batch.override_of_batch_id == first_result.batch_id


def test_force_mode_requires_override_reason(db_session: Session) -> None:
    _seed_account(db_session)
    with pytest.raises(ValueError, match="override_reason"):
        ingest_transactions(
            _build_request(conflict_mode=ConflictMode.FORCE, override_reason=""),
            db_session,
        )


def test_non_force_retry_allowed_when_only_previous_failed_batch_exists(db_session: Session) -> None:
    _seed_account(db_session)
    request = _build_request()
    failed_fingerprint, _ = compute_source_fingerprint(
        source_type=request.source_type,
        schema_version=request.schema_version,
        payload_bytes=request.payload_bytes,
        manual_payload=request.manual_payload,
    )
    db_session.add(
        ImportBatch(
            id="failed-batch",
            source_type=request.source_type.value,
            source_ref=request.source_ref,
            source_fingerprint=failed_fingerprint,
            fingerprint_algo="sha256",
            schema_version=request.schema_version,
            conflict_mode=ConflictMode.NORMAL.value,
            status=ImportBatchStatus.FAILED.value,
            received_at=datetime.now(UTC).replace(tzinfo=None),
            error_summary="simulated failure",
        )
    )
    db_session.commit()

    replayable_result = ingest_transactions(request, db_session)
    db_session.commit()

    assert replayable_result.created_new_batch is True
    assert replayable_result.replayed is False
    assert replayable_result.final_status == ImportBatchStatus.FINALIZED

    statuses = db_session.scalars(select(ImportBatch.status).order_by(ImportBatch.received_at.asc())).all()
    assert ImportBatchStatus.FAILED.value in statuses
    assert ImportBatchStatus.FINALIZED.value in statuses
