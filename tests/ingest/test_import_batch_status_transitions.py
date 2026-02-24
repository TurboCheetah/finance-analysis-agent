from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import ImportBatch, ImportBatchStatusEvent
from finance_analysis_agent.ingest.import_batch_service import transition_import_batch_status
from finance_analysis_agent.ingest.types import ConflictMode, ImportBatchStatus


def _seed_batch(session: Session, batch_id: str = "batch-1", status: ImportBatchStatus = ImportBatchStatus.RECEIVED) -> ImportBatch:
    batch = ImportBatch(
        id=batch_id,
        source_type="csv",
        source_ref="input.csv",
        source_fingerprint="f" * 64,
        fingerprint_algo="sha256",
        schema_version="1.0.0",
        conflict_mode=ConflictMode.NORMAL.value,
        status=status.value,
        received_at=datetime.now(UTC).replace(tzinfo=None),
    )
    session.add(batch)
    session.commit()
    return batch


def test_transition_rejects_invalid_jump_and_writes_no_event(db_session: Session) -> None:
    batch = _seed_batch(db_session)

    transition_import_batch_status(
        batch_id=batch.id,
        to_status=ImportBatchStatus.PARSED,
        reason="parsed rows",
        actor="test-suite",
        session=db_session,
    )
    db_session.commit()

    with pytest.raises(ValueError, match="Invalid ImportBatch transition"):
        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.FINALIZED,
            reason="skip ahead",
            actor="test-suite",
            session=db_session,
        )

    db_session.rollback()
    assert db_session.scalar(select(func.count()).select_from(ImportBatchStatusEvent)) == 1


def test_failed_is_terminal_after_transition(db_session: Session) -> None:
    batch = _seed_batch(db_session, status=ImportBatchStatus.PARSED)

    transition_import_batch_status(
        batch_id=batch.id,
        to_status=ImportBatchStatus.FAILED,
        reason="parse mismatch",
        actor="test-suite",
        session=db_session,
    )
    db_session.commit()

    persisted = db_session.get(ImportBatch, batch.id)
    assert persisted is not None
    assert persisted.status == ImportBatchStatus.FAILED.value
    assert persisted.error_summary == "parse mismatch"

    with pytest.raises(ValueError, match="Invalid ImportBatch transition"):
        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.REVIEWED,
            reason="should fail",
            actor="test-suite",
            session=db_session,
        )
