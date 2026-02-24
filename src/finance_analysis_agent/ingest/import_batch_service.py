"""ImportBatch idempotency and ingestion service."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    ImportBatch,
    ImportBatchStatusEvent,
    RawTransaction,
    Transaction,
)
from finance_analysis_agent.ingest.fingerprints import compute_source_fingerprint
from finance_analysis_agent.ingest.types import (
    CanonicalTransactionInput,
    ConflictMode,
    ImportBatchStatus,
    IngestRequest,
    IngestResult,
)
from finance_analysis_agent.utils.time import utcnow

STATUS_SEQUENCE = [
    ImportBatchStatus.RECEIVED,
    ImportBatchStatus.PARSED,
    ImportBatchStatus.STAGED,
    ImportBatchStatus.NORMALIZED,
    ImportBatchStatus.DEDUPED,
    ImportBatchStatus.REVIEWED,
    ImportBatchStatus.FINALIZED,
]
TERMINAL_STATUSES = {ImportBatchStatus.FINALIZED, ImportBatchStatus.FAILED}
LOGGER = logging.getLogger(__name__)


def _normalize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _canonical_raw_payload(txn: CanonicalTransactionInput, row_index: int) -> dict[str, object]:
    return {
        "row_index": row_index,
        "account_id": txn.account_id,
        "posted_date": txn.posted_date.isoformat(),
        "effective_date": txn.effective_date.isoformat() if txn.effective_date else None,
        "amount": _normalize_decimal(txn.amount),
        "currency": txn.currency,
        "pending_status": txn.pending_status,
        "source_kind": txn.source_kind,
        "source_transaction_id": txn.source_transaction_id,
    }


def _derive_synthetic_source_transaction_id(
    *,
    source_fingerprint: str,
    row_index: int,
    txn: CanonicalTransactionInput,
) -> str:
    canonical = {
        "source_fingerprint": source_fingerprint,
        "row_index": row_index,
        "account_id": txn.account_id,
        "posted_date": txn.posted_date.isoformat(),
        "effective_date": txn.effective_date.isoformat() if txn.effective_date else None,
        "amount": _normalize_decimal(txn.amount),
        "currency": txn.currency,
        "pending_status": txn.pending_status,
        "source_kind": txn.source_kind,
        "original_statement": txn.original_statement,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"syn_{digest}"


def _is_valid_transition(from_status: ImportBatchStatus, to_status: ImportBatchStatus) -> bool:
    if from_status not in STATUS_SEQUENCE and from_status != ImportBatchStatus.FAILED:
        label = from_status.value if isinstance(from_status, ImportBatchStatus) else str(from_status)
        raise ValueError(f"Unknown status: {label}")
    if to_status not in STATUS_SEQUENCE and to_status != ImportBatchStatus.FAILED:
        label = to_status.value if isinstance(to_status, ImportBatchStatus) else str(to_status)
        raise ValueError(f"Unknown status: {label}")

    if from_status in TERMINAL_STATUSES:
        return False
    if to_status == ImportBatchStatus.FAILED:
        return True
    if to_status == from_status:
        return False
    return STATUS_SEQUENCE.index(to_status) == STATUS_SEQUENCE.index(from_status) + 1


def _record_status_event(
    *,
    batch_id: str,
    from_status: ImportBatchStatus | None,
    to_status: ImportBatchStatus,
    reason: str | None,
    actor: str,
    session: Session,
    changed_at: datetime | None = None,
) -> None:
    session.add(
        ImportBatchStatusEvent(
            id=str(uuid4()),
            batch_id=batch_id,
            from_status=from_status.value if from_status else None,
            to_status=to_status.value,
            reason=reason,
            actor=actor,
            changed_at=changed_at or utcnow(),
        )
    )


def transition_import_batch_status(
    batch_id: str,
    to_status: ImportBatchStatus,
    reason: str | None,
    actor: str,
    session: Session,
) -> None:
    """Apply strict ImportBatch status transitions and persist status events."""

    batch = session.get(ImportBatch, batch_id)
    if batch is None:
        raise ValueError(f"ImportBatch not found: {batch_id}")

    from_status = ImportBatchStatus(batch.status)
    if not _is_valid_transition(from_status, to_status):
        raise ValueError(f"Invalid ImportBatch transition: {from_status.value} -> {to_status.value}")

    event_timestamp = utcnow()
    batch.status = to_status.value
    if to_status == ImportBatchStatus.FINALIZED:
        batch.finalized_at = event_timestamp
    if to_status == ImportBatchStatus.FAILED and reason:
        batch.error_summary = reason

    _record_status_event(
        batch_id=batch_id,
        from_status=from_status,
        to_status=to_status,
        reason=reason,
        actor=actor,
        session=session,
        changed_at=event_timestamp,
    )


def ingest_transactions(request: IngestRequest, session: Session) -> IngestResult:
    """Ingest canonical transactions with idempotency and ImportBatch lifecycle tracking."""

    source_fingerprint, fingerprint_algo = compute_source_fingerprint(
        source_type=request.source_type,
        schema_version=request.schema_version,
        payload_bytes=request.payload_bytes,
        manual_payload=request.manual_payload,
    )

    existing_batches = session.scalars(
        select(ImportBatch)
        .where(
            ImportBatch.source_type == request.source_type.value,
            ImportBatch.source_fingerprint == source_fingerprint,
        )
        .order_by(ImportBatch.received_at.desc())
    ).all()

    latest_finalized = next(
        (batch for batch in existing_batches if batch.status == ImportBatchStatus.FINALIZED.value),
        None,
    )
    if request.conflict_mode == ConflictMode.NORMAL and latest_finalized is not None:
        return IngestResult(
            batch_id=latest_finalized.id,
            source_fingerprint=source_fingerprint,
            replayed=True,
            created_new_batch=False,
            inserted_transactions_count=0,
            skipped_transactions_count=0,
            final_status=ImportBatchStatus.FINALIZED,
            status_history=[ImportBatchStatus.FINALIZED],
        )

    if request.conflict_mode == ConflictMode.FORCE and not (request.override_reason or "").strip():
        raise ValueError("Force mode requires a non-empty override_reason")

    override_of_batch_id = existing_batches[0].id if (request.conflict_mode == ConflictMode.FORCE and existing_batches) else None

    batch = ImportBatch(
        id=str(uuid4()),
        source_type=request.source_type.value,
        source_ref=request.source_ref,
        source_fingerprint=source_fingerprint,
        fingerprint_algo=fingerprint_algo,
        schema_version=request.schema_version,
        conflict_mode=request.conflict_mode.value,
        override_reason=request.override_reason,
        override_of_batch_id=override_of_batch_id,
        status=ImportBatchStatus.RECEIVED.value,
        received_at=utcnow(),
    )
    session.add(batch)
    session.flush()

    status_history = [ImportBatchStatus.RECEIVED]
    _record_status_event(
        batch_id=batch.id,
        from_status=None,
        to_status=ImportBatchStatus.RECEIVED,
        reason="batch created",
        actor=request.actor,
        session=session,
    )

    inserted_transactions = 0
    skipped_transactions = 0

    try:
        for index, txn in enumerate(request.transactions, start=1):
            session.add(
                RawTransaction(
                    id=str(uuid4()),
                    import_batch_id=batch.id,
                    raw_payload_json=(
                        txn.raw_payload
                        if txn.raw_payload is not None
                        else _canonical_raw_payload(txn, index)
                    ),
                    page_no=txn.page_no,
                    row_no=txn.row_no or index,
                    extraction_confidence=txn.extraction_confidence,
                    parse_status=txn.parse_status,
                    error_code=txn.error_code,
                )
            )

        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.PARSED,
            reason="raw rows persisted",
            actor=request.actor,
            session=session,
        )
        status_history.append(ImportBatchStatus.PARSED)

        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.STAGED,
            reason="rows staged",
            actor=request.actor,
            session=session,
        )
        status_history.append(ImportBatchStatus.STAGED)

        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.NORMALIZED,
            reason="canonical normalization complete",
            actor=request.actor,
            session=session,
        )
        status_history.append(ImportBatchStatus.NORMALIZED)

        for index, txn in enumerate(request.transactions, start=1):
            source_kind = txn.source_kind or request.source_type.value
            source_transaction_id = txn.source_transaction_id or _derive_synthetic_source_transaction_id(
                source_fingerprint=source_fingerprint,
                row_index=index,
                txn=txn,
            )

            existing_transaction_id = session.scalar(
                select(Transaction.id).where(
                    Transaction.account_id == txn.account_id,
                    Transaction.source_kind == source_kind,
                    Transaction.source_transaction_id == source_transaction_id,
                )
            )
            if existing_transaction_id is not None:
                skipped_transactions += 1
                continue

            transaction_timestamp = utcnow()
            session.add(
                Transaction(
                    id=str(uuid4()),
                    account_id=txn.account_id,
                    posted_date=txn.posted_date,
                    effective_date=txn.effective_date,
                    amount=txn.amount,
                    currency=txn.currency,
                    original_amount=txn.original_amount,
                    original_currency=txn.original_currency,
                    pending_status=txn.pending_status,
                    original_statement=txn.original_statement,
                    merchant_id=txn.merchant_id,
                    category_id=txn.category_id,
                    excluded=txn.excluded,
                    notes=txn.notes,
                    source_kind=source_kind,
                    source_transaction_id=source_transaction_id,
                    import_batch_id=batch.id,
                    transfer_group_id=txn.transfer_group_id,
                    created_at=transaction_timestamp,
                    updated_at=transaction_timestamp,
                )
            )
            inserted_transactions += 1

        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.DEDUPED,
            reason="duplicate-safe transaction write complete",
            actor=request.actor,
            session=session,
        )
        status_history.append(ImportBatchStatus.DEDUPED)

        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.REVIEWED,
            reason="no review hold in TUR-32",
            actor=request.actor,
            session=session,
        )
        status_history.append(ImportBatchStatus.REVIEWED)

        transition_import_batch_status(
            batch_id=batch.id,
            to_status=ImportBatchStatus.FINALIZED,
            reason="ingest complete",
            actor=request.actor,
            session=session,
        )
        status_history.append(ImportBatchStatus.FINALIZED)

        session.flush()
    except Exception as exc:
        if ImportBatchStatus(batch.status) not in TERMINAL_STATUSES:
            try:
                transition_import_batch_status(
                    batch_id=batch.id,
                    to_status=ImportBatchStatus.FAILED,
                    reason=str(exc),
                    actor=request.actor,
                    session=session,
                )
                status_history.append(ImportBatchStatus.FAILED)
                session.flush()
            except Exception:
                LOGGER.exception(
                    "Failed to transition ImportBatch %s to failed status after ingest error",
                    batch.id,
                )
        raise

    return IngestResult(
        batch_id=batch.id,
        source_fingerprint=source_fingerprint,
        replayed=False,
        created_new_batch=True,
        inserted_transactions_count=inserted_transactions,
        skipped_transactions_count=skipped_transactions,
        final_status=ImportBatchStatus.FINALIZED,
        status_history=status_history,
    )
