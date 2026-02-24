"""Typed request/response contracts for ingestion and ImportBatch idempotency."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any


class SourceType(StrEnum):
    PDF = "pdf"
    CSV = "csv"
    MANUAL = "manual"


class ConflictMode(StrEnum):
    NORMAL = "normal"
    FORCE = "force"


class ImportBatchStatus(StrEnum):
    RECEIVED = "received"
    PARSED = "parsed"
    STAGED = "staged"
    NORMALIZED = "normalized"
    DEDUPED = "deduped"
    REVIEWED = "reviewed"
    FINALIZED = "finalized"
    FAILED = "failed"


@dataclass(slots=True)
class CanonicalTransactionInput:
    account_id: str
    posted_date: date
    amount: Decimal
    currency: str
    pending_status: str
    source_kind: str | None = None
    effective_date: date | None = None
    original_amount: Decimal | None = None
    original_currency: str | None = None
    original_statement: str | None = None
    merchant_id: str | None = None
    category_id: str | None = None
    excluded: bool = False
    notes: str | None = None
    source_transaction_id: str | None = None
    transfer_group_id: str | None = None
    raw_payload: dict[str, Any] | None = None
    page_no: int | None = None
    row_no: int | None = None
    extraction_confidence: float | None = None
    parse_status: str = "parsed"
    error_code: str | None = None


@dataclass(slots=True)
class IngestRequest:
    source_type: SourceType
    schema_version: str
    transactions: list[CanonicalTransactionInput]
    source_ref: str | None = None
    conflict_mode: ConflictMode = ConflictMode.NORMAL
    override_reason: str | None = None
    payload_bytes: bytes | None = None
    manual_payload: dict[str, Any] | list[Any] | None = None
    actor: str = "system"


@dataclass(slots=True)
class IngestResult:
    batch_id: str
    source_fingerprint: str
    replayed: bool
    created_new_batch: bool
    inserted_transactions_count: int
    skipped_transactions_count: int
    final_status: ImportBatchStatus
    status_history: list[ImportBatchStatus] = field(default_factory=list)

