"""Types for PDF subagent contract and orchestrator handoff."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any

from finance_analysis_agent.ingest.types import ConflictMode


class PdfExtractionTier(StrEnum):
    TEXT_HEURISTIC = "text_heuristic"
    TABLE_ASSIST = "table_assist"
    OCR_FALLBACK = "ocr_fallback"


class PdfOcrMode(StrEnum):
    AUTO = "auto"
    OFF = "off"
    FORCE = "force"


@dataclass(slots=True)
class PdfContractError:
    code: str
    message: str
    stage: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PdfDiagnostics:
    run_summary: dict[str, Any] = field(default_factory=dict)
    page_notes: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class PdfExtractedBalance:
    balance_type: str
    amount: Decimal | str
    currency: str
    as_of_date: date | str | None = None
    confidence: float | None = None
    provenance: dict[str, Any] | None = None


@dataclass(slots=True)
class PdfExtractedRow:
    posted_date: date | str | None
    amount: Decimal | str | None
    currency: str | None
    pending_status: str | None
    account_id: str | None = None
    effective_date: date | str | None = None
    original_amount: Decimal | str | None = None
    original_currency: str | None = None
    original_statement: str | None = None
    merchant_id: str | None = None
    category_id: str | None = None
    excluded: bool | None = None
    notes: str | None = None
    source_transaction_id: str | None = None
    transfer_group_id: str | None = None
    confidence: float | None = None
    parse_status: str = "parsed"
    error_code: str | None = None
    page_no: int | None = None
    row_no: int | None = None
    provenance: dict[str, Any] | None = None


@dataclass(slots=True)
class PdfSubagentRequest:
    contract_version: str
    statement_path: str
    account_id: str
    schema_version: str
    actor: str
    confidence_threshold: float
    template_hint: str | None = None
    source_ref: str | None = None
    conflict_mode: ConflictMode = ConflictMode.NORMAL
    override_reason: str | None = None
    ocr_mode: PdfOcrMode = PdfOcrMode.AUTO
    page_range: tuple[int, int] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PdfSubagentResponse:
    contract_version: str
    subagent_version_hash: str
    extraction_tiers_used: list[PdfExtractionTier | str]
    rows: list[PdfExtractedRow]
    diagnostics: PdfDiagnostics
    balances: list[PdfExtractedBalance] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PdfOrchestratorResult:
    ok: bool
    status: str
    batch_id: str | None
    run_metadata_id: str
    inserted_rows: int
    skipped_rows: int
    warnings: list[PdfContractError] = field(default_factory=list)
    errors: list[PdfContractError] = field(default_factory=list)

