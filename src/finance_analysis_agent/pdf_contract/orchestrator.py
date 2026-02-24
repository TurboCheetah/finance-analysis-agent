"""Runtime orchestrator for PDF subagent contract handoff and ingest persistence."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from finance_analysis_agent.ingest.import_batch_service import ingest_transactions
from finance_analysis_agent.ingest.types import (
    CanonicalTransactionInput,
    ConflictMode,
    IngestRequest,
    SourceType,
)
from finance_analysis_agent.pdf_contract.adapter import PdfSubagentAdapter
from finance_analysis_agent.pdf_contract.types import (
    PdfContractError,
    PdfExtractedRow,
    PdfOcrMode,
    PdfOrchestratorResult,
    PdfSubagentRequest,
)
from finance_analysis_agent.pdf_contract.validators import (
    validate_contract_version,
    validate_pdf_subagent_request,
    validate_pdf_subagent_response,
)
from finance_analysis_agent.provenance.audit_writers import finish_run_metadata, start_run_metadata
from finance_analysis_agent.provenance.types import RunMetadataFinishRequest, RunMetadataStartRequest

EXPECTED_CONTRACT_VERSION = "1.0.0"
ORCHESTRATOR_COMPONENT_VERSION = "tur34-pdf-orchestrator-1"
PIPELINE_NAME = "pdf_subagent_handoff"


def _error(code: str, message: str, stage: str, details: dict[str, Any] | None = None) -> PdfContractError:
    return PdfContractError(code=code, message=message, stage=stage, details=details or {})


def _serialize_error(error: PdfContractError) -> dict[str, Any]:
    return {
        "code": error.code,
        "message": error.message,
        "stage": error.stage,
        "details": error.details,
    }


def _normalize_for_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _enumish_value(value: Any, enum_type: type[Any]) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    if isinstance(value, str):
        try:
            return str(enum_type(value).value)
        except Exception:
            return value
    return str(value)


def _start_run(request: PdfSubagentRequest, session: Session) -> str:
    ocr_mode = _enumish_value(request.ocr_mode, PdfOcrMode)
    conflict_mode = _enumish_value(request.conflict_mode, ConflictMode)

    run = start_run_metadata(
        RunMetadataStartRequest(
            pipeline_name=PIPELINE_NAME,
            code_version=ORCHESTRATOR_COMPONENT_VERSION,
            schema_version=request.schema_version,
            config_hash=_normalize_for_hash(
                {
                    "expected_contract_version": EXPECTED_CONTRACT_VERSION,
                    "ocr_mode": ocr_mode,
                    "conflict_mode": conflict_mode,
                    "confidence_threshold": request.confidence_threshold,
                }
            ),
            status="running",
            diagnostics_json={
                "contract_version_expected": EXPECTED_CONTRACT_VERSION,
                "contract_version_received": request.contract_version,
                "subagent_version_hash": None,
                "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
                "validation_summary": {
                    "request_errors": 0,
                    "response_errors": 0,
                    "version_errors": 0,
                },
            },
        ),
        session,
    )
    return run.id


def _finalize_run(
    *,
    run_metadata_id: str,
    status: str,
    diagnostics_json: dict[str, Any],
    session: Session,
) -> None:
    finish_run_metadata(
        RunMetadataFinishRequest(
            run_metadata_id=run_metadata_id,
            status=status,
            diagnostics_json=diagnostics_json,
        ),
        session,
    )


def _parse_date(value: date | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _parse_decimal(value: Decimal | str | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _row_to_canonical(
    *,
    row: PdfExtractedRow,
    request: PdfSubagentRequest,
) -> CanonicalTransactionInput:
    account_id = row.account_id or request.account_id
    posted_date = _parse_date(row.posted_date)
    amount = _parse_decimal(row.amount)
    currency = row.currency
    pending_status = row.pending_status

    if posted_date is None or amount is None or currency is None or pending_status is None:
        raise ValueError("parsed row missing required canonical values after mapping")

    original_amount = _parse_decimal(row.original_amount)
    effective_date = _parse_date(row.effective_date)

    return CanonicalTransactionInput(
        account_id=account_id,
        posted_date=posted_date,
        amount=amount,
        currency=currency,
        pending_status=pending_status,
        source_kind=SourceType.PDF.value,
        effective_date=effective_date,
        original_amount=original_amount,
        original_currency=row.original_currency,
        original_statement=row.original_statement,
        merchant_id=row.merchant_id,
        category_id=row.category_id,
        excluded=bool(row.excluded) if row.excluded is not None else False,
        notes=row.notes,
        source_transaction_id=row.source_transaction_id,
        transfer_group_id=row.transfer_group_id,
        raw_payload={
            "posted_date": row.posted_date.isoformat() if isinstance(row.posted_date, date) else row.posted_date,
            "amount": str(row.amount) if row.amount is not None else None,
            "currency": row.currency,
            "pending_status": row.pending_status,
            "parse_status": row.parse_status,
            "confidence": row.confidence,
            "error_code": row.error_code,
            "page_no": row.page_no,
            "row_no": row.row_no,
            "provenance": row.provenance,
        },
        page_no=row.page_no,
        row_no=row.row_no,
        extraction_confidence=row.confidence,
        parse_status=row.parse_status,
        error_code=row.error_code,
    )


def _row_passes_threshold(row: PdfExtractedRow, threshold: float) -> bool:
    # Treat missing confidence as unknown/low confidence so thresholding is conservative.
    confidence = 0.0 if row.confidence is None else float(row.confidence)
    return confidence >= threshold


def run_pdf_subagent_handoff(
    request: PdfSubagentRequest,
    adapter: PdfSubagentAdapter,
    session: Session,
) -> PdfOrchestratorResult:
    """Execute PDF subagent extraction, validate payloads, and hand off valid rows to ingest."""

    run_metadata_id = _start_run(request, session)

    request_errors = validate_pdf_subagent_request(request)
    if request_errors:
        diagnostics = {
            "contract_version_expected": EXPECTED_CONTRACT_VERSION,
            "contract_version_received": request.contract_version,
            "subagent_version_hash": None,
            "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
            "validation_summary": {
                "request_errors": len(request_errors),
                "response_errors": 0,
                "version_errors": 0,
            },
            "errors": [_serialize_error(error) for error in request_errors],
        }
        _finalize_run(
            run_metadata_id=run_metadata_id,
            status="failed",
            diagnostics_json=diagnostics,
            session=session,
        )
        session.flush()
        return PdfOrchestratorResult(
            ok=False,
            status="failed",
            batch_id=None,
            run_metadata_id=run_metadata_id,
            inserted_rows=0,
            skipped_rows=0,
            warnings=[],
            errors=request_errors,
        )

    try:
        response = adapter.extract(request)
    except Exception as exc:
        error = _error(
            code="adapter_failure",
            message="PDF subagent adapter failed",
            stage="adapter_call",
            details={"exception": str(exc)},
        )
        diagnostics = {
            "contract_version_expected": EXPECTED_CONTRACT_VERSION,
            "contract_version_received": request.contract_version,
            "subagent_version_hash": None,
            "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
            "validation_summary": {
                "request_errors": 0,
                "response_errors": 0,
                "version_errors": 0,
            },
            "errors": [_serialize_error(error)],
        }
        _finalize_run(
            run_metadata_id=run_metadata_id,
            status="failed",
            diagnostics_json=diagnostics,
            session=session,
        )
        session.flush()
        return PdfOrchestratorResult(
            ok=False,
            status="failed",
            batch_id=None,
            run_metadata_id=run_metadata_id,
            inserted_rows=0,
            skipped_rows=0,
            warnings=[],
            errors=[error],
        )

    response_errors = validate_pdf_subagent_response(response)
    version_error = validate_contract_version(EXPECTED_CONTRACT_VERSION, response.contract_version)
    version_errors = [version_error] if version_error is not None else []
    if response_errors or version_errors:
        errors = response_errors + version_errors
        diagnostics = {
            "contract_version_expected": EXPECTED_CONTRACT_VERSION,
            "contract_version_received": response.contract_version,
            "subagent_version_hash": response.subagent_version_hash,
            "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
            "validation_summary": {
                "request_errors": 0,
                "response_errors": len(response_errors),
                "version_errors": len(version_errors),
            },
            "errors": [_serialize_error(error) for error in errors],
        }
        _finalize_run(
            run_metadata_id=run_metadata_id,
            status="failed",
            diagnostics_json=diagnostics,
            session=session,
        )
        session.flush()
        return PdfOrchestratorResult(
            ok=False,
            status="failed",
            batch_id=None,
            run_metadata_id=run_metadata_id,
            inserted_rows=0,
            skipped_rows=0,
            warnings=[],
            errors=errors,
        )

    warnings: list[PdfContractError] = [
        _error(
            code="response_invalid",
            message=warning,
            stage="response_warnings",
            details={"from_subagent": True},
        )
        for warning in response.warnings
    ]

    valid_rows: list[CanonicalTransactionInput] = []
    skipped_rows = 0
    row_diagnostics: list[dict[str, Any]] = []
    for idx, row in enumerate(response.rows, start=1):
        normalized_status = (row.parse_status or "").strip()
        if normalized_status != "parsed":
            skipped_rows += 1
            warnings.append(
                _error(
                    code="response_invalid",
                    message="row skipped due to parse_status",
                    stage="row_filter",
                    details={"row_index": idx, "parse_status": normalized_status},
                )
            )
            row_diagnostics.append(
                {
                    "row_index": idx,
                    "ingested": False,
                    "reason": "parse_status",
                    "parse_status": normalized_status,
                }
            )
            continue

        if not _row_passes_threshold(row, request.confidence_threshold):
            skipped_rows += 1
            warnings.append(
                _error(
                    code="response_invalid",
                    message="row skipped due to confidence threshold",
                    stage="row_filter",
                    details={
                        "row_index": idx,
                        "confidence": row.confidence,
                        "threshold": request.confidence_threshold,
                    },
                )
            )
            row_diagnostics.append(
                {
                    "row_index": idx,
                    "ingested": False,
                    "reason": "confidence_threshold",
                    "confidence": row.confidence,
                    "threshold": request.confidence_threshold,
                }
            )
            continue

        try:
            valid_rows.append(_row_to_canonical(row=row, request=request))
            row_diagnostics.append({"row_index": idx, "ingested": True})
        except (ValueError, InvalidOperation) as exc:
            skipped_rows += 1
            warnings.append(
                _error(
                    code="response_invalid",
                    message="row skipped due to canonical mapping failure",
                    stage="row_mapping",
                    details={"row_index": idx, "exception": str(exc)},
                )
            )
            row_diagnostics.append(
                {
                    "row_index": idx,
                    "ingested": False,
                    "reason": "mapping_failure",
                    "exception": str(exc),
                }
            )

    statement_path = Path(request.statement_path)
    try:
        payload_bytes = statement_path.read_bytes()
    except OSError as exc:
        error = _error(
            code="file_read_failure",
            message="Failed to read statement file",
            stage="file_read",
            details={"statement_path": request.statement_path, "exception": str(exc)},
        )
        diagnostics = {
            "contract_version_expected": EXPECTED_CONTRACT_VERSION,
            "contract_version_received": response.contract_version,
            "subagent_version_hash": response.subagent_version_hash,
            "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
            "validation_summary": {
                "request_errors": 0,
                "response_errors": 0,
                "version_errors": 0,
            },
            "row_summary": {
                "total_rows": len(response.rows),
                "valid_rows": len(valid_rows),
                "skipped_rows": skipped_rows,
                "details": row_diagnostics,
            },
            "errors": [_serialize_error(error)],
            "warnings": [_serialize_error(warning) for warning in warnings],
        }
        _finalize_run(
            run_metadata_id=run_metadata_id,
            status="failed",
            diagnostics_json=diagnostics,
            session=session,
        )
        session.flush()
        return PdfOrchestratorResult(
            ok=False,
            status="failed",
            batch_id=None,
            run_metadata_id=run_metadata_id,
            inserted_rows=0,
            skipped_rows=skipped_rows,
            warnings=warnings,
            errors=[error],
        )

    try:
        ingest_result = ingest_transactions(
            IngestRequest(
                source_type=SourceType.PDF,
                schema_version=request.schema_version,
                transactions=valid_rows,
                source_ref=request.source_ref or request.statement_path,
                conflict_mode=request.conflict_mode,
                override_reason=request.override_reason,
                payload_bytes=payload_bytes,
                actor=request.actor,
            ),
            session,
        )
    except Exception as exc:
        error = _error(
            code="ingest_failure",
            message="Failed to ingest validated PDF rows",
            stage="ingest",
            details={"exception": str(exc)},
        )
        diagnostics = {
            "contract_version_expected": EXPECTED_CONTRACT_VERSION,
            "contract_version_received": response.contract_version,
            "subagent_version_hash": response.subagent_version_hash,
            "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
            "validation_summary": {
                "request_errors": 0,
                "response_errors": 0,
                "version_errors": 0,
            },
            "row_summary": {
                "total_rows": len(response.rows),
                "valid_rows": len(valid_rows),
                "skipped_rows": skipped_rows,
                "details": row_diagnostics,
            },
            "errors": [_serialize_error(error)],
            "warnings": [_serialize_error(warning) for warning in warnings],
        }
        _finalize_run(
            run_metadata_id=run_metadata_id,
            status="failed",
            diagnostics_json=diagnostics,
            session=session,
        )
        session.flush()
        return PdfOrchestratorResult(
            ok=False,
            status="failed",
            batch_id=None,
            run_metadata_id=run_metadata_id,
            inserted_rows=0,
            skipped_rows=skipped_rows,
            warnings=warnings,
            errors=[error],
        )

    total_skipped = skipped_rows + ingest_result.skipped_transactions_count
    final_status = "success_with_warnings" if total_skipped > 0 else "success"
    diagnostics = {
        "contract_version_expected": EXPECTED_CONTRACT_VERSION,
        "contract_version_received": response.contract_version,
        "subagent_version_hash": response.subagent_version_hash,
        "orchestrator_component_version": ORCHESTRATOR_COMPONENT_VERSION,
        "validation_summary": {
            "request_errors": 0,
            "response_errors": 0,
            "version_errors": 0,
        },
        "row_summary": {
            "total_rows": len(response.rows),
            "valid_rows": len(valid_rows),
            "skipped_rows": skipped_rows,
            "ingest_inserted_rows": ingest_result.inserted_transactions_count,
            "ingest_skipped_rows": ingest_result.skipped_transactions_count,
            "details": row_diagnostics,
        },
        "warnings": [_serialize_error(warning) for warning in warnings],
    }

    _finalize_run(
        run_metadata_id=run_metadata_id,
        status=final_status,
        diagnostics_json=diagnostics,
        session=session,
    )
    session.flush()

    return PdfOrchestratorResult(
        ok=True,
        status=final_status,
        batch_id=ingest_result.batch_id,
        run_metadata_id=run_metadata_id,
        inserted_rows=ingest_result.inserted_transactions_count,
        skipped_rows=total_skipped,
        warnings=warnings,
        errors=[],
    )
