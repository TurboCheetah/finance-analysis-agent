from __future__ import annotations

from datetime import date
from pathlib import Path

from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.types import (
    PdfDiagnostics,
    PdfExtractedRow,
    PdfExtractionTier,
    PdfOcrMode,
    PdfSubagentRequest,
    PdfSubagentResponse,
)
from finance_analysis_agent.pdf_contract.validators import (
    validate_pdf_subagent_request,
    validate_pdf_subagent_response,
)


def _valid_request(statement_path: Path) -> PdfSubagentRequest:
    return PdfSubagentRequest(
        contract_version="1.2.3",
        statement_path=str(statement_path),
        account_id="acct-1",
        schema_version="1.0.0",
        actor="tester",
        confidence_threshold=0.8,
        conflict_mode=ConflictMode.NORMAL,
        ocr_mode=PdfOcrMode.AUTO,
    )


def _valid_response() -> PdfSubagentResponse:
    return PdfSubagentResponse(
        contract_version="1.5.0",
        subagent_version_hash="subagent-sha",
        extraction_tiers_used=[PdfExtractionTier.TEXT_HEURISTIC],
        rows=[
            PdfExtractedRow(
                account_id="acct-1",
                posted_date=date(2026, 1, 2),
                amount="-8.55",
                currency="USD",
                pending_status="posted",
                confidence=0.95,
                parse_status="parsed",
                page_no=1,
                row_no=1,
            )
        ],
        diagnostics=PdfDiagnostics(run_summary={"pages": 1}),
    )


def test_request_and_response_validation_accept_valid_payloads(tmp_path: Path) -> None:
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake pdf")

    request_errors = validate_pdf_subagent_request(_valid_request(statement))
    response_errors = validate_pdf_subagent_response(_valid_response())

    assert request_errors == []
    assert response_errors == []


def test_request_validation_rejects_relative_path_and_invalid_fields() -> None:
    bad = PdfSubagentRequest(
        contract_version="one.two.three",
        statement_path="relative/path.pdf",
        account_id="",
        schema_version="1.0.0",
        actor="",
        confidence_threshold=1.2,
    )

    errors = validate_pdf_subagent_request(bad)
    fields = {error.details.get("field") for error in errors}

    assert len(errors) >= 4
    assert "statement_path" in fields
    assert "account_id" in fields
    assert "actor" in fields
    assert "confidence_threshold" in fields


def test_response_validation_rejects_invalid_rows_and_tiers() -> None:
    response = PdfSubagentResponse(
        contract_version="1.0.0",
        subagent_version_hash="subagent-sha",
        extraction_tiers_used=["unknown-tier"],
        rows=[
            PdfExtractedRow(
                account_id="acct-1",
                posted_date=None,
                amount=None,
                currency=None,
                pending_status=None,
                confidence=1.4,
                parse_status="parsed",
                page_no=0,
                row_no=-1,
            )
        ],
        diagnostics=PdfDiagnostics(),
    )

    errors = validate_pdf_subagent_response(response)
    messages = [error.message for error in errors]

    assert any("unknown extraction tier" in message for message in messages)
    assert any("missing required field: posted_date" in message for message in messages)
    assert any("missing required field: amount" in message for message in messages)
    assert any("row confidence" in message for message in messages)
