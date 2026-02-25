from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, ImportBatch, ReviewItem, RunMetadata, Transaction
from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.adapter import DeterministicFakePdfSubagentAdapter
from finance_analysis_agent.pdf_contract.orchestrator import run_pdf_subagent_handoff
from finance_analysis_agent.pdf_contract.types import (
    PdfDiagnostics,
    PdfExtractedRow,
    PdfExtractionTier,
    PdfSubagentRequest,
    PdfSubagentResponse,
)

def _seed_account(session: Session, account_id: str = "acct-pdf") -> None:
    session.add(
        Account(
            id=account_id,
            name="Checking",
            type="checking",
            currency="USD",
            opened_at=None,
            closed_at=None,
            institution=None,
            metadata_json=None,
        )
    )
    session.commit()


def _request(statement_path: Path) -> PdfSubagentRequest:
    return PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(statement_path),
        account_id="acct-pdf",
        schema_version="1.0.0",
        actor="pdf-orchestrator-test",
        confidence_threshold=0.8,
        conflict_mode=ConflictMode.NORMAL,
        source_ref="fixtures/statement.pdf",
    )


def _response(rows: list[PdfExtractedRow], contract_version: str = "1.3.0") -> PdfSubagentResponse:
    return PdfSubagentResponse(
        contract_version=contract_version,
        subagent_version_hash="subagent-abc123",
        extraction_tiers_used=[PdfExtractionTier.TEXT_HEURISTIC],
        rows=rows,
        diagnostics=PdfDiagnostics(
            run_summary={"pages": 1},
            page_notes=[{"page_no": 1, "note": "ok"}],
        ),
    )


def test_valid_handoff_persists_run_metadata_and_ingests_rows(db_session: Session, tmp_path: Path) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake-pdf-content")

    adapter = DeterministicFakePdfSubagentAdapter(
        response=_response(
            [
                PdfExtractedRow(
                    account_id="acct-pdf",
                    posted_date=date(2026, 5, 1),
                    amount=Decimal("-4.25"),
                    currency="USD",
                    pending_status="posted",
                    confidence=0.95,
                    parse_status="parsed",
                    page_no=1,
                    row_no=1,
                    source_transaction_id="pdf-1",
                )
            ]
        )
    )

    result = run_pdf_subagent_handoff(_request(statement), adapter, db_session)
    db_session.commit()

    assert result.ok is True
    assert result.status == "success"
    assert result.batch_id is not None
    assert result.inserted_rows == 1
    assert result.skipped_rows == 0

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.status == "success"
    assert run_metadata.completed_at is not None
    assert run_metadata.diagnostics_json is not None
    assert run_metadata.diagnostics_json["contract_version_expected"] == "1.0.0"
    assert run_metadata.diagnostics_json["contract_version_received"] == "1.3.0"
    assert run_metadata.diagnostics_json["subagent_version_hash"] == "subagent-abc123"
    assert run_metadata.diagnostics_json["effective_threshold"]["row_confidence_threshold"] == 0.8
    assert run_metadata.diagnostics_json["threshold_source"] == "request.confidence_threshold"
    assert run_metadata.diagnostics_json["review_summary"]["total_items_created"] == 0

    assert db_session.scalar(select(func.count()).select_from(ImportBatch)) == 1
    assert db_session.scalar(select(func.count()).select_from(Transaction)) == 1
    assert db_session.scalar(select(func.count()).select_from(ReviewItem)) == 0


def test_invalid_request_fails_without_ingest(db_session: Session) -> None:
    request = PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path="relative/path.pdf",
        account_id="acct-pdf",
        schema_version="1.0.0",
        actor="tester",
        confidence_threshold=0.8,
    )
    adapter = DeterministicFakePdfSubagentAdapter(error=RuntimeError("should not run"))

    result = run_pdf_subagent_handoff(request, adapter, db_session)
    db_session.commit()

    assert result.ok is False
    assert result.status == "failed"
    assert result.batch_id is None
    assert any(error.code == "request_invalid" for error in result.errors)
    assert db_session.scalar(select(func.count()).select_from(ImportBatch)) == 0

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.status == "failed"


def test_invalid_response_fails_without_ingest(db_session: Session, tmp_path: Path) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake-pdf-content")

    adapter = DeterministicFakePdfSubagentAdapter(
        response=PdfSubagentResponse(
            contract_version="1.0.0",
            subagent_version_hash="subagent-abc123",
            extraction_tiers_used=["bad-tier"],
            rows=[],
            diagnostics=PdfDiagnostics(),
        )
    )

    result = run_pdf_subagent_handoff(_request(statement), adapter, db_session)
    db_session.commit()

    assert result.ok is False
    assert result.status == "failed"
    assert any(error.code == "response_invalid" for error in result.errors)
    assert db_session.scalar(select(func.count()).select_from(ImportBatch)) == 0


def test_partial_rows_ingest_valid_rows_and_mark_warnings(db_session: Session, tmp_path: Path) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake-pdf-content")

    adapter = DeterministicFakePdfSubagentAdapter(
        response=_response(
            [
                PdfExtractedRow(
                    account_id="acct-pdf",
                    posted_date="2026-05-01",
                    amount="-10.00",
                    currency="USD",
                    pending_status="posted",
                    confidence=0.99,
                    parse_status="parsed",
                    row_no=1,
                    page_no=1,
                    source_transaction_id="row-1",
                ),
                PdfExtractedRow(
                    account_id="acct-pdf",
                    posted_date="2026-05-02",
                    amount="-11.00",
                    currency="USD",
                    pending_status="posted",
                    confidence=0.4,
                    parse_status="parsed",
                    row_no=2,
                    page_no=1,
                    source_transaction_id="row-2",
                ),
                PdfExtractedRow(
                    account_id="acct-pdf",
                    posted_date="2026-05-03",
                    amount="-12.00",
                    currency="USD",
                    pending_status="posted",
                    confidence=0.9,
                    parse_status="parse_error",
                    error_code="bad_date",
                    row_no=3,
                    page_no=1,
                    source_transaction_id="row-3",
                ),
            ]
        )
    )

    result = run_pdf_subagent_handoff(_request(statement), adapter, db_session)
    db_session.commit()

    assert result.ok is True
    assert result.status == "success_with_warnings"
    assert result.inserted_rows == 1
    assert result.skipped_rows == 2
    assert len(result.warnings) >= 2

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.status == "success_with_warnings"
    assert run_metadata.diagnostics_json is not None
    assert run_metadata.diagnostics_json["effective_threshold"]["row_confidence_threshold"] == 0.8
    assert run_metadata.diagnostics_json["effective_threshold"]["page_confidence_threshold"] == 0.75
    assert run_metadata.diagnostics_json["threshold_source"] == "request.confidence_threshold"
    assert run_metadata.diagnostics_json["row_summary"]["total_rows"] == 3
    assert run_metadata.diagnostics_json["row_summary"]["valid_rows"] == 1
    assert run_metadata.diagnostics_json["row_summary"]["skipped_rows"] == 2
    assert run_metadata.diagnostics_json["review_summary"]["total_items_created"] == 3
    assert run_metadata.diagnostics_json["review_summary"]["by_reason"]["low_confidence_row"] == 1
    assert run_metadata.diagnostics_json["review_summary"]["by_reason"]["parse_error_row"] == 1
    assert run_metadata.diagnostics_json["review_summary"]["by_reason"]["low_confidence_page"] == 1

    review_items = db_session.scalars(select(ReviewItem).order_by(ReviewItem.reason_code)).all()
    assert len(review_items) == 3
    assert {item.reason_code for item in review_items} == {
        "low_confidence_page",
        "low_confidence_row",
        "parse_error_row",
    }
    assert all(item.ref_table == "run_metadata" for item in review_items)
    assert all(item.ref_id == result.run_metadata_id for item in review_items)
    assert all(item.status == "to_review" for item in review_items)


def test_rows_with_missing_confidence_are_skipped_by_threshold(
    db_session: Session, tmp_path: Path
) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake-pdf-content")

    adapter = DeterministicFakePdfSubagentAdapter(
        response=_response(
            [
                PdfExtractedRow(
                    account_id="acct-pdf",
                    posted_date=date(2026, 5, 4),
                    amount=Decimal("-9.99"),
                    currency="USD",
                    pending_status="posted",
                    confidence=None,
                    parse_status="parsed",
                    row_no=1,
                    page_no=1,
                    source_transaction_id="missing-conf-1",
                ),
            ]
        )
    )

    result = run_pdf_subagent_handoff(_request(statement), adapter, db_session)
    db_session.commit()

    assert result.ok is True
    assert result.status == "success_with_warnings"
    assert result.inserted_rows == 0
    assert result.skipped_rows == 1
    assert any("confidence threshold" in warning.message for warning in result.warnings)

    review_items = db_session.scalars(select(ReviewItem)).all()
    assert len(review_items) == 2
    assert {item.reason_code for item in review_items} == {"low_confidence_row", "low_confidence_page"}


def test_adapter_exception_returns_structured_failure(db_session: Session, tmp_path: Path) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake-pdf-content")

    adapter = DeterministicFakePdfSubagentAdapter(error=RuntimeError("adapter crashed"))

    result = run_pdf_subagent_handoff(_request(statement), adapter, db_session)
    db_session.commit()

    assert result.ok is False
    assert result.status == "failed"
    assert any(error.code == "adapter_failure" for error in result.errors)

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.status == "failed"


def test_start_run_accepts_string_modes_without_crashing(db_session: Session, tmp_path: Path) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"fake-pdf-content")

    # Intentionally provide string values to ensure run metadata setup is resilient.
    request = PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(statement),
        account_id="acct-pdf",
        schema_version="1.0.0",
        actor="pdf-orchestrator-test",
        confidence_threshold=0.8,
        conflict_mode="normal",  # type: ignore[arg-type]
        ocr_mode="auto",  # type: ignore[arg-type]
        source_ref="fixtures/statement.pdf",
    )

    result = run_pdf_subagent_handoff(
        request,
        DeterministicFakePdfSubagentAdapter(error=RuntimeError("boom")),
        db_session,
    )
    db_session.commit()

    assert result.ok is False
    assert any(error.code == "adapter_failure" for error in result.errors)
    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
