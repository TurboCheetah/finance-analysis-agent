from __future__ import annotations

from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, RunMetadata
from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.adapter import DeterministicFakePdfSubagentAdapter
from finance_analysis_agent.pdf_contract.orchestrator import (
    EXPECTED_CONTRACT_VERSION,
    ORCHESTRATOR_COMPONENT_VERSION,
    run_pdf_subagent_handoff,
)
from finance_analysis_agent.pdf_contract.types import (
    PdfDiagnostics,
    PdfExtractedRow,
    PdfExtractionTier,
    PdfSubagentRequest,
    PdfSubagentResponse,
)


def _seed_account(session: Session) -> None:
    session.add(Account(id="acct-pin", name="Pin", type="checking", currency="USD"))
    session.commit()


def _request(statement: Path) -> PdfSubagentRequest:
    return PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(statement),
        account_id="acct-pin",
        schema_version="1.0.0",
        actor="pin-test",
        confidence_threshold=0.7,
        conflict_mode=ConflictMode.NORMAL,
    )


def _response(contract_version: str) -> PdfSubagentResponse:
    return PdfSubagentResponse(
        contract_version=contract_version,
        subagent_version_hash="subagent-pin-hash",
        extraction_tiers_used=[PdfExtractionTier.TABLE_ASSIST],
        rows=[
            PdfExtractedRow(
                account_id="acct-pin",
                posted_date=date(2026, 6, 1),
                amount="-2.00",
                currency="USD",
                pending_status="posted",
                confidence=0.9,
                parse_status="parsed",
                source_transaction_id="pin-1",
            )
        ],
        diagnostics=PdfDiagnostics(run_summary={"mode": "pin"}),
    )


def test_run_metadata_pins_expected_and_received_versions(db_session: Session, tmp_path: Path) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"pdf")

    result = run_pdf_subagent_handoff(
        _request(statement),
        DeterministicFakePdfSubagentAdapter(response=_response("1.9.0")),
        db_session,
    )
    db_session.commit()

    assert result.ok is True

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.diagnostics_json is not None
    assert run_metadata.diagnostics_json["contract_version_expected"] == EXPECTED_CONTRACT_VERSION
    assert run_metadata.diagnostics_json["contract_version_received"] == "1.9.0"
    assert run_metadata.diagnostics_json["subagent_version_hash"] == "subagent-pin-hash"
    assert (
        run_metadata.diagnostics_json["orchestrator_component_version"]
        == ORCHESTRATOR_COMPONENT_VERSION
    )


def test_major_version_mismatch_fails_and_records_version_details(
    db_session: Session,
    tmp_path: Path,
) -> None:
    _seed_account(db_session)
    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"pdf")

    result = run_pdf_subagent_handoff(
        _request(statement),
        DeterministicFakePdfSubagentAdapter(response=_response("2.0.0")),
        db_session,
    )
    db_session.commit()

    assert result.ok is False
    assert result.status == "failed"
    assert any(error.code == "version_incompatible" for error in result.errors)

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.status == "failed"
    assert run_metadata.diagnostics_json is not None
    assert run_metadata.diagnostics_json["contract_version_expected"] == EXPECTED_CONTRACT_VERSION
    assert run_metadata.diagnostics_json["contract_version_received"] == "2.0.0"
    assert run_metadata.diagnostics_json["subagent_version_hash"] == "subagent-pin-hash"
