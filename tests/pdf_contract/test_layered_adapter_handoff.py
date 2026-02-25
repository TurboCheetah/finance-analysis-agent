from __future__ import annotations

from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, RawTransaction, RunMetadata, Transaction
from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.orchestrator import run_pdf_subagent_handoff
from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest
from finance_analysis_agent.pdf_extract.adapter import LayeredPdfSubagentAdapter


def _seed_account(session: Session, account_id: str = "acct-layered") -> None:
    session.add(
        Account(
            id=account_id,
            name="Layered Checking",
            type="checking",
            currency="USD",
            opened_at=None,
            closed_at=None,
            institution=None,
            metadata_json=None,
        )
    )
    session.commit()


def test_layered_adapter_integrates_with_orchestrator_handoff(
    db_session: Session,
    tmp_path: Path,
) -> None:
    _seed_account(db_session)

    statement = tmp_path / "statement.pdf"
    statement.write_bytes(b"%PDF-1.4 fake")

    request = PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(statement),
        account_id="acct-layered",
        schema_version="1.0.0",
        actor="layered-handoff-test",
        confidence_threshold=0.7,
        conflict_mode=ConflictMode.NORMAL,
        source_ref="fixtures/layered-statement.pdf",
        metadata={"statement_year": 2026, "currency": "USD"},
    )

    adapter = LayeredPdfSubagentAdapter(
        text_page_supplier=lambda _: (
            [
                "01/06 Coffee Shop -4.50\n"
                "01/07 Utilities -100.00\n"
                "footer text"
            ],
            [],
        )
    )

    result = run_pdf_subagent_handoff(request, adapter, db_session)
    db_session.commit()

    assert result.ok is True
    assert result.status == "success"
    assert result.inserted_rows == 2
    assert result.skipped_rows == 0

    assert db_session.scalar(select(func.count()).select_from(Transaction)) == 2
    assert db_session.scalar(select(func.count()).select_from(RawTransaction)) == 2

    run_metadata = db_session.get(RunMetadata, result.run_metadata_id)
    assert run_metadata is not None
    assert run_metadata.status == "success"
    assert run_metadata.diagnostics_json is not None
    assert run_metadata.diagnostics_json["subagent_version_hash"] == "tur35-layered-pipeline-1"

    raw_row = db_session.scalars(select(RawTransaction)).first()
    assert raw_row is not None
    assert raw_row.raw_payload_json["provenance"]["tier"] == "text_heuristic"
