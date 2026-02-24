from __future__ import annotations

from dataclasses import dataclass

from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.types import (
    PdfExtractedRow,
    PdfExtractionTier,
    PdfOcrMode,
    PdfSubagentRequest,
)
from finance_analysis_agent.pdf_extract.ocr import OcrEngine, OcrResult
from finance_analysis_agent.pdf_extract.pipeline import run_layered_extraction
from finance_analysis_agent.pdf_extract.table_assist import TableAssistResult, TableExtractor


@dataclass(slots=True)
class _StubTableExtractor(TableExtractor):
    result: TableAssistResult

    def extract(self, request: PdfSubagentRequest) -> TableAssistResult:
        del request
        return self.result


@dataclass(slots=True)
class _StubOcrEngine(OcrEngine):
    result: OcrResult

    def extract_text_pages(self, request: PdfSubagentRequest) -> OcrResult:
        del request
        return self.result


def _request() -> PdfSubagentRequest:
    return PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path="/tmp/fake-statement.pdf",
        account_id="acct-1",
        schema_version="1.0.0",
        actor="pdf-test",
        confidence_threshold=0.8,
        conflict_mode=ConflictMode.NORMAL,
        metadata={"statement_year": 2026, "currency": "USD"},
    )


def test_tier1_parses_non_tabular_lines_and_emits_provenance() -> None:
    request = _request()

    response = run_layered_extraction(
        request,
        text_page_supplier=lambda _: (
            ["01/02 Coffee Shop -4.25\n01/03 Grocery Store -18.00"],
            [],
        ),
    )

    assert response.extraction_tiers_used == [PdfExtractionTier.TEXT_HEURISTIC]
    assert len(response.rows) == 2
    assert all(row.parse_status == "parsed" for row in response.rows)
    assert all(row.provenance is not None for row in response.rows)
    assert response.rows[0].provenance["tier"] == PdfExtractionTier.TEXT_HEURISTIC.value
    assert response.rows[0].provenance["field_sources"]["posted_date"] == "regex_date_v1"
    assert response.diagnostics.run_summary["parsed_rows"] == 2
    assert response.diagnostics.run_summary["ocr_invoked"] is False


def test_table_assist_runs_when_tier1_confidence_is_low() -> None:
    request = _request()

    table_row = PdfExtractedRow(
        account_id="acct-1",
        posted_date="2026-01-12",
        amount="-9.99",
        currency="USD",
        pending_status="posted",
        parse_status="parsed",
        confidence=0.95,
        page_no=1,
        row_no=1,
        provenance={"tier": PdfExtractionTier.TABLE_ASSIST.value},
    )

    response = run_layered_extraction(
        request,
        text_page_supplier=lambda _: (["header only no transactions"], []),
        table_extractor=_StubTableExtractor(
            TableAssistResult(
                rows=[table_row],
                page_notes=[{"page_no": 1, "rows_found": 1}],
            )
        ),
    )

    assert response.extraction_tiers_used == [
        PdfExtractionTier.TEXT_HEURISTIC,
        PdfExtractionTier.TABLE_ASSIST,
    ]
    assert any(row.provenance and row.provenance.get("tier") == PdfExtractionTier.TABLE_ASSIST.value for row in response.rows)
    assert response.diagnostics.run_summary["ocr_invoked"] is False


def test_ocr_runs_only_under_low_confidence_and_auto_mode() -> None:
    request = _request()

    response = run_layered_extraction(
        request,
        text_page_supplier=lambda _: (["no useful lines"], []),
        table_extractor=_StubTableExtractor(TableAssistResult(rows=[], available=False)),
        ocr_engine=_StubOcrEngine(OcrResult(text_pages=["01/15 Pharmacy -12.30"])),
    )

    assert response.extraction_tiers_used == [
        PdfExtractionTier.TEXT_HEURISTIC,
        PdfExtractionTier.TABLE_ASSIST,
        PdfExtractionTier.OCR_FALLBACK,
    ]
    assert response.diagnostics.run_summary["ocr_invoked"] is True
    assert any(
        row.provenance and row.provenance.get("tier") == PdfExtractionTier.OCR_FALLBACK.value
        for row in response.rows
    )


def test_ocr_is_not_invoked_when_mode_is_off() -> None:
    request = _request()
    request.ocr_mode = PdfOcrMode.OFF

    response = run_layered_extraction(
        request,
        text_page_supplier=lambda _: (["no useful lines"], []),
        table_extractor=_StubTableExtractor(TableAssistResult(rows=[], available=False)),
        ocr_engine=_StubOcrEngine(OcrResult(text_pages=["01/15 Pharmacy -12.30"])),
    )

    assert response.extraction_tiers_used == [
        PdfExtractionTier.TEXT_HEURISTIC,
        PdfExtractionTier.TABLE_ASSIST,
    ]
    assert response.diagnostics.run_summary["ocr_invoked"] is False
