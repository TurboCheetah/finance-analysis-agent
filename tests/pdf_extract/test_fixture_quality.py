from __future__ import annotations

import json
from pathlib import Path

from pypdf import PdfReader

from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest
from finance_analysis_agent.pdf_extract.pipeline import run_layered_extraction
from finance_analysis_agent.pdf_extract.text_heuristic import load_statement_text_pages
from finance_analysis_agent.pdf_extract.thresholds import (
    resolve_pdf_threshold_policy,
    resolve_quality_floors,
)
from tests.pdf_extract.quality_metrics import evaluate_row_precision_recall

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "pdf_quality"
SMOKE_PDF_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "pdf_smoke"


def _build_request(fixture: dict[str, object]) -> PdfSubagentRequest:
    template_hint = fixture.get("template_hint")
    metadata = fixture.get("metadata")

    request = PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(Path("/") / "tmp" / "fixture.pdf"),
        account_id="acct-fixture",
        schema_version="1.0.0",
        actor="fixture-quality-test",
        confidence_threshold=0.8,
        conflict_mode=ConflictMode.NORMAL,
        template_hint=template_hint if isinstance(template_hint, str) else None,
        metadata=metadata if isinstance(metadata, dict) else {},
    )
    policy = resolve_pdf_threshold_policy(request)
    request.confidence_threshold = policy.row_confidence_threshold
    return request


def test_fixture_suite_meets_row_quality_floors() -> None:
    fixture_paths = sorted(FIXTURES_DIR.glob("*.json"))
    assert fixture_paths, "expected at least one fixture json"

    total_true_positives = 0
    total_false_positives = 0
    total_false_negatives = 0

    for fixture_path in fixture_paths:
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        request = _build_request(fixture)
        text_pages = fixture["text_pages"]
        assert isinstance(text_pages, list)

        response = run_layered_extraction(
            request,
            text_page_supplier=lambda _request, pages=text_pages: (pages, []),
        )

        expected_rows = fixture["expected_rows"]
        assert isinstance(expected_rows, list)

        metrics = evaluate_row_precision_recall(rows=response.rows, expected_rows=expected_rows)
        floors = resolve_quality_floors(
            template_hint=request.template_hint,
            issuer=request.metadata.get("issuer") if isinstance(request.metadata, dict) else None,
        )

        assert metrics.precision >= floors.precision_min, (
            f"{fixture_path.name}: precision {metrics.precision:.4f} "
            f"below floor {floors.precision_min:.4f}; "
            f"tp={metrics.true_positives} fp={metrics.false_positives} fn={metrics.false_negatives}"
        )
        assert metrics.recall >= floors.recall_min, (
            f"{fixture_path.name}: recall {metrics.recall:.4f} "
            f"below floor {floors.recall_min:.4f}; "
            f"tp={metrics.true_positives} fp={metrics.false_positives} fn={metrics.false_negatives}"
        )

        total_true_positives += metrics.true_positives
        total_false_positives += metrics.false_positives
        total_false_negatives += metrics.false_negatives

    global_floors = resolve_quality_floors()
    micro_precision_denominator = total_true_positives + total_false_positives
    micro_recall_denominator = total_true_positives + total_false_negatives

    total_rows = total_true_positives + total_false_positives + total_false_negatives
    assert total_rows > 0, "no rows found in fixtures; quality metrics would be vacuously true"

    micro_precision = 1.0 if micro_precision_denominator == 0 else total_true_positives / micro_precision_denominator
    micro_recall = 1.0 if micro_recall_denominator == 0 else total_true_positives / micro_recall_denominator

    assert micro_precision >= global_floors.precision_min, (
        f"micro precision {micro_precision:.4f} below floor {global_floors.precision_min:.4f}; "
        f"tp={total_true_positives} fp={total_false_positives} fn={total_false_negatives}"
    )
    assert micro_recall >= global_floors.recall_min, (
        f"micro recall {micro_recall:.4f} below floor {global_floors.recall_min:.4f}; "
        f"tp={total_true_positives} fp={total_false_positives} fn={total_false_negatives}"
    )


def test_smoke_pdf_fixtures_are_readable() -> None:
    pdf_paths = sorted(SMOKE_PDF_DIR.glob("*.pdf"))
    assert len(pdf_paths) >= 2, "expected at least two smoke PDFs"

    for pdf_path in pdf_paths:
        reader = PdfReader(str(pdf_path))
        assert len(reader.pages) >= 1

        request = PdfSubagentRequest(
            contract_version="1.0.0",
            statement_path=str(pdf_path.resolve()),
            account_id="acct-smoke",
            schema_version="1.0.0",
            actor="fixture-quality-test",
            confidence_threshold=0.8,
            conflict_mode=ConflictMode.NORMAL,
            metadata={"statement_year": 2026, "currency": "USD"},
        )

        pages, warnings = load_statement_text_pages(request)
        assert len(pages) >= 1
        assert not any("failed to open PDF" in warning for warning in warnings)
