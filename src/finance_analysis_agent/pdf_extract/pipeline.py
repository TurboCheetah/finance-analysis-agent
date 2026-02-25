"""Layered extraction pipeline: text heuristics, table assist, OCR fallback."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any

from finance_analysis_agent.pdf_contract.types import (
    PdfDiagnostics,
    PdfExtractedRow,
    PdfExtractionTier,
    PdfOcrMode,
    PdfSubagentRequest,
    PdfSubagentResponse,
)
from finance_analysis_agent.pdf_extract import taxonomy
from finance_analysis_agent.pdf_extract.ocr import OcrEngine, UnavailableOcrEngine
from finance_analysis_agent.pdf_extract.profiles import (
    TemplateProfileRegistry,
    build_default_profile_registry,
)
from finance_analysis_agent.pdf_extract.table_assist import TableExtractor, UnavailableTableExtractor
from finance_analysis_agent.pdf_extract.text_heuristic import (
    TextPageSupplier,
    load_statement_text_pages,
    parse_statement_pages,
)

DEFAULT_SUBAGENT_VERSION_HASH = "tur35-layered-pipeline-1"


def _row_key(row: PdfExtractedRow) -> tuple[Any, ...]:
    if row.parse_status == "parsed":
        amount = str(row.amount) if isinstance(row.amount, Decimal) else row.amount
        posted_date = row.posted_date.isoformat() if hasattr(row.posted_date, "isoformat") else row.posted_date
        return (
            "parsed",
            posted_date,
            amount,
            row.currency,
            row.original_statement,
            row.page_no,
        )
    return (
        "error",
        row.original_statement,
        row.page_no,
    )


def _row_strength(row: PdfExtractedRow) -> tuple[int, float]:
    parsed_weight = 1 if row.parse_status == "parsed" else 0
    confidence = float(row.confidence or 0.0)
    return parsed_weight, confidence


def _merge_rows(base_rows: list[PdfExtractedRow], new_rows: list[PdfExtractedRow]) -> list[PdfExtractedRow]:
    merged: dict[tuple[Any, ...], PdfExtractedRow] = {}
    for row in base_rows + new_rows:
        key = _row_key(row)
        current = merged.get(key)
        if current is None or _row_strength(row) > _row_strength(current):
            merged[key] = row

    sorted_rows = sorted(
        merged.values(),
        key=lambda row: (
            row.page_no if row.page_no is not None else 10**9,
            row.row_no if row.row_no is not None else 10**9,
        ),
    )
    return sorted_rows


def _overall_confidence(rows: list[PdfExtractedRow]) -> float:
    if not rows:
        return 0.0

    parsed_rows = [row for row in rows if row.parse_status == "parsed"]
    parsed_ratio = len(parsed_rows) / len(rows)
    if not parsed_rows:
        return round(parsed_ratio * 0.4, 4)

    mean_conf = sum(float(row.confidence or 0.0) for row in parsed_rows) / len(parsed_rows)
    return round((parsed_ratio * 0.4) + (mean_conf * 0.6), 4)


def _normalize_page_notes(notes: list[dict[str, object]], tier: PdfExtractionTier) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for note in notes:
        normalized_note = dict(note)
        normalized_note.setdefault("tier", tier.value)
        normalized.append(normalized_note)
    return normalized


def _warnings_to_error_counts(rows: list[PdfExtractedRow], warnings: list[str]) -> dict[str, int]:
    """Summarize error codes from rows and warnings of shape '<code>: <detail>'."""

    counts: Counter[str] = Counter()
    for row in rows:
        if row.error_code:
            counts[row.error_code] += 1
    for warning in warnings:
        if ":" in warning:
            code, _ = warning.split(":", 1)
            normalized = code.strip()
            if normalized and ":" not in normalized:
                counts[normalized] += 1
    return dict(counts)


def run_layered_extraction(
    request: PdfSubagentRequest,
    *,
    table_extractor: TableExtractor | None = None,
    ocr_engine: OcrEngine | None = None,
    profile_registry: TemplateProfileRegistry | None = None,
    text_page_supplier: TextPageSupplier | None = None,
    subagent_version_hash: str = DEFAULT_SUBAGENT_VERSION_HASH,
) -> PdfSubagentResponse:
    """Execute layered extraction and return contract-compliant subagent response."""

    table_extractor = table_extractor or UnavailableTableExtractor()
    ocr_engine = ocr_engine or UnavailableOcrEngine()
    profile_registry = profile_registry or build_default_profile_registry()
    text_page_supplier = text_page_supplier or load_statement_text_pages

    profile = profile_registry.resolve(request.template_hint)

    extraction_tiers_used: list[PdfExtractionTier] = [PdfExtractionTier.TEXT_HEURISTIC]
    warnings: list[str] = []
    page_notes: list[dict[str, object]] = []
    rows: list[PdfExtractedRow] = []
    ocr_invoked = False

    text_pages, text_warnings = text_page_supplier(request)
    warnings.extend(text_warnings)

    tier1_result = parse_statement_pages(
        text_pages,
        request=request,
        profile=profile,
        tier=PdfExtractionTier.TEXT_HEURISTIC,
    )
    rows = tier1_result.rows
    warnings.extend(tier1_result.warnings)
    page_notes.extend(_normalize_page_notes(tier1_result.page_notes, PdfExtractionTier.TEXT_HEURISTIC))

    overall_confidence = _overall_confidence(rows)

    if overall_confidence < request.confidence_threshold:
        extraction_tiers_used.append(PdfExtractionTier.TABLE_ASSIST)
        table_result = table_extractor.extract(request)
        warnings.extend(table_result.warnings)
        page_notes.extend(_normalize_page_notes(table_result.page_notes, PdfExtractionTier.TABLE_ASSIST))
        if table_result.rows:
            rows = _merge_rows(rows, table_result.rows)
            overall_confidence = _overall_confidence(rows)

    ocr_mode = request.ocr_mode.value if hasattr(request.ocr_mode, "value") else str(request.ocr_mode)
    should_ocr = ocr_mode == PdfOcrMode.FORCE.value or (
        ocr_mode == PdfOcrMode.AUTO.value and overall_confidence < request.confidence_threshold
    )
    if should_ocr:
        ocr_invoked = True
        extraction_tiers_used.append(PdfExtractionTier.OCR_FALLBACK)
        ocr_result = ocr_engine.extract_text_pages(request)
        warnings.extend(ocr_result.warnings)
        page_notes.extend(_normalize_page_notes(ocr_result.page_notes, PdfExtractionTier.OCR_FALLBACK))

        if ocr_result.text_pages:
            ocr_parse_result = parse_statement_pages(
                ocr_result.text_pages,
                request=request,
                profile=profile,
                tier=PdfExtractionTier.OCR_FALLBACK,
            )
            warnings.extend(ocr_parse_result.warnings)
            page_notes.extend(
                _normalize_page_notes(ocr_parse_result.page_notes, PdfExtractionTier.OCR_FALLBACK)
            )
            rows = _merge_rows(rows, ocr_parse_result.rows)
            overall_confidence = _overall_confidence(rows)
        elif ocr_result.available:
            warnings.append(f"{taxonomy.OCR_LOW_CONFIDENCE}: OCR returned no text pages")

    parsed_rows = sum(1 for row in rows if row.parse_status == "parsed")
    parse_error_rows = len(rows) - parsed_rows

    diagnostics = PdfDiagnostics(
        run_summary={
            "total_pages": len(text_pages),
            "total_rows": len(rows),
            "parsed_rows": parsed_rows,
            "parse_error_rows": parse_error_rows,
            "overall_confidence": overall_confidence,
            "ocr_invoked": ocr_invoked,
            "tier_sequence": [tier.value for tier in extraction_tiers_used],
            "error_counts": _warnings_to_error_counts(rows, warnings),
        },
        page_notes=page_notes,
    )

    return PdfSubagentResponse(
        contract_version=request.contract_version,
        subagent_version_hash=subagent_version_hash,
        extraction_tiers_used=extraction_tiers_used,
        rows=rows,
        diagnostics=diagnostics,
        warnings=warnings,
    )
