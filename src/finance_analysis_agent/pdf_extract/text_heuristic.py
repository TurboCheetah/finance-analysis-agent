"""Tier-1 text extraction and heuristic row parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

from finance_analysis_agent.pdf_contract.types import PdfExtractedRow, PdfExtractionTier, PdfSubagentRequest
from finance_analysis_agent.pdf_extract import taxonomy
from finance_analysis_agent.pdf_extract.profiles import TemplateProfile

_DATE_PREFIX_RE = re.compile(r"^\s*(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{2}/\d{2})\b")
_AMOUNT_SUFFIX_RE = re.compile(r"(?P<amount>\(?-?\$?\d[\d,]*\.\d{2}\)?)\s*$")


@dataclass(slots=True)
class TextParseResult:
    rows: list[PdfExtractedRow] = field(default_factory=list)
    page_notes: list[dict[str, object]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


TextPageSupplier = Callable[[PdfSubagentRequest], tuple[list[str], list[str]]]


def load_statement_text_pages(request: PdfSubagentRequest) -> tuple[list[str], list[str]]:
    """Read text content from statement pages using pypdf when available."""

    statement_path = Path(request.statement_path)
    if not statement_path.exists():
        return [], [f"{taxonomy.TEXT_UNAVAILABLE}: statement path does not exist"]

    try:
        from pypdf import PdfReader
    except ImportError:
        return [], [f"{taxonomy.TEXT_UNAVAILABLE}: pypdf dependency is not installed"]

    warnings: list[str] = []
    try:
        reader = PdfReader(str(statement_path))
    except Exception as exc:  # pragma: no cover - defensive guard for parser errors
        return [], [f"{taxonomy.TEXT_UNAVAILABLE}: failed to open PDF ({exc})"]

    start_idx = 0
    end_idx = len(reader.pages)
    if request.page_range is not None:
        start, end = request.page_range
        start_idx = max(start - 1, 0)
        end_idx = min(end, len(reader.pages))

    pages: list[str] = []
    for page_index in range(start_idx, end_idx):
        try:
            page_text = reader.pages[page_index].extract_text() or ""
        except Exception as exc:  # pragma: no cover - defensive guard for parser errors
            warnings.append(f"{taxonomy.TEXT_UNAVAILABLE}: page {page_index + 1} extract failed ({exc})")
            page_text = ""
        pages.append(page_text)

    if not pages:
        warnings.append(f"{taxonomy.TEXT_UNAVAILABLE}: no pages available in selected page range")
    return pages, warnings


def _parse_date(date_token: str, year_hint: int | None) -> tuple[date | None, str | None]:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_token):
        return date.fromisoformat(date_token), None

    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", date_token):
        month, day, year = date_token.split("/")
        return date(int(year), int(month), int(day)), None

    if re.fullmatch(r"\d{2}/\d{2}", date_token):
        if year_hint is None:
            return None, taxonomy.DATE_PARSE
        month, day = date_token.split("/")
        return date(year_hint, int(month), int(day)), None

    return None, taxonomy.DATE_PARSE


def _parse_amount(amount_token: str) -> tuple[Decimal | None, str | None]:
    normalized = amount_token.replace("$", "").replace(",", "").strip()
    is_parenthesized = normalized.startswith("(") and normalized.endswith(")")
    if is_parenthesized:
        normalized = normalized[1:-1]

    if not normalized:
        return None, taxonomy.AMOUNT_PARSE

    try:
        amount = Decimal(normalized)
    except InvalidOperation:
        return None, taxonomy.AMOUNT_PARSE

    if is_parenthesized and amount > 0:
        amount = -amount
    return amount, None


def _estimate_confidence(*, description: str, date_ok: bool, amount_ok: bool, tier: PdfExtractionTier) -> float:
    confidence = 0.45
    if tier == PdfExtractionTier.OCR_FALLBACK:
        confidence -= 0.1

    if description:
        confidence += 0.15
    if date_ok:
        confidence += 0.2
    if amount_ok:
        confidence += 0.2

    return max(0.0, min(0.99, round(confidence, 4)))


def _is_candidate_transaction_line(line: str) -> bool:
    if not line.strip():
        return False
    has_date_prefix = _DATE_PREFIX_RE.search(line) is not None
    has_amount_suffix = _AMOUNT_SUFFIX_RE.search(line) is not None
    return has_date_prefix or has_amount_suffix


def parse_statement_pages(
    pages: list[str],
    *,
    request: PdfSubagentRequest,
    profile: TemplateProfile,
    tier: PdfExtractionTier,
) -> TextParseResult:
    """Parse candidate lines from text pages into extracted rows."""

    warnings: list[str] = []
    page_notes: list[dict[str, object]] = []
    rows: list[PdfExtractedRow] = []

    year_hint_raw = request.metadata.get("statement_year") if request.metadata else None
    year_hint = int(year_hint_raw) if isinstance(year_hint_raw, int) else None
    currency_hint_raw = request.metadata.get("currency") if request.metadata else None
    currency_hint = currency_hint_raw if isinstance(currency_hint_raw, str) else profile.default_currency

    row_no = 0
    for page_idx, page_text in enumerate(pages, start=1):
        non_empty_lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        candidate_lines = [line for line in non_empty_lines if _is_candidate_transaction_line(line)]

        parse_errors = 0
        parsed_rows = 0

        for line in candidate_lines:
            row_no += 1

            date_match = _DATE_PREFIX_RE.search(line)
            amount_match = _AMOUNT_SUFFIX_RE.search(line)
            if date_match is None or amount_match is None:
                parse_errors += 1
                rows.append(
                    PdfExtractedRow(
                        account_id=request.account_id,
                        posted_date=None,
                        amount=None,
                        currency=currency_hint,
                        pending_status=None,
                        original_statement=line,
                        confidence=0.2,
                        parse_status="parse_error",
                        error_code=taxonomy.LAYOUT_SHIFT,
                        page_no=page_idx,
                        row_no=row_no,
                        provenance={"tier": tier.value, "profile": profile.name},
                    )
                )
                continue

            date_token = date_match.group(1)
            amount_token = amount_match.group("amount")

            description_start = date_match.end()
            description_end = amount_match.start()
            description = line[description_start:description_end].strip()

            posted_date, date_error = _parse_date(date_token, year_hint)
            amount, amount_error = _parse_amount(amount_token)
            parse_error = date_error or amount_error

            if parse_error is not None:
                parse_errors += 1
                rows.append(
                    PdfExtractedRow(
                        account_id=request.account_id,
                        posted_date=None,
                        amount=None,
                        currency=currency_hint,
                        pending_status=None,
                        original_statement=line,
                        confidence=0.25,
                        parse_status="parse_error",
                        error_code=parse_error,
                        page_no=page_idx,
                        row_no=row_no,
                        provenance={"tier": tier.value, "profile": profile.name},
                    )
                )
                continue

            parsed_rows += 1
            confidence = _estimate_confidence(
                description=description,
                date_ok=True,
                amount_ok=True,
                tier=tier,
            )
            rows.append(
                PdfExtractedRow(
                    account_id=request.account_id,
                    posted_date=posted_date,
                    amount=amount,
                    currency=currency_hint,
                    pending_status=profile.default_pending_status,
                    original_statement=description,
                    confidence=confidence,
                    parse_status="parsed",
                    error_code=None,
                    page_no=page_idx,
                    row_no=row_no,
                    provenance={
                        "tier": tier.value,
                        "profile": profile.name,
                        "field_sources": {
                            "posted_date": "regex_date_v1",
                            "amount": "regex_amount_v1",
                            "original_statement": "line_slice_v1",
                        },
                    },
                )
            )

        page_notes.append(
            {
                "page_no": page_idx,
                "tier": tier.value,
                "text_density": len(non_empty_lines),
                "rows_found": len(candidate_lines),
                "parsed_rows": parsed_rows,
                "parse_errors": parse_errors,
            }
        )

    if not rows:
        warnings.append(f"{taxonomy.LAYOUT_SHIFT}: no candidate transaction lines detected")

    return TextParseResult(rows=rows, page_notes=page_notes, warnings=warnings)
