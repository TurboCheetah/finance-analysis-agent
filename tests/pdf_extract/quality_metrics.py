from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from finance_analysis_agent.pdf_contract.types import PdfExtractedRow


@dataclass(slots=True, frozen=True)
class RowQualityMetrics:
    true_positives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float


def _normalize_date(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _normalize_amount(value: Decimal | str | int | float | None) -> str | None:
    if value is None:
        return None
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, ValueError):
        return str(value)


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else None
    return str(value)


def _expected_key(expected: Mapping[str, Any]) -> tuple[str | None, str | None, str | None, str | None, int | None]:
    return (
        _normalize_date(expected.get("posted_date")),
        _normalize_amount(expected.get("amount")),
        _normalize_text(expected.get("currency")),
        _normalize_text(expected.get("original_statement")),
        expected.get("page_no"),
    )


def _row_key(row: PdfExtractedRow) -> tuple[str | None, str | None, str | None, str | None, int | None]:
    return (
        _normalize_date(row.posted_date),
        _normalize_amount(row.amount),
        _normalize_text(row.currency),
        _normalize_text(row.original_statement),
        row.page_no,
    )


def evaluate_row_precision_recall(
    *,
    rows: list[PdfExtractedRow],
    expected_rows: list[Mapping[str, Any]],
) -> RowQualityMetrics:
    predicted = {_row_key(row) for row in rows if row.parse_status == "parsed"}
    expected = {_expected_key(row) for row in expected_rows}

    true_positives = len(predicted & expected)
    false_positives = len(predicted - expected)
    false_negatives = len(expected - predicted)

    precision_denominator = true_positives + false_positives
    recall_denominator = true_positives + false_negatives

    precision = 1.0 if precision_denominator == 0 else true_positives / precision_denominator
    recall = 1.0 if recall_denominator == 0 else true_positives / recall_denominator

    return RowQualityMetrics(
        true_positives=true_positives,
        false_positives=false_positives,
        false_negatives=false_negatives,
        precision=precision,
        recall=recall,
    )
