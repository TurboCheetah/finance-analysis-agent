from __future__ import annotations

from datetime import date
from decimal import Decimal

from finance_analysis_agent.pdf_contract.types import PdfExtractedRow
from tests.pdf_extract.quality_metrics import evaluate_row_precision_recall


def _parsed_row(*, statement: str) -> PdfExtractedRow:
    return PdfExtractedRow(
        account_id="acct-1",
        posted_date=date(2026, 1, 1),
        amount=Decimal("-10.00"),
        currency="USD",
        pending_status="posted",
        original_statement=statement,
        confidence=0.9,
        parse_status="parsed",
        page_no=1,
        row_no=1,
    )


def test_precision_recall_counts_duplicates_using_multiset_semantics() -> None:
    rows = [_parsed_row(statement="Coffee Shop"), _parsed_row(statement="Coffee Shop")]
    expected_rows = [
        {
            "posted_date": "2026-01-01",
            "amount": "-10.00",
            "currency": "USD",
            "original_statement": "Coffee Shop",
            "page_no": 1,
        }
    ]

    metrics = evaluate_row_precision_recall(rows=rows, expected_rows=expected_rows)

    assert metrics.true_positives == 1
    assert metrics.false_positives == 1
    assert metrics.false_negatives == 0
    assert metrics.precision == 0.5
    assert metrics.recall == 1.0


def test_precision_is_zero_when_no_predictions_exist() -> None:
    metrics = evaluate_row_precision_recall(
        rows=[],
        expected_rows=[
            {
                "posted_date": "2026-01-01",
                "amount": "-10.00",
                "currency": "USD",
                "original_statement": "Coffee Shop",
                "page_no": 1,
            }
        ],
    )

    assert metrics.precision == 0.0
    assert metrics.recall == 0.0


def test_vacuous_recall_when_predictions_and_expected_are_empty() -> None:
    metrics = evaluate_row_precision_recall(rows=[], expected_rows=[])

    assert metrics.precision == 0.0
    assert metrics.recall == 1.0
