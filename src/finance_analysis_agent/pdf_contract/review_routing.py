"""Review item routing for low-confidence PDF extraction outcomes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import ReviewItem
from finance_analysis_agent.pdf_contract.types import PdfExtractedRow
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow

REASON_LOW_CONFIDENCE_ROW = "low_confidence_row"
REASON_PARSE_ERROR_ROW = "parse_error_row"
REASON_CANONICAL_MAPPING_FAILURE = "canonical_mapping_failure"
REASON_LOW_CONFIDENCE_PAGE = "low_confidence_page"

ITEM_TYPE_PDF_ROW = "pdf_row"
ITEM_TYPE_PDF_PAGE = "pdf_page"


@dataclass(slots=True, frozen=True)
class ReviewItemDraft:
    item_type: str
    reason_code: str
    confidence: float | None
    payload_json: dict[str, Any]


def _normalize_date(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _normalize_amount(value: Decimal | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    return str(value)


def _row_payload(row: PdfExtractedRow) -> dict[str, Any]:
    return {
        "account_id": row.account_id,
        "posted_date": _normalize_date(row.posted_date),
        "amount": _normalize_amount(row.amount),
        "currency": row.currency,
        "pending_status": row.pending_status,
        "parse_status": row.parse_status,
        "confidence": row.confidence,
        "error_code": row.error_code,
        "page_no": row.page_no,
        "row_no": row.row_no,
        "original_statement": row.original_statement,
        "provenance": row.provenance,
    }


def build_row_review_draft(
    *,
    row: PdfExtractedRow,
    row_index: int,
    reason_code: str,
    threshold: float | None = None,
    exception: str | None = None,
) -> ReviewItemDraft:
    payload_json: dict[str, Any] = {
        "row_index": row_index,
        "reason_code": reason_code,
        "threshold": threshold,
        "exception": exception,
        "row": _row_payload(row),
    }
    return ReviewItemDraft(
        item_type=ITEM_TYPE_PDF_ROW,
        reason_code=reason_code,
        confidence=row.confidence,
        payload_json=payload_json,
    )


def _page_confidence(rows: list[PdfExtractedRow]) -> float:
    if not rows:
        return 0.0

    parsed_rows = [row for row in rows if row.parse_status == "parsed"]
    parsed_ratio = len(parsed_rows) / len(rows)
    if not parsed_rows:
        return round(parsed_ratio * 0.4, 4)

    mean_conf = sum(float(row.confidence or 0.0) for row in parsed_rows) / len(parsed_rows)
    return round((parsed_ratio * 0.4) + (mean_conf * 0.6), 4)


def build_low_confidence_page_drafts(
    *,
    rows: list[PdfExtractedRow],
    page_threshold: float,
) -> list[ReviewItemDraft]:
    page_groups: dict[int, list[PdfExtractedRow]] = defaultdict(list)
    for row in rows:
        if row.page_no is None:
            continue
        page_groups[row.page_no].append(row)

    drafts: list[ReviewItemDraft] = []
    for page_no in sorted(page_groups):
        page_rows = page_groups[page_no]
        confidence = _page_confidence(page_rows)
        if confidence >= page_threshold:
            continue

        parse_error_count = sum(1 for row in page_rows if row.parse_status != "parsed")
        drafts.append(
            ReviewItemDraft(
                item_type=ITEM_TYPE_PDF_PAGE,
                reason_code=REASON_LOW_CONFIDENCE_PAGE,
                confidence=confidence,
                payload_json={
                    "page_no": page_no,
                    "page_confidence": confidence,
                    "page_confidence_threshold": page_threshold,
                    "row_count": len(page_rows),
                    "parse_error_count": parse_error_count,
                },
            )
        )

    return drafts


def persist_review_items(
    *,
    run_metadata_id: str,
    drafts: list[ReviewItemDraft],
    session: Session,
) -> None:
    timestamp = utcnow()
    for draft in drafts:
        session.add(
            ReviewItem(
                id=str(uuid4()),
                item_type=draft.item_type,
                ref_table="run_metadata",
                ref_id=run_metadata_id,
                reason_code=draft.reason_code,
                confidence=draft.confidence,
                status=ReviewItemStatus.TO_REVIEW.value,
                source=ReviewSource.PDF_EXTRACT.value,
                assigned_to=None,
                payload_json=draft.payload_json,
                created_at=timestamp,
                resolved_at=None,
            )
        )
