"""Optional table extraction interface for layered PDF extraction."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from finance_analysis_agent.pdf_contract.types import PdfExtractedRow, PdfSubagentRequest
from finance_analysis_agent.pdf_extract import taxonomy


@dataclass(slots=True)
class TableAssistResult:
    rows: list[PdfExtractedRow] = field(default_factory=list)
    page_notes: list[dict[str, object]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    available: bool = True


class TableExtractor(Protocol):
    def extract(self, request: PdfSubagentRequest) -> TableAssistResult:
        """Extract candidate rows from table-like layout features."""


@dataclass(slots=True)
class UnavailableTableExtractor:
    reason: str = "table extractor not configured"

    def extract(self, request: PdfSubagentRequest) -> TableAssistResult:
        del request
        return TableAssistResult(
            rows=[],
            page_notes=[],
            warnings=[f"{taxonomy.TABLE_UNAVAILABLE}: {self.reason}"],
            available=False,
        )
