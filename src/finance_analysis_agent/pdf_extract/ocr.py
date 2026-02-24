"""OCR interface for layered PDF extraction fallback."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest
from finance_analysis_agent.pdf_extract import taxonomy


@dataclass(slots=True)
class OcrResult:
    text_pages: list[str] = field(default_factory=list)
    page_notes: list[dict[str, object]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    available: bool = True


class OcrEngine(Protocol):
    def extract_text_pages(self, request: PdfSubagentRequest) -> OcrResult:
        """Extract OCR text pages from a statement request."""


@dataclass(slots=True)
class UnavailableOcrEngine:
    reason: str = "ocr engine not configured"

    def extract_text_pages(self, request: PdfSubagentRequest) -> OcrResult:
        del request
        return OcrResult(
            text_pages=[],
            page_notes=[],
            warnings=[f"{taxonomy.OCR_UNAVAILABLE}: {self.reason}"],
            available=False,
        )
