"""Adapter interface for PDF extraction subagent handoff."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest, PdfSubagentResponse


class PdfSubagentAdapter(Protocol):
    """Protocol for any PDF subagent implementation."""

    def extract(self, request: PdfSubagentRequest) -> PdfSubagentResponse:
        """Extract canonical rows from a PDF statement request."""


@dataclass(slots=True)
class DeterministicFakePdfSubagentAdapter:
    """Deterministic test adapter that returns a fixed response or raises."""

    response: PdfSubagentResponse | None = None
    error: Exception | None = None

    def extract(self, request: PdfSubagentRequest) -> PdfSubagentResponse:
        del request
        if self.error is not None:
            raise self.error
        if self.response is None:
            raise RuntimeError("DeterministicFakePdfSubagentAdapter requires response or error")
        return self.response
