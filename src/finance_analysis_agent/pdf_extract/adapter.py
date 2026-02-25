"""Concrete PDF subagent adapter backed by layered extraction pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from finance_analysis_agent.pdf_contract.adapter import PdfSubagentAdapter
from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest, PdfSubagentResponse
from finance_analysis_agent.pdf_extract.ocr import OcrEngine
from finance_analysis_agent.pdf_extract.pipeline import (
    DEFAULT_SUBAGENT_VERSION_HASH,
    run_layered_extraction,
)
from finance_analysis_agent.pdf_extract.profiles import TemplateProfileRegistry
from finance_analysis_agent.pdf_extract.table_assist import TableExtractor
from finance_analysis_agent.pdf_extract.text_heuristic import TextPageSupplier


@dataclass(slots=True)
class LayeredPdfSubagentAdapter(PdfSubagentAdapter):
    """Production adapter that executes the TUR-35 layered extraction flow."""

    table_extractor: TableExtractor | None = None
    ocr_engine: OcrEngine | None = None
    profile_registry: TemplateProfileRegistry | None = None
    text_page_supplier: TextPageSupplier | None = None
    subagent_version_hash: str = DEFAULT_SUBAGENT_VERSION_HASH

    def extract(self, request: PdfSubagentRequest) -> PdfSubagentResponse:
        return run_layered_extraction(
            request,
            table_extractor=self.table_extractor,
            ocr_engine=self.ocr_engine,
            profile_registry=self.profile_registry,
            text_page_supplier=self.text_page_supplier,
            subagent_version_hash=self.subagent_version_hash,
        )
