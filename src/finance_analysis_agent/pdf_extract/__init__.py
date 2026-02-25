"""Layered PDF extraction package."""

from finance_analysis_agent.pdf_extract.adapter import LayeredPdfSubagentAdapter
from finance_analysis_agent.pdf_extract.ocr import OcrEngine, OcrResult, UnavailableOcrEngine
from finance_analysis_agent.pdf_extract.pipeline import (
    DEFAULT_SUBAGENT_VERSION_HASH,
    run_layered_extraction,
)
from finance_analysis_agent.pdf_extract.profiles import (
    TemplateProfile,
    TemplateProfileRegistry,
    build_default_profile_registry,
)
from finance_analysis_agent.pdf_extract.table_assist import (
    TableAssistResult,
    TableExtractor,
    UnavailableTableExtractor,
)
from finance_analysis_agent.pdf_extract.text_heuristic import (
    TextPageSupplier,
    load_statement_text_pages,
    parse_statement_pages,
)

__all__ = [
    "DEFAULT_SUBAGENT_VERSION_HASH",
    "LayeredPdfSubagentAdapter",
    "OcrEngine",
    "OcrResult",
    "TableAssistResult",
    "TableExtractor",
    "TemplateProfile",
    "TemplateProfileRegistry",
    "TextPageSupplier",
    "UnavailableOcrEngine",
    "UnavailableTableExtractor",
    "build_default_profile_registry",
    "load_statement_text_pages",
    "parse_statement_pages",
    "run_layered_extraction",
]
