"""Threshold policy resolution for PDF extraction and quality gates."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest

DEFAULT_ROW_THRESHOLD = 0.8
DEFAULT_PAGE_THRESHOLD = 0.75
DEFAULT_PRECISION_MIN = 0.99
DEFAULT_RECALL_MIN = 0.9

CONFIG_ENV_VAR = "FINANCE_PDF_THRESHOLD_CONFIG"
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PdfThresholdPolicy:
    row_confidence_threshold: float
    page_confidence_threshold: float
    precision_min: float
    recall_min: float
    source: str
    config_path: str


@dataclass(frozen=True, slots=True)
class PdfQualityFloors:
    precision_min: float
    recall_min: float


def _default_config_path() -> Path:
    return Path(__file__).resolve().parent / "config" / "confidence_thresholds.json"


def _normalize_key(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized if normalized else None


def _metadata_value(metadata: Mapping[str, Any] | None, key: str) -> Any:
    if metadata is None:
        return None
    return metadata.get(key)


def _float_between_0_and_1(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str) and value.strip():
        try:
            numeric = float(value.strip())
        except ValueError:
            return None
    else:
        return None

    if numeric < 0 or numeric > 1:
        return None
    return numeric


def _load_config(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("Failed to load PDF threshold config at %s: %s", path, exc)
        return {}
    if not isinstance(loaded, dict):
        LOGGER.warning("Invalid PDF threshold config at %s: expected a JSON object", path)
        return {}
    return loaded


def _resolve_section_threshold(
    section_payload: Any,
    key: str,
    fallback: float,
) -> float:
    if not isinstance(section_payload, Mapping):
        return fallback
    value = _float_between_0_and_1(section_payload.get(key))
    return fallback if value is None else value


def _quality_floors_from_payload(
    payload: Mapping[str, Any] | None,
    *,
    fallback: PdfQualityFloors | None = None,
) -> PdfQualityFloors:
    precision = fallback.precision_min if fallback is not None else DEFAULT_PRECISION_MIN
    recall = fallback.recall_min if fallback is not None else DEFAULT_RECALL_MIN
    if payload is not None:
        configured_precision = _float_between_0_and_1(payload.get("precision_min"))
        configured_recall = _float_between_0_and_1(payload.get("recall_min"))
        precision = precision if configured_precision is None else configured_precision
        recall = recall if configured_recall is None else configured_recall
    return PdfQualityFloors(precision_min=precision, recall_min=recall)


def _section_for_key(config: Mapping[str, Any], section_name: str, key: str | None) -> Mapping[str, Any] | None:
    if key is None:
        return None

    section = config.get(section_name)
    if not isinstance(section, Mapping):
        return None

    payload = section.get(key)
    if isinstance(payload, Mapping):
        return payload
    return None


def _resolve_config_path() -> Path:
    env_value = os.environ.get(CONFIG_ENV_VAR)
    if env_value and env_value.strip():
        return Path(env_value.strip())
    return _default_config_path()


def resolve_pdf_threshold_policy(request: PdfSubagentRequest) -> PdfThresholdPolicy:
    """Resolve row/page thresholds and quality floors from config + request context."""

    config_path = _resolve_config_path()
    config = _load_config(config_path)

    defaults = config.get("defaults") if isinstance(config.get("defaults"), Mapping) else None
    row_threshold = _resolve_section_threshold(defaults, "row_confidence_threshold", DEFAULT_ROW_THRESHOLD)
    page_threshold = _resolve_section_threshold(defaults, "page_confidence_threshold", DEFAULT_PAGE_THRESHOLD)
    floors = _quality_floors_from_payload(defaults)
    source = "config.defaults"

    metadata = request.metadata if isinstance(request.metadata, Mapping) else {}

    template_key = _normalize_key(request.template_hint)
    template_payload = _section_for_key(config, "templates", template_key)
    if template_payload is not None:
        row_threshold = _resolve_section_threshold(template_payload, "row_confidence_threshold", row_threshold)
        page_threshold = _resolve_section_threshold(template_payload, "page_confidence_threshold", page_threshold)
        floors = _quality_floors_from_payload(template_payload, fallback=floors)
        source = f"config.templates.{template_key}"

    issuer_key = _normalize_key(_metadata_value(metadata, "issuer"))
    issuer_payload = _section_for_key(config, "issuers", issuer_key)
    if template_payload is None and issuer_payload is not None:
        row_threshold = _resolve_section_threshold(issuer_payload, "row_confidence_threshold", row_threshold)
        page_threshold = _resolve_section_threshold(issuer_payload, "page_confidence_threshold", page_threshold)
        floors = _quality_floors_from_payload(issuer_payload, fallback=floors)
        source = f"config.issuers.{issuer_key}"

    row_override = _float_between_0_and_1(_metadata_value(metadata, "confidence_threshold_override"))
    page_override = _float_between_0_and_1(_metadata_value(metadata, "page_confidence_threshold_override"))

    if row_override is not None:
        row_threshold = row_override
        source = "request.metadata.confidence_threshold_override"
    if page_override is not None:
        page_threshold = page_override
        if source == "request.metadata.confidence_threshold_override":
            source = "request.metadata.confidence_threshold_override+page_confidence_threshold_override"
        else:
            source = "request.metadata.page_confidence_threshold_override"

    if row_override is None and template_payload is None and issuer_payload is None:
        row_threshold = request.confidence_threshold
        source = "request.confidence_threshold"

    return PdfThresholdPolicy(
        row_confidence_threshold=row_threshold,
        page_confidence_threshold=page_threshold,
        precision_min=floors.precision_min,
        recall_min=floors.recall_min,
        source=source,
        config_path=str(config_path),
    )


def resolve_quality_floors(
    *,
    template_hint: str | None = None,
    issuer: str | None = None,
) -> PdfQualityFloors:
    """Resolve quality floors for fixture assertions outside orchestrator request flow."""

    config_path = _resolve_config_path()
    config = _load_config(config_path)

    defaults_payload = config.get("defaults") if isinstance(config.get("defaults"), Mapping) else None
    floors = _quality_floors_from_payload(defaults_payload)

    template_payload = _section_for_key(config, "templates", _normalize_key(template_hint))
    if template_payload is not None:
        floors = _quality_floors_from_payload(template_payload, fallback=floors)
    else:
        issuer_payload = _section_for_key(config, "issuers", _normalize_key(issuer))
        if issuer_payload is not None:
            floors = _quality_floors_from_payload(issuer_payload, fallback=floors)

    return floors
