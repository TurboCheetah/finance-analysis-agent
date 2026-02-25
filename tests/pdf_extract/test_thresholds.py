from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest
from finance_analysis_agent.pdf_extract.thresholds import (
    CONFIG_ENV_VAR,
    resolve_pdf_threshold_policy,
    resolve_quality_floors,
)


def _request(*, template_hint: str | None = None, metadata: dict | None = None) -> PdfSubagentRequest:
    return PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(Path("/") / "tmp" / "fixture.pdf"),
        account_id="acct-threshold",
        schema_version="1.0.0",
        actor="threshold-test",
        confidence_threshold=0.8,
        conflict_mode=ConflictMode.NORMAL,
        template_hint=template_hint,
        metadata=metadata or {},
    )


def test_resolves_template_thresholds_from_config() -> None:
    policy = resolve_pdf_threshold_policy(_request(template_hint="capital_one_credit"))

    assert policy.row_confidence_threshold == 0.83
    assert policy.page_confidence_threshold == 0.77
    assert policy.source == "config.templates.capital_one_credit"


def test_resolves_issuer_thresholds_when_template_missing() -> None:
    policy = resolve_pdf_threshold_policy(_request(metadata={"issuer": "chime"}))

    assert policy.row_confidence_threshold == 0.78
    assert policy.page_confidence_threshold == 0.72
    assert policy.source == "config.issuers.chime"


def test_metadata_override_takes_priority() -> None:
    policy = resolve_pdf_threshold_policy(
        _request(
            template_hint="chime_non_tabular",
            metadata={
                "issuer": "chime",
                "confidence_threshold_override": "0.66",
                "page_confidence_threshold_override": 0.61,
            },
        )
    )

    assert policy.row_confidence_threshold == 0.66
    assert policy.page_confidence_threshold == 0.61
    assert policy.source == "request.metadata.confidence_threshold_override+page_confidence_threshold_override"


def test_request_threshold_is_used_when_no_config_match() -> None:
    request = _request(template_hint="unknown_template")
    request.confidence_threshold = 0.74

    policy = resolve_pdf_threshold_policy(request)

    assert policy.row_confidence_threshold == 0.74
    assert policy.page_confidence_threshold == 0.75
    assert policy.source == "request.confidence_threshold"


def test_non_string_issuer_metadata_does_not_crash_and_is_ignored() -> None:
    request = _request(metadata={"issuer": 42})
    request.confidence_threshold = 0.73

    policy = resolve_pdf_threshold_policy(request)

    assert policy.row_confidence_threshold == 0.73
    assert policy.source == "request.confidence_threshold"


def test_page_override_preserves_mixed_source_provenance_when_row_uses_request_fallback() -> None:
    request = _request(metadata={"page_confidence_threshold_override": 0.61})
    request.confidence_threshold = 0.74

    policy = resolve_pdf_threshold_policy(request)

    assert policy.row_confidence_threshold == 0.74
    assert policy.page_confidence_threshold == 0.61
    assert policy.source == "request.confidence_threshold+request.metadata.page_confidence_threshold_override"


def test_quality_floors_default_to_global_values() -> None:
    floors = resolve_quality_floors()

    assert floors.precision_min == 0.99
    assert floors.recall_min == 0.9


def test_zero_threshold_values_are_not_overwritten_by_fallbacks(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "thresholds.json"
    config_path.write_text(
        json.dumps(
            {
                "defaults": {
                    "row_confidence_threshold": 0.0,
                    "page_confidence_threshold": 0.0,
                    "precision_min": 0.0,
                    "recall_min": 0.0,
                },
                "templates": {"zero_template": {"row_confidence_threshold": 0.0, "page_confidence_threshold": 0.0}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    policy = resolve_pdf_threshold_policy(_request(template_hint="zero_template"))
    floors = resolve_quality_floors(template_hint="zero_template")

    assert policy.row_confidence_threshold == 0.0
    assert policy.page_confidence_threshold == 0.0
    assert floors.precision_min == 0.0
    assert floors.recall_min == 0.0


def test_invalid_config_logs_warning_and_falls_back(monkeypatch, tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-thresholds.json"
    monkeypatch.setenv(CONFIG_ENV_VAR, str(missing_path))

    with patch("finance_analysis_agent.pdf_extract.thresholds.LOGGER.warning") as warning_mock:
        policy = resolve_pdf_threshold_policy(_request())

    assert policy.row_confidence_threshold == 0.8
    assert policy.source == "request.confidence_threshold"
    warning_mock.assert_called()
    warning_text = str(warning_mock.call_args.args[0])
    assert "Failed to load PDF threshold config" in warning_text
