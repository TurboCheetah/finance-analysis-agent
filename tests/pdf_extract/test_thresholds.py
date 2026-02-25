from __future__ import annotations

from pathlib import Path

from finance_analysis_agent.ingest.types import ConflictMode
from finance_analysis_agent.pdf_contract.types import PdfSubagentRequest
from finance_analysis_agent.pdf_extract.thresholds import (
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


def test_quality_floors_default_to_global_values() -> None:
    floors = resolve_quality_floors()

    assert floors.precision_min == 0.99
    assert floors.recall_min == 0.9
