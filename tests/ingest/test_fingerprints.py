from __future__ import annotations

import pytest

from finance_analysis_agent.ingest.fingerprints import compute_source_fingerprint
from finance_analysis_agent.ingest.types import SourceType


def test_manual_payload_fingerprint_is_order_independent() -> None:
    payload_a = {"amount": 12.34, "merchant": "Coffee", "tags": ["food", "work"]}
    payload_b = {"tags": ["food", "work"], "merchant": "Coffee", "amount": 12.34}

    fp_a, algo_a = compute_source_fingerprint(
        source_type=SourceType.MANUAL,
        schema_version="1.0.0",
        manual_payload=payload_a,
    )
    fp_b, algo_b = compute_source_fingerprint(
        source_type=SourceType.MANUAL,
        schema_version="1.0.0",
        manual_payload=payload_b,
    )

    assert algo_a == "sha256"
    assert algo_b == "sha256"
    assert fp_a == fp_b


def test_schema_version_changes_fingerprint() -> None:
    payload = b"date,description,amount\n2026-01-01,Coffee,-4.50\n"

    fp_v1, _ = compute_source_fingerprint(
        source_type=SourceType.CSV,
        schema_version="1.0.0",
        payload_bytes=payload,
    )
    fp_v2, _ = compute_source_fingerprint(
        source_type=SourceType.CSV,
        schema_version="2.0.0",
        payload_bytes=payload,
    )

    assert fp_v1 != fp_v2


def test_csv_requires_payload_bytes_when_none() -> None:
    with pytest.raises(ValueError, match="payload_bytes"):
        compute_source_fingerprint(
            source_type=SourceType.CSV,
            schema_version="1.0.0",
            payload_bytes=None,
        )


def test_manual_requires_manual_payload() -> None:
    with pytest.raises(ValueError, match="manual_payload"):
        compute_source_fingerprint(
            source_type=SourceType.MANUAL,
            schema_version="1.0.0",
            manual_payload=None,
        )


def test_csv_allows_empty_bytes_payload() -> None:
    fingerprint, algo = compute_source_fingerprint(
        source_type=SourceType.CSV,
        schema_version="1.0.0",
        payload_bytes=b"",
    )

    assert algo == "sha256"
    assert isinstance(fingerprint, str)
    assert fingerprint
