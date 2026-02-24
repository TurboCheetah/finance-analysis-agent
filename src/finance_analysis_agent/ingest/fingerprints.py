"""Deterministic fingerprint helpers for idempotent ImportBatch ingestion."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from finance_analysis_agent.ingest.types import SourceType

FINGERPRINT_ALGO = "sha256"


def _sha256_hexdigest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonicalize_manual_payload(payload: dict[str, Any] | list[Any]) -> bytes:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return encoded.encode("utf-8")


def compute_source_fingerprint(
    *,
    source_type: SourceType,
    schema_version: str,
    payload_bytes: bytes | None = None,
    manual_payload: dict[str, Any] | list[Any] | None = None,
) -> tuple[str, str]:
    """Compute deterministic source fingerprint for ImportBatch idempotency."""

    if source_type in {SourceType.PDF, SourceType.CSV}:
        if payload_bytes is None:
            raise ValueError("payload_bytes is required for PDF/CSV source types")
        payload_digest = _sha256_hexdigest(payload_bytes)
    else:
        if manual_payload is None:
            raise ValueError("manual_payload is required for manual source type")
        payload_digest = _sha256_hexdigest(_canonicalize_manual_payload(manual_payload))

    envelope = f"{source_type.value}|{schema_version}|{payload_digest}".encode("utf-8")
    return _sha256_hexdigest(envelope), FINGERPRINT_ALGO
