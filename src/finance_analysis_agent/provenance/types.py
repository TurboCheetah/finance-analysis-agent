"""Types for immutable transaction event trail and provenance services."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

TRACKED_TRANSACTION_FIELDS = ("category_id", "merchant_id", "amount", "excluded")


class ProvenanceSource(StrEnum):
    MANUAL = "manual"
    RULE = "rule"
    HEURISTIC = "heuristic"
    MODEL = "model"


@dataclass(slots=True)
class TransactionMutationRequest:
    transaction_id: str
    actor: str
    reason: str
    provenance: ProvenanceSource | str
    changes: dict[str, Any]


@dataclass(slots=True)
class TransactionMutationResult:
    transaction_id: str
    changed_fields: list[str]
    event_ids: list[str]
    noop: bool


@dataclass(slots=True)
class FieldProvenance:
    field: str
    source: ProvenanceSource
    actor: str | None
    reason: str | None
    event_id: str
    event_type: str
    changed_at: datetime


@dataclass(slots=True)
class TransactionProvenanceResult:
    transaction_id: str
    current_values: dict[str, Any]
    latest_by_field: dict[str, FieldProvenance | None]


@dataclass(slots=True)
class ReplayTransition:
    event_id: str
    field: str
    old_value: Any
    new_value: Any
    source: ProvenanceSource | None
    actor: str | None
    reason: str | None
    changed_at: datetime
    state_after: dict[str, Any]


@dataclass(slots=True)
class TransactionReplayResult:
    transaction_id: str
    transitions: list[ReplayTransition] = field(default_factory=list)
    final_state: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RuleAuditWriteRequest:
    rule_run_id: str
    transaction_id: str
    matched: bool
    changes_json: dict[str, Any] | None = None
    confidence: float | None = None


@dataclass(slots=True)
class RunMetadataStartRequest:
    pipeline_name: str
    code_version: str
    schema_version: str
    config_hash: str
    status: str = "running"
    diagnostics_json: dict[str, Any] | None = None


@dataclass(slots=True)
class RunMetadataFinishRequest:
    run_metadata_id: str
    status: str
    diagnostics_json: dict[str, Any] | None = None


def normalize_tracked_value(field: str, value: Any) -> Any:
    """Normalize tracked field values for JSON/event payloads."""

    if value is None:
        return None
    if field == "amount" and isinstance(value, Decimal):
        return format(value, "f")
    return value

