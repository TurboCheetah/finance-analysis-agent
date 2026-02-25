"""Typed contracts for categorize suggestion and suggestion metrics services."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class CategorizeSuggestRequest:
    actor: str
    reason: str
    scope_transaction_ids: list[str] = field(default_factory=list)
    include_pending: bool = False
    confidence_threshold: float | None = None
    provider: str = "heuristic_v1"
    history_limit: int = 5000
    limit: int = 500


@dataclass(slots=True)
class SuggestionCandidate:
    transaction_id: str
    suggested_category_id: str
    confidence: float
    reason_codes: list[str] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    queued_review_item_id: str | None = None


@dataclass(slots=True)
class CategorizeSuggestResult:
    run_metadata_id: str
    provider: str
    threshold_used: float
    generated: int
    queued: int
    low_confidence: int
    high_confidence: int
    skipped: dict[str, int] = field(default_factory=dict)
    suggestions: list[SuggestionCandidate] = field(default_factory=list)


@dataclass(slots=True)
class SuggestionMetricsRequest:
    since: datetime | None = None
    until: datetime | None = None
    actor: str | None = None


@dataclass(slots=True)
class SuggestionMetricsResult:
    approved_count: int
    rejected_count: int
    approval_rate: float | None
    by_reason_code: dict[str, int] = field(default_factory=dict)
    by_suggestion_kind: dict[str, int] = field(default_factory=dict)
