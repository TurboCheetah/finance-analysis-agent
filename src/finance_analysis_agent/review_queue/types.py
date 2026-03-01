"""Typed contracts for review queue listing and bulk triage workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from finance_analysis_agent.db.models import ReviewItem


class ReviewItemStatus(StrEnum):
    TO_REVIEW = "to_review"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    REJECTED = "rejected"


class ReviewSource(StrEnum):
    PDF_EXTRACT = "pdf_extract"
    RULES = "rules"
    DEDUPE = "dedupe"
    CATEGORIZE = "categorize"
    RECURRING = "recurring"
    UNKNOWN = "unknown"


class BulkActionType(StrEnum):
    RECATEGORIZE = "recategorize"
    MARK_DUPLICATE = "mark_duplicate"
    APPROVE_SUGGESTION = "approve_suggestion"
    REJECT_SUGGESTION = "reject_suggestion"
    ASSIGN = "assign"
    UNASSIGN = "unassign"
    MARK_IN_PROGRESS = "mark_in_progress"


@dataclass(slots=True)
class ReviewQueueListRequest:
    statuses: list[ReviewItemStatus | str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    sources: list[ReviewSource | str] = field(default_factory=list)
    assigned_to: str | None = None
    confidence_min: float | None = None
    confidence_max: float | None = None
    limit: int = 100
    offset: int = 0


@dataclass(slots=True)
class ReviewQueueListResult:
    total_count: int
    items: list[ReviewItem]


@dataclass(slots=True)
class ItemTriageOutcome:
    review_item_id: str
    outcome: str
    status: str
    message: str | None = None


@dataclass(slots=True)
class BulkTriageRequest:
    action: BulkActionType | str
    review_item_ids: list[str]
    actor: str
    reason: str
    category_id: str | None = None
    assignee: str | None = None


@dataclass(slots=True)
class BulkTriageResult:
    action: BulkActionType
    total_targeted: int
    updated: int
    failed: int
    skipped: int
    item_outcomes: list[ItemTriageOutcome] = field(default_factory=list)
