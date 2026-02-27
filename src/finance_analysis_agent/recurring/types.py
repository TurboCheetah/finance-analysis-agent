"""Typed contracts for recurring detection and scheduling workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass(slots=True)
class RecurringDetectCause:
    code: str
    message: str
    severity: str


@dataclass(slots=True)
class RecurringScheduleSnapshot:
    recurring_id: str
    merchant_id: str | None
    category_id: str | None
    schedule_type: str
    interval_n: int
    anchor_date: date
    tolerance_days: int
    observed_count: int
    expected_count: int
    missed_count: int
    last_observed_date: date | None


@dataclass(slots=True)
class RecurringEventWarning:
    recurring_event_id: str
    recurring_id: str
    expected_date: date
    tolerance_days: int
    reason_code: str
    review_item_id: str | None


@dataclass(slots=True)
class RecurringDetectRequest:
    as_of_date: date
    actor: str
    reason: str
    lookback_days: int = 365
    minimum_occurrences: int = 3
    tolerance_days_default: int = 3
    max_expected_iterations: int = 400
    create_review_items: bool = True


@dataclass(slots=True)
class RecurringDetectResult:
    as_of_date: date
    schedules: list[RecurringScheduleSnapshot] = field(default_factory=list)
    warnings: list[RecurringEventWarning] = field(default_factory=list)
    causes: list[RecurringDetectCause] = field(default_factory=list)
