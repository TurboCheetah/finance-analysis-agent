"""Typed contracts for persisted quality and trust metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any


class MetricAlertStatus(StrEnum):
    OK = "ok"
    ALERT = "alert"
    NO_DATA = "no_data"


@dataclass(slots=True)
class MetricObservationRecord:
    metric_group: str
    metric_key: str
    period_start: date
    period_end: date
    alert_status: MetricAlertStatus
    metric_value: float | None = None
    account_id: str | None = None
    template_key: str | None = None
    numerator: float | None = None
    denominator: float | None = None
    threshold_value: float | None = None
    threshold_operator: str | None = None
    dimensions: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None


@dataclass(slots=True)
class QualityMetricsGenerateRequest:
    actor: str
    reason: str
    period_month: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    account_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class QualityMetricsGenerateResult:
    run_metadata_id: str
    period_start: date
    period_end: date
    observations: list[MetricObservationRecord] = field(default_factory=list)
    alert_count: int = 0


@dataclass(slots=True)
class MetricObservationQueryRequest:
    period_start: date | None = None
    period_end: date | None = None
    account_ids: list[str] = field(default_factory=list)
    template_keys: list[str] = field(default_factory=list)
    metric_keys: list[str] = field(default_factory=list)
    metric_groups: list[str] = field(default_factory=list)
    alert_statuses: list[MetricAlertStatus | str] = field(default_factory=list)


@dataclass(slots=True)
class MetricObservationQueryResult:
    observations: list[MetricObservationRecord] = field(default_factory=list)
