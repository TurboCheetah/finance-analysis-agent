"""Typed contracts for deterministic finance reporting workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any


class ReportType(StrEnum):
    CASH_FLOW = "cash_flow"
    CATEGORY_TRENDS = "category_trends"
    NET_WORTH = "net_worth"
    BUDGET_VS_ACTUAL = "budget_vs_actual"
    GOAL_PROGRESS = "goal_progress"
    QUALITY_TRUST_DASHBOARD = "quality_trust_dashboard"


@dataclass(slots=True)
class ReportRunCause:
    code: str
    message: str
    severity: str


@dataclass(slots=True)
class GeneratedReport:
    report_id: str
    report_type: ReportType
    payload_hash: str
    payload_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ReportingGenerateRequest:
    actor: str
    reason: str
    period_month: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    report_types: list[ReportType | str] = field(default_factory=list)
    account_ids: list[str] = field(default_factory=list)
    budget_id: str | None = None


@dataclass(slots=True)
class ReportingGenerateResult:
    run_metadata_id: str
    period_start: date
    period_end: date
    report_types: list[ReportType] = field(default_factory=list)
    reports: list[GeneratedReport] = field(default_factory=list)
    causes: list[ReportRunCause] = field(default_factory=list)
