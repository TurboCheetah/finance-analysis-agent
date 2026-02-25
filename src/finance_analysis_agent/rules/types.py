"""Typed contracts for deterministic rules engine execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any


class RuleRunMode(StrEnum):
    MANUAL = "manual"
    RETROACTIVE = "retroactive"


@dataclass(slots=True)
class RuleScope:
    date_from: date | None = None
    date_to: date | None = None
    account_ids: list[str] = field(default_factory=list)
    transaction_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RulesApplyRequest:
    scope: RuleScope
    dry_run: bool
    run_mode: RuleRunMode
    actor: str
    reason: str


@dataclass(slots=True)
class RuleDiff:
    transaction_id: str
    rule_ids: list[str]
    before: dict[str, Any]
    after: dict[str, Any]
    details: dict[str, Any]


@dataclass(slots=True)
class RuleApplyResult:
    dry_run: bool
    evaluated_rules: int
    matched_rules: int
    changed_transactions: int
    rule_run_ids: list[str]
    diffs: list[RuleDiff]
