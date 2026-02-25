"""Deterministic rules engine with ordered matchers, actions, and dry-run support."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

from sqlalchemy import Select, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    Category,
    Goal,
    GoalEvent,
    Merchant,
    ReviewItem,
    Rule,
    RuleRun,
    Tag,
    Transaction,
    TransactionTag,
)
from finance_analysis_agent.provenance.audit_writers import record_rule_audit
from finance_analysis_agent.provenance.transaction_events_service import mutate_transaction_fields
from finance_analysis_agent.provenance.types import ProvenanceSource, RuleAuditWriteRequest, TransactionMutationRequest
from finance_analysis_agent.rules.types import RuleApplyResult, RuleDiff, RuleRunMode, RuleScope, RulesApplyRequest
from finance_analysis_agent.utils.time import utcnow

_REVIEW_REASON_CODE = "rule.needs_review"
_REVIEW_ITEM_TYPE = "transaction_rule"
_REVIEW_REF_TABLE = "transactions"
_REVIEW_STATUS_OPEN = "open"
_REVIEW_STATUS_RESOLVED = "resolved"
_REVIEW_STATUS_NEEDS_REVIEW = "needs_review"
_REVIEW_STATUS_REVIEWED = "reviewed"
_GOAL_EVENT_TYPE = "rule.linked_transaction"
_UNSET = object()


@dataclass(slots=True)
class _AmountPredicate:
    op: str
    value: Decimal | None = None
    min_value: Decimal | None = None
    max_value: Decimal | None = None
    inclusive: bool = True


@dataclass(slots=True)
class _TextMatcher:
    op: str
    value: str


@dataclass(slots=True)
class _MatcherSpec:
    merchant: _TextMatcher | None = None
    original_statement: _TextMatcher | None = None
    amount: _AmountPredicate | None = None
    account_in: set[str] | None = None
    category_in: set[str | None] | None = None
    pending_status_in: set[str] | None = None


@dataclass(slots=True)
class _ActionSpec:
    rename_merchant: str | None = None
    set_category: str | None | object = _UNSET
    add_tags: list[str] = field(default_factory=list)
    set_excluded: bool | object = _UNSET
    set_review_status: str | None = None
    link_goal: str | None = None


@dataclass(slots=True)
class _RuleSpec:
    rule: Rule
    matcher: _MatcherSpec
    action: _ActionSpec


@dataclass(slots=True)
class _TransactionState:
    transaction_id: str
    account_id: str
    pending_status: str
    posted_date: date
    amount: Decimal
    original_statement: str | None
    merchant_name: str | None
    category_id: str | None
    excluded: bool
    tags: set[str]
    has_open_review: bool
    linked_goal_ids: set[str]

    def snapshot(self) -> dict[str, Any]:
        return {
            "merchant_name": self.merchant_name,
            "category_id": self.category_id,
            "excluded": self.excluded,
            "tags": sorted(self.tags),
            "goal_links": sorted(self.linked_goal_ids),
            "review_status": (
                _REVIEW_STATUS_NEEDS_REVIEW if self.has_open_review else _REVIEW_STATUS_REVIEWED
            ),
        }


@dataclass(slots=True)
class _RuleEvaluation:
    matched: bool
    changes_json: dict[str, Any] | None
    merchant_name_target: str | None = None
    category_target: str | None | object = _UNSET
    excluded_target: bool | object = _UNSET
    added_tags: list[str] = field(default_factory=list)
    review_action: str | None = None
    goal_id: str | None = None

    @property
    def has_effect(self) -> bool:
        return bool(
            self.merchant_name_target is not None
            or self.category_target is not _UNSET
            or self.excluded_target is not _UNSET
            or self.added_tags
            or self.review_action is not None
            or self.goal_id is not None
        )


@dataclass(slots=True)
class _TransactionTrace:
    before: dict[str, Any]
    matched_rule_ids: list[str] = field(default_factory=list)
    per_rule: list[dict[str, Any]] = field(default_factory=list)


def _parse_decimal(raw: Any, *, field_name: str, rule_id: str) -> Decimal:
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid decimal for {field_name} in rule {rule_id}") from exc


def _parse_text_matcher(raw: Any, *, field_name: str, rule_id: str) -> _TextMatcher:
    if not isinstance(raw, dict):
        raise ValueError(f"{field_name} matcher must be an object in rule {rule_id}")
    if set(raw) not in ({"exact"}, {"contains"}):
        raise ValueError(
            f"{field_name} matcher must contain exactly one of 'exact' or 'contains' in rule {rule_id}"
        )
    key = "exact" if "exact" in raw else "contains"
    value = raw[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name}.{key} must be a non-empty string in rule {rule_id}")
    return _TextMatcher(op=key, value=value.strip())


def _parse_amount_matcher(raw: Any, *, rule_id: str) -> _AmountPredicate:
    if not isinstance(raw, dict):
        raise ValueError(f"amount matcher must be an object in rule {rule_id}")
    if len(raw) != 1:
        raise ValueError(
            f"amount matcher must contain exactly one predicate in rule {rule_id}"
        )
    op = next(iter(raw))
    payload = raw[op]
    if op in {"eq", "gt", "lt"}:
        return _AmountPredicate(op=op, value=_parse_decimal(payload, field_name=f"amount.{op}", rule_id=rule_id))
    if op != "between":
        raise ValueError(f"Unsupported amount predicate '{op}' in rule {rule_id}")
    if not isinstance(payload, dict):
        raise ValueError(f"amount.between must be an object in rule {rule_id}")
    if "min" not in payload or "max" not in payload:
        raise ValueError(f"amount.between requires min and max in rule {rule_id}")
    min_value = _parse_decimal(payload["min"], field_name="amount.between.min", rule_id=rule_id)
    max_value = _parse_decimal(payload["max"], field_name="amount.between.max", rule_id=rule_id)
    if min_value > max_value:
        raise ValueError(f"amount.between min must be <= max in rule {rule_id}")
    inclusive = payload.get("inclusive", True)
    if not isinstance(inclusive, bool):
        raise ValueError(f"amount.between.inclusive must be boolean in rule {rule_id}")
    return _AmountPredicate(op="between", min_value=min_value, max_value=max_value, inclusive=inclusive)


def _parse_in_list(
    raw: Any,
    *,
    field_name: str,
    rule_id: str,
    allow_null: bool = False,
) -> set[str | None]:
    if not isinstance(raw, dict) or set(raw) != {"in"}:
        raise ValueError(f"{field_name} matcher must be {{\"in\": [...]}} in rule {rule_id}")
    values = raw["in"]
    if not isinstance(values, list):
        raise ValueError(f"{field_name}.in must be a list in rule {rule_id}")
    parsed: set[str | None] = set()
    for value in values:
        if value is None:
            if not allow_null:
                raise ValueError(f"{field_name}.in cannot include null in rule {rule_id}")
            parsed.add(None)
            continue
        if not isinstance(value, str):
            raise ValueError(f"{field_name}.in must contain non-empty strings in rule {rule_id}")
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError(f"{field_name}.in must contain non-empty strings in rule {rule_id}")
        parsed.add(normalized_value)
    return parsed


def _parse_matcher(raw: Any, *, rule_id: str) -> _MatcherSpec:
    if not isinstance(raw, dict):
        raise ValueError(f"matcher_json must be an object in rule {rule_id}")
    allowed_keys = {
        "merchant",
        "original_statement",
        "amount",
        "account",
        "category",
        "pending_status",
    }
    unknown = set(raw) - allowed_keys
    if unknown:
        raise ValueError(
            f"Unsupported matcher keys for rule {rule_id}: {', '.join(sorted(unknown))}"
        )

    spec = _MatcherSpec()
    if "merchant" in raw:
        spec.merchant = _parse_text_matcher(raw["merchant"], field_name="merchant", rule_id=rule_id)
    if "original_statement" in raw:
        spec.original_statement = _parse_text_matcher(
            raw["original_statement"], field_name="original_statement", rule_id=rule_id
        )
    if "amount" in raw:
        spec.amount = _parse_amount_matcher(raw["amount"], rule_id=rule_id)
    if "account" in raw:
        account_values = _parse_in_list(raw["account"], field_name="account", rule_id=rule_id)
        spec.account_in = {value for value in account_values if isinstance(value, str)}
    if "category" in raw:
        spec.category_in = _parse_in_list(
            raw["category"], field_name="category", rule_id=rule_id, allow_null=True
        )
    if "pending_status" in raw:
        status_values = _parse_in_list(
            raw["pending_status"], field_name="pending_status", rule_id=rule_id
        )
        spec.pending_status_in = {value for value in status_values if isinstance(value, str)}

    return spec


def _parse_action(raw: Any, *, rule_id: str) -> _ActionSpec:
    if not isinstance(raw, dict):
        raise ValueError(f"action_json must be an object in rule {rule_id}")
    allowed_keys = {
        "rename_merchant",
        "set_category",
        "add_tags",
        "set_excluded",
        "set_review_status",
        "link_goal",
    }
    unknown = set(raw) - allowed_keys
    if unknown:
        raise ValueError(
            f"Unsupported action keys for rule {rule_id}: {', '.join(sorted(unknown))}"
        )
    if not raw:
        raise ValueError(f"action_json cannot be empty in rule {rule_id}")

    action = _ActionSpec()

    if "rename_merchant" in raw:
        rename_value = raw["rename_merchant"]
        if not isinstance(rename_value, str) or not rename_value.strip():
            raise ValueError(f"rename_merchant must be a non-empty string in rule {rule_id}")
        action.rename_merchant = rename_value.strip()

    if "set_category" in raw:
        category_value = raw["set_category"]
        if category_value is not None and (
            not isinstance(category_value, str) or not category_value.strip()
        ):
            raise ValueError(f"set_category must be string or null in rule {rule_id}")
        action.set_category = category_value

    if "add_tags" in raw:
        tag_values = raw["add_tags"]
        if not isinstance(tag_values, list):
            raise ValueError(f"add_tags must be a list in rule {rule_id}")
        normalized: list[str] = []
        seen: set[str] = set()
        for value in tag_values:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"add_tags must contain non-empty strings in rule {rule_id}")
            name = value.strip()
            if name not in seen:
                seen.add(name)
                normalized.append(name)
        action.add_tags = normalized

    if "set_excluded" in raw:
        excluded_value = raw["set_excluded"]
        if not isinstance(excluded_value, bool):
            raise ValueError(f"set_excluded must be boolean in rule {rule_id}")
        action.set_excluded = excluded_value

    if "set_review_status" in raw:
        review_value = raw["set_review_status"]
        if review_value not in {_REVIEW_STATUS_NEEDS_REVIEW, _REVIEW_STATUS_REVIEWED}:
            raise ValueError(
                f"set_review_status must be '{_REVIEW_STATUS_NEEDS_REVIEW}' or '{_REVIEW_STATUS_REVIEWED}' in rule {rule_id}"
            )
        action.set_review_status = review_value

    if "link_goal" in raw:
        goal_value = raw["link_goal"]
        if not isinstance(goal_value, str) or not goal_value.strip():
            raise ValueError(f"link_goal must be a non-empty string in rule {rule_id}")
        action.link_goal = goal_value.strip()

    return action


def _build_scope_statement(scope: RuleScope) -> Select[tuple[Transaction]]:
    stmt = select(Transaction).order_by(
        Transaction.posted_date.asc(),
        Transaction.created_at.asc(),
        Transaction.id.asc(),
    )
    if scope.date_from is not None:
        stmt = stmt.where(Transaction.posted_date >= scope.date_from)
    if scope.date_to is not None:
        stmt = stmt.where(Transaction.posted_date <= scope.date_to)
    if scope.account_ids:
        stmt = stmt.where(Transaction.account_id.in_(scope.account_ids))
    if scope.transaction_ids:
        stmt = stmt.where(Transaction.id.in_(scope.transaction_ids))
    return stmt


def _apply_text_matcher(matcher: _TextMatcher, candidate: str | None) -> bool:
    if candidate is None:
        return False
    needle = matcher.value.casefold()
    haystack = candidate.casefold()
    if matcher.op == "exact":
        return haystack == needle
    return needle in haystack


def _apply_amount_matcher(matcher: _AmountPredicate, amount: Decimal) -> bool:
    if matcher.op == "eq":
        return amount == matcher.value
    if matcher.op == "gt":
        return amount > matcher.value
    if matcher.op == "lt":
        return amount < matcher.value
    if matcher.inclusive:
        return matcher.min_value <= amount <= matcher.max_value
    return matcher.min_value < amount < matcher.max_value


def _matches(rule: _RuleSpec, state: _TransactionState) -> bool:
    matcher = rule.matcher
    if matcher.merchant is not None and not _apply_text_matcher(matcher.merchant, state.merchant_name):
        return False
    if matcher.original_statement is not None and not _apply_text_matcher(
        matcher.original_statement, state.original_statement
    ):
        return False
    if matcher.amount is not None and not _apply_amount_matcher(matcher.amount, state.amount):
        return False
    if matcher.account_in is not None and state.account_id not in matcher.account_in:
        return False
    if matcher.category_in is not None and state.category_id not in matcher.category_in:
        return False
    if matcher.pending_status_in is not None and state.pending_status not in matcher.pending_status_in:
        return False
    return True


def _simulate_action(rule: _RuleSpec, state: _TransactionState) -> _RuleEvaluation:
    changes: dict[str, Any] = {}
    action = rule.action
    eval_result = _RuleEvaluation(matched=True, changes_json=None)

    if action.rename_merchant is not None and state.merchant_name != action.rename_merchant:
        changes["rename_merchant"] = {
            "old": state.merchant_name,
            "new": action.rename_merchant,
        }
        state.merchant_name = action.rename_merchant
        eval_result.merchant_name_target = action.rename_merchant

    if action.set_category is not _UNSET and state.category_id != action.set_category:
        changes["set_category"] = {
            "old": state.category_id,
            "new": action.set_category,
        }
        state.category_id = action.set_category
        eval_result.category_target = action.set_category

    if action.set_excluded is not _UNSET and state.excluded != action.set_excluded:
        changes["set_excluded"] = {
            "old": state.excluded,
            "new": action.set_excluded,
        }
        state.excluded = action.set_excluded
        eval_result.excluded_target = action.set_excluded

    if action.add_tags:
        added: list[str] = []
        for tag_name in action.add_tags:
            if tag_name not in state.tags:
                state.tags.add(tag_name)
                added.append(tag_name)
        if added:
            changes["add_tags"] = {"added": added}
            eval_result.added_tags = added

    if action.set_review_status == _REVIEW_STATUS_NEEDS_REVIEW and not state.has_open_review:
        state.has_open_review = True
        eval_result.review_action = "open"
        changes["set_review_status"] = {"old": _REVIEW_STATUS_REVIEWED, "new": _REVIEW_STATUS_NEEDS_REVIEW}
    elif action.set_review_status == _REVIEW_STATUS_REVIEWED and state.has_open_review:
        state.has_open_review = False
        eval_result.review_action = "resolve"
        changes["set_review_status"] = {"old": _REVIEW_STATUS_NEEDS_REVIEW, "new": _REVIEW_STATUS_REVIEWED}

    if action.link_goal is not None:
        if action.link_goal not in state.linked_goal_ids:
            state.linked_goal_ids.add(action.link_goal)
            eval_result.goal_id = action.link_goal
            changes["link_goal"] = {"goal_id": action.link_goal}

    if not changes:
        eval_result.changes_json = {"noop": True}
    else:
        eval_result.changes_json = changes

    return eval_result


def _validate_references(rule_specs: list[_RuleSpec], session: Session) -> None:
    category_ids = {
        spec.action.set_category
        for spec in rule_specs
        if spec.action.set_category is not _UNSET and spec.action.set_category is not None
    }
    goal_ids = {spec.action.link_goal for spec in rule_specs if spec.action.link_goal is not None}

    if category_ids:
        found = set(
            session.scalars(select(Category.id).where(Category.id.in_(sorted(category_ids))))
        )
        missing = sorted(category_ids - found)
        if missing:
            raise ValueError(f"Unknown category id(s) in rule actions: {', '.join(missing)}")

    if goal_ids:
        found = set(session.scalars(select(Goal.id).where(Goal.id.in_(sorted(goal_ids)))))
        missing = sorted(goal_ids - found)
        if missing:
            raise ValueError(f"Unknown goal id(s) in rule actions: {', '.join(missing)}")


def _build_rule_specs(session: Session) -> list[_RuleSpec]:
    rules = session.scalars(
        select(Rule)
        .where(Rule.enabled.is_(True))
        .order_by(Rule.priority.asc(), Rule.id.asc())
    ).all()
    specs: list[_RuleSpec] = []
    for rule in rules:
        specs.append(
            _RuleSpec(
                rule=rule,
                matcher=_parse_matcher(rule.matcher_json, rule_id=rule.id),
                action=_parse_action(rule.action_json, rule_id=rule.id),
            )
        )
    return specs


def _run_simulation(
    *,
    transactions: list[Transaction],
    rule_specs: list[_RuleSpec],
    merchant_names_by_id: dict[str, str],
    tag_names_by_transaction: dict[str, set[str]],
    open_reviews_by_transaction: dict[str, list[ReviewItem]],
    linked_goals_by_transaction: dict[str, set[str]],
) -> tuple[
    dict[str, _TransactionState],
    dict[str, _TransactionTrace],
    dict[str, list[_RuleEvaluation]],
    int,
    set[str],
    dict[str, dict[str, int]],
]:
    states: dict[str, _TransactionState] = {}
    traces: dict[str, _TransactionTrace] = {}
    evaluations_by_rule: dict[str, list[_RuleEvaluation]] = {spec.rule.id: [] for spec in rule_specs}
    matched_rules = 0
    changed_transactions: set[str] = set()
    rule_summary: dict[str, dict[str, int]] = {
        spec.rule.id: {"evaluated": len(transactions), "matched": 0, "changed": 0}
        for spec in rule_specs
    }

    for transaction in transactions:
        state = _TransactionState(
            transaction_id=transaction.id,
            account_id=transaction.account_id,
            pending_status=transaction.pending_status,
            posted_date=transaction.posted_date,
            amount=transaction.amount,
            original_statement=transaction.original_statement,
            merchant_name=merchant_names_by_id.get(transaction.merchant_id or ""),
            category_id=transaction.category_id,
            excluded=transaction.excluded,
            tags=set(tag_names_by_transaction.get(transaction.id, set())),
            has_open_review=bool(open_reviews_by_transaction.get(transaction.id)),
            linked_goal_ids=set(linked_goals_by_transaction.get(transaction.id, set())),
        )
        states[transaction.id] = state
        traces[transaction.id] = _TransactionTrace(before=state.snapshot())

    for spec in rule_specs:
        for transaction in transactions:
            state = states[transaction.id]
            matched = True
            if state.pending_status == "pending" and not spec.rule.apply_to_pending:
                matched = False
            elif not _matches(spec, state):
                matched = False

            if not matched:
                evaluations_by_rule[spec.rule.id].append(_RuleEvaluation(matched=False, changes_json=None))
                continue

            matched_rules += 1
            rule_summary[spec.rule.id]["matched"] += 1
            trace = traces[transaction.id]
            trace.matched_rule_ids.append(spec.rule.id)

            eval_result = _simulate_action(spec, state)
            if eval_result.has_effect:
                changed_transactions.add(transaction.id)
                rule_summary[spec.rule.id]["changed"] += 1
            trace.per_rule.append(
                {
                    "rule_id": spec.rule.id,
                    "changes": eval_result.changes_json,
                }
            )
            evaluations_by_rule[spec.rule.id].append(eval_result)

    return states, traces, evaluations_by_rule, matched_rules, changed_transactions, rule_summary


def _build_diffs(
    states: dict[str, _TransactionState],
    traces: dict[str, _TransactionTrace],
    changed_transactions: set[str],
) -> list[RuleDiff]:
    diffs: list[RuleDiff] = []
    for transaction_id in sorted(changed_transactions):
        trace = traces[transaction_id]
        after = states[transaction_id].snapshot()
        diffs.append(
            RuleDiff(
                transaction_id=transaction_id,
                rule_ids=trace.matched_rule_ids,
                before=trace.before,
                after=after,
                details={"per_rule": trace.per_rule},
            )
        )
    return diffs


def _merchant_id_for_name(
    canonical_name: str,
    *,
    merchant_ids_by_name: dict[str, str],
    session: Session,
) -> str:
    existing_id = merchant_ids_by_name.get(canonical_name)
    if existing_id is not None:
        return existing_id

    try:
        with session.begin_nested():
            merchant = Merchant(
                id=str(uuid4()),
                canonical_name=canonical_name,
                confidence=None,
                created_at=utcnow(),
            )
            session.add(merchant)
            session.flush()
            merchant_ids_by_name[canonical_name] = merchant.id
            return merchant.id
    except IntegrityError:
        existing_id = session.scalar(
            select(Merchant.id).where(Merchant.canonical_name == canonical_name)
        )
        if existing_id is None:
            raise
        merchant_ids_by_name[canonical_name] = existing_id
        return existing_id


def _tag_id_for_name(
    name: str,
    *,
    tag_ids_by_name: dict[str, str],
    session: Session,
) -> str:
    existing_id = tag_ids_by_name.get(name)
    if existing_id is not None:
        return existing_id
    try:
        with session.begin_nested():
            tag = Tag(id=str(uuid4()), name=name, created_at=utcnow())
            session.add(tag)
            session.flush()
            tag_ids_by_name[name] = tag.id
            return tag.id
    except IntegrityError:
        existing_id = session.scalar(select(Tag.id).where(Tag.name == name))
        if existing_id is None:
            raise
        tag_ids_by_name[name] = existing_id
        return existing_id


def _scope_payload(scope: RuleScope) -> dict[str, Any]:
    return {
        "date_from": scope.date_from.isoformat() if scope.date_from else None,
        "date_to": scope.date_to.isoformat() if scope.date_to else None,
        "account_ids": sorted(scope.account_ids),
        "transaction_ids": sorted(scope.transaction_ids),
    }


def _apply_simulation_results(
    *,
    request: RulesApplyRequest,
    rule_specs: list[_RuleSpec],
    transactions: list[Transaction],
    evaluations_by_rule: dict[str, list[_RuleEvaluation]],
    open_reviews_by_transaction: dict[str, list[ReviewItem]],
    rule_summary: dict[str, dict[str, int]],
    session: Session,
) -> list[str]:
    actor = request.actor.strip()
    reason = request.reason.strip()

    merchant_names_needed = {
        evaluation.merchant_name_target
        for spec in rule_specs
        for evaluation in evaluations_by_rule[spec.rule.id]
        if evaluation.merchant_name_target is not None
    }
    merchant_ids_by_name = {
        merchant.canonical_name: merchant.id
        for merchant in session.scalars(
            select(Merchant).where(Merchant.canonical_name.in_(sorted(merchant_names_needed)))
        ).all()
    }

    tag_names_needed = {
        tag_name
        for spec in rule_specs
        for evaluation in evaluations_by_rule[spec.rule.id]
        for tag_name in evaluation.added_tags
    }
    tag_ids_by_name = {
        tag.name: tag.id
        for tag in session.scalars(select(Tag).where(Tag.name.in_(sorted(tag_names_needed)))).all()
    }
    existing_tag_names_by_transaction: dict[str, set[str]] = {}
    if transactions:
        tx_ids = [txn.id for txn in transactions]
        tag_rows = session.execute(
            select(TransactionTag.transaction_id, Tag.name)
            .join(Tag, Tag.id == TransactionTag.tag_id)
            .where(TransactionTag.transaction_id.in_(tx_ids))
        ).all()
        for transaction_id, tag_name in tag_rows:
            existing_tag_names_by_transaction.setdefault(transaction_id, set()).add(tag_name)

    open_reviews = {
        transaction_id: sorted(
            reviews,
            key=lambda item: (item.created_at, item.id),
        )
        for transaction_id, reviews in open_reviews_by_transaction.items()
    }

    created_run_ids: list[str] = []
    run_started_at = utcnow()
    for spec in rule_specs:
        rule_run = RuleRun(
            id=str(uuid4()),
            rule_id=spec.rule.id,
            run_mode=request.run_mode.value,
            dry_run=False,
            started_at=run_started_at,
            completed_at=None,
            summary_json=None,
        )
        session.add(rule_run)
        session.flush()
        created_run_ids.append(rule_run.id)

        evaluations = evaluations_by_rule[spec.rule.id]
        for transaction, evaluation in zip(transactions, evaluations, strict=True):
            if evaluation.matched and evaluation.has_effect:
                field_changes: dict[str, Any] = {}
                if evaluation.merchant_name_target is not None:
                    field_changes["merchant_id"] = _merchant_id_for_name(
                        evaluation.merchant_name_target,
                        merchant_ids_by_name=merchant_ids_by_name,
                        session=session,
                    )
                if evaluation.category_target is not _UNSET:
                    field_changes["category_id"] = evaluation.category_target
                if evaluation.excluded_target is not _UNSET:
                    field_changes["excluded"] = evaluation.excluded_target

                if field_changes:
                    mutate_transaction_fields(
                        TransactionMutationRequest(
                            transaction_id=transaction.id,
                            actor=actor,
                            reason=f"{reason} [rule:{spec.rule.id}]",
                            provenance=ProvenanceSource.RULE,
                            changes=field_changes,
                        ),
                        session,
                    )

                if evaluation.added_tags:
                    known_tags = existing_tag_names_by_transaction.setdefault(transaction.id, set())
                    for tag_name in evaluation.added_tags:
                        if tag_name in known_tags:
                            continue
                        tag_id = _tag_id_for_name(tag_name, tag_ids_by_name=tag_ids_by_name, session=session)
                        session.add(TransactionTag(transaction_id=transaction.id, tag_id=tag_id))
                        known_tags.add(tag_name)

                if evaluation.review_action == "open":
                    review_item = ReviewItem(
                        id=str(uuid4()),
                        item_type=_REVIEW_ITEM_TYPE,
                        ref_table=_REVIEW_REF_TABLE,
                        ref_id=transaction.id,
                        reason_code=_REVIEW_REASON_CODE,
                        confidence=None,
                        status=_REVIEW_STATUS_OPEN,
                        assigned_to=None,
                        payload_json={"rule_id": spec.rule.id, "actor": actor, "reason": reason},
                        created_at=utcnow(),
                        resolved_at=None,
                    )
                    session.add(review_item)
                    session.flush()
                    open_reviews.setdefault(transaction.id, []).append(review_item)
                elif evaluation.review_action == "resolve":
                    for review_item in open_reviews.get(transaction.id, []):
                        review_item.status = _REVIEW_STATUS_RESOLVED
                        review_item.resolved_at = utcnow()
                    open_reviews[transaction.id] = []

                if evaluation.goal_id is not None:
                    session.add(
                        GoalEvent(
                            id=str(uuid4()),
                            goal_id=evaluation.goal_id,
                            event_date=transaction.posted_date,
                            event_type=_GOAL_EVENT_TYPE,
                            amount=transaction.amount,
                            related_transaction_id=transaction.id,
                            metadata_json={
                                "rule_id": spec.rule.id,
                                "actor": actor,
                                "reason": reason,
                            },
                        )
                    )

            record_rule_audit(
                RuleAuditWriteRequest(
                    rule_run_id=rule_run.id,
                    transaction_id=transaction.id,
                    matched=evaluation.matched,
                    changes_json=evaluation.changes_json,
                    confidence=None,
                ),
                session,
            )

        rule_run.completed_at = utcnow()
        rule_run.summary_json = {
            "scope": _scope_payload(request.scope),
            "evaluated_transactions": rule_summary[spec.rule.id]["evaluated"],
            "matched_transactions": rule_summary[spec.rule.id]["matched"],
            "changed_transactions": rule_summary[spec.rule.id]["changed"],
        }

    return created_run_ids


def apply_rules(request: RulesApplyRequest, session: Session) -> RuleApplyResult:
    """Run enabled rules in deterministic order and return applied or dry-run diffs."""

    if not request.actor.strip():
        raise ValueError("actor is required")
    if not request.reason.strip():
        raise ValueError("reason is required")
    if request.run_mode == RuleRunMode.RETROACTIVE:
        if request.scope.date_from is None or request.scope.date_to is None:
            raise ValueError("retroactive mode requires date_from and date_to")
        if request.scope.date_from > request.scope.date_to:
            raise ValueError("date_from must be <= date_to")

    rule_specs = _build_rule_specs(session)
    _validate_references(rule_specs, session)
    transactions = session.scalars(_build_scope_statement(request.scope)).all()

    transaction_ids = [transaction.id for transaction in transactions]
    merchant_names_by_id: dict[str, str] = {}
    if transaction_ids:
        merchant_ids = sorted(
            {
                transaction.merchant_id
                for transaction in transactions
                if transaction.merchant_id is not None
            }
        )
        if merchant_ids:
            merchant_names_by_id = {
                merchant.id: merchant.canonical_name
                for merchant in session.scalars(select(Merchant).where(Merchant.id.in_(merchant_ids))).all()
            }

    tag_names_by_transaction: dict[str, set[str]] = {}
    if transaction_ids:
        tag_rows = session.execute(
            select(TransactionTag.transaction_id, Tag.name)
            .join(Tag, Tag.id == TransactionTag.tag_id)
            .where(TransactionTag.transaction_id.in_(transaction_ids))
        ).all()
        for transaction_id, tag_name in tag_rows:
            tag_names_by_transaction.setdefault(transaction_id, set()).add(tag_name)

    open_reviews_by_transaction: dict[str, list[ReviewItem]] = {}
    if transaction_ids:
        review_rows = session.scalars(
            select(ReviewItem).where(
                ReviewItem.ref_table == _REVIEW_REF_TABLE,
                ReviewItem.ref_id.in_(transaction_ids),
                ReviewItem.reason_code == _REVIEW_REASON_CODE,
                ReviewItem.status == _REVIEW_STATUS_OPEN,
            )
        ).all()
        for review_item in review_rows:
            open_reviews_by_transaction.setdefault(review_item.ref_id, []).append(review_item)

    linked_goals_by_transaction: dict[str, set[str]] = {}
    if transaction_ids:
        goal_rows = session.execute(
            select(GoalEvent.related_transaction_id, GoalEvent.goal_id).where(
                GoalEvent.event_type == _GOAL_EVENT_TYPE,
                GoalEvent.related_transaction_id.in_(transaction_ids),
                GoalEvent.related_transaction_id.is_not(None),
            )
        ).all()
        for transaction_id, goal_id in goal_rows:
            if transaction_id is None:
                continue
            linked_goals_by_transaction.setdefault(transaction_id, set()).add(goal_id)

    states, traces, evaluations_by_rule, matched_rules, changed_transactions, rule_summary = _run_simulation(
        transactions=transactions,
        rule_specs=rule_specs,
        merchant_names_by_id=merchant_names_by_id,
        tag_names_by_transaction=tag_names_by_transaction,
        open_reviews_by_transaction=open_reviews_by_transaction,
        linked_goals_by_transaction=linked_goals_by_transaction,
    )
    diffs = _build_diffs(states, traces, changed_transactions)

    if request.dry_run:
        return RuleApplyResult(
            dry_run=True,
            evaluated_rules=len(rule_specs),
            matched_rules=matched_rules,
            changed_transactions=len(changed_transactions),
            rule_run_ids=[],
            diffs=diffs,
        )

    created_run_ids = _apply_simulation_results(
        request=request,
        rule_specs=rule_specs,
        transactions=transactions,
        evaluations_by_rule=evaluations_by_rule,
        open_reviews_by_transaction=open_reviews_by_transaction,
        rule_summary=rule_summary,
        session=session,
    )

    return RuleApplyResult(
        dry_run=False,
        evaluated_rules=len(rule_specs),
        matched_rules=matched_rules,
        changed_transactions=len(changed_transactions),
        rule_run_ids=created_run_ids,
        diffs=diffs,
    )
