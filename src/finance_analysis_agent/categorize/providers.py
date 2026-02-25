"""Suggestion provider contracts and deterministic heuristic implementation."""

from __future__ import annotations

import re
from collections import defaultdict
from collections import Counter
from dataclasses import dataclass, field
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.categorize.types import CategorizeSuggestRequest
from finance_analysis_agent.categorize.utils import normalize_scope_ids
from finance_analysis_agent.db.models import Transaction

DEFAULT_PROVIDER_NAME = "heuristic_v1"

_MERCHANT_WEIGHT = 0.65
_STATEMENT_WEIGHT = 0.25
_RECENCY_WEIGHT = 0.10
_STATEMENT_SIMILARITY_MIN = 0.2
_MIN_CONFIDENCE = 0.2
_DEFAULT_HISTORY_LIMIT = 5000
_STOPWORDS = {
    "payment",
    "purchase",
    "debit",
    "credit",
    "online",
    "transfer",
    "card",
    "inc",
    "llc",
    "the",
}


@dataclass(slots=True)
class ProviderSuggestion:
    transaction_id: str
    suggested_category_id: str
    confidence: float
    reason_codes: list[str] = field(default_factory=list)
    provenance: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ProviderSuggestResult:
    suggestions: list[ProviderSuggestion] = field(default_factory=list)
    skipped: dict[str, int] = field(default_factory=dict)


class SuggestionProvider(Protocol):
    name: str

    def suggest(self, request: CategorizeSuggestRequest, session: Session) -> ProviderSuggestResult:
        """Return suggestion candidates for uncategorized transactions."""


def _tokenize(value: str | None) -> set[str]:
    if not value:
        return set()
    tokens = set(re.findall(r"[a-z0-9]{2,}", value.lower()))
    return {token for token in tokens if token not in _STOPWORDS}


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    overlap = left & right
    union = left | right
    return len(overlap) / len(union)


def _ordered_unique(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        ordered.append(value)
        seen.add(value)
    return ordered


class HeuristicSuggestionProvider:
    """Deterministic rule-like scorer based on historical categorized transactions."""

    name = DEFAULT_PROVIDER_NAME

    def suggest(self, request: CategorizeSuggestRequest, session: Session) -> ProviderSuggestResult:
        scope_ids = normalize_scope_ids(request.scope_transaction_ids)
        history_limit = request.history_limit if request.history_limit > 0 else _DEFAULT_HISTORY_LIMIT

        target_stmt = select(Transaction).where(Transaction.category_id.is_(None))
        if not request.include_pending:
            target_stmt = target_stmt.where(Transaction.pending_status == "posted")
        if scope_ids:
            target_stmt = target_stmt.where(Transaction.id.in_(scope_ids))
        target_stmt = target_stmt.order_by(Transaction.posted_date.asc(), Transaction.id.asc()).limit(request.limit)

        targets = session.scalars(target_stmt).all()
        if not targets:
            return ProviderSuggestResult()

        account_ids = sorted({target.account_id for target in targets})
        history_stmt = select(Transaction).where(Transaction.category_id.is_not(None))
        history_stmt = history_stmt.where(Transaction.account_id.in_(account_ids))
        if not request.include_pending:
            history_stmt = history_stmt.where(Transaction.pending_status == "posted")
        history_stmt = history_stmt.order_by(Transaction.posted_date.desc(), Transaction.id.desc()).limit(history_limit)
        history = list(reversed(session.scalars(history_stmt).all()))

        skipped = Counter()
        if not history:
            skipped["no_categorized_history"] = len(targets)
            return ProviderSuggestResult(skipped=dict(skipped))

        history_tokens_by_id: dict[str, set[str]] = {}
        history_dates_by_category: dict[str, list] = defaultdict(list)
        for row in history:
            if row.category_id is not None and row.posted_date is not None:
                history_dates_by_category[row.category_id].append(row.posted_date)
            if row.category_id is not None and row.original_statement:
                history_tokens_by_id[row.id] = _tokenize(row.original_statement)

        suggestions: list[ProviderSuggestion] = []

        for target in targets:
            category_scores: dict[str, float] = {}
            reason_codes: list[str] = []
            provenance: dict[str, object] = {}

            if target.merchant_id:
                merchant_history = [
                    row
                    for row in history
                    if row.id != target.id and row.merchant_id == target.merchant_id and row.category_id is not None
                ]
                if merchant_history:
                    counts = Counter(row.category_id for row in merchant_history if row.category_id is not None)
                    top_category, top_count = sorted(
                        counts.items(),
                        key=lambda item: (-item[1], item[0]),
                    )[0]
                    support_ratio = top_count / len(merchant_history)
                    category_scores[top_category] = category_scores.get(top_category, 0.0) + (
                        _MERCHANT_WEIGHT * support_ratio
                    )
                    reason_codes.append("categorize.history.merchant_majority")
                    provenance["merchant_majority"] = {
                        "support_count": top_count,
                        "total_count": len(merchant_history),
                        "support_ratio": round(support_ratio, 4),
                        "category_id": top_category,
                    }

            target_tokens = _tokenize(target.original_statement)
            if target_tokens:
                statement_similarity_by_category: dict[str, float] = {}
                for row in history:
                    if row.id == target.id or row.category_id is None or not row.original_statement:
                        continue
                    row_tokens = history_tokens_by_id.get(row.id)
                    if not row_tokens:
                        continue
                    similarity = _jaccard_similarity(target_tokens, row_tokens)
                    if similarity > statement_similarity_by_category.get(row.category_id, 0.0):
                        statement_similarity_by_category[row.category_id] = similarity

                if statement_similarity_by_category:
                    top_category, top_similarity = sorted(
                        statement_similarity_by_category.items(),
                        key=lambda item: (-item[1], item[0]),
                    )[0]
                    if top_similarity >= _STATEMENT_SIMILARITY_MIN:
                        category_scores[top_category] = category_scores.get(top_category, 0.0) + (
                            _STATEMENT_WEIGHT * top_similarity
                        )
                        reason_codes.append("categorize.history.statement_similarity")
                        provenance["statement_similarity"] = {
                            "category_id": top_category,
                            "similarity": round(top_similarity, 4),
                        }

            if not category_scores:
                skipped["insufficient_signal"] += 1
                continue

            winning_category = sorted(
                category_scores.items(),
                key=lambda item: (-item[1], item[0]),
            )[0][0]
            winning_history_dates = history_dates_by_category.get(winning_category, [])
            if winning_history_dates:
                recent_date = max(winning_history_dates)
                day_delta = abs((target.posted_date - recent_date).days)
                recency_score = 1.0 / (1.0 + (day_delta / 30.0))
                contribution = _RECENCY_WEIGHT * recency_score
                category_scores[winning_category] = category_scores.get(winning_category, 0.0) + contribution
                reason_codes.append("categorize.history.recency_support")
                provenance["recency_support"] = {
                    "recent_transaction_date": recent_date.isoformat(),
                    "days_since_recent": day_delta,
                    "contribution": round(contribution, 4),
                }

            winning_category, winning_score = sorted(
                category_scores.items(),
                key=lambda item: (-item[1], item[0]),
            )[0]
            confidence = round(max(0.0, min(0.99, winning_score)), 4)
            if confidence < _MIN_CONFIDENCE:
                skipped["insufficient_signal"] += 1
                continue

            suggestions.append(
                ProviderSuggestion(
                    transaction_id=target.id,
                    suggested_category_id=winning_category,
                    confidence=confidence,
                    reason_codes=_ordered_unique(reason_codes),
                    provenance=provenance,
                )
            )

        return ProviderSuggestResult(
            suggestions=suggestions,
            skipped=dict(sorted(skipped.items())),
        )


_PROVIDERS: dict[str, SuggestionProvider] = {
    DEFAULT_PROVIDER_NAME: HeuristicSuggestionProvider(),
}


def resolve_suggestion_provider(provider_name: str) -> SuggestionProvider:
    normalized_name = provider_name.strip()
    provider = _PROVIDERS.get(normalized_name)
    if provider is None:
        allowed = ", ".join(sorted(_PROVIDERS))
        raise ValueError(f"Unknown suggestion provider '{provider_name}'. Expected one of: {allowed}")
    return provider
