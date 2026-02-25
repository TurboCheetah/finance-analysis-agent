"""Service-layer categorize suggestion generation and reporting metrics."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.categorize.providers import (
    DEFAULT_PROVIDER_NAME,
    ProviderSuggestion,
    resolve_suggestion_provider,
)
from finance_analysis_agent.categorize.types import (
    CategorizeSuggestRequest,
    CategorizeSuggestResult,
    SuggestionCandidate,
    SuggestionMetricsRequest,
    SuggestionMetricsResult,
)
from finance_analysis_agent.db.models import ReviewItem, ReviewItemEvent
from finance_analysis_agent.provenance.audit_writers import finish_run_metadata, start_run_metadata
from finance_analysis_agent.provenance.types import RunMetadataFinishRequest, RunMetadataStartRequest
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow

PIPELINE_NAME = "categorize_suggest"
SERVICE_VERSION = "categorize-suggest-v1"
SCHEMA_VERSION = "1.0.0"
DEFAULT_CONFIDENCE_THRESHOLD = 0.8
_REASON_LOW_CONFIDENCE = "categorize.low_confidence"
_REASON_SUGGESTION = "categorize.suggestion"
_ACTIVE_REVIEW_STATUSES = {
    ReviewItemStatus.TO_REVIEW.value,
    ReviewItemStatus.IN_PROGRESS.value,
}
_SUGGESTION_ITEM_TYPE = "transaction_category_suggestion"
_SUGGESTION_REF_TABLE = "transactions"
_SUGGESTION_KIND_TRANSACTION_CATEGORY = "transaction_category"


def _normalize_for_hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_scope_ids(scope_transaction_ids: list[str]) -> list[str]:
    normalized = {transaction_id.strip() for transaction_id in scope_transaction_ids if transaction_id.strip()}
    return sorted(normalized)


def _validate_request(request: CategorizeSuggestRequest) -> tuple[str, str, str, float, int, list[str]]:
    actor = request.actor.strip()
    if not actor:
        raise ValueError("actor is required")
    reason = request.reason.strip()
    if not reason:
        raise ValueError("reason is required")

    provider = request.provider.strip() or DEFAULT_PROVIDER_NAME

    threshold = DEFAULT_CONFIDENCE_THRESHOLD if request.confidence_threshold is None else request.confidence_threshold
    if threshold < 0 or threshold > 1:
        raise ValueError("confidence_threshold must be between 0 and 1")

    if request.limit <= 0:
        raise ValueError("limit must be > 0")

    return actor, reason, provider, float(threshold), int(request.limit), _normalize_scope_ids(request.scope_transaction_ids)


def _extract_suggestion_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    nested = payload.get("suggestion")
    if isinstance(nested, dict):
        return nested
    return payload


def _extract_suggested_category_id(payload: dict[str, object] | None) -> str | None:
    suggestion_payload = _extract_suggestion_payload(payload)
    if not isinstance(suggestion_payload, dict):
        return None
    category_id = suggestion_payload.get("category_id")
    if isinstance(category_id, str) and category_id.strip():
        return category_id.strip()
    return None


def _confidence_bucket(confidence: float) -> str:
    if confidence < 0.5:
        return "lt_0_50"
    if confidence < DEFAULT_CONFIDENCE_THRESHOLD:
        return "gte_0_50_lt_0_80"
    return "gte_0_80"


def _start_run(
    *,
    provider: str,
    threshold: float,
    include_pending: bool,
    limit: int,
    scope_size: int,
    session: Session,
) -> str:
    run = start_run_metadata(
        RunMetadataStartRequest(
            pipeline_name=PIPELINE_NAME,
            code_version=SERVICE_VERSION,
            schema_version=SCHEMA_VERSION,
            config_hash=_normalize_for_hash(
                {
                    "provider": provider,
                    "threshold": threshold,
                    "include_pending": include_pending,
                    "limit": limit,
                    "scope_size": scope_size,
                }
            ),
            status="running",
            diagnostics_json={
                "provider": provider,
                "threshold": threshold,
                "phase": "start",
            },
        ),
        session,
    )
    return run.id


def _finish_run(*, run_metadata_id: str, status: str, diagnostics_json: dict[str, object], session: Session) -> None:
    finish_run_metadata(
        RunMetadataFinishRequest(
            run_metadata_id=run_metadata_id,
            status=status,
            diagnostics_json=diagnostics_json,
        ),
        session,
    )


def _open_rule_review_transaction_ids(transaction_ids: list[str], session: Session) -> set[str]:
    if not transaction_ids:
        return set()
    rows = session.scalars(
        select(ReviewItem)
        .where(
            ReviewItem.ref_table == _SUGGESTION_REF_TABLE,
            ReviewItem.ref_id.in_(transaction_ids),
            ReviewItem.reason_code == "rule.needs_review",
            ReviewItem.source == ReviewSource.RULES.value,
            ReviewItem.status.in_(sorted(_ACTIVE_REVIEW_STATUSES)),
        )
        .order_by(ReviewItem.created_at.asc(), ReviewItem.id.asc())
    ).all()
    return {row.ref_id for row in rows}


def _active_categorize_review_map(transaction_ids: list[str], session: Session) -> dict[tuple[str, str], ReviewItem]:
    if not transaction_ids:
        return {}
    rows = session.scalars(
        select(ReviewItem)
        .where(
            ReviewItem.ref_table == _SUGGESTION_REF_TABLE,
            ReviewItem.ref_id.in_(transaction_ids),
            ReviewItem.source == ReviewSource.CATEGORIZE.value,
            ReviewItem.status.in_(sorted(_ACTIVE_REVIEW_STATUSES)),
        )
        .order_by(ReviewItem.created_at.asc(), ReviewItem.id.asc())
    ).all()

    result: dict[tuple[str, str], ReviewItem] = {}
    for row in rows:
        category_id = _extract_suggested_category_id(row.payload_json)
        if category_id is None:
            continue
        result.setdefault((row.ref_id, category_id), row)
    return result


def _build_review_payload(
    *,
    suggestion: ProviderSuggestion,
    provider: str,
    generated_at: datetime,
) -> dict[str, object]:
    return {
        "suggestion": {
            "kind": _SUGGESTION_KIND_TRANSACTION_CATEGORY,
            "transaction_id": suggestion.transaction_id,
            "category_id": suggestion.suggested_category_id,
            "reason_codes": suggestion.reason_codes,
            "provider": provider,
            "confidence": suggestion.confidence,
            "generated_at": generated_at.isoformat(),
            "provenance": suggestion.provenance,
        }
    }


def categorize_suggest(request: CategorizeSuggestRequest, session: Session) -> CategorizeSuggestResult:
    """Generate explainable category suggestions and queue them for review."""

    actor, reason, provider_name, threshold, limit, scope_ids = _validate_request(request)
    run_metadata_id = _start_run(
        provider=provider_name,
        threshold=threshold,
        include_pending=request.include_pending,
        limit=limit,
        scope_size=len(scope_ids),
        session=session,
    )

    try:
        provider = resolve_suggestion_provider(provider_name)
        provider_result = provider.suggest(
            CategorizeSuggestRequest(
                actor=actor,
                reason=reason,
                scope_transaction_ids=scope_ids,
                include_pending=request.include_pending,
                confidence_threshold=threshold,
                provider=provider_name,
                limit=limit,
            ),
            session,
        )

        skip_counts = Counter(provider_result.skipped)
        generated = len(provider_result.suggestions)

        transaction_ids = sorted({suggestion.transaction_id for suggestion in provider_result.suggestions})
        blocked_transaction_ids = _open_rule_review_transaction_ids(transaction_ids, session)
        active_reviews = _active_categorize_review_map(transaction_ids, session)

        low_confidence = 0
        high_confidence = 0
        confidence_histogram = Counter()
        queued_suggestions: list[SuggestionCandidate] = []
        generation_timestamp = utcnow()

        for suggestion in provider_result.suggestions:
            if suggestion.transaction_id in blocked_transaction_ids:
                skip_counts["rule_review_open"] += 1
                continue

            key = (suggestion.transaction_id, suggestion.suggested_category_id)
            existing_review = active_reviews.get(key)
            if existing_review is None:
                review_reason_code = (
                    _REASON_LOW_CONFIDENCE if suggestion.confidence < threshold else _REASON_SUGGESTION
                )
                existing_review = ReviewItem(
                    id=str(uuid4()),
                    item_type=_SUGGESTION_ITEM_TYPE,
                    ref_table=_SUGGESTION_REF_TABLE,
                    ref_id=suggestion.transaction_id,
                    reason_code=review_reason_code,
                    confidence=suggestion.confidence,
                    status=ReviewItemStatus.TO_REVIEW.value,
                    source=ReviewSource.CATEGORIZE.value,
                    assigned_to=None,
                    payload_json=_build_review_payload(
                        suggestion=suggestion,
                        provider=provider_name,
                        generated_at=generation_timestamp,
                    ),
                    created_at=utcnow(),
                    resolved_at=None,
                )
                session.add(existing_review)
                active_reviews[key] = existing_review

            if suggestion.confidence < threshold:
                low_confidence += 1
            else:
                high_confidence += 1
            confidence_histogram[_confidence_bucket(suggestion.confidence)] += 1

            queued_suggestions.append(
                SuggestionCandidate(
                    transaction_id=suggestion.transaction_id,
                    suggested_category_id=suggestion.suggested_category_id,
                    confidence=suggestion.confidence,
                    reason_codes=suggestion.reason_codes,
                    provenance=suggestion.provenance,
                    queued_review_item_id=existing_review.id,
                )
            )

        queued = len(queued_suggestions)
        diagnostics_json: dict[str, object] = {
            "provider": provider_name,
            "threshold": threshold,
            "generated": generated,
            "queued": queued,
            "low_confidence": low_confidence,
            "high_confidence": high_confidence,
            "skipped": dict(sorted(skip_counts.items())),
            "confidence_histogram": dict(sorted(confidence_histogram.items())),
        }
        _finish_run(
            run_metadata_id=run_metadata_id,
            status="success",
            diagnostics_json=diagnostics_json,
            session=session,
        )

        return CategorizeSuggestResult(
            run_metadata_id=run_metadata_id,
            provider=provider_name,
            threshold_used=threshold,
            generated=generated,
            queued=queued,
            low_confidence=low_confidence,
            high_confidence=high_confidence,
            skipped=dict(sorted(skip_counts.items())),
            suggestions=queued_suggestions,
        )
    except Exception as exc:
        _finish_run(
            run_metadata_id=run_metadata_id,
            status="failed",
            diagnostics_json={
                "provider": provider_name,
                "threshold": threshold,
                "error": str(exc),
            },
            session=session,
        )
        raise


def get_suggestion_metrics(request: SuggestionMetricsRequest, session: Session) -> SuggestionMetricsResult:
    """Aggregate approval/rejection outcomes for categorize suggestions."""

    if request.since is not None and request.until is not None and request.since > request.until:
        raise ValueError("since must be <= until")

    actor = request.actor.strip() if request.actor is not None else None
    if actor == "":
        actor = None

    stmt = (
        select(ReviewItemEvent, ReviewItem)
        .join(ReviewItem, ReviewItem.id == ReviewItemEvent.review_item_id)
        .where(
            ReviewItem.source == ReviewSource.CATEGORIZE.value,
            ReviewItemEvent.event_type == "bulk_action_applied",
            ReviewItemEvent.action.in_(["approve_suggestion", "reject_suggestion"]),
        )
        .order_by(ReviewItemEvent.created_at.asc(), ReviewItemEvent.id.asc())
    )
    if request.since is not None:
        stmt = stmt.where(ReviewItemEvent.created_at >= request.since)
    if request.until is not None:
        stmt = stmt.where(ReviewItemEvent.created_at <= request.until)
    if actor is not None:
        stmt = stmt.where(ReviewItemEvent.actor == actor)

    rows = session.execute(stmt).all()

    approved_count = 0
    rejected_count = 0
    by_reason_code = Counter()
    by_suggestion_kind = Counter()

    for event, review_item in rows:
        if event.action == "approve_suggestion":
            approved_count += 1
        elif event.action == "reject_suggestion":
            rejected_count += 1

        by_reason_code[review_item.reason_code] += 1

        suggestion_payload = _extract_suggestion_payload(review_item.payload_json)
        if not isinstance(suggestion_payload, dict):
            continue
        suggestion_kind = suggestion_payload.get("kind") or suggestion_payload.get("type")
        if isinstance(suggestion_kind, str) and suggestion_kind.strip():
            by_suggestion_kind[suggestion_kind.strip()] += 1

    total_decisions = approved_count + rejected_count
    approval_rate = None if total_decisions == 0 else approved_count / total_decisions
    return SuggestionMetricsResult(
        approved_count=approved_count,
        rejected_count=rejected_count,
        approval_rate=approval_rate,
        by_reason_code=dict(sorted(by_reason_code.items())),
        by_suggestion_kind=dict(sorted(by_suggestion_kind.items())),
    )
