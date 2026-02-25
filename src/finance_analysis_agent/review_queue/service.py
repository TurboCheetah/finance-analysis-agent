"""Service-layer review queue listing and bulk triage workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Category, DedupeCandidate, ReviewItem, ReviewItemEvent
from finance_analysis_agent.provenance.transaction_events_service import mutate_transaction_fields
from finance_analysis_agent.provenance.types import ProvenanceSource, TransactionMutationRequest
from finance_analysis_agent.review_queue.types import (
    BulkActionType,
    BulkTriageRequest,
    BulkTriageResult,
    ItemTriageOutcome,
    ReviewItemStatus,
    ReviewQueueListRequest,
    ReviewQueueListResult,
    ReviewSource,
)
from finance_analysis_agent.utils.time import utcnow


_TERMINAL_REVIEW_STATUSES = {ReviewItemStatus.RESOLVED.value, ReviewItemStatus.REJECTED.value}


class _SkipItemAction(Exception):
    """Internal signal for per-item no-op/skip semantics."""


@dataclass(slots=True)
class _ActionContext:
    action: BulkActionType
    actor: str
    reason: str
    category_id: str | None
    assignee: str | None


def _parse_review_status(value: ReviewItemStatus | str) -> ReviewItemStatus:
    try:
        return value if isinstance(value, ReviewItemStatus) else ReviewItemStatus(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in ReviewItemStatus)
        raise ValueError(f"Invalid review status '{value}'. Expected one of: {allowed}") from exc


def _parse_review_source(value: ReviewSource | str) -> ReviewSource:
    try:
        return value if isinstance(value, ReviewSource) else ReviewSource(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in ReviewSource)
        raise ValueError(f"Invalid review source '{value}'. Expected one of: {allowed}") from exc


def _parse_bulk_action(value: BulkActionType | str) -> BulkActionType:
    try:
        return value if isinstance(value, BulkActionType) else BulkActionType(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in BulkActionType)
        raise ValueError(f"Invalid bulk action '{value}'. Expected one of: {allowed}") from exc


def _non_empty_string(value: str | None, *, field_name: str) -> str:
    if value is None or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _normalize_review_ids(review_item_ids: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for review_item_id in review_item_ids:
        normalized_id = review_item_id.strip()
        if not normalized_id or normalized_id in seen:
            continue
        deduped.append(normalized_id)
        seen.add(normalized_id)
    if not deduped:
        raise ValueError("review_item_ids must include at least one id")
    return deduped


def list_review_items(request: ReviewQueueListRequest, session: Session) -> ReviewQueueListResult:
    """Return review items with deterministic ordering and queue-oriented filters."""

    if request.limit <= 0:
        raise ValueError("limit must be > 0")
    if request.offset < 0:
        raise ValueError("offset must be >= 0")
    if request.confidence_min is not None and request.confidence_max is not None:
        if request.confidence_min > request.confidence_max:
            raise ValueError("confidence_min must be <= confidence_max")

    statuses = [_parse_review_status(value).value for value in request.statuses]
    sources = [_parse_review_source(value).value for value in request.sources]
    reason_codes = sorted({reason.strip() for reason in request.reason_codes if reason.strip()})

    conditions = []
    if statuses:
        conditions.append(ReviewItem.status.in_(statuses))
    if sources:
        conditions.append(ReviewItem.source.in_(sources))
    if reason_codes:
        conditions.append(ReviewItem.reason_code.in_(reason_codes))
    if request.assigned_to is not None:
        conditions.append(ReviewItem.assigned_to == request.assigned_to)
    if request.confidence_min is not None:
        conditions.append(ReviewItem.confidence >= request.confidence_min)
    if request.confidence_max is not None:
        conditions.append(ReviewItem.confidence <= request.confidence_max)

    total_count = session.scalar(select(func.count()).select_from(ReviewItem).where(*conditions)) or 0
    rows = session.scalars(
        select(ReviewItem)
        .where(*conditions)
        .order_by(ReviewItem.created_at.asc(), ReviewItem.id.asc())
        .limit(request.limit)
        .offset(request.offset)
    ).all()

    return ReviewQueueListResult(total_count=total_count, items=rows)


def _record_review_event(
    *,
    review_item_id: str,
    event_type: str,
    action: BulkActionType,
    actor: str,
    reason: str,
    session: Session,
    from_status: str | None = None,
    to_status: str | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> None:
    session.add(
        ReviewItemEvent(
            id=str(uuid4()),
            review_item_id=review_item_id,
            event_type=event_type,
            action=action.value,
            from_status=from_status,
            to_status=to_status,
            actor=actor,
            reason=reason,
            metadata_json=metadata_json,
            created_at=utcnow(),
        )
    )


def _set_status(review_item: ReviewItem, target_status: ReviewItemStatus) -> None:
    review_item.status = target_status.value
    if target_status in {ReviewItemStatus.RESOLVED, ReviewItemStatus.REJECTED}:
        review_item.resolved_at = utcnow()
    else:
        review_item.resolved_at = None


def _resolve_transaction_id(review_item: ReviewItem) -> str | None:
    if review_item.ref_table == "transactions":
        return review_item.ref_id

    payload = review_item.payload_json or {}
    transaction_id = payload.get("transaction_id")
    if isinstance(transaction_id, str) and transaction_id.strip():
        return transaction_id.strip()
    return None


def _resolve_dedupe_candidate_id(review_item: ReviewItem) -> str | None:
    if review_item.ref_table == "dedupe_candidates":
        return review_item.ref_id

    payload = review_item.payload_json or {}
    candidate_id = payload.get("dedupe_candidate_id")
    if isinstance(candidate_id, str) and candidate_id.strip():
        return candidate_id.strip()
    return None


def _resolve_suggestion_payload(review_item: ReviewItem) -> dict[str, Any]:
    payload = review_item.payload_json
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("Review item payload must be a JSON object")

    if isinstance(payload.get("suggestion"), dict):
        return payload["suggestion"]
    return payload


def _apply_transaction_changes(
    *,
    transaction_id: str,
    changes: dict[str, Any],
    actor: str,
    reason: str,
    review_item_id: str,
    session: Session,
) -> None:
    mutate_transaction_fields(
        TransactionMutationRequest(
            transaction_id=transaction_id,
            actor=actor,
            reason=f"{reason} [review_item:{review_item_id}]",
            provenance=ProvenanceSource.MANUAL,
            changes=changes,
        ),
        session,
    )


def _apply_suggestion(
    *,
    review_item: ReviewItem,
    actor: str,
    reason: str,
    session: Session,
) -> dict[str, Any]:
    suggestion = _resolve_suggestion_payload(review_item)
    kind = suggestion.get("kind") or suggestion.get("type")
    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("Suggestion payload is missing kind/type")

    normalized_kind = kind.strip()
    if normalized_kind in {"transaction_category", "set_category"}:
        transaction_id = suggestion.get("transaction_id")
        category_id = suggestion.get("category_id")
        if not isinstance(transaction_id, str) or not transaction_id.strip():
            raise ValueError("Suggestion payload is missing transaction_id")
        if not isinstance(category_id, str) or not category_id.strip():
            raise ValueError("Suggestion payload is missing category_id")

        _apply_transaction_changes(
            transaction_id=transaction_id.strip(),
            changes={"category_id": category_id.strip()},
            actor=actor,
            reason=reason,
            review_item_id=review_item.id,
            session=session,
        )
        return {
            "suggestion_kind": normalized_kind,
            "transaction_id": transaction_id.strip(),
            "changes": {"category_id": category_id.strip()},
        }

    if normalized_kind == "transaction_field_update":
        transaction_id = suggestion.get("transaction_id")
        changes = suggestion.get("changes")
        if not isinstance(transaction_id, str) or not transaction_id.strip():
            raise ValueError("Suggestion payload is missing transaction_id")
        if not isinstance(changes, dict) or not changes:
            raise ValueError("Suggestion payload is missing non-empty changes map")

        _apply_transaction_changes(
            transaction_id=transaction_id.strip(),
            changes=changes,
            actor=actor,
            reason=reason,
            review_item_id=review_item.id,
            session=session,
        )
        return {
            "suggestion_kind": normalized_kind,
            "transaction_id": transaction_id.strip(),
            "changes": changes,
        }

    if normalized_kind == "dedupe_decision":
        candidate_id = suggestion.get("dedupe_candidate_id") or suggestion.get("candidate_id")
        decision = suggestion.get("decision")
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise ValueError("Suggestion payload is missing dedupe_candidate_id")
        if not isinstance(decision, str) or not decision.strip():
            raise ValueError("Suggestion payload is missing decision")

        candidate = session.get(DedupeCandidate, candidate_id.strip())
        if candidate is None:
            raise ValueError(f"DedupeCandidate not found: {candidate_id.strip()}")

        candidate.decision = decision.strip()
        candidate.decided_at = utcnow()
        reason_json = dict(candidate.reason_json or {})
        reason_json.update(
            {
                "source": "review_queue",
                "review_item_id": review_item.id,
                "actor": actor,
                "reason": reason,
                "action": BulkActionType.APPROVE_SUGGESTION.value,
            }
        )
        candidate.reason_json = reason_json
        return {
            "suggestion_kind": normalized_kind,
            "dedupe_candidate_id": candidate_id.strip(),
            "decision": decision.strip(),
        }

    raise ValueError(f"Unsupported suggestion kind: {normalized_kind}")


def _apply_action_to_item(
    *,
    review_item: ReviewItem,
    context: _ActionContext,
    session: Session,
) -> dict[str, Any]:
    action = context.action
    if action in {
        BulkActionType.RECATEGORIZE,
        BulkActionType.MARK_DUPLICATE,
        BulkActionType.APPROVE_SUGGESTION,
        BulkActionType.REJECT_SUGGESTION,
        BulkActionType.MARK_IN_PROGRESS,
    } and review_item.status in _TERMINAL_REVIEW_STATUSES:
        raise _SkipItemAction("review item is already terminal")

    if action == BulkActionType.RECATEGORIZE:
        if context.category_id is None:
            raise ValueError("category_id is required for recategorize")

        transaction_id = _resolve_transaction_id(review_item)
        if transaction_id is None:
            raise ValueError("Could not resolve transaction target for recategorize")

        _apply_transaction_changes(
            transaction_id=transaction_id,
            changes={"category_id": context.category_id},
            actor=context.actor,
            reason=context.reason,
            review_item_id=review_item.id,
            session=session,
        )
        _set_status(review_item, ReviewItemStatus.RESOLVED)
        return {
            "transaction_id": transaction_id,
            "changes": {"category_id": context.category_id},
        }

    if action == BulkActionType.MARK_DUPLICATE:
        candidate_id = _resolve_dedupe_candidate_id(review_item)
        if candidate_id is None:
            raise ValueError("Could not resolve dedupe candidate target for mark_duplicate")

        candidate = session.get(DedupeCandidate, candidate_id)
        if candidate is None:
            raise ValueError(f"DedupeCandidate not found: {candidate_id}")

        candidate.decision = "duplicate"
        candidate.decided_at = utcnow()
        reason_json = dict(candidate.reason_json or {})
        reason_json.update(
            {
                "source": "review_queue",
                "review_item_id": review_item.id,
                "actor": context.actor,
                "reason": context.reason,
                "action": action.value,
            }
        )
        candidate.reason_json = reason_json
        _set_status(review_item, ReviewItemStatus.RESOLVED)
        return {"dedupe_candidate_id": candidate_id, "decision": "duplicate"}

    if action == BulkActionType.APPROVE_SUGGESTION:
        details = _apply_suggestion(
            review_item=review_item,
            actor=context.actor,
            reason=context.reason,
            session=session,
        )
        _set_status(review_item, ReviewItemStatus.RESOLVED)
        return details

    if action == BulkActionType.REJECT_SUGGESTION:
        _set_status(review_item, ReviewItemStatus.REJECTED)
        return {"rejected": True}

    if action == BulkActionType.ASSIGN:
        if context.assignee is None:
            raise ValueError("assignee is required for assign")
        review_item.assigned_to = context.assignee
        return {"assignee": review_item.assigned_to}

    if action == BulkActionType.UNASSIGN:
        review_item.assigned_to = None
        return {"assignee": None}

    if action == BulkActionType.MARK_IN_PROGRESS:
        if review_item.status == ReviewItemStatus.IN_PROGRESS.value:
            raise _SkipItemAction("review item is already in_progress")
        _set_status(review_item, ReviewItemStatus.IN_PROGRESS)
        return {"status": ReviewItemStatus.IN_PROGRESS.value}

    raise ValueError(f"Unsupported action: {action.value}")


def bulk_triage(request: BulkTriageRequest, session: Session) -> BulkTriageResult:
    """Apply bulk queue actions with per-item outcomes and append-only event logs."""

    action = _parse_bulk_action(request.action)
    actor = _non_empty_string(request.actor, field_name="actor")
    reason = _non_empty_string(request.reason, field_name="reason")
    review_item_ids = _normalize_review_ids(request.review_item_ids)

    category_id = request.category_id.strip() if request.category_id and request.category_id.strip() else None
    assignee = request.assignee.strip() if request.assignee and request.assignee.strip() else None

    if action == BulkActionType.RECATEGORIZE:
        if category_id is None:
            raise ValueError("category_id is required for recategorize")
        if session.get(Category, category_id) is None:
            raise ValueError(f"Category not found: {category_id}")

    context = _ActionContext(
        action=action,
        actor=actor,
        reason=reason,
        category_id=category_id,
        assignee=assignee,
    )

    rows = session.scalars(select(ReviewItem).where(ReviewItem.id.in_(review_item_ids))).all()
    items_by_id = {item.id: item for item in rows}

    updated = 0
    failed = 0
    skipped = 0
    outcomes: list[ItemTriageOutcome] = []

    for review_item_id in review_item_ids:
        review_item = items_by_id.get(review_item_id)
        if review_item is None:
            failed += 1
            outcomes.append(
                ItemTriageOutcome(
                    review_item_id=review_item_id,
                    outcome="failed",
                    status="missing",
                    message="review item not found",
                )
            )
            continue

        from_status = review_item.status
        from_assignee = review_item.assigned_to
        try:
            with session.begin_nested():
                metadata = _apply_action_to_item(review_item=review_item, context=context, session=session)
                to_status = review_item.status
                to_assignee = review_item.assigned_to

                if from_status != to_status:
                    _record_review_event(
                        review_item_id=review_item.id,
                        event_type="status_transition",
                        action=action,
                        actor=actor,
                        reason=reason,
                        from_status=from_status,
                        to_status=to_status,
                        metadata_json={"via": "bulk_triage"},
                        session=session,
                    )
                if from_assignee != to_assignee:
                    _record_review_event(
                        review_item_id=review_item.id,
                        event_type="assignment_changed",
                        action=action,
                        actor=actor,
                        reason=reason,
                        metadata_json={
                            "from_assignee": from_assignee,
                            "to_assignee": to_assignee,
                        },
                        session=session,
                    )

                _record_review_event(
                    review_item_id=review_item.id,
                    event_type="bulk_action_applied",
                    action=action,
                    actor=actor,
                    reason=reason,
                    from_status=from_status,
                    to_status=to_status,
                    metadata_json=metadata,
                    session=session,
                )
                session.flush()

            updated += 1
            outcomes.append(
                ItemTriageOutcome(
                    review_item_id=review_item.id,
                    outcome="updated",
                    status=review_item.status,
                    message=None,
                )
            )
        except _SkipItemAction as exc:
            session.refresh(review_item)
            with session.begin_nested():
                _record_review_event(
                    review_item_id=review_item.id,
                    event_type="bulk_action_skipped",
                    action=action,
                    actor=actor,
                    reason=reason,
                    from_status=from_status,
                    to_status=review_item.status,
                    metadata_json={"message": str(exc)},
                    session=session,
                )
                session.flush()
            skipped += 1
            outcomes.append(
                ItemTriageOutcome(
                    review_item_id=review_item.id,
                    outcome="skipped",
                    status=review_item.status,
                    message=str(exc),
                )
            )
        except Exception as exc:
            session.refresh(review_item)
            with session.begin_nested():
                _record_review_event(
                    review_item_id=review_item.id,
                    event_type="bulk_action_failed",
                    action=action,
                    actor=actor,
                    reason=reason,
                    from_status=from_status,
                    to_status=review_item.status,
                    metadata_json={"error": str(exc)},
                    session=session,
                )
                session.flush()
            failed += 1
            outcomes.append(
                ItemTriageOutcome(
                    review_item_id=review_item.id,
                    outcome="failed",
                    status=review_item.status,
                    message=str(exc),
                )
            )

    session.flush()
    return BulkTriageResult(
        action=action,
        total_targeted=len(review_item_ids),
        updated=updated,
        failed=failed,
        skipped=skipped,
        item_outcomes=outcomes,
    )
