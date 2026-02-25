from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from finance_analysis_agent.categorize import (
    SuggestionMetricsRequest,
    get_suggestion_metrics,
)
from finance_analysis_agent.db.models import ReviewItem, ReviewItemEvent
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource


def _seed_review_item(
    session: Session,
    *,
    review_item_id: str,
    reason_code: str,
    source: str,
    suggestion_kind: str = "transaction_category",
) -> None:
    session.add(
        ReviewItem(
            id=review_item_id,
            item_type="transaction_category_suggestion",
            ref_table="transactions",
            ref_id=f"txn-{review_item_id}",
            reason_code=reason_code,
            confidence=0.75,
            status=ReviewItemStatus.RESOLVED.value,
            source=source,
            assigned_to=None,
            payload_json={
                "suggestion": {
                    "kind": suggestion_kind,
                    "transaction_id": f"txn-{review_item_id}",
                    "category_id": "cat-food",
                }
            },
            created_at=datetime(2026, 2, 20, 10, 0, 0),
            resolved_at=datetime(2026, 2, 20, 10, 30, 0),
        )
    )


def _seed_review_event(
    session: Session,
    *,
    event_id: str,
    review_item_id: str,
    action: str,
    actor: str,
    created_at: datetime,
) -> None:
    session.add(
        ReviewItemEvent(
            id=event_id,
            review_item_id=review_item_id,
            event_type="bulk_action_applied",
            action=action,
            from_status=ReviewItemStatus.TO_REVIEW.value,
            to_status=ReviewItemStatus.RESOLVED.value,
            actor=actor,
            reason="seed metrics",
            metadata_json=None,
            created_at=created_at,
        )
    )


def test_get_suggestion_metrics_aggregates_actions_reasons_and_kinds(db_session: Session) -> None:
    _seed_review_item(
        db_session,
        review_item_id="ri-approve",
        reason_code="categorize.suggestion",
        source=ReviewSource.CATEGORIZE.value,
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-reject",
        reason_code="categorize.low_confidence",
        source=ReviewSource.CATEGORIZE.value,
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-rules-noise",
        reason_code="rule.needs_review",
        source=ReviewSource.RULES.value,
    )

    _seed_review_event(
        db_session,
        event_id="ev-approve",
        review_item_id="ri-approve",
        action="approve_suggestion",
        actor="reviewer-a",
        created_at=datetime(2026, 2, 21, 9, 0, 0),
    )
    _seed_review_event(
        db_session,
        event_id="ev-reject",
        review_item_id="ri-reject",
        action="reject_suggestion",
        actor="reviewer-b",
        created_at=datetime(2026, 2, 22, 9, 0, 0),
    )
    _seed_review_event(
        db_session,
        event_id="ev-noise",
        review_item_id="ri-rules-noise",
        action="approve_suggestion",
        actor="reviewer-a",
        created_at=datetime(2026, 2, 22, 12, 0, 0),
    )
    db_session.flush()

    result = get_suggestion_metrics(SuggestionMetricsRequest(), db_session)

    assert result.approved_count == 1
    assert result.rejected_count == 1
    assert result.approval_rate == 0.5
    assert result.by_reason_code == {
        "categorize.low_confidence": 1,
        "categorize.suggestion": 1,
    }
    assert result.by_suggestion_kind == {"transaction_category": 2}


def test_get_suggestion_metrics_supports_actor_and_time_filters(db_session: Session) -> None:
    _seed_review_item(
        db_session,
        review_item_id="ri-1",
        reason_code="categorize.suggestion",
        source=ReviewSource.CATEGORIZE.value,
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-2",
        reason_code="categorize.low_confidence",
        source=ReviewSource.CATEGORIZE.value,
    )
    _seed_review_event(
        db_session,
        event_id="ev-1",
        review_item_id="ri-1",
        action="approve_suggestion",
        actor="reviewer-a",
        created_at=datetime(2026, 2, 21, 9, 0, 0),
    )
    _seed_review_event(
        db_session,
        event_id="ev-2",
        review_item_id="ri-2",
        action="reject_suggestion",
        actor="reviewer-b",
        created_at=datetime(2026, 2, 23, 9, 0, 0),
    )
    db_session.flush()

    actor_filtered = get_suggestion_metrics(
        SuggestionMetricsRequest(actor="reviewer-a"),
        db_session,
    )
    assert actor_filtered.approved_count == 1
    assert actor_filtered.rejected_count == 0
    assert actor_filtered.approval_rate == 1.0

    time_filtered = get_suggestion_metrics(
        SuggestionMetricsRequest(since=datetime(2026, 2, 22, 0, 0, 0)),
        db_session,
    )
    assert time_filtered.approved_count == 0
    assert time_filtered.rejected_count == 1
    assert time_filtered.approval_rate == 0.0
