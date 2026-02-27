from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Merchant, Recurring, RecurringEvent, ReviewItem, Transaction
from finance_analysis_agent.recurring import RecurringDetectRequest, recurring_detect_and_schedule
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session, *, account_id: str = "acct-1") -> None:
    session.add(Account(id=account_id, name="Checking", type="checking", currency="USD"))


def _seed_merchant(session: Session, *, merchant_id: str = "mer-1", name: str = "Gym") -> None:
    session.add(Merchant(id=merchant_id, canonical_name=name, confidence=1.0, created_at=utcnow()))


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    posted_date: date,
    merchant_id: str | None,
    category_id: str | None = None,
    amount: str = "-25.00",
) -> None:
    now = utcnow()
    decimal_amount = Decimal(amount)
    session.add(
        Transaction(
            id=transaction_id,
            account_id="acct-1",
            posted_date=posted_date,
            effective_date=posted_date,
            amount=decimal_amount,
            currency="USD",
            original_amount=decimal_amount,
            original_currency="USD",
            pending_status="posted",
            original_statement="seed recurring",
            merchant_id=merchant_id,
            category_id=category_id,
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id=f"src-{transaction_id}",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=now,
            updated_at=now,
        )
    )


def test_recurring_detect_weekly_generates_missed_events_and_review_items(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, merchant_id="mer-gym", name="Gym")
    _seed_transaction(db_session, transaction_id="txn-1", posted_date=date(2026, 1, 1), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-2", posted_date=date(2026, 1, 8), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-3", posted_date=date(2026, 1, 15), merchant_id="mer-gym")
    db_session.flush()

    request = RecurringDetectRequest(
        as_of_date=date(2026, 1, 29),
        actor="scheduler",
        reason="monthly recurring refresh",
        lookback_days=90,
        minimum_occurrences=3,
        tolerance_days_default=1,
        create_review_items=True,
    )

    first = recurring_detect_and_schedule(request, db_session)
    second = recurring_detect_and_schedule(request, db_session)

    recurring = db_session.scalar(select(Recurring).where(Recurring.merchant_id == "mer-gym"))
    assert recurring is not None
    recurring_events = db_session.scalars(
        select(RecurringEvent)
        .where(RecurringEvent.recurring_id == recurring.id)
        .order_by(RecurringEvent.expected_date.asc())
    ).all()
    review_items = db_session.scalars(
        select(ReviewItem)
        .where(
            ReviewItem.ref_table == "recurring_events",
            ReviewItem.reason_code == "recurring.missed_event",
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_(
                [
                    ReviewItemStatus.TO_REVIEW.value,
                    ReviewItemStatus.IN_PROGRESS.value,
                ]
            ),
        )
        .order_by(ReviewItem.created_at.asc(), ReviewItem.id.asc())
    ).all()

    assert first.schedules[0].schedule_type == "weekly"
    assert first.schedules[0].expected_count == 5
    assert first.schedules[0].observed_count == 3
    assert first.schedules[0].missed_count == 2
    assert len(first.warnings) == 2

    assert len(recurring_events) == 5
    assert sum(1 for row in recurring_events if row.status == "missed") == 2
    assert sum(1 for row in recurring_events if row.status == "observed") == 3
    assert len(review_items) == 2

    recurring_event_count_after_second = db_session.scalar(
        select(func.count()).select_from(RecurringEvent).where(RecurringEvent.recurring_id == recurring.id)
    )
    review_item_count_after_second = db_session.scalar(
        select(func.count())
        .select_from(ReviewItem)
        .where(
            ReviewItem.ref_table == "recurring_events",
            ReviewItem.reason_code == "recurring.missed_event",
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_(
                [
                    ReviewItemStatus.TO_REVIEW.value,
                    ReviewItemStatus.IN_PROGRESS.value,
                ]
            ),
        )
    )

    assert second.schedules[0].recurring_id == first.schedules[0].recurring_id
    assert recurring_event_count_after_second == 5
    assert review_item_count_after_second == 2


def test_recurring_detect_skips_low_confidence_pattern(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, merchant_id="mer-noisy", name="Noisy")
    _seed_transaction(db_session, transaction_id="txn-1", posted_date=date(2026, 1, 1), merchant_id="mer-noisy")
    _seed_transaction(db_session, transaction_id="txn-2", posted_date=date(2026, 1, 5), merchant_id="mer-noisy")
    _seed_transaction(db_session, transaction_id="txn-3", posted_date=date(2026, 1, 25), merchant_id="mer-noisy")
    db_session.flush()

    result = recurring_detect_and_schedule(
        RecurringDetectRequest(
            as_of_date=date(2026, 1, 31),
            actor="scheduler",
            reason="quality check",
            lookback_days=90,
            minimum_occurrences=3,
            tolerance_days_default=1,
        ),
        db_session,
    )

    cause_codes = {cause.code for cause in result.causes}
    assert result.schedules == []
    assert result.warnings == []
    assert "recurring_pattern_low_confidence" in cause_codes
    assert "no_recurring_schedules_detected" in cause_codes


def test_recurring_detect_reports_skipped_transactions_without_keys(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_transaction(db_session, transaction_id="txn-a", posted_date=date(2026, 1, 3), merchant_id=None)
    _seed_transaction(db_session, transaction_id="txn-b", posted_date=date(2026, 1, 11), merchant_id=None)
    _seed_transaction(db_session, transaction_id="txn-c", posted_date=date(2026, 1, 20), merchant_id=None)
    db_session.flush()

    result = recurring_detect_and_schedule(
        RecurringDetectRequest(
            as_of_date=date(2026, 1, 31),
            actor="scheduler",
            reason="skip test",
            lookback_days=60,
        ),
        db_session,
    )

    assert any(cause.code == "transactions_skipped_without_group_key" for cause in result.causes)


def test_recurring_detect_resolves_missed_review_items_when_event_becomes_observed(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, merchant_id="mer-gym", name="Gym")
    _seed_transaction(db_session, transaction_id="txn-1", posted_date=date(2026, 1, 1), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-2", posted_date=date(2026, 1, 8), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-3", posted_date=date(2026, 1, 15), merchant_id="mer-gym")
    db_session.flush()

    recurring_detect_and_schedule(
        RecurringDetectRequest(
            as_of_date=date(2026, 1, 29),
            actor="scheduler",
            reason="initial run",
            lookback_days=90,
            minimum_occurrences=3,
            tolerance_days_default=1,
            create_review_items=True,
        ),
        db_session,
    )

    _seed_transaction(db_session, transaction_id="txn-4", posted_date=date(2026, 1, 22), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-5", posted_date=date(2026, 1, 29), merchant_id="mer-gym")
    db_session.flush()

    recurring_detect_and_schedule(
        RecurringDetectRequest(
            as_of_date=date(2026, 1, 29),
            actor="scheduler",
            reason="refresh with posted matches",
            lookback_days=90,
            minimum_occurrences=3,
            tolerance_days_default=1,
            create_review_items=False,
        ),
        db_session,
    )

    active_review_count = db_session.scalar(
        select(func.count())
        .select_from(ReviewItem)
        .where(
            ReviewItem.ref_table == "recurring_events",
            ReviewItem.reason_code == "recurring.missed_event",
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_(
                [
                    ReviewItemStatus.TO_REVIEW.value,
                    ReviewItemStatus.IN_PROGRESS.value,
                ]
            ),
        )
    )
    resolved_review_count = db_session.scalar(
        select(func.count())
        .select_from(ReviewItem)
        .where(
            ReviewItem.ref_table == "recurring_events",
            ReviewItem.reason_code == "recurring.missed_event",
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status == ReviewItemStatus.RESOLVED.value,
        )
    )

    assert active_review_count == 0
    assert resolved_review_count == 2


def test_recurring_detect_raises_when_expected_date_generation_exceeds_limit(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, merchant_id="mer-gym", name="Gym")
    _seed_transaction(db_session, transaction_id="txn-1", posted_date=date(2026, 1, 1), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-2", posted_date=date(2026, 1, 8), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-3", posted_date=date(2026, 1, 15), merchant_id="mer-gym")
    db_session.flush()

    with pytest.raises(ValueError, match="max_expected_iterations exceeded"):
        recurring_detect_and_schedule(
            RecurringDetectRequest(
                as_of_date=date(2026, 1, 29),
                actor="scheduler",
                reason="guard check",
                lookback_days=90,
                minimum_occurrences=3,
                tolerance_days_default=1,
                max_expected_iterations=2,
            ),
            db_session,
        )


def test_recurring_detect_requires_string_actor(db_session: Session) -> None:
    with pytest.raises(ValueError, match="actor is required"):
        recurring_detect_and_schedule(
            RecurringDetectRequest(
                as_of_date=date(2026, 1, 31),
                actor=None,  # type: ignore[arg-type]
                reason="validation",
            ),
            db_session,
        )


def test_recurring_detect_accepts_string_false_for_create_review_items(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_merchant(db_session, merchant_id="mer-gym", name="Gym")
    _seed_transaction(db_session, transaction_id="txn-1", posted_date=date(2026, 1, 1), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-2", posted_date=date(2026, 1, 8), merchant_id="mer-gym")
    _seed_transaction(db_session, transaction_id="txn-3", posted_date=date(2026, 1, 15), merchant_id="mer-gym")
    db_session.flush()

    result = recurring_detect_and_schedule(
        RecurringDetectRequest(
            as_of_date=date(2026, 1, 29),
            actor="scheduler",
            reason="string bool parse",
            lookback_days=90,
            minimum_occurrences=3,
            tolerance_days_default=1,
            create_review_items="false",  # type: ignore[arg-type]
        ),
        db_session,
    )

    active_review_count = db_session.scalar(
        select(func.count())
        .select_from(ReviewItem)
        .where(
            ReviewItem.ref_table == "recurring_events",
            ReviewItem.reason_code == "recurring.missed_event",
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_(
                [
                    ReviewItemStatus.TO_REVIEW.value,
                    ReviewItemStatus.IN_PROGRESS.value,
                ]
            ),
        )
    )

    assert len(result.warnings) == 2
    assert all(item.review_item_id is None for item in result.warnings)
    assert active_review_count == 0


def test_recurring_detect_rejects_invalid_string_for_create_review_items(db_session: Session) -> None:
    with pytest.raises(ValueError, match="create_review_items must be a boolean"):
        recurring_detect_and_schedule(
            RecurringDetectRequest(
                as_of_date=date(2026, 1, 31),
                actor="scheduler",
                reason="validation",
                create_review_items="not-a-bool",  # type: ignore[arg-type]
            ),
            db_session,
        )


def test_recurring_detect_rejects_non_date_as_of_date(db_session: Session) -> None:
    with pytest.raises(ValueError, match="as_of_date must be a date"):
        recurring_detect_and_schedule(
            RecurringDetectRequest(
                as_of_date="2026-01-31",  # type: ignore[arg-type]
                actor="scheduler",
                reason="validation",
            ),
            db_session,
        )
