from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, MetricObservation, Reconciliation, ReviewItem, ReviewItemEvent, Transaction
from finance_analysis_agent.quality import (
    MetricAlertStatus,
    MetricObservationQueryRequest,
    QualityMetricsGenerateRequest,
    generate_quality_metrics,
    query_metric_observations,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session, *, account_id: str) -> None:
    session.add(Account(id=account_id, name=account_id, type="checking", currency="USD"))


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    account_id: str,
    posted_date: date,
    amount: str,
    merchant_id: str | None,
) -> None:
    now = utcnow()
    decimal_amount = Decimal(amount)
    session.add(
        Transaction(
            id=transaction_id,
            account_id=account_id,
            posted_date=posted_date,
            effective_date=posted_date,
            amount=decimal_amount,
            currency="USD",
            original_amount=decimal_amount,
            original_currency="USD",
            pending_status="posted",
            original_statement="seed",
            merchant_id=merchant_id,
            category_id=None,
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


def _seed_reconciliation(
    session: Session,
    *,
    reconciliation_id: str,
    account_id: str,
    period_end: date,
    status: str,
) -> None:
    session.add(
        Reconciliation(
            id=reconciliation_id,
            account_id=account_id,
            statement_id=None,
            period_start=date(period_end.year, period_end.month, 1),
            period_end=period_end,
            expected_balance=Decimal("100.00"),
            computed_balance=Decimal("100.00"),
            delta=Decimal("0.00"),
            match_rate=1.0 if status == "pass" else 0.5,
            trust_score=1.0 if status == "pass" else 0.5,
            unresolved_count=0,
            adjustment_magnitude=Decimal("0.00"),
            details_json=None,
            approved_adjustment_txn_id=None,
            approved_by=None,
            approved_at=None,
            status=status,
            created_at=utcnow(),
        )
    )


def _seed_review_item(
    session: Session,
    *,
    review_item_id: str,
    transaction_id: str,
    status: str,
    reason_code: str,
    created_at: datetime,
    resolved_at: datetime | None = None,
    source: str = ReviewSource.CATEGORIZE.value,
) -> None:
    session.add(
        ReviewItem(
            id=review_item_id,
            item_type="transaction_category_suggestion",
            ref_table="transactions",
            ref_id=transaction_id,
            reason_code=reason_code,
            confidence=0.42,
            status=status,
            source=source,
            assigned_to=None,
            payload_json={"suggestion": {"kind": "transaction_category", "transaction_id": transaction_id}},
            created_at=created_at,
            resolved_at=resolved_at,
        )
    )


def _seed_review_event(
    session: Session,
    *,
    event_id: str,
    review_item_id: str,
    action: str,
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
            actor="reviewer",
            reason="seed metrics",
            metadata_json=None,
            created_at=created_at,
        )
    )


def test_generate_quality_metrics_validates_request(db_session: Session) -> None:
    with pytest.raises(ValueError, match="actor is required"):
        generate_quality_metrics(
            QualityMetricsGenerateRequest(
                actor="",
                reason="missing actor",
                period_month="2026-02",
            ),
            db_session,
        )


def test_query_metric_observations_rejects_inverted_period_range(db_session: Session) -> None:
    with pytest.raises(ValueError, match="period_end must be >= period_start"):
        query_metric_observations(
            MetricObservationQueryRequest(
                period_start=date(2026, 2, 28),
                period_end=date(2026, 2, 1),
            ),
            db_session,
        )


def test_query_metric_observations_rejects_blank_string_filters(db_session: Session) -> None:
    with pytest.raises(ValueError, match="metric_groups\\[0\\] is required"):
        query_metric_observations(
            MetricObservationQueryRequest(metric_groups=["   "]),
            db_session,
        )


def test_generate_quality_metrics_replaces_existing_snapshot_rows(db_session: Session) -> None:
    _seed_account(db_session, account_id="acct-1")
    _seed_transaction(
        db_session,
        transaction_id="txn-1",
        account_id="acct-1",
        posted_date=date(2026, 2, 5),
        amount="-10.00",
        merchant_id=None,
    )
    _seed_reconciliation(
        db_session,
        reconciliation_id="rec-1",
        account_id="acct-1",
        period_end=date(2026, 2, 28),
        status="pass",
    )
    db_session.flush()

    first = generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="first run",
            period_month="2026-02",
        ),
        db_session,
    )
    db_session.flush()
    second = generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="second run",
            period_month="2026-02",
        ),
        db_session,
    )
    db_session.flush()

    rows = query_metric_observations(
        MetricObservationQueryRequest(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
        ),
        db_session,
    ).observations

    assert rows
    assert len(rows) == len(first.observations) == len(second.observations)
    assert {row.run_id for row in rows} == {second.run_metadata_id}

    persisted_count = db_session.query(MetricObservation).count()
    assert persisted_count == len(second.observations)


def test_query_metric_observations_filters_by_account_template_and_alert_status(db_session: Session) -> None:
    _seed_account(db_session, account_id="acct-a")
    _seed_transaction(
        db_session,
        transaction_id="txn-a",
        account_id="acct-a",
        posted_date=date(2026, 2, 5),
        amount="-25.00",
        merchant_id=None,
    )
    _seed_reconciliation(
        db_session,
        reconciliation_id="rec-a",
        account_id="acct-a",
        period_end=date(2026, 2, 28),
        status="fail",
    )
    db_session.flush()

    generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="filters",
            period_month="2026-02",
        ),
        db_session,
    )
    db_session.flush()

    account_rows = query_metric_observations(
        MetricObservationQueryRequest(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            account_ids=["acct-a"],
            metric_keys=["unknown_merchant_rate"],
        ),
        db_session,
    ).observations
    assert account_rows
    assert {row.account_id for row in account_rows} == {"acct-a"}

    template_rows = query_metric_observations(
        MetricObservationQueryRequest(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            template_keys=["capital_one_credit"],
            metric_groups=["parsing_quality"],
        ),
        db_session,
    ).observations
    assert template_rows
    assert all(row.template_key == "capital_one_credit" for row in template_rows)

    alert_rows = query_metric_observations(
        MetricObservationQueryRequest(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            alert_statuses=[MetricAlertStatus.ALERT],
        ),
        db_session,
    ).observations
    assert alert_rows
    assert all(row.alert_status is MetricAlertStatus.ALERT for row in alert_rows)


def test_generate_quality_metrics_emits_no_data_for_missing_operational_inputs(db_session: Session) -> None:
    result = generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="empty",
            period_month="2026-02",
        ),
        db_session,
    )
    db_session.flush()

    by_key = {(row.metric_group, row.metric_key, row.account_id, row.template_key): row for row in result.observations}
    suggestion = by_key[("automation_quality", "suggestion_acceptance_rate", None, None)]
    unknown_merchant = by_key[("trust_health", "unknown_merchant_rate", None, None)]

    assert suggestion.alert_status is MetricAlertStatus.NO_DATA
    assert unknown_merchant.alert_status is MetricAlertStatus.NO_DATA


def test_generate_quality_metrics_raises_alerts_for_threshold_breaches(db_session: Session) -> None:
    _seed_account(db_session, account_id="acct-alert")
    _seed_transaction(
        db_session,
        transaction_id="txn-alert",
        account_id="acct-alert",
        posted_date=date(2026, 2, 7),
        amount="-15.00",
        merchant_id=None,
    )
    _seed_reconciliation(
        db_session,
        reconciliation_id="rec-alert",
        account_id="acct-alert",
        period_end=date(2026, 2, 28),
        status="fail",
    )
    created_at = datetime(2026, 2, 1, 9, 0, 0)
    resolved_at = created_at + timedelta(hours=96)
    _seed_review_item(
        db_session,
        review_item_id="ri-alert",
        transaction_id="txn-alert",
        status=ReviewItemStatus.REJECTED.value,
        reason_code="categorize.low_confidence",
        created_at=created_at,
        resolved_at=resolved_at,
    )
    _seed_review_event(
        db_session,
        event_id="ev-alert",
        review_item_id="ri-alert",
        action="reject_suggestion",
        created_at=resolved_at,
    )
    db_session.flush()

    result = generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="alerts",
            period_month="2026-02",
        ),
        db_session,
    )

    by_key = {(row.metric_group, row.metric_key, row.account_id): row for row in result.observations if row.template_key is None}
    assert by_key[("correctness", "reconciliation_pass_rate", "acct-alert")].alert_status is MetricAlertStatus.ALERT
    assert by_key[("automation_quality", "suggestion_acceptance_rate", None)].alert_status is MetricAlertStatus.ALERT
    assert by_key[("automation_quality", "review_time_to_inbox_zero_hours", None)].alert_status is MetricAlertStatus.ALERT
    assert by_key[("trust_health", "unknown_merchant_rate", "acct-alert")].alert_status is MetricAlertStatus.ALERT
    assert by_key[("trust_health", "low_confidence_transaction_rate", "acct-alert")].alert_status is MetricAlertStatus.ALERT


def test_query_metric_observations_orders_multi_row_metrics_deterministically(db_session: Session) -> None:
    _seed_account(db_session, account_id="acct-order")
    _seed_transaction(
        db_session,
        transaction_id="txn-order",
        account_id="acct-order",
        posted_date=date(2026, 2, 2),
        amount="-15.00",
        merchant_id=None,
    )
    db_session.flush()

    generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="order",
            period_month="2026-02",
        ),
        db_session,
    )
    db_session.flush()

    result = query_metric_observations(
        MetricObservationQueryRequest(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            metric_keys=["pdf_error_count"],
        ),
        db_session,
    )

    rendered = [
        (row.template_key, row.dimensions.get("error_code"), row.metric_value)
        for row in result.observations
    ]
    assert rendered == sorted(rendered, key=lambda item: (item[0] or "", item[1] or "", item[2] or 0))


def test_generate_quality_metrics_scopes_automation_observations_by_account(db_session: Session) -> None:
    _seed_account(db_session, account_id="acct-a")
    _seed_account(db_session, account_id="acct-b")
    _seed_transaction(
        db_session,
        transaction_id="txn-a1",
        account_id="acct-a",
        posted_date=date(2026, 2, 3),
        amount="-20.00",
        merchant_id="merchant-a",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-b1",
        account_id="acct-b",
        posted_date=date(2026, 2, 4),
        amount="-30.00",
        merchant_id="merchant-b",
    )
    created_at = datetime(2026, 2, 1, 9, 0, 0)
    _seed_review_item(
        db_session,
        review_item_id="ri-a1",
        transaction_id="txn-a1",
        status=ReviewItemStatus.RESOLVED.value,
        reason_code="categorize.low_confidence",
        created_at=created_at,
        resolved_at=created_at + timedelta(hours=12),
    )
    _seed_review_event(
        db_session,
        event_id="ev-a1",
        review_item_id="ri-a1",
        action="approve_suggestion",
        created_at=created_at + timedelta(hours=12),
    )
    _seed_review_item(
        db_session,
        review_item_id="ri-b1",
        transaction_id="txn-b1",
        status=ReviewItemStatus.REJECTED.value,
        reason_code="categorize.low_confidence",
        created_at=created_at,
        resolved_at=created_at + timedelta(hours=96),
    )
    _seed_review_event(
        db_session,
        event_id="ev-b1",
        review_item_id="ri-b1",
        action="reject_suggestion",
        created_at=created_at + timedelta(hours=96),
    )
    db_session.flush()

    first = generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="scoped acct-a",
            period_month="2026-02",
            account_ids=["acct-a"],
        ),
        db_session,
    )
    second = generate_quality_metrics(
        QualityMetricsGenerateRequest(
            actor="tester",
            reason="scoped acct-b",
            period_month="2026-02",
            account_ids=["acct-b"],
        ),
        db_session,
    )
    db_session.flush()

    first_automation = {
        (row.metric_key, row.account_id): row
        for row in first.observations
        if row.metric_group == "automation_quality"
    }
    second_automation = {
        (row.metric_key, row.account_id): row
        for row in second.observations
        if row.metric_group == "automation_quality"
    }
    assert set(first_automation) == {
        ("suggestion_acceptance_rate", "acct-a"),
        ("review_time_to_inbox_zero_hours", "acct-a"),
    }
    assert set(second_automation) == {
        ("suggestion_acceptance_rate", "acct-b"),
        ("review_time_to_inbox_zero_hours", "acct-b"),
    }

    scoped_rows = query_metric_observations(
        MetricObservationQueryRequest(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            account_ids=["acct-a", "acct-b"],
            metric_groups=["automation_quality"],
        ),
        db_session,
    ).observations
    assert {(row.metric_key, row.account_id) for row in scoped_rows} == {
        ("suggestion_acceptance_rate", "acct-a"),
        ("review_time_to_inbox_zero_hours", "acct-a"),
        ("suggestion_acceptance_rate", "acct-b"),
        ("review_time_to_inbox_zero_hours", "acct-b"),
    }
