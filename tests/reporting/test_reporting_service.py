from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    Account,
    BalanceSnapshot,
    Budget,
    BudgetAllocation,
    BudgetCategory,
    BudgetPeriod,
    Category,
    Goal,
    GoalAllocation,
    GoalEvent,
    MetricObservation,
    Report,
    RunMetadata,
    Transaction,
)
from finance_analysis_agent.reporting import ReportType, ReportingGenerateRequest, reporting_generate
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session, *, account_id: str, name: str) -> None:
    session.add(Account(id=account_id, name=name, type="checking", currency="USD"))


def _seed_category(session: Session, *, category_id: str, name: str) -> None:
    session.add(
        Category(
            id=category_id,
            parent_id=None,
            name=name,
            system_flag=False,
            active=True,
            created_at=utcnow(),
        )
    )


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    account_id: str,
    posted_date: date,
    amount: str,
    category_id: str | None,
    pending_status: str = "posted",
    excluded: bool = False,
    transfer_group_id: str | None = None,
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
            pending_status=pending_status,
            original_statement="seed",
            merchant_id=None,
            category_id=category_id,
            excluded=excluded,
            notes=None,
            source_kind="manual",
            source_transaction_id=f"src-{transaction_id}",
            import_batch_id=None,
            transfer_group_id=transfer_group_id,
            created_at=now,
            updated_at=now,
        )
    )


def _seed_balance_snapshot(
    session: Session,
    *,
    snapshot_id: str,
    account_id: str,
    snapshot_date: date,
    balance: str,
    source: str,
) -> None:
    session.add(
        BalanceSnapshot(
            id=snapshot_id,
            account_id=account_id,
            snapshot_date=snapshot_date,
            balance=Decimal(balance),
            source=source,
            statement_id=None,
            created_at=utcnow(),
        )
    )


def _seed_budget_baseline(session: Session) -> None:
    session.add(
        Budget(
            id="budget-main",
            name="Household",
            method="zero_based",
            base_currency="USD",
            active=True,
            created_at=utcnow(),
        )
    )
    session.add(
        BudgetCategory(
            id="bc-food",
            budget_id="budget-main",
            category_id="cat-food",
            policy_json=None,
            rollover_policy=None,
        )
    )
    session.add(
        BudgetCategory(
            id="bc-rent",
            budget_id="budget-main",
            category_id="cat-rent",
            policy_json=None,
            rollover_policy=None,
        )
    )
    session.add(
        BudgetPeriod(
            id="period-2026-02",
            budget_id="budget-main",
            period_month="2026-02",
            to_assign=Decimal("0.00"),
            assigned_total=Decimal("1200.00"),
            spent_total=Decimal("1120.00"),
            rollover_total=Decimal("0.00"),
            status="open",
        )
    )
    session.add(
        BudgetAllocation(
            id="alloc-food",
            budget_period_id="period-2026-02",
            budget_category_id="bc-food",
            assigned_amount=Decimal("200.00"),
            source="seed",
        )
    )
    session.add(
        BudgetAllocation(
            id="alloc-rent",
            budget_period_id="period-2026-02",
            budget_category_id="bc-rent",
            assigned_amount=Decimal("1000.00"),
            source="seed",
        )
    )


def _seed_goal_baseline(session: Session) -> None:
    session.add(
        Goal(
            id="goal-car",
            name="Car Fund",
            target_amount=Decimal("5000.00"),
            target_date=date(2027, 12, 31),
            monthly_contribution=Decimal("300.00"),
            spending_reduces_progress=False,
            status="active",
            metadata_json=None,
        )
    )
    session.add(
        GoalAllocation(
            id="goal-alloc-1",
            goal_id="goal-car",
            account_id="acct-checking",
            period_month="2026-02",
            amount=Decimal("300.00"),
            allocation_type="manual",
            created_at=utcnow(),
        )
    )


def _seed_goal_event(
    session: Session,
    *,
    goal_event_id: str,
    goal_id: str,
    event_date: date,
    amount: str,
    related_transaction_id: str | None = None,
) -> None:
    session.add(
        GoalEvent(
            id=goal_event_id,
            goal_id=goal_id,
            event_date=event_date,
            event_type="manual_adjustment",
            amount=Decimal(amount),
            related_transaction_id=related_transaction_id,
            metadata_json=None,
        )
    )


def _seed_reporting_baseline(session: Session) -> None:
    _seed_account(session, account_id="acct-checking", name="Checking")
    _seed_account(session, account_id="acct-savings", name="Savings")

    _seed_category(session, category_id="cat-food", name="Food")
    _seed_category(session, category_id="cat-rent", name="Rent")

    _seed_transaction(
        session,
        transaction_id="txn-income",
        account_id="acct-checking",
        posted_date=date(2026, 2, 1),
        amount="3000.00",
        category_id=None,
    )
    _seed_transaction(
        session,
        transaction_id="txn-food",
        account_id="acct-checking",
        posted_date=date(2026, 2, 5),
        amount="-120.00",
        category_id="cat-food",
    )
    _seed_transaction(
        session,
        transaction_id="txn-rent",
        account_id="acct-checking",
        posted_date=date(2026, 2, 10),
        amount="-1000.00",
        category_id="cat-rent",
    )
    _seed_transaction(
        session,
        transaction_id="txn-transfer",
        account_id="acct-checking",
        posted_date=date(2026, 2, 15),
        amount="-200.00",
        category_id=None,
        transfer_group_id="transfer-1",
    )
    _seed_transaction(
        session,
        transaction_id="txn-pending",
        account_id="acct-checking",
        posted_date=date(2026, 2, 18),
        amount="-80.00",
        category_id="cat-food",
        pending_status="pending",
    )
    _seed_transaction(
        session,
        transaction_id="txn-excluded",
        account_id="acct-checking",
        posted_date=date(2026, 2, 20),
        amount="-40.00",
        category_id="cat-food",
        excluded=True,
    )

    _seed_balance_snapshot(
        session,
        snapshot_id="snap-checking-1",
        account_id="acct-checking",
        snapshot_date=date(2026, 1, 31),
        balance="1000.00",
        source="statement",
    )
    _seed_balance_snapshot(
        session,
        snapshot_id="snap-checking-2",
        account_id="acct-checking",
        snapshot_date=date(2026, 2, 28),
        balance="1880.00",
        source="reconciliation",
    )
    _seed_balance_snapshot(
        session,
        snapshot_id="snap-savings-1",
        account_id="acct-savings",
        snapshot_date=date(2026, 1, 31),
        balance="500.00",
        source="statement",
    )
    _seed_balance_snapshot(
        session,
        snapshot_id="snap-savings-2",
        account_id="acct-savings",
        snapshot_date=date(2026, 2, 28),
        balance="700.00",
        source="statement",
    )

    _seed_budget_baseline(session)
    _seed_goal_baseline(session)


def test_reporting_generate_all_reports_persists_and_records_run_metadata(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    result = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="reporting acceptance",
            period_month="2026-02",
            budget_id="budget-main",
        ),
        db_session,
    )
    db_session.flush()

    assert result.report_types == list(ReportType)
    assert len(result.reports) == 6

    reports = db_session.scalars(
        select(Report)
        .where(Report.run_id == result.run_metadata_id)
        .order_by(Report.report_type.asc())
    ).all()
    assert len(reports) == 6

    run = db_session.get(RunMetadata, result.run_metadata_id)
    assert run is not None
    assert run.pipeline_name == "reporting_generate"
    assert run.status == "success"
    assert run.diagnostics_json is not None
    assert run.diagnostics_json["report_count"] == 6

    cashflow = next(item for item in result.reports if item.report_type is ReportType.CASH_FLOW)
    assert cashflow.payload_json["summary"]["inflow"] == "3000.00"
    assert cashflow.payload_json["summary"]["outflow"] == "1120.00"
    assert cashflow.payload_json["summary"]["net"] == "1880.00"

    dashboard = next(item for item in result.reports if item.report_type is ReportType.QUALITY_TRUST_DASHBOARD)
    assert dashboard.payload_json["metric_run_id"]
    assert set(dashboard.payload_json["groups"]) == {
        "correctness",
        "automation_quality",
        "parsing_quality",
        "trust_health",
    }


def test_reporting_generate_is_deterministic_for_same_snapshot(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    request = ReportingGenerateRequest(
        actor="tester",
        reason="deterministic check",
        period_month="2026-02",
        budget_id="budget-main",
    )

    first = reporting_generate(request, db_session)
    db_session.flush()
    second = reporting_generate(request, db_session)
    db_session.flush()

    first_by_type = {item.report_type: item for item in first.reports}
    second_by_type = {item.report_type: item for item in second.reports}

    assert set(first_by_type) == set(second_by_type)
    for report_type in first_by_type:
        assert first_by_type[report_type].payload_hash == second_by_type[report_type].payload_hash
        if report_type is ReportType.QUALITY_TRUST_DASHBOARD:
            first_payload = dict(first_by_type[report_type].payload_json)
            second_payload = dict(second_by_type[report_type].payload_json)
            first_payload.pop("metric_run_id", None)
            second_payload.pop("metric_run_id", None)
            assert first_payload == second_payload
            assert first_by_type[report_type].payload_json["metric_snapshot_id"] == second_by_type[report_type].payload_json["metric_snapshot_id"]
        else:
            assert first_by_type[report_type].payload_json == second_by_type[report_type].payload_json


def test_reporting_generate_quality_dashboard_can_run_with_other_reports(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    result = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="quality dashboard",
            period_month="2026-02",
            report_types=[ReportType.CASH_FLOW, ReportType.QUALITY_TRUST_DASHBOARD],
        ),
        db_session,
    )
    db_session.flush()

    report_types = [item.report_type for item in result.reports]
    assert report_types == [ReportType.CASH_FLOW, ReportType.QUALITY_TRUST_DASHBOARD]

    dashboard = next(item for item in result.reports if item.report_type is ReportType.QUALITY_TRUST_DASHBOARD)
    assert dashboard.payload_json["summary"]["metric_count"] >= 1
    assert isinstance(dashboard.payload_json["alerts"], list)
    assert dashboard.payload_json["metric_run_id"]
    assert dashboard.payload_json["metric_snapshot_id"]


def test_reporting_generate_preserves_requested_report_order_when_quality_dashboard_is_first(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    result = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="quality dashboard order",
            period_month="2026-02",
            report_types=[ReportType.QUALITY_TRUST_DASHBOARD, ReportType.CASH_FLOW],
        ),
        db_session,
    )
    db_session.flush()

    assert [item.report_type for item in result.reports] == [
        ReportType.QUALITY_TRUST_DASHBOARD,
        ReportType.CASH_FLOW,
    ]


def test_reporting_generate_defers_quality_metrics_until_after_other_payload_validation(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    with pytest.raises(ValueError, match="Budget not found"):
        reporting_generate(
            ReportingGenerateRequest(
                actor="tester",
                reason="quality after validation",
                period_month="2026-02",
                report_types=[ReportType.QUALITY_TRUST_DASHBOARD, ReportType.BUDGET_VS_ACTUAL],
                budget_id="budget-missing",
            ),
            db_session,
        )
    db_session.flush()

    assert db_session.query(MetricObservation).count() == 0


def test_reporting_generate_applies_account_scope_filter(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    _seed_transaction(
        db_session,
        transaction_id="txn-savings-income",
        account_id="acct-savings",
        posted_date=date(2026, 2, 21),
        amount="250.00",
        category_id=None,
    )
    db_session.flush()

    scoped = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="scoped",
            period_month="2026-02",
            report_types=[ReportType.CASH_FLOW],
            account_ids=["acct-checking"],
        ),
        db_session,
    )
    db_session.flush()

    unscoped = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="unscoped",
            period_month="2026-02",
            report_types=[ReportType.CASH_FLOW],
        ),
        db_session,
    )
    db_session.flush()

    scoped_payload = scoped.reports[0].payload_json
    unscoped_payload = unscoped.reports[0].payload_json

    assert scoped_payload["summary"]["net"] == "1880.00"
    assert unscoped_payload["summary"]["net"] == "2130.00"


def test_reporting_generate_requires_budget_id_for_budget_vs_actual(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    with pytest.raises(ValueError, match="budget_id is required"):
        reporting_generate(
            ReportingGenerateRequest(
                actor="tester",
                reason="budget check",
                period_month="2026-02",
                report_types=[ReportType.BUDGET_VS_ACTUAL],
            ),
            db_session,
        )


def test_goal_progress_account_scope_excludes_goal_events_without_related_transaction(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()
    goal = db_session.get(Goal, "goal-car")
    assert goal is not None
    goal.spending_reduces_progress = True
    _seed_goal_event(
        db_session,
        goal_event_id="goal-event-no-txn",
        goal_id="goal-car",
        event_date=date(2026, 2, 22),
        amount="-50.00",
        related_transaction_id=None,
    )
    db_session.flush()

    scoped = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="goal progress account scope",
            period_month="2026-02",
            report_types=[ReportType.GOAL_PROGRESS],
            account_ids=["acct-checking"],
        ),
        db_session,
    )
    db_session.flush()

    unscoped = reporting_generate(
        ReportingGenerateRequest(
            actor="tester",
            reason="goal progress unscoped",
            period_month="2026-02",
            report_types=[ReportType.GOAL_PROGRESS],
        ),
        db_session,
    )
    db_session.flush()

    scoped_goal_payload = scoped.reports[0].payload_json["goals"][0]
    unscoped_goal_payload = unscoped.reports[0].payload_json["goals"][0]
    assert scoped_goal_payload["spending_total"] == "0.00"
    assert scoped_goal_payload["progress_amount"] == "300.00"
    assert unscoped_goal_payload["spending_total"] == "50.00"
    assert unscoped_goal_payload["progress_amount"] == "250.00"


def test_reporting_generate_marks_run_failed_when_budget_is_missing(db_session: Session) -> None:
    _seed_reporting_baseline(db_session)
    db_session.flush()

    with pytest.raises(ValueError, match="Budget not found"):
        reporting_generate(
            ReportingGenerateRequest(
                actor="tester",
                reason="failing run",
                period_month="2026-02",
                report_types=[ReportType.BUDGET_VS_ACTUAL],
                budget_id="budget-missing",
            ),
            db_session,
        )
    db_session.flush()

    latest_run = db_session.scalars(
        select(RunMetadata)
        .where(RunMetadata.pipeline_name == "reporting_generate")
        .order_by(RunMetadata.started_at.desc(), RunMetadata.id.desc())
    ).first()

    assert latest_run is not None
    assert latest_run.status == "failed"
    assert latest_run.diagnostics_json is not None
    assert "Budget not found" in latest_run.diagnostics_json["error"]
