from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Goal, GoalAllocation, GoalEvent, Transaction
from finance_analysis_agent.goals import GoalAllocationInput, GoalLedgerComputeRequest, goal_ledger_compute
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session, *, account_id: str = "acct-1") -> None:
    session.add(Account(id=account_id, name="Checking", type="checking", currency="USD"))


def _seed_goal(
    session: Session,
    *,
    goal_id: str,
    target_amount: str = "1000.00",
    target_date: date | None = None,
    monthly_contribution: str | None = None,
    spending_reduces_progress: bool = False,
    status: str = "active",
) -> None:
    session.add(
        Goal(
            id=goal_id,
            name=f"Goal {goal_id}",
            target_amount=Decimal(target_amount),
            target_date=target_date,
            monthly_contribution=(Decimal(monthly_contribution) if monthly_contribution is not None else None),
            spending_reduces_progress=spending_reduces_progress,
            status=status,
            metadata_json=None,
        )
    )


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    posted_date: date,
    amount: str,
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
            original_statement="seed",
            merchant_id=None,
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


def _seed_goal_event(
    session: Session,
    *,
    event_id: str,
    goal_id: str,
    event_date: date,
    amount: str,
    related_transaction_id: str | None = None,
) -> None:
    session.add(
        GoalEvent(
            id=event_id,
            goal_id=goal_id,
            event_date=event_date,
            event_type="rule.linked_transaction",
            amount=Decimal(amount),
            related_transaction_id=related_transaction_id,
            metadata_json=None,
        )
    )


def test_goal_ledger_ignores_spending_when_toggle_disabled(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_goal(db_session, goal_id="goal-save", spending_reduces_progress=False)
    _seed_transaction(
        db_session,
        transaction_id="txn-1",
        posted_date=date(2026, 2, 10),
        amount="-120.00",
    )
    _seed_goal_event(
        db_session,
        event_id="ge-1",
        goal_id="goal-save",
        event_date=date(2026, 2, 10),
        amount="-120.00",
        related_transaction_id="txn-1",
    )
    db_session.flush()

    result = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="goal-planner",
            reason="monthly funding",
            allocations=[
                GoalAllocationInput(
                    goal_id="goal-save",
                    account_id="acct-1",
                    amount="200.00",
                )
            ],
        ),
        db_session,
    )

    assert result.allocated_this_period_total == Decimal("200.00")
    assert len(result.goals) == 1
    snapshot = result.goals[0]
    assert snapshot.goal_id == "goal-save"
    assert snapshot.spending_total == Decimal("0.00")
    assert snapshot.progress_amount == Decimal("200.00")
    assert snapshot.remaining_amount == Decimal("800.00")


def test_goal_ledger_applies_spending_when_toggle_enabled(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_goal(db_session, goal_id="goal-save", spending_reduces_progress=True)
    _seed_transaction(
        db_session,
        transaction_id="txn-1",
        posted_date=date(2026, 2, 10),
        amount="-120.00",
    )
    _seed_goal_event(
        db_session,
        event_id="ge-1",
        goal_id="goal-save",
        event_date=date(2026, 2, 10),
        amount="-120.00",
        related_transaction_id="txn-1",
    )
    db_session.flush()

    result = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="goal-planner",
            reason="monthly funding",
            allocations=[
                GoalAllocationInput(
                    goal_id="goal-save",
                    account_id="acct-1",
                    amount="200.00",
                )
            ],
        ),
        db_session,
    )

    snapshot = result.goals[0]
    assert snapshot.spending_total == Decimal("120.00")
    assert snapshot.progress_amount == Decimal("80.00")
    assert snapshot.remaining_amount == Decimal("920.00")


def test_goal_ledger_rejects_over_allocation(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_goal(db_session, goal_id="goal-a")
    _seed_goal(db_session, goal_id="goal-b")
    db_session.flush()

    with pytest.raises(ValueError, match="Goal allocations exceed available funds"):
        goal_ledger_compute(
            GoalLedgerComputeRequest(
                period_month="2026-02",
                available_funds="500.00",
                actor="goal-planner",
                reason="monthly funding",
                allocations=[
                    GoalAllocationInput(
                        goal_id="goal-a",
                        account_id="acct-1",
                        amount="300.00",
                    ),
                    GoalAllocationInput(
                        goal_id="goal-b",
                        account_id="acct-1",
                        amount="300.00",
                    ),
                ],
            ),
            db_session,
        )


def test_goal_ledger_upserts_period_allocations_idempotently(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_goal(db_session, goal_id="goal-save")
    db_session.flush()

    first = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="goal-planner",
            reason="initial funding",
            allocations=[
                GoalAllocationInput(
                    goal_id="goal-save",
                    account_id="acct-1",
                    amount="150.00",
                )
            ],
        ),
        db_session,
    )
    second = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="goal-planner",
            reason="adjust funding",
            allocations=[
                GoalAllocationInput(
                    goal_id="goal-save",
                    account_id="acct-1",
                    amount="175.00",
                )
            ],
        ),
        db_session,
    )

    row_count = db_session.scalar(
        select(func.count())
        .select_from(GoalAllocation)
        .where(
            GoalAllocation.goal_id == "goal-save",
            GoalAllocation.account_id == "acct-1",
            GoalAllocation.period_month == "2026-02",
            GoalAllocation.allocation_type == "manual",
        )
    )
    allocation = db_session.scalar(
        select(GoalAllocation).where(
            GoalAllocation.goal_id == "goal-save",
            GoalAllocation.account_id == "acct-1",
            GoalAllocation.period_month == "2026-02",
            GoalAllocation.allocation_type == "manual",
        )
    )

    assert first.allocated_this_period_total == Decimal("150.00")
    assert second.allocated_this_period_total == Decimal("175.00")
    assert row_count == 1
    assert allocation is not None and Decimal(allocation.amount) == Decimal("175.00")


def test_goal_ledger_projection_uses_monthly_contribution_precedence(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_goal(
        db_session,
        goal_id="goal-save",
        target_amount="500.00",
        target_date=date(2026, 6, 30),
        monthly_contribution="50.00",
    )
    db_session.flush()

    result = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="goal-planner",
            reason="monthly funding",
            allocations=[
                GoalAllocationInput(
                    goal_id="goal-save",
                    account_id="acct-1",
                    amount="200.00",
                )
            ],
        ),
        db_session,
    )

    snapshot = result.goals[0]
    assert snapshot.months_to_completion == 6
    assert snapshot.projected_completion_date == date(2026, 7, 1)
    assert snapshot.status == "at_risk"
    assert any(cause.code == "goal_projection_after_target" for cause in result.causes)


def test_goal_ledger_marks_unfunded_when_no_projection_pace(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_goal(db_session, goal_id="goal-save")
    db_session.flush()

    result = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="goal-planner",
            reason="snapshot only",
            allocations=[],
        ),
        db_session,
    )

    snapshot = result.goals[0]
    assert snapshot.status == "unfunded"
    assert snapshot.projected_completion_date is None
    assert snapshot.months_to_completion is None
