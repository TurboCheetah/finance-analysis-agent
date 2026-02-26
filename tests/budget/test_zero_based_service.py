from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.budget import (
    BudgetCategoryAllocationInput,
    BudgetComputeZeroBasedRequest,
    BudgetTargetPolicyInput,
    budget_compute_zero_based,
)
from finance_analysis_agent.db.models import (
    Account,
    Budget,
    BudgetAllocation,
    BudgetCategory,
    BudgetPeriod,
    BudgetRollover,
    BudgetTarget,
    Category,
    Transaction,
)
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session, *, account_id: str = "acct-1") -> None:
    session.add(Account(id=account_id, name="Checking", type="checking", currency="USD"))


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


def _seed_budget(
    session: Session,
    *,
    budget_id: str = "budget-1",
    method: str = "zero_based",
    active: bool = True,
) -> None:
    session.add(
        Budget(
            id=budget_id,
            name="Household",
            method=method,
            base_currency="USD",
            active=active,
            created_at=utcnow(),
        )
    )


def _seed_budget_category(
    session: Session,
    *,
    budget_category_id: str,
    budget_id: str,
    category_id: str,
) -> None:
    session.add(
        BudgetCategory(
            id=budget_category_id,
            budget_id=budget_id,
            category_id=category_id,
            policy_json=None,
        )
    )


def _seed_target(
    session: Session,
    *,
    target_id: str,
    budget_category_id: str,
    amount: str,
    cadence: str = "monthly",
    top_up: bool = False,
    snoozed_until: date | None = None,
    metadata_json: dict[str, object] | None = None,
) -> None:
    session.add(
        BudgetTarget(
            id=target_id,
            budget_category_id=budget_category_id,
            target_type="scheduled",
            amount=Decimal(amount),
            cadence=cadence,
            top_up=top_up,
            snoozed_until=snoozed_until,
            metadata_json=metadata_json,
        )
    )


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    category_id: str,
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


def test_budget_compute_zero_based_persists_period_and_allocations(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-grocery", name="Grocery")
    _seed_category(db_session, category_id="cat-rent", name="Rent")
    _seed_budget(db_session, budget_id="budget-main")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-grocery",
        budget_id="budget-main",
        category_id="cat-grocery",
    )
    _seed_budget_category(
        db_session,
        budget_category_id="bc-rent",
        budget_id="budget-main",
        category_id="cat-rent",
    )
    _seed_target(
        db_session,
        target_id="target-grocery",
        budget_category_id="bc-grocery",
        amount="500.00",
    )
    _seed_target(
        db_session,
        target_id="target-rent",
        budget_category_id="bc-rent",
        amount="1200.00",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-grocery",
        category_id="cat-grocery",
        posted_date=date(2026, 2, 8),
        amount="-150.00",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-rent",
        category_id="cat-rent",
        posted_date=date(2026, 2, 1),
        amount="-1200.00",
    )
    db_session.flush()

    result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-main",
            period_month="2026-02",
            available_cash="2500.00",
            actor="budgeter",
            reason="monthly allocation",
            category_allocations=[
                BudgetCategoryAllocationInput(
                    budget_category_id="bc-grocery",
                    assigned_amount="300.00",
                ),
                BudgetCategoryAllocationInput(
                    budget_category_id="bc-rent",
                    assigned_amount="1200.00",
                ),
            ],
        ),
        db_session,
    )
    db_session.flush()

    period = db_session.get(BudgetPeriod, result.budget_period_id)
    allocations = db_session.scalars(
        select(BudgetAllocation)
        .where(BudgetAllocation.budget_period_id == result.budget_period_id)
        .order_by(BudgetAllocation.budget_category_id.asc())
    ).all()

    assert result.assigned_total == Decimal("1500.00")
    assert result.spent_total == Decimal("1350.00")
    assert result.to_assign == Decimal("1000.00")
    assert result.underfunded_total == Decimal("200.00")
    assert result.overspent_total == Decimal("0.00")
    assert period is not None
    assert period.to_assign == Decimal("1000.00")
    assert period.assigned_total == Decimal("1500.00")
    assert len(allocations) == 2
    assert [allocation.budget_category_id for allocation in allocations] == ["bc-grocery", "bc-rent"]


def test_budget_compute_zero_based_is_idempotent_for_same_request(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-idem")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-idem",
        category_id="cat-food",
    )
    _seed_target(
        db_session,
        target_id="target-food",
        budget_category_id="bc-food",
        amount="100.00",
    )
    db_session.add(
        BudgetPeriod(
            id="period-idem",
            budget_id="budget-idem",
            period_month="2026-03",
            to_assign=Decimal("0.00"),
            assigned_total=Decimal("0.00"),
            spent_total=Decimal("0.00"),
            rollover_total=Decimal("0.00"),
            status="open",
        )
    )
    db_session.add(
        BudgetAllocation(
            id="legacy-allocation-id",
            budget_period_id="period-idem",
            budget_category_id="bc-food",
            assigned_amount=Decimal("25.00"),
            source="legacy",
        )
    )
    db_session.flush()

    request = BudgetComputeZeroBasedRequest(
        budget_id="budget-idem",
        period_month="2026-03",
        available_cash="1000.00",
        actor="budgeter",
        reason="idempotency",
        category_allocations=[BudgetCategoryAllocationInput(budget_category_id="bc-food", assigned_amount="100.00")],
    )
    first = budget_compute_zero_based(request, db_session)
    db_session.flush()
    second = budget_compute_zero_based(request, db_session)
    db_session.flush()

    period_count = db_session.scalar(
        select(func.count())
        .select_from(BudgetPeriod)
        .where(BudgetPeriod.budget_id == "budget-idem", BudgetPeriod.period_month == "2026-03")
    )
    allocations = db_session.scalars(
        select(BudgetAllocation)
        .where(BudgetAllocation.budget_period_id == first.budget_period_id)
        .order_by(BudgetAllocation.id.asc())
    ).all()

    assert first == second
    assert period_count == 1
    assert len(allocations) == 1
    assert allocations[0].id == f"alloc:{first.budget_period_id}:bc-food"
    assert allocations[0].budget_category_id == "bc-food"
    assert allocations[0].assigned_amount == Decimal("100.00")


def test_budget_compute_zero_based_respects_snoozed_targets(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-fun", name="Fun")
    _seed_budget(db_session, budget_id="budget-snooze")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-fun",
        budget_id="budget-snooze",
        category_id="cat-fun",
    )
    _seed_target(
        db_session,
        target_id="target-fun",
        budget_category_id="bc-fun",
        amount="200.00",
        snoozed_until=date(2026, 3, 20),
    )
    db_session.flush()

    result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-snooze",
            period_month="2026-03",
            available_cash="500.00",
            actor="budgeter",
            reason="snoozed target",
        ),
        db_session,
    )
    snapshot = result.categories[0]

    assert snapshot.snoozed is True
    assert snapshot.target_required == Decimal("0.00")
    assert snapshot.underfunded == Decimal("0.00")


def test_budget_compute_zero_based_uses_updated_target_policy_values_same_run(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-updated-target", name="Updated Target")
    _seed_budget(db_session, budget_id="budget-target-update")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-updated-target",
        budget_id="budget-target-update",
        category_id="cat-updated-target",
    )
    _seed_target(
        db_session,
        target_id="target-updated-target",
        budget_category_id="bc-updated-target",
        amount="100.00",
        cadence="monthly",
    )
    db_session.flush()

    result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-target-update",
            period_month="2026-03",
            available_cash="500.00",
            actor="budgeter",
            reason="update target policy and compute",
            target_policies=[
                BudgetTargetPolicyInput(
                    budget_category_id="bc-updated-target",
                    amount="250.00",
                    cadence="monthly",
                    top_up=False,
                    target_type="scheduled",
                )
            ],
        ),
        db_session,
    )

    snapshot = result.categories[0]
    assert snapshot.target_required == Decimal("250.00")
    assert snapshot.underfunded == Decimal("250.00")


def test_budget_compute_zero_based_supports_every_n_months_cadence(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-auto", name="Auto")
    _seed_budget(db_session, budget_id="budget-cadence")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-auto",
        budget_id="budget-cadence",
        category_id="cat-auto",
    )
    _seed_target(
        db_session,
        target_id="target-auto",
        budget_category_id="bc-auto",
        amount="300.00",
        cadence="every_n_months",
        metadata_json={"anchor_month": "2026-01", "months_interval": 3},
    )
    db_session.flush()

    inactive_result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-cadence",
            period_month="2026-02",
            available_cash="500.00",
            actor="budgeter",
            reason="inactive cadence month",
        ),
        db_session,
    )
    active_result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-cadence",
            period_month="2026-04",
            available_cash="500.00",
            actor="budgeter",
            reason="active cadence month",
        ),
        db_session,
    )

    assert inactive_result.categories[0].target_required == Decimal("0.00")
    assert active_result.categories[0].target_required == Decimal("300.00")


def test_budget_compute_zero_based_top_up_uses_previous_available_balance(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-home", name="Home")
    _seed_budget(db_session, budget_id="budget-topup")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-home",
        budget_id="budget-topup",
        category_id="cat-home",
    )
    _seed_target(
        db_session,
        target_id="target-home",
        budget_category_id="bc-home",
        amount="500.00",
        top_up=True,
    )
    previous_period = BudgetPeriod(
        id="period-prev",
        budget_id="budget-topup",
        period_month="2026-01",
        to_assign=Decimal("0.00"),
        assigned_total=Decimal("400.00"),
        spent_total=Decimal("150.00"),
        rollover_total=Decimal("0.00"),
        status="open",
    )
    db_session.add(previous_period)
    db_session.add(
        BudgetAllocation(
            id="alloc-prev-home",
            budget_period_id="period-prev",
            budget_category_id="bc-home",
            assigned_amount=Decimal("400.00"),
            source="manual",
        )
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-prev-home",
        category_id="cat-home",
        posted_date=date(2026, 1, 10),
        amount="-150.00",
    )
    db_session.flush()

    result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-topup",
            period_month="2026-02",
            available_cash="1000.00",
            actor="budgeter",
            reason="top-up behavior",
            category_allocations=[BudgetCategoryAllocationInput(budget_category_id="bc-home", assigned_amount="100.00")],
        ),
        db_session,
    )
    snapshot = result.categories[0]

    assert snapshot.available_before_assignment == Decimal("250.00")
    assert snapshot.target_required == Decimal("250.00")
    assert snapshot.underfunded == Decimal("150.00")


def test_budget_compute_zero_based_carries_closed_month_overspending_to_next_to_assign(
    db_session: Session,
) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-gas", name="Gas")
    _seed_budget(db_session, budget_id="budget-carry")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-gas",
        budget_id="budget-carry",
        category_id="cat-gas",
    )
    db_session.add(
        BudgetPeriod(
            id="period-2026-01",
            budget_id="budget-carry",
            period_month="2026-01",
            to_assign=Decimal("0.00"),
            assigned_total=Decimal("100.00"),
            spent_total=Decimal("175.00"),
            rollover_total=Decimal("0.00"),
            status="closed",
        )
    )
    db_session.flush()

    result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-carry",
            period_month="2026-02",
            available_cash="500.00",
            actor="budgeter",
            reason="carry overspent",
            status="open",
            category_allocations=[BudgetCategoryAllocationInput(budget_category_id="bc-gas", assigned_amount="200.00")],
        ),
        db_session,
    )
    db_session.flush()

    rollover = db_session.get(
        BudgetRollover,
        "rollover:budget:budget-carry:2026-01->2026-02:overspent",
    )

    assert result.carry_in_overspent == Decimal("75.00")
    assert result.rollover_total == Decimal("75.00")
    assert result.to_assign == Decimal("225.00")
    assert rollover is not None
    assert rollover.carry_amount == Decimal("75.00")
    assert any(cause.code == "overspent_carry_applied" for cause in result.causes)


def test_budget_compute_zero_based_does_not_carry_overspend_from_open_period(
    db_session: Session,
) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-util", name="Utilities")
    _seed_budget(db_session, budget_id="budget-open-prev")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-util",
        budget_id="budget-open-prev",
        category_id="cat-util",
    )
    db_session.add(
        BudgetPeriod(
            id="period-open-prev",
            budget_id="budget-open-prev",
            period_month="2026-01",
            to_assign=Decimal("0.00"),
            assigned_total=Decimal("100.00"),
            spent_total=Decimal("200.00"),
            rollover_total=Decimal("0.00"),
            status="open",
        )
    )
    db_session.flush()

    result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-open-prev",
            period_month="2026-02",
            available_cash="500.00",
            actor="budgeter",
            reason="no carry from open period",
        ),
        db_session,
    )

    assert result.carry_in_overspent == Decimal("0.00")
    assert result.rollover_total == Decimal("0.00")
    assert result.to_assign == Decimal("500.00")


def test_budget_compute_zero_based_rejects_invalid_period_month(db_session: Session) -> None:
    _seed_budget(db_session, budget_id="budget-invalid-period")
    with pytest.raises(ValueError, match="period_month must be in YYYY-MM format"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-invalid-period",
                period_month="2026/02",
                available_cash="100.00",
                actor="budgeter",
                reason="invalid month",
            ),
            db_session,
        )
    with pytest.raises(ValueError, match="period_month must be in YYYY-MM format"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-invalid-period",
                period_month="2026-2",
                available_cash="100.00",
                actor="budgeter",
                reason="invalid month padding",
            ),
            db_session,
        )


def test_budget_compute_zero_based_rejects_negative_assignments(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-neg", name="Negative")
    _seed_budget(db_session, budget_id="budget-neg")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-neg",
        budget_id="budget-neg",
        category_id="cat-neg",
    )

    with pytest.raises(ValueError, match="assigned_amount must be >= 0"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-neg",
                period_month="2026-02",
                available_cash="100.00",
                actor="budgeter",
                reason="negative allocation",
                category_allocations=[
                    BudgetCategoryAllocationInput(
                        budget_category_id="bc-neg",
                        assigned_amount="-1.00",
                    )
                ],
            ),
            db_session,
        )


def test_budget_compute_zero_based_rejects_duplicate_category_ids_across_budget_categories(
    db_session: Session,
) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-shared", name="Shared")
    _seed_budget(db_session, budget_id="budget-duplicate-category-id")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-shared-1",
        budget_id="budget-duplicate-category-id",
        category_id="cat-shared",
    )
    _seed_budget_category(
        db_session,
        budget_category_id="bc-shared-2",
        budget_id="budget-duplicate-category-id",
        category_id="cat-shared",
    )
    db_session.flush()

    with pytest.raises(
        ValueError,
        match="Duplicate category_id across budget_categories is not supported: cat-shared",
    ):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-duplicate-category-id",
                period_month="2026-03",
                available_cash="100.00",
                actor="budgeter",
                reason="duplicate category id mapping",
            ),
            db_session,
        )


def test_budget_compute_zero_based_rejects_non_zero_based_method(db_session: Session) -> None:
    _seed_budget(db_session, budget_id="budget-flex", method="flex")
    db_session.flush()
    with pytest.raises(ValueError, match="Budget method must be 'zero_based'"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-flex",
                period_month="2026-02",
                available_cash="100.00",
                actor="budgeter",
                reason="wrong method",
            ),
            db_session,
        )


def test_budget_compute_zero_based_rejects_unsupported_target_cadence(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-target", name="Target")
    _seed_budget(db_session, budget_id="budget-target")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-target",
        budget_id="budget-target",
        category_id="cat-target",
    )
    db_session.flush()

    with pytest.raises(ValueError, match=r"target_policies\[0\].cadence"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-target",
                period_month="2026-02",
                available_cash="1000.00",
                actor="budgeter",
                reason="invalid cadence",
                target_policies=[
                    BudgetTargetPolicyInput(
                        budget_category_id="bc-target",
                        cadence="weekly",
                        amount="50.00",
                    )
                ],
            ),
            db_session,
        )


def test_budget_compute_zero_based_rejects_duplicate_target_policy_budget_category_ids(
    db_session: Session,
) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-target-dup", name="Target Duplicate")
    _seed_budget(db_session, budget_id="budget-target-dup")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-target-dup",
        budget_id="budget-target-dup",
        category_id="cat-target-dup",
    )
    db_session.flush()

    with pytest.raises(ValueError, match=r"target_policies\[1\]\.budget_category_id is duplicated"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-target-dup",
                period_month="2026-02",
                available_cash="1000.00",
                actor="budgeter",
                reason="duplicate target policy category",
                target_policies=[
                    BudgetTargetPolicyInput(
                        budget_category_id="bc-target-dup",
                        target_type="scheduled",
                        amount="10.00",
                    ),
                    BudgetTargetPolicyInput(
                        budget_category_id="bc-target-dup",
                        target_type="scheduled",
                        amount="20.00",
                    ),
                ],
            ),
            db_session,
        )


def test_budget_compute_zero_based_refreshes_budget_period_in_same_session(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-refresh", name="Refresh")
    _seed_budget(db_session, budget_id="budget-refresh")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-refresh",
        budget_id="budget-refresh",
        category_id="cat-refresh",
    )
    db_session.flush()

    first = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-refresh",
            period_month="2026-03",
            available_cash="1000.00",
            actor="budgeter",
            reason="first run",
            category_allocations=[
                BudgetCategoryAllocationInput(
                    budget_category_id="bc-refresh",
                    assigned_amount="100.00",
                )
            ],
        ),
        db_session,
    )
    db_session.flush()

    loaded_period = db_session.get(BudgetPeriod, first.budget_period_id)
    assert loaded_period is not None
    assert loaded_period.to_assign == Decimal("900.00")

    second = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-refresh",
            period_month="2026-03",
            available_cash="800.00",
            actor="budgeter",
            reason="second run",
            category_allocations=[
                BudgetCategoryAllocationInput(
                    budget_category_id="bc-refresh",
                    assigned_amount="100.00",
                )
            ],
        ),
        db_session,
    )
    db_session.flush()

    assert second.to_assign == Decimal("700.00")
    assert loaded_period.to_assign == Decimal("700.00")


def test_budget_compute_zero_based_rejects_every_n_months_zero_interval(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-target-interval", name="Target Interval")
    _seed_budget(db_session, budget_id="budget-target-interval")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-target-interval",
        budget_id="budget-target-interval",
        category_id="cat-target-interval",
    )
    _seed_target(
        db_session,
        target_id="target-interval",
        budget_category_id="bc-target-interval",
        amount="50.00",
        cadence="every_n_months",
        metadata_json={"months_interval": 0, "anchor_month": "2026-01"},
    )
    db_session.flush()

    with pytest.raises(ValueError, match="every_n_months interval must be > 0"):
        budget_compute_zero_based(
            BudgetComputeZeroBasedRequest(
                budget_id="budget-target-interval",
                period_month="2026-02",
                available_cash="1000.00",
                actor="budgeter",
                reason="invalid zero interval",
            ),
            db_session,
        )
