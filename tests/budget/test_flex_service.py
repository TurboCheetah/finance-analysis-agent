from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from finance_analysis_agent.budget import (
    BudgetBucketPlanInput,
    BudgetCategoryPlanInput,
    BudgetComputeFlexRequest,
    budget_compute_flex,
)
from finance_analysis_agent.db.models import (
    Account,
    Budget,
    BudgetAllocation,
    BudgetBucket,
    BudgetBucketCategoryMapping,
    BudgetBucketDefinition,
    BudgetCategory,
    BudgetPeriod,
    BudgetRollover,
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
    budget_id: str = "budget-flex",
    method: str = "flex",
    active: bool = True,
) -> None:
    session.add(
        Budget(
            id=budget_id,
            name="Household Flex",
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
            rollover_policy=None,
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


def _bucket_plans(
    *,
    fixed: str,
    non_monthly: str,
    flex: str,
    fixed_policy: str | None = None,
    non_monthly_policy: str | None = None,
    flex_policy: str | None = None,
) -> list[BudgetBucketPlanInput]:
    return [
        BudgetBucketPlanInput(
            bucket_key="fixed",
            planned_amount=fixed,
            rollover_policy=fixed_policy,
        ),
        BudgetBucketPlanInput(
            bucket_key="non_monthly",
            planned_amount=non_monthly,
            rollover_policy=non_monthly_policy,
        ),
        BudgetBucketPlanInput(
            bucket_key="flex",
            planned_amount=flex,
            rollover_policy=flex_policy,
        ),
    ]


def test_budget_compute_flex_persists_period_buckets_and_category_plans(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-rent", name="Rent")
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-main")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-rent",
        budget_id="budget-main",
        category_id="cat-rent",
    )
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-main",
        category_id="cat-food",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-rent-1",
        category_id="cat-rent",
        posted_date=date(2026, 2, 1),
        amount="-1200.00",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-food-1",
        category_id="cat-food",
        posted_date=date(2026, 2, 8),
        amount="-300.00",
    )
    db_session.flush()

    result = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-main",
            period_month="2026-02",
            available_cash="2500.00",
            actor="budgeter",
            reason="monthly flex plan",
            bucket_plans=_bucket_plans(
                fixed="1500.00",
                non_monthly="400.00",
                flex="600.00",
                fixed_policy="carry_positive",
                non_monthly_policy="carry_both",
                flex_policy="carry_negative",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-rent",
                    bucket_key="fixed",
                    planned_amount="1200.00",
                    rollover_policy="carry_positive",
                ),
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="600.00",
                    rollover_policy="carry_both",
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
    bucket_rows = db_session.scalars(
        select(BudgetBucket)
        .where(BudgetBucket.budget_id == "budget-main", BudgetBucket.period_month == "2026-02")
        .order_by(BudgetBucket.bucket_name.asc())
    ).all()
    definitions = db_session.scalars(
        select(BudgetBucketDefinition)
        .where(BudgetBucketDefinition.budget_id == "budget-main")
        .order_by(BudgetBucketDefinition.bucket_key.asc())
    ).all()

    assert result.fixed_planned == Decimal("1500.00")
    assert result.non_monthly_planned == Decimal("400.00")
    assert result.flex_planned == Decimal("600.00")
    assert result.assigned_total == Decimal("2500.00")
    assert result.spent_total == Decimal("1500.00")
    assert result.rollover_total == Decimal("0.00")
    assert result.flex_available == Decimal("600.00")
    assert period is not None
    assert period.to_assign == Decimal("600.00")
    assert period.rollover_total == Decimal("0.00")
    assert [allocation.budget_category_id for allocation in allocations] == ["bc-food", "bc-rent"]
    assert all(allocation.source == "budget_compute_flex" for allocation in allocations)
    assert len(bucket_rows) == 3
    assert len(definitions) == 3

    bucket_actual_by_key = {bucket.bucket_key: bucket.actual_amount for bucket in result.buckets}
    assert bucket_actual_by_key == {
        "fixed": Decimal("1200.00"),
        "non_monthly": Decimal("0.00"),
        "flex": Decimal("300.00"),
    }


def test_budget_compute_flex_is_idempotent_for_same_request(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-idem")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-idem",
        category_id="cat-food",
    )
    db_session.flush()

    request = BudgetComputeFlexRequest(
        budget_id="budget-idem",
        period_month="2026-03",
        available_cash="1000.00",
        actor="budgeter",
        reason="idempotent run",
        bucket_plans=_bucket_plans(
            fixed="500.00",
            non_monthly="100.00",
            flex="400.00",
        ),
        category_plans=[
            BudgetCategoryPlanInput(
                budget_category_id="bc-food",
                bucket_key="flex",
                planned_amount="400.00",
            ),
        ],
    )

    first = budget_compute_flex(request, db_session)
    db_session.flush()
    second = budget_compute_flex(request, db_session)
    db_session.flush()

    bucket_count = db_session.scalar(
        select(func.count()).select_from(BudgetBucket).where(
            BudgetBucket.budget_id == "budget-idem",
            BudgetBucket.period_month == "2026-03",
        )
    )
    allocation_count = db_session.scalar(
        select(func.count()).select_from(BudgetAllocation).where(
            BudgetAllocation.budget_period_id == first.budget_period_id
        )
    )

    assert first.budget_period_id == second.budget_period_id
    assert bucket_count == 3
    assert allocation_count == 1


def test_budget_compute_flex_rejects_non_flex_budget_method(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_budget(db_session, budget_id="budget-zb", method="zero_based")
    db_session.flush()

    with pytest.raises(ValueError, match="Budget method must be 'flex'"):
        budget_compute_flex(
            BudgetComputeFlexRequest(
                budget_id="budget-zb",
                period_month="2026-03",
                available_cash="500.00",
                actor="budgeter",
                reason="invalid method",
                bucket_plans=_bucket_plans(fixed="100.00", non_monthly="50.00", flex="350.00"),
            ),
            db_session,
        )


def test_budget_compute_flex_rejects_invalid_bucket_key(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_budget(db_session, budget_id="budget-invalid")
    db_session.flush()

    with pytest.raises(ValueError, match=r"bucket_plans\[2\]\.bucket_key must be one of"):
        budget_compute_flex(
            BudgetComputeFlexRequest(
                budget_id="budget-invalid",
                period_month="2026-03",
                available_cash="1000.00",
                actor="budgeter",
                reason="invalid bucket",
                bucket_plans=[
                    BudgetBucketPlanInput(bucket_key="fixed", planned_amount="200.00"),
                    BudgetBucketPlanInput(bucket_key="non_monthly", planned_amount="100.00"),
                    BudgetBucketPlanInput(bucket_key="other", planned_amount="700.00"),
                ],
            ),
            db_session,
        )


def test_budget_compute_flex_rejects_duplicate_bucket_keys(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_budget(db_session, budget_id="budget-dup")
    db_session.flush()

    with pytest.raises(ValueError, match="Duplicate bucket_key in bucket_plans: fixed"):
        budget_compute_flex(
            BudgetComputeFlexRequest(
                budget_id="budget-dup",
                period_month="2026-03",
                available_cash="1000.00",
                actor="budgeter",
                reason="duplicate bucket",
                bucket_plans=[
                    BudgetBucketPlanInput(bucket_key="fixed", planned_amount="200.00"),
                    BudgetBucketPlanInput(bucket_key="fixed", planned_amount="200.00"),
                    BudgetBucketPlanInput(bucket_key="non_monthly", planned_amount="100.00"),
                    BudgetBucketPlanInput(bucket_key="flex", planned_amount="500.00"),
                ],
            ),
            db_session,
        )


def test_budget_compute_flex_applies_rollover_policies_across_closed_months(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-rent", name="Rent")
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-roll")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-rent",
        budget_id="budget-roll",
        category_id="cat-rent",
    )
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-roll",
        category_id="cat-food",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-jan-rent",
        category_id="cat-rent",
        posted_date=date(2026, 1, 2),
        amount="-1200.00",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-jan-food",
        category_id="cat-food",
        posted_date=date(2026, 1, 12),
        amount="-400.00",
    )
    db_session.flush()

    january = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-roll",
            period_month="2026-01",
            available_cash="2500.00",
            actor="budgeter",
            reason="jan plan",
            status="closed",
            bucket_plans=_bucket_plans(
                fixed="1300.00",
                non_monthly="200.00",
                flex="500.00",
                fixed_policy="carry_positive",
                non_monthly_policy="carry_both",
                flex_policy="carry_negative",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-rent",
                    bucket_key="fixed",
                    planned_amount="1300.00",
                    rollover_policy="carry_positive",
                ),
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="300.00",
                    rollover_policy="carry_negative",
                ),
            ],
        ),
        db_session,
    )
    assert january.rollover_total == Decimal("0.00")

    _seed_transaction(
        db_session,
        transaction_id="txn-feb-rent",
        category_id="cat-rent",
        posted_date=date(2026, 2, 3),
        amount="-1200.00",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-feb-food",
        category_id="cat-food",
        posted_date=date(2026, 2, 8),
        amount="-250.00",
    )
    db_session.flush()

    february = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-roll",
            period_month="2026-02",
            available_cash="2500.00",
            actor="budgeter",
            reason="feb plan",
            bucket_plans=_bucket_plans(
                fixed="1300.00",
                non_monthly="200.00",
                flex="500.00",
                fixed_policy="carry_positive",
                non_monthly_policy="carry_both",
                flex_policy="carry_negative",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-rent",
                    bucket_key="fixed",
                    planned_amount="1300.00",
                    rollover_policy="carry_positive",
                ),
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="300.00",
                    rollover_policy="carry_negative",
                ),
            ],
        ),
        db_session,
    )
    db_session.flush()

    rollovers = db_session.scalars(
        select(BudgetRollover)
        .where(
            BudgetRollover.budget_id == "budget-roll",
            BudgetRollover.from_period == "2026-01",
            BudgetRollover.to_period == "2026-02",
        )
        .order_by(BudgetRollover.dimension_type.asc(), BudgetRollover.dimension_id.asc())
    ).all()

    assert february.rollover_total == Decimal("300.00")
    assert february.flex_available == Decimal("1300.00")
    assert len(rollovers) == 4
    assert sorted(Decimal(str(row.carry_amount)) for row in rollovers) == [
        Decimal("-100.00"),
        Decimal("100.00"),
        Decimal("100.00"),
        Decimal("200.00"),
    ]


def test_budget_compute_flex_does_not_carry_from_open_previous_period(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-open")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-open",
        category_id="cat-food",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-jan-food-open",
        category_id="cat-food",
        posted_date=date(2026, 1, 10),
        amount="-250.00",
    )
    db_session.flush()

    budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-open",
            period_month="2026-01",
            available_cash="600.00",
            actor="budgeter",
            reason="jan open",
            status="open",
            bucket_plans=_bucket_plans(
                fixed="200.00",
                non_monthly="100.00",
                flex="300.00",
                fixed_policy="carry_both",
                non_monthly_policy="carry_both",
                flex_policy="carry_both",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="200.00",
                    rollover_policy="carry_both",
                ),
            ],
        ),
        db_session,
    )
    db_session.flush()

    february = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-open",
            period_month="2026-02",
            available_cash="600.00",
            actor="budgeter",
            reason="feb open carry check",
            bucket_plans=_bucket_plans(
                fixed="200.00",
                non_monthly="100.00",
                flex="300.00",
                fixed_policy="carry_both",
                non_monthly_policy="carry_both",
                flex_policy="carry_both",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="200.00",
                ),
            ],
        ),
        db_session,
    )
    db_session.flush()

    rollovers = db_session.scalars(
        select(BudgetRollover)
        .where(
            BudgetRollover.budget_id == "budget-open",
            BudgetRollover.from_period == "2026-01",
            BudgetRollover.to_period == "2026-02",
        )
        .order_by(BudgetRollover.id.asc())
    ).all()

    assert february.rollover_total == Decimal("0.00")
    assert rollovers == []


def test_budget_compute_flex_uses_outflow_only_actuals(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-actuals")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-actuals",
        category_id="cat-food",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-food-expense",
        category_id="cat-food",
        posted_date=date(2026, 2, 4),
        amount="-100.00",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-food-refund",
        category_id="cat-food",
        posted_date=date(2026, 2, 20),
        amount="30.00",
    )
    db_session.flush()

    result = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-actuals",
            period_month="2026-02",
            available_cash="500.00",
            actor="budgeter",
            reason="outflow-only test",
            bucket_plans=_bucket_plans(
                fixed="100.00",
                non_monthly="100.00",
                flex="300.00",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="300.00",
                ),
            ],
        ),
        db_session,
    )

    actual_by_bucket = {bucket.bucket_key: bucket.actual_amount for bucket in result.buckets}
    assert result.spent_total == Decimal("100.00")
    assert actual_by_bucket["flex"] == Decimal("100.00")


def test_budget_compute_flex_rollover_total_uses_bucket_level_carry_once(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-rent", name="Rent")
    _seed_budget(db_session, budget_id="budget-carry-once")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-rent",
        budget_id="budget-carry-once",
        category_id="cat-rent",
    )
    _seed_transaction(
        db_session,
        transaction_id="txn-jan-rent-carry",
        category_id="cat-rent",
        posted_date=date(2026, 1, 5),
        amount="-50.00",
    )
    db_session.flush()

    budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-carry-once",
            period_month="2026-01",
            available_cash="200.00",
            actor="budgeter",
            reason="jan close for carry",
            status="closed",
            bucket_plans=_bucket_plans(
                fixed="100.00",
                non_monthly="0.00",
                flex="0.00",
                fixed_policy="carry_both",
                non_monthly_policy="none",
                flex_policy="none",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-rent",
                    bucket_key="fixed",
                    planned_amount="100.00",
                    rollover_policy="carry_both",
                ),
            ],
        ),
        db_session,
    )
    db_session.flush()

    february = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-carry-once",
            period_month="2026-02",
            available_cash="200.00",
            actor="budgeter",
            reason="feb carry",
            bucket_plans=_bucket_plans(
                fixed="100.00",
                non_monthly="0.00",
                flex="0.00",
                fixed_policy="carry_both",
                non_monthly_policy="none",
                flex_policy="none",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-rent",
                    bucket_key="fixed",
                    planned_amount="100.00",
                    rollover_policy="carry_both",
                ),
            ],
        ),
        db_session,
    )

    assert february.rollover_total == Decimal("50.00")


def test_budget_compute_flex_rejects_unmapped_budget_categories(db_session: Session) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-rent", name="Rent")
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-unmapped")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-rent",
        budget_id="budget-unmapped",
        category_id="cat-rent",
    )
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-unmapped",
        category_id="cat-food",
    )
    db_session.flush()

    with pytest.raises(ValueError, match="Missing bucket mapping for budget_category_id\\(s\\): bc-food"):
        budget_compute_flex(
            BudgetComputeFlexRequest(
                budget_id="budget-unmapped",
                period_month="2026-02",
                available_cash="2000.00",
                actor="budgeter",
                reason="missing category mapping",
                bucket_plans=_bucket_plans(
                    fixed="1200.00",
                    non_monthly="300.00",
                    flex="500.00",
                ),
                category_plans=[
                    BudgetCategoryPlanInput(
                        budget_category_id="bc-rent",
                        bucket_key="fixed",
                        planned_amount="1200.00",
                    ),
                ],
            ),
            db_session,
        )


def test_budget_compute_flex_cleans_stale_category_mapping_when_plan_omits_category(
    db_session: Session,
) -> None:
    _seed_account(db_session)
    _seed_category(db_session, category_id="cat-rent", name="Rent")
    _seed_category(db_session, category_id="cat-food", name="Food")
    _seed_budget(db_session, budget_id="budget-stale-map")
    _seed_budget_category(
        db_session,
        budget_category_id="bc-rent",
        budget_id="budget-stale-map",
        category_id="cat-rent",
    )
    _seed_budget_category(
        db_session,
        budget_category_id="bc-food",
        budget_id="budget-stale-map",
        category_id="cat-food",
    )
    db_session.flush()

    budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-stale-map",
            period_month="2026-02",
            available_cash="2000.00",
            actor="budgeter",
            reason="seed mappings",
            bucket_plans=_bucket_plans(
                fixed="1200.00",
                non_monthly="300.00",
                flex="500.00",
            ),
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-rent",
                    bucket_key="fixed",
                    planned_amount="1200.00",
                ),
                BudgetCategoryPlanInput(
                    budget_category_id="bc-food",
                    bucket_key="flex",
                    planned_amount="500.00",
                ),
            ],
        ),
        db_session,
    )
    db_session.flush()

    with pytest.raises(ValueError, match="Missing bucket mapping for budget_category_id\\(s\\): bc-food"):
        budget_compute_flex(
            BudgetComputeFlexRequest(
                budget_id="budget-stale-map",
                period_month="2026-03",
                available_cash="2000.00",
                actor="budgeter",
                reason="omit one mapping",
                bucket_plans=_bucket_plans(
                    fixed="1200.00",
                    non_monthly="300.00",
                    flex="500.00",
                ),
                category_plans=[
                    BudgetCategoryPlanInput(
                        budget_category_id="bc-rent",
                        bucket_key="fixed",
                        planned_amount="1200.00",
                    ),
                ],
            ),
            db_session,
        )
    db_session.flush()

    mapping_rows = db_session.scalars(
        select(BudgetBucketCategoryMapping)
        .where(BudgetBucketCategoryMapping.budget_category_id.in_(["bc-rent", "bc-food"]))
        .order_by(BudgetBucketCategoryMapping.budget_category_id.asc())
    ).all()
    assert [row.budget_category_id for row in mapping_rows] == ["bc-rent"]
