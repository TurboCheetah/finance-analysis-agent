from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from finance_analysis_agent.budget import (
    BudgetBucketPlanInput,
    BudgetCategoryAllocationInput,
    BudgetCategoryPlanInput,
    BudgetComputeFlexRequest,
    BudgetComputeZeroBasedRequest,
    budget_compute_flex,
    budget_compute_zero_based,
)
from finance_analysis_agent.categorize import CategorizeSuggestRequest, categorize_suggest
from finance_analysis_agent.db.models import Budget, BudgetCategory, BudgetTarget, ReviewItem, Transaction
from finance_analysis_agent.goals import GoalAllocationInput, GoalLedgerComputeRequest, goal_ledger_compute
from finance_analysis_agent.recurring import RecurringDetectRequest, recurring_detect_and_schedule
from finance_analysis_agent.reporting import ReportType, ReportingGenerateRequest, reporting_generate
from finance_analysis_agent.review_queue import BulkActionType, BulkTriageRequest, bulk_triage
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.rules import RuleRunMode, RuleScope, RulesApplyRequest, apply_rules
from finance_analysis_agent.utils.time import utcnow
from tests.e2e.helpers import persist_artifact, seed_merchant, seed_transaction, write_json_artifact
from tests.rules.helpers import add_rule, seed_rules_baseline

pytestmark = pytest.mark.e2e


def test_journey_budget_flex_goals_recurring(db_session, tmp_path: Path) -> None:
    ids = seed_rules_baseline(db_session)

    seed_transaction(
        db_session,
        transaction_id="txn-coffee-budget",
        account_id="acct-1",
        posted_date=date(2026, 2, 6),
        amount="-15.00",
        merchant_id="mer-coffee",
        category_id="cat-food",
        original_statement="COFFEE SHOP FEBRUARY",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-market-hist-1",
        account_id="acct-2",
        posted_date=date(2026, 1, 20),
        amount="-80.00",
        merchant_id="mer-market",
        category_id="cat-travel",
        original_statement="MARKETPLACE JAN",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-market-hist-2",
        account_id="acct-2",
        posted_date=date(2026, 2, 5),
        amount="-75.00",
        merchant_id="mer-market",
        category_id="cat-travel",
        original_statement="MARKETPLACE FEB",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-market-target",
        account_id="acct-2",
        posted_date=date(2026, 2, 10),
        amount="-90.00",
        merchant_id="mer-market",
        category_id=None,
        original_statement="MARKET TARGET",
    )
    seed_merchant(db_session, merchant_id="mer-gym", canonical_name="Gym Membership")
    seed_transaction(
        db_session,
        transaction_id="txn-gym-1",
        account_id="acct-1",
        posted_date=date(2026, 1, 1),
        amount="-25.00",
        merchant_id="mer-gym",
        original_statement="GYM MEMBERSHIP",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-gym-2",
        account_id="acct-1",
        posted_date=date(2026, 1, 8),
        amount="-25.00",
        merchant_id="mer-gym",
        original_statement="GYM MEMBERSHIP",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-gym-3",
        account_id="acct-1",
        posted_date=date(2026, 1, 15),
        amount="-25.00",
        merchant_id="mer-gym",
        original_statement="GYM MEMBERSHIP",
    )
    db_session.flush()

    add_rule(
        db_session,
        rule_id="rule-e2e-coffee",
        priority=1,
        matcher_json={"merchant": {"contains": "coffee"}},
        action_json={"set_category": "cat-coffee", "add_tags": ["latte"], "link_goal": "goal-1"},
    )

    dry_run = apply_rules(
        RulesApplyRequest(
            scope=RuleScope(date_from=date(2026, 1, 1), date_to=date(2026, 2, 28)),
            dry_run=True,
            run_mode=RuleRunMode.MANUAL,
            actor="e2e-rules",
            reason="preview coffee recategorization",
        ),
        db_session,
    )
    assert dry_run.changed_transactions == 2
    assert dry_run.rule_run_ids == []
    assert db_session.get(Transaction, ids["txn_posted_a"]).category_id == "cat-food"

    retroactive_apply = apply_rules(
        RulesApplyRequest(
            scope=RuleScope(date_from=date(2026, 1, 1), date_to=date(2026, 2, 28)),
            dry_run=False,
            run_mode=RuleRunMode.RETROACTIVE,
            actor="e2e-rules",
            reason="retroactive coffee cleanup",
        ),
        db_session,
    )
    db_session.flush()

    assert retroactive_apply.changed_transactions == 2
    assert retroactive_apply.rule_run_ids
    assert db_session.get(Transaction, ids["txn_posted_a"]).category_id == "cat-coffee"
    assert db_session.get(Transaction, "txn-coffee-budget").category_id == "cat-coffee"
    assert db_session.get(Transaction, ids["txn_pending_a"]).category_id == "cat-food"

    suggestion_result = categorize_suggest(
        CategorizeSuggestRequest(
            actor="e2e-categorize",
            reason="review market suggestion",
            confidence_threshold=0.8,
            scope_transaction_ids=["txn-market-target"],
        ),
        db_session,
    )
    db_session.flush()

    assert len(suggestion_result.suggestions) == 1
    suggestion = suggestion_result.suggestions[0]
    assert suggestion.transaction_id == "txn-market-target"
    assert suggestion.queued_review_item_id is not None

    approve_result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.APPROVE_SUGGESTION,
            review_item_ids=[suggestion.queued_review_item_id],
            actor="e2e-reviewer",
            reason="approve market category suggestion",
        ),
        db_session,
    )
    db_session.flush()

    assert approve_result.updated == 1
    assert db_session.get(Transaction, "txn-market-target").category_id == "cat-travel"

    db_session.add(
        Budget(
            id="budget-zero",
            name="Zero Budget",
            method="zero_based",
            base_currency="USD",
            active=True,
            created_at=utcnow(),
        )
    )
    db_session.add(
        Budget(
            id="budget-flex",
            name="Flex Budget",
            method="flex",
            base_currency="USD",
            active=True,
            created_at=utcnow(),
        )
    )
    db_session.add(
        BudgetCategory(id="bc-zero-coffee", budget_id="budget-zero", category_id="cat-coffee", policy_json=None)
    )
    db_session.add(
        BudgetCategory(id="bc-zero-travel", budget_id="budget-zero", category_id="cat-travel", policy_json=None)
    )
    db_session.add(
        BudgetCategory(
            id="bc-flex-coffee",
            budget_id="budget-flex",
            category_id="cat-coffee",
            policy_json=None,
            rollover_policy="carry_both",
        )
    )
    db_session.add(
        BudgetCategory(
            id="bc-flex-travel",
            budget_id="budget-flex",
            category_id="cat-travel",
            policy_json=None,
            rollover_policy="carry_positive",
        )
    )
    db_session.add(
        BudgetTarget(
            id="budget-target-coffee",
            budget_category_id="bc-zero-coffee",
            target_type="scheduled",
            amount=Decimal("25.00"),
            cadence="monthly",
            top_up=False,
            snoozed_until=None,
            metadata_json=None,
        )
    )
    db_session.add(
        BudgetTarget(
            id="budget-target-travel",
            budget_category_id="bc-zero-travel",
            target_type="scheduled",
            amount=Decimal("120.00"),
            cadence="monthly",
            top_up=False,
            snoozed_until=None,
            metadata_json=None,
        )
    )
    db_session.flush()

    january_flex_result = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-flex",
            period_month="2026-01",
            available_cash="180.00",
            actor="e2e-budget",
            reason="seed january rollover carry",
            status="closed",
            bucket_plans=[
                BudgetBucketPlanInput(bucket_key="fixed", planned_amount="100.00", rollover_policy="carry_positive"),
                BudgetBucketPlanInput(
                    bucket_key="non_monthly",
                    planned_amount="0.00",
                    rollover_policy="carry_both",
                ),
                BudgetBucketPlanInput(bucket_key="flex", planned_amount="30.00", rollover_policy="carry_both"),
            ],
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-flex-travel",
                    bucket_key="fixed",
                    planned_amount="100.00",
                    rollover_policy="carry_positive",
                ),
                BudgetCategoryPlanInput(
                    budget_category_id="bc-flex-coffee",
                    bucket_key="flex",
                    planned_amount="30.00",
                    rollover_policy="carry_both",
                ),
            ],
        ),
        db_session,
    )
    zero_based_result = budget_compute_zero_based(
        BudgetComputeZeroBasedRequest(
            budget_id="budget-zero",
            period_month="2026-02",
            available_cash="300.00",
            actor="e2e-budget",
            reason="monthly zero based",
            category_allocations=[
                BudgetCategoryAllocationInput(budget_category_id="bc-zero-coffee", assigned_amount="40.00"),
                BudgetCategoryAllocationInput(budget_category_id="bc-zero-travel", assigned_amount="120.00"),
            ],
        ),
        db_session,
    )
    flex_result = budget_compute_flex(
        BudgetComputeFlexRequest(
            budget_id="budget-flex",
            period_month="2026-02",
            available_cash="200.00",
            actor="e2e-budget",
            reason="monthly flex",
            bucket_plans=[
                BudgetBucketPlanInput(bucket_key="fixed", planned_amount="120.00", rollover_policy="carry_positive"),
                BudgetBucketPlanInput(
                    bucket_key="non_monthly",
                    planned_amount="20.00",
                    rollover_policy="carry_both",
                ),
                BudgetBucketPlanInput(bucket_key="flex", planned_amount="60.00", rollover_policy="carry_negative"),
            ],
            category_plans=[
                BudgetCategoryPlanInput(
                    budget_category_id="bc-flex-travel",
                    bucket_key="fixed",
                    planned_amount="120.00",
                    rollover_policy="carry_positive",
                ),
                BudgetCategoryPlanInput(
                    budget_category_id="bc-flex-coffee",
                    bucket_key="flex",
                    planned_amount="60.00",
                    rollover_policy="carry_both",
                ),
            ],
        ),
        db_session,
    )
    recurring_result = recurring_detect_and_schedule(
        RecurringDetectRequest(
            as_of_date=date(2026, 1, 29),
            actor="e2e-recurring",
            reason="weekly schedule refresh",
            lookback_days=90,
            minimum_occurrences=3,
            tolerance_days_default=1,
            create_review_items=True,
        ),
        db_session,
    )
    goal_result = goal_ledger_compute(
        GoalLedgerComputeRequest(
            period_month="2026-02",
            available_funds="500.00",
            actor="e2e-goals",
            reason="fund vacation goal",
            allocations=[GoalAllocationInput(goal_id="goal-1", account_id="acct-1", amount="200.00")],
        ),
        db_session,
    )
    report_result = reporting_generate(
        ReportingGenerateRequest(
            actor="e2e-reporting",
            reason="budget and goal closeout",
            period_month="2026-02",
            report_types=[ReportType.BUDGET_VS_ACTUAL, ReportType.GOAL_PROGRESS],
            budget_id="budget-zero",
        ),
        db_session,
    )
    db_session.flush()

    assert zero_based_result.assigned_total == 160
    assert zero_based_result.spent_total == 180
    assert zero_based_result.to_assign == 140
    assert {category.category_id: category.spent_amount for category in zero_based_result.categories} == {
        "cat-coffee": 15,
        "cat-travel": 165,
    }
    assert {category.category_id: category.overspent for category in zero_based_result.categories} == {
        "cat-coffee": 0,
        "cat-travel": 45,
    }
    assert january_flex_result.status == "closed"
    assert flex_result.fixed_planned == 120
    assert flex_result.spent_total == 180
    assert flex_result.rollover_total == 50
    assert flex_result.flex_available == 110
    assert {bucket.bucket_key: bucket.actual_amount for bucket in flex_result.buckets} == {
        "fixed": 165,
        "non_monthly": 0,
        "flex": 15,
    }
    assert {bucket.bucket_key: bucket.rollover_carry for bucket in flex_result.buckets} == {
        "fixed": 20,
        "non_monthly": 0,
        "flex": 30,
    }
    assert len(recurring_result.schedules) == 1
    assert len(recurring_result.warnings) == 2
    recurring_review_items = db_session.scalars(
        select(ReviewItem).where(
            ReviewItem.source == ReviewSource.RECURRING.value,
            ReviewItem.status.in_([ReviewItemStatus.TO_REVIEW.value, ReviewItemStatus.IN_PROGRESS.value]),
        )
    ).all()
    assert len(recurring_review_items) == 2
    assert goal_result.allocated_this_period_total == 200
    assert any(snapshot.goal_id == "goal-1" and snapshot.progress_amount >= 200 for snapshot in goal_result.goals)
    assert {report.report_type for report in report_result.reports} == {
        ReportType.BUDGET_VS_ACTUAL,
        ReportType.GOAL_PROGRESS,
    }

    goal_1_snapshot = next(snapshot for snapshot in goal_result.goals if snapshot.goal_id == "goal-1")
    summary_path = write_json_artifact(
        tmp_path / "journey-budget-flex-goals-recurring.json",
        {
            "rule_run_ids": retroactive_apply.rule_run_ids,
            "categorize_run_id": suggestion_result.run_metadata_id,
            "budget_period_id": zero_based_result.budget_period_id,
            "january_flex_period_id": january_flex_result.budget_period_id,
            "flex_period_id": flex_result.budget_period_id,
            "flex_rollover_total": str(flex_result.rollover_total),
            "recurring_warning_count": len(recurring_result.warnings),
            "goal_progress_amount": str(goal_1_snapshot.progress_amount),
            "report_types": sorted(report.report_type.value for report in report_result.reports),
        },
    )
    persist_artifact("journey-budget-flex-goals-recurring.json", summary_path)
