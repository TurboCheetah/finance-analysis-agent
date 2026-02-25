from __future__ import annotations

from sqlalchemy import func, select

from finance_analysis_agent.db.models import (
    GoalEvent,
    ReviewItem,
    RuleAudit,
    RuleRun,
    Transaction,
    TransactionEvent,
    TransactionTag,
)
from finance_analysis_agent.rules import RuleRunMode, RuleScope, RulesApplyRequest, apply_rules
from tests.rules.helpers import add_rule, seed_rules_baseline


def _table_count(session, model) -> int:
    return int(session.scalar(select(func.count()).select_from(model)) or 0)


def test_dry_run_produces_diffs_with_zero_writes(db_session) -> None:
    ids = seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-dry-1",
        priority=1,
        matcher_json={"merchant": {"exact": "Coffee Shop"}},
        action_json={
            "set_category": "cat-coffee",
            "add_tags": ["preview-tag"],
            "set_review_status": "needs_review",
            "link_goal": "goal-1",
        },
    )

    before = {
        "rule_runs": _table_count(db_session, RuleRun),
        "rule_audits": _table_count(db_session, RuleAudit),
        "events": _table_count(db_session, TransactionEvent),
        "txn_tags": _table_count(db_session, TransactionTag),
        "reviews": _table_count(db_session, ReviewItem),
        "goal_events": _table_count(db_session, GoalEvent),
    }

    result = apply_rules(
        RulesApplyRequest(
            scope=RuleScope(),
            dry_run=True,
            run_mode=RuleRunMode.MANUAL,
            actor="tester",
            reason="preview",
        ),
        db_session,
    )

    after = {
        "rule_runs": _table_count(db_session, RuleRun),
        "rule_audits": _table_count(db_session, RuleAudit),
        "events": _table_count(db_session, TransactionEvent),
        "txn_tags": _table_count(db_session, TransactionTag),
        "reviews": _table_count(db_session, ReviewItem),
        "goal_events": _table_count(db_session, GoalEvent),
    }

    transaction = db_session.get(Transaction, ids["txn_posted_a"])
    assert transaction is not None
    assert transaction.category_id == "cat-food"
    assert result.dry_run is True
    assert result.diffs
    assert before == after
