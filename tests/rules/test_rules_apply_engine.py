from __future__ import annotations

from sqlalchemy import func, select

from finance_analysis_agent.db.models import GoalEvent
from finance_analysis_agent.rules import RuleRunMode, RuleScope, RulesApplyRequest, apply_rules

from tests.rules.helpers import add_rule, seed_rules_baseline


def test_ordered_rules_last_wins_and_pending_default_skip(db_session) -> None:
    ids = seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-1",
        priority=1,
        matcher_json={"merchant": {"contains": "coffee"}},
        action_json={
            "set_category": "cat-coffee",
            "set_excluded": True,
            "add_tags": ["latte"],
            "set_review_status": "needs_review",
            "link_goal": "goal-1",
        },
    )
    add_rule(
        db_session,
        rule_id="rule-2",
        priority=2,
        matcher_json={"original_statement": {"contains": "COFFEE"}},
        action_json={
            "set_category": "cat-travel",
            "set_excluded": False,
            "add_tags": ["commute"],
        },
    )

    request = RulesApplyRequest(
        scope=RuleScope(),
        dry_run=True,
        run_mode=RuleRunMode.MANUAL,
        actor="tester",
        reason="rule preview",
    )

    first = apply_rules(request, db_session)
    second = apply_rules(request, db_session)

    assert first == second
    assert first.evaluated_rules == 2
    assert first.matched_rules == 2
    assert first.changed_transactions == 1
    assert first.rule_run_ids == []

    assert len(first.diffs) == 1
    diff = first.diffs[0]
    assert diff.transaction_id == ids["txn_posted_a"]
    assert diff.before["category_id"] == "cat-food"
    assert diff.after["category_id"] == "cat-travel"
    assert diff.after["excluded"] is False
    assert diff.after["tags"] == ["commute", "existing", "latte"]
    assert diff.after["review_status"] == "needs_review"


def test_in_list_matcher_values_are_trimmed_before_matching(db_session) -> None:
    ids = seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-trim-1",
        priority=1,
        matcher_json={"account": {"in": [" acct-1 "]}},
        action_json={"set_category": "cat-coffee"},
    )

    result = apply_rules(
        RulesApplyRequest(
            scope=RuleScope(),
            dry_run=True,
            run_mode=RuleRunMode.MANUAL,
            actor="tester",
            reason="trim-check",
        ),
        db_session,
    )

    assert result.changed_transactions == 1
    assert len(result.diffs) == 1
    assert result.diffs[0].transaction_id == ids["txn_posted_a"]


def test_link_goal_is_idempotent_across_repeated_runs(db_session) -> None:
    ids = seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-goal-1",
        priority=1,
        matcher_json={"account": {"in": ["acct-1"]}},
        action_json={"link_goal": "goal-1"},
    )

    request = RulesApplyRequest(
        scope=RuleScope(),
        dry_run=False,
        run_mode=RuleRunMode.MANUAL,
        actor="tester",
        reason="goal-link apply",
    )
    first = apply_rules(request, db_session)
    db_session.commit()
    second = apply_rules(request, db_session)
    db_session.commit()

    assert first.changed_transactions == 1
    assert second.changed_transactions == 0
    event_count = db_session.scalar(
        select(func.count()).select_from(GoalEvent).where(
            GoalEvent.related_transaction_id == ids["txn_posted_a"],
            GoalEvent.goal_id == ids["goal_1"],
            GoalEvent.event_type == "rule.linked_transaction",
        )
    )
    assert event_count == 1
