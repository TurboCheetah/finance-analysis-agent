from __future__ import annotations

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

    diff = first.diffs[0]
    assert diff.transaction_id == ids["txn_posted_a"]
    assert diff.before["category_id"] == "cat-food"
    assert diff.after["category_id"] == "cat-travel"
    assert diff.after["excluded"] is False
    assert diff.after["tags"] == ["commute", "existing", "latte"]
    assert diff.after["review_status"] == "needs_review"
