from __future__ import annotations

import pytest

from finance_analysis_agent.rules import RuleRunMode, RuleScope, RulesApplyRequest, apply_rules
from tests.rules.helpers import add_rule, seed_rules_baseline


def test_validation_rejects_invalid_matcher_and_action_shapes(db_session) -> None:
    seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-bad-matcher",
        priority=1,
        matcher_json={"merchant": {"regex": "coffee"}},
        action_json={"set_category": "cat-coffee"},
    )
    with pytest.raises(ValueError, match="exactly one of 'exact' or 'contains'"):
        apply_rules(
            RulesApplyRequest(
                scope=RuleScope(),
                dry_run=True,
                run_mode=RuleRunMode.MANUAL,
                actor="tester",
                reason="preview",
            ),
            db_session,
        )


def test_validation_rejects_unknown_category_and_goal_ids(db_session) -> None:
    seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-bad-refs",
        priority=1,
        matcher_json={"account": {"in": ["acct-1"]}},
        action_json={"set_category": "cat-missing", "link_goal": "goal-missing"},
    )
    with pytest.raises(ValueError, match="Unknown category id"):
        apply_rules(
            RulesApplyRequest(
                scope=RuleScope(),
                dry_run=True,
                run_mode=RuleRunMode.MANUAL,
                actor="tester",
                reason="preview",
            ),
            db_session,
        )


def test_validation_rejects_missing_actor_reason_and_retroactive_dates(db_session) -> None:
    seed_rules_baseline(db_session)
    add_rule(
        db_session,
        rule_id="rule-valid",
        priority=1,
        matcher_json={"account": {"in": ["acct-1"]}},
        action_json={"set_category": "cat-coffee"},
    )

    with pytest.raises(ValueError, match="actor is required"):
        apply_rules(
            RulesApplyRequest(
                scope=RuleScope(),
                dry_run=True,
                run_mode=RuleRunMode.MANUAL,
                actor=" ",
                reason="x",
            ),
            db_session,
        )

    with pytest.raises(ValueError, match="reason is required"):
        apply_rules(
            RulesApplyRequest(
                scope=RuleScope(),
                dry_run=True,
                run_mode=RuleRunMode.MANUAL,
                actor="tester",
                reason=" ",
            ),
            db_session,
        )

    with pytest.raises(ValueError, match="requires date_from and date_to"):
        apply_rules(
            RulesApplyRequest(
                scope=RuleScope(),
                dry_run=True,
                run_mode=RuleRunMode.RETROACTIVE,
                actor="tester",
                reason="retro",
            ),
            db_session,
        )
