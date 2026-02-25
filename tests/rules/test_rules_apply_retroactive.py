from __future__ import annotations

from datetime import date

from sqlalchemy import func, select

from finance_analysis_agent.db.models import RuleAudit, RuleRun, Transaction, TransactionEvent
from finance_analysis_agent.rules import RuleRunMode, RuleScope, RulesApplyRequest, apply_rules
from tests.rules.helpers import add_rule, add_open_rule_review, seed_rules_baseline


def test_retroactive_window_updates_only_in_range_with_audits(db_session) -> None:
    ids = seed_rules_baseline(db_session)
    add_open_rule_review(db_session, transaction_id=ids["txn_posted_a"])
    add_rule(
        db_session,
        rule_id="rule-retro-1",
        priority=1,
        matcher_json={"amount": {"gt": "1.00"}},
        action_json={
            "set_excluded": True,
            "set_review_status": "reviewed",
            "link_goal": "goal-1",
        },
    )

    result = apply_rules(
        RulesApplyRequest(
            scope=RuleScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 31)),
            dry_run=False,
            run_mode=RuleRunMode.RETROACTIVE,
            actor="tester",
            reason="retro apply",
        ),
        db_session,
    )
    db_session.commit()

    jan_posted = db_session.get(Transaction, ids["txn_posted_a"])
    jan_pending = db_session.get(Transaction, ids["txn_pending_a"])
    feb_posted = db_session.get(Transaction, ids["txn_posted_b"])
    assert jan_posted is not None and jan_posted.excluded is True
    assert jan_pending is not None and jan_pending.excluded is False
    assert feb_posted is not None and feb_posted.excluded is False

    assert result.rule_run_ids
    assert result.changed_transactions == 1
    assert result.evaluated_rules == 1
    assert db_session.scalar(select(func.count()).select_from(RuleRun)) == 1
    assert db_session.scalar(select(func.count()).select_from(RuleAudit)) == 2
    assert db_session.scalar(select(func.count()).select_from(TransactionEvent)) == 1

    run = db_session.get(RuleRun, result.rule_run_ids[0])
    assert run is not None
    assert run.summary_json is not None
    assert run.summary_json["scope"]["date_from"] == "2026-01-01"
    assert run.summary_json["scope"]["date_to"] == "2026-01-31"
