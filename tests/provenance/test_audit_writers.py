from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Rule, RuleAudit, RuleRun, RunMetadata, Transaction
from finance_analysis_agent.provenance.audit_writers import (
    finish_run_metadata,
    record_rule_audit,
    start_run_metadata,
)
from finance_analysis_agent.provenance.types import (
    RuleAuditWriteRequest,
    RunMetadataFinishRequest,
    RunMetadataStartRequest,
)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _seed_rule_run_and_transaction(session: Session) -> tuple[str, str]:
    session.add(Account(id="acct-audit", name="Audit", type="checking", currency="USD"))
    session.add(
        Transaction(
            id="txn-audit",
            account_id="acct-audit",
            posted_date=date(2026, 4, 1),
            effective_date=date(2026, 4, 1),
            amount=Decimal("77.00"),
            currency="USD",
            original_amount=Decimal("77.00"),
            original_currency="USD",
            pending_status="posted",
            original_statement="seed",
            merchant_id=None,
            category_id=None,
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id="audit-1",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
    )
    session.add(
        Rule(
            id="rule-1",
            name="Test Rule",
            priority=1,
            enabled=True,
            apply_to_pending=False,
            matcher_json={"contains": "coffee"},
            action_json={"set_category": "coffee"},
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
    )
    session.add(
        RuleRun(
            id="run-1",
            rule_id="rule-1",
            run_mode="manual",
            dry_run=False,
            started_at=_utcnow(),
            completed_at=None,
            summary_json=None,
        )
    )
    session.commit()
    return "run-1", "txn-audit"


def test_record_rule_audit_persists_expected_row(db_session: Session) -> None:
    run_id, transaction_id = _seed_rule_run_and_transaction(db_session)

    created = record_rule_audit(
        RuleAuditWriteRequest(
            rule_run_id=run_id,
            transaction_id=transaction_id,
            matched=True,
            changes_json={"category_id": {"old": None, "new": "coffee"}},
            confidence=0.98,
        ),
        db_session,
    )
    db_session.commit()

    persisted = db_session.get(RuleAudit, created.id)
    assert persisted is not None
    assert persisted.rule_run_id == run_id
    assert persisted.transaction_id == transaction_id
    assert persisted.matched is True
    assert persisted.confidence == 0.98
    assert persisted.changes_json is not None


def test_start_and_finish_run_metadata_lifecycle(db_session: Session) -> None:
    started = start_run_metadata(
        RunMetadataStartRequest(
            pipeline_name="rules_apply",
            code_version="abc123",
            schema_version="1.0.0",
            config_hash="cfg-hash",
            status="running",
            diagnostics_json={"phase": "start"},
        ),
        db_session,
    )
    db_session.commit()

    assert started.started_at is not None
    assert started.completed_at is None
    assert started.status == "running"

    finished = finish_run_metadata(
        RunMetadataFinishRequest(
            run_metadata_id=started.id,
            status="success",
            diagnostics_json={"phase": "done", "events": 3},
        ),
        db_session,
    )
    db_session.commit()

    persisted = db_session.get(RunMetadata, started.id)
    assert persisted is not None
    assert finished.id == started.id
    assert persisted.status == "success"
    assert persisted.completed_at is not None
    assert persisted.diagnostics_json == {"phase": "done", "events": 3}

