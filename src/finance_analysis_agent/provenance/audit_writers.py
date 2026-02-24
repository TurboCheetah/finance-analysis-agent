"""Generic write helpers for rule audit and run metadata surfaces."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import RuleAudit, RunMetadata
from finance_analysis_agent.provenance.types import (
    RuleAuditWriteRequest,
    RunMetadataFinishRequest,
    RunMetadataStartRequest,
)
from finance_analysis_agent.utils.time import utcnow


def record_rule_audit(request: RuleAuditWriteRequest, session: Session) -> RuleAudit:
    """Persist a rule audit row for future rules-engine integration."""

    rule_audit = RuleAudit(
        id=str(uuid4()),
        rule_run_id=request.rule_run_id,
        transaction_id=request.transaction_id,
        matched=request.matched,
        changes_json=request.changes_json,
        confidence=request.confidence,
    )
    session.add(rule_audit)
    session.flush()
    return rule_audit


def start_run_metadata(request: RunMetadataStartRequest, session: Session) -> RunMetadata:
    """Create a run metadata row with start timestamp."""

    run_metadata = RunMetadata(
        id=str(uuid4()),
        pipeline_name=request.pipeline_name,
        code_version=request.code_version,
        schema_version=request.schema_version,
        config_hash=request.config_hash,
        started_at=utcnow(),
        status=request.status,
        diagnostics_json=request.diagnostics_json,
    )
    session.add(run_metadata)
    session.flush()
    return run_metadata


def finish_run_metadata(request: RunMetadataFinishRequest, session: Session) -> RunMetadata:
    """Complete a run metadata row with terminal status and diagnostics."""

    run_metadata = session.get(RunMetadata, request.run_metadata_id)
    if run_metadata is None:
        raise ValueError(f"RunMetadata not found: {request.run_metadata_id}")

    run_metadata.status = request.status
    run_metadata.completed_at = utcnow()
    run_metadata.diagnostics_json = request.diagnostics_json
    session.flush()
    return run_metadata
