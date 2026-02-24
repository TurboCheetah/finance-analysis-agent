"""Read-side provenance and replay utilities for transaction event lineage."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Transaction, TransactionEvent
from finance_analysis_agent.provenance.transaction_events_service import EVENT_TYPE_PREFIX
from finance_analysis_agent.provenance.types import (
    TRACKED_TRANSACTION_FIELDS,
    FieldProvenance,
    ProvenanceSource,
    ReplayTransition,
    TransactionProvenanceResult,
    TransactionReplayResult,
    normalize_tracked_value,
)


def _ordered_transaction_events(transaction_id: str, session: Session) -> list[TransactionEvent]:
    return session.scalars(
        select(TransactionEvent)
        .where(TransactionEvent.transaction_id == transaction_id)
        .where(TransactionEvent.event_type.like(f"{EVENT_TYPE_PREFIX}%"))
        .order_by(TransactionEvent.created_at.asc(), TransactionEvent.id.asc())
    ).all()


def _parse_field(event_type: str) -> str | None:
    if not event_type.startswith(EVENT_TYPE_PREFIX):
        return None
    field = event_type.removeprefix(EVENT_TYPE_PREFIX)
    return field if field in TRACKED_TRANSACTION_FIELDS else None


def _parse_source(value: str | None) -> ProvenanceSource | None:
    if value is None:
        return None
    try:
        return ProvenanceSource(value)
    except ValueError:
        return None


def _current_values(transaction: Transaction) -> dict[str, Any]:
    return {
        field: normalize_tracked_value(field, getattr(transaction, field))
        for field in TRACKED_TRANSACTION_FIELDS
    }


def get_transaction_provenance(
    transaction_id: str,
    session: Session,
) -> TransactionProvenanceResult:
    """Return current tracked values and latest provenance source metadata by field."""

    transaction = session.get(Transaction, transaction_id)
    if transaction is None:
        raise ValueError(f"Transaction not found: {transaction_id}")

    latest_by_field: dict[str, FieldProvenance | None] = {
        field: None for field in TRACKED_TRANSACTION_FIELDS
    }
    for event in _ordered_transaction_events(transaction_id, session):
        field = _parse_field(event.event_type)
        source = _parse_source(event.provenance)
        if field is None or source is None:
            continue
        latest_by_field[field] = FieldProvenance(
            field=field,
            source=source,
            actor=event.actor,
            reason=event.reason,
            event_id=event.id,
            event_type=event.event_type,
            changed_at=event.created_at,
        )

    return TransactionProvenanceResult(
        transaction_id=transaction.id,
        current_values=_current_values(transaction),
        latest_by_field=latest_by_field,
    )


def replay_transaction_field_history(
    transaction_id: str,
    session: Session,
) -> TransactionReplayResult:
    """Reconstruct tracked field transitions from immutable transaction events."""

    transaction = session.get(Transaction, transaction_id)
    if transaction is None:
        raise ValueError(f"Transaction not found: {transaction_id}")

    state: dict[str, Any] = {}
    transitions: list[ReplayTransition] = []

    for event in _ordered_transaction_events(transaction_id, session):
        field = _parse_field(event.event_type)
        if field is None:
            continue

        old_value = (event.old_value_json or {}).get("value")
        new_value = (event.new_value_json or {}).get("value")
        if field not in state:
            state[field] = old_value
        state[field] = new_value

        transitions.append(
            ReplayTransition(
                event_id=event.id,
                field=field,
                old_value=old_value,
                new_value=new_value,
                source=_parse_source(event.provenance),
                actor=event.actor,
                reason=event.reason,
                changed_at=event.created_at,
                state_after=deepcopy(state),
            )
        )

    return TransactionReplayResult(
        transaction_id=transaction.id,
        transitions=transitions,
        final_state=state,
    )

