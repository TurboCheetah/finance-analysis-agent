"""Service-layer transaction mutation API with immutable event emission."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Transaction, TransactionEvent
from finance_analysis_agent.provenance.types import (
    TRACKED_TRANSACTION_FIELDS,
    ProvenanceSource,
    TransactionMutationRequest,
    TransactionMutationResult,
    normalize_tracked_value,
)

EVENT_TYPE_PREFIX = "transaction.field_updated."


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _coerce_field_value(field: str, value: Any) -> Any:
    if field in {"category_id", "merchant_id"}:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{field} must be a string or null")
        return value

    if field == "amount":
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("amount must be Decimal-compatible") from exc

    if field == "excluded":
        if not isinstance(value, bool):
            raise ValueError("excluded must be a boolean")
        return value

    raise ValueError(f"Unsupported mutation field: {field}")


def _parse_provenance(value: ProvenanceSource | str) -> ProvenanceSource:
    try:
        return value if isinstance(value, ProvenanceSource) else ProvenanceSource(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in ProvenanceSource)
        raise ValueError(f"Invalid provenance source. Expected one of: {allowed}") from exc


def _validated_changes(changes: dict[str, Any]) -> dict[str, Any]:
    unsupported = set(changes) - set(TRACKED_TRANSACTION_FIELDS)
    if unsupported:
        fields = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported mutation field(s): {fields}")
    return {field: _coerce_field_value(field, value) for field, value in changes.items()}


def mutate_transaction_fields(
    request: TransactionMutationRequest,
    session: Session,
) -> TransactionMutationResult:
    """Mutate tracked fields and emit append-only transaction events."""

    if not request.actor.strip():
        raise ValueError("actor is required")
    if not request.reason.strip():
        raise ValueError("reason is required")

    provenance = _parse_provenance(request.provenance)
    validated_changes = _validated_changes(request.changes)

    transaction = session.get(Transaction, request.transaction_id)
    if transaction is None:
        raise ValueError(f"Transaction not found: {request.transaction_id}")

    diffs: list[tuple[str, Any, Any]] = []
    for field, new_value in validated_changes.items():
        old_value = getattr(transaction, field)
        if old_value != new_value:
            diffs.append((field, old_value, new_value))

    if not diffs:
        return TransactionMutationResult(
            transaction_id=transaction.id,
            changed_fields=[],
            event_ids=[],
            noop=True,
        )

    event_ids: list[str] = []
    for field, old_value, new_value in diffs:
        setattr(transaction, field, new_value)

        event_id = str(uuid4())
        event_ids.append(event_id)
        session.add(
            TransactionEvent(
                id=event_id,
                transaction_id=transaction.id,
                event_type=f"{EVENT_TYPE_PREFIX}{field}",
                old_value_json={"field": field, "value": normalize_tracked_value(field, old_value)},
                new_value_json={"field": field, "value": normalize_tracked_value(field, new_value)},
                reason=request.reason,
                actor=request.actor,
                provenance=provenance.value,
                created_at=_utcnow(),
            )
        )

    transaction.updated_at = _utcnow()
    session.flush()

    return TransactionMutationResult(
        transaction_id=transaction.id,
        changed_fields=[field for field, _, _ in diffs],
        event_ids=event_ids,
        noop=False,
    )
