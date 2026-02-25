"""Small shared helpers for categorize services/providers."""

from __future__ import annotations


def normalize_scope_ids(scope_transaction_ids: list[str]) -> list[str]:
    """Return deterministic, deduplicated, non-empty transaction ids."""

    normalized = {transaction_id.strip() for transaction_id in scope_transaction_ids if transaction_id.strip()}
    return sorted(normalized)
