"""Shared UTC time helpers."""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return a UTC timestamp stored as naive datetime for SQLite compatibility."""

    return datetime.now(UTC).replace(tzinfo=None)

