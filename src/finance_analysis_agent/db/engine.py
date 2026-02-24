"""Engine and session helpers for SQLite-first local deployments."""

from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

DEFAULT_DATABASE_URL = "sqlite:///finance_os.db"


@lru_cache(maxsize=None)
def _get_cached_engine(resolved_url: str) -> Engine:
    return create_engine(resolved_url)


def get_engine(database_url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine for the configured database URL."""

    resolved_url = database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    return _get_cached_engine(resolved_url)


def get_session_factory(database_url: str | None = None) -> sessionmaker:
    """Return a configured session factory for the resolved database URL."""

    return sessionmaker(bind=get_engine(database_url), autoflush=False)
