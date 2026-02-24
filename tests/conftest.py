from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from tests.helpers import alembic_config


@pytest.fixture()
def db_filename() -> str:
    return "test.db"


@pytest.fixture()
def db_session(tmp_path: Path, db_filename: str) -> Iterator[Session]:
    database_file = tmp_path / db_filename
    database_url = f"sqlite:///{database_file}"

    command.upgrade(alembic_config(database_url), "head")

    engine = create_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()

