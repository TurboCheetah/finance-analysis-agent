from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _alembic_config(database_url: str) -> Config:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


@pytest.fixture()
def db_session(tmp_path: Path) -> Session:
    database_file = tmp_path / "tur34_pdf_contract.db"
    database_url = f"sqlite:///{database_file}"

    command.upgrade(_alembic_config(database_url), "head")

    engine = create_engine(database_url, future=True)
    session_factory = sessionmaker(bind=engine, autoflush=False, future=True)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()
