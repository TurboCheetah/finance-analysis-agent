from __future__ import annotations

import pytest


@pytest.fixture()
def db_filename() -> str:
    return "tur32_ingest.db"
