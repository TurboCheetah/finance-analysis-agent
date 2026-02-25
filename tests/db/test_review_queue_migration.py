from __future__ import annotations

from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_review_queue_migration_normalizes_status_and_backfills_source(tmp_path: Path) -> None:
    database_file = tmp_path / "tur38_migration.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "9d2c4a7f3b11")

    engine = sa.create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO review_items (
                        id,
                        item_type,
                        ref_table,
                        ref_id,
                        reason_code,
                        confidence,
                        status,
                        assigned_to,
                        payload_json,
                        created_at,
                        resolved_at
                    ) VALUES (
                        :id,
                        'transaction_rule',
                        :ref_table,
                        :ref_id,
                        :reason_code,
                        NULL,
                        :status,
                        NULL,
                        '{}',
                        :created_at,
                        NULL
                    )
                    """
                ),
                [
                    {
                        "id": "ri-rules-open",
                        "ref_table": "transactions",
                        "ref_id": "txn-1",
                        "reason_code": "rule.needs_review",
                        "status": "open",
                        "created_at": datetime(2026, 2, 25, 1, 0, 0),
                    },
                    {
                        "id": "ri-pdf-legacy",
                        "ref_table": "run_metadata",
                        "ref_id": "run-1",
                        "reason_code": "low_confidence_row",
                        "status": "legacy_state",
                        "created_at": datetime(2026, 2, 25, 1, 1, 0),
                    },
                    {
                        "id": "ri-unmapped",
                        "ref_table": "legacy_table",
                        "ref_id": "legacy-1",
                        "reason_code": "legacy_reason",
                        "status": "open",
                        "created_at": datetime(2026, 2, 25, 1, 2, 0),
                    },
                ],
            )
    finally:
        engine.dispose()

    command.upgrade(config, "head")

    upgraded_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(upgraded_engine)
        assert "review_item_events" in inspector.get_table_names()

        with upgraded_engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    "SELECT id, status, source FROM review_items ORDER BY id"
                )
            ).all()

        assert rows == [
            ("ri-pdf-legacy", "to_review", "pdf_extract"),
            ("ri-rules-open", "to_review", "rules"),
            ("ri-unmapped", "to_review", "unknown"),
        ]
    finally:
        upgraded_engine.dispose()
