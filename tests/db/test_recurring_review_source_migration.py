from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_recurring_review_source_allowed_after_migration(tmp_path: Path) -> None:
    database_file = tmp_path / "tur45_review_source.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "7e3b4c2d1f90")

    engine_pre = sa.create_engine(database_url)
    try:
        with engine_pre.begin() as connection:
            with pytest.raises(sa.exc.IntegrityError):
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
                            source,
                            assigned_to,
                            payload_json,
                            created_at,
                            resolved_at
                        ) VALUES (
                            :id,
                            'recurring_missed_event',
                            'recurring_events',
                            :ref_id,
                            'recurring.missed_event',
                            NULL,
                            'to_review',
                            'recurring',
                            NULL,
                            '{}',
                            :created_at,
                            NULL
                        )
                        """
                    ),
                    {
                        "id": "ri-pre",
                        "ref_id": "re-1",
                        "created_at": datetime(2026, 2, 27, 0, 0, 0),
                    },
                )
    finally:
        engine_pre.dispose()

    command.upgrade(config, "head")

    engine_post = sa.create_engine(database_url)
    try:
        with engine_post.begin() as connection:
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
                        source,
                        assigned_to,
                        payload_json,
                        created_at,
                        resolved_at
                    ) VALUES (
                        :id,
                        'recurring_missed_event',
                        'recurring_events',
                        :ref_id,
                        'recurring.missed_event',
                        NULL,
                        'to_review',
                        'recurring',
                        NULL,
                        '{}',
                        :created_at,
                        NULL
                    )
                    """
                ),
                {
                    "id": "ri-post",
                    "ref_id": "re-2",
                    "created_at": datetime(2026, 2, 27, 0, 1, 0),
                },
            )
            inserted = connection.execute(
                sa.text("SELECT source FROM review_items WHERE id = 'ri-post'")
            ).scalar_one()
            dedupe_partial_index_sql = connection.execute(
                sa.text(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type = 'index' "
                    "AND name = 'ux_review_items_active_dedupe_candidate'"
                )
            ).scalar_one()
        assert inserted == "recurring"
        assert "AND item_type = 'dedupe_candidate_suggestion'" in dedupe_partial_index_sql
        assert "AND source = 'dedupe'" in dedupe_partial_index_sql
        assert "AND status IN ('to_review', 'in_progress')" in dedupe_partial_index_sql
    finally:
        engine_post.dispose()
