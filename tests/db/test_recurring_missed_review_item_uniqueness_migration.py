from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_recurring_missed_review_item_uniqueness_migration_dedupes_active_rows(tmp_path: Path) -> None:
    database_file = tmp_path / "tur45_recurring_missed_review_unique.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "6a7b3c1d9e2f")

    engine_pre = sa.create_engine(database_url)
    try:
        with engine_pre.begin() as connection:
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
                    ) VALUES
                        (
                            'ri-1',
                            'recurring_missed_event',
                            'recurring_events',
                            're-1',
                            'recurring.missed_event',
                            NULL,
                            'to_review',
                            'recurring',
                            NULL,
                            '{}',
                            :created_at_1,
                            NULL
                        ),
                        (
                            'ri-2',
                            'recurring_missed_event',
                            'recurring_events',
                            're-1',
                            'recurring.missed_event',
                            NULL,
                            'in_progress',
                            'recurring',
                            NULL,
                            '{}',
                            :created_at_2,
                            NULL
                        )
                    """
                ),
                {
                    "created_at_1": datetime(2026, 2, 27, 12, 0, 0),
                    "created_at_2": datetime(2026, 2, 27, 12, 1, 0),
                },
            )
    finally:
        engine_pre.dispose()

    command.upgrade(config, "head")

    engine_post = sa.create_engine(database_url)
    try:
        with engine_post.begin() as connection:
            rows = connection.execute(
                sa.text(
                    """
                    SELECT id, status, resolved_at
                    FROM review_items
                    WHERE ref_table = 'recurring_events'
                      AND ref_id = 're-1'
                      AND item_type = 'recurring_missed_event'
                      AND source = 'recurring'
                    ORDER BY id ASC
                    """
                )
            ).all()
            active_count = connection.execute(
                sa.text(
                    """
                    SELECT COUNT(*)
                    FROM review_items
                    WHERE ref_table = 'recurring_events'
                      AND ref_id = 're-1'
                      AND item_type = 'recurring_missed_event'
                      AND source = 'recurring'
                      AND status IN ('to_review', 'in_progress')
                    """
                )
            ).scalar_one()

            assert rows[0][1] == "resolved"
            assert rows[0][2] is not None
            assert rows[0][2] != datetime(2026, 2, 27, 12, 0, 0)
            assert rows[1][1] == "in_progress"
            assert active_count == 1

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
                            'ri-3',
                            'recurring_missed_event',
                            'recurring_events',
                            're-1',
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
                    {"created_at": datetime(2026, 2, 27, 12, 2, 0)},
                )
    finally:
        engine_post.dispose()
