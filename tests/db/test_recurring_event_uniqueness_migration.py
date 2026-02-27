from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_recurring_event_uniqueness_migration_dedupes_and_remaps_review_refs(tmp_path: Path) -> None:
    database_file = tmp_path / "tur45_recurring_event_unique.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "3c9f2e1b7a4d")

    engine_pre = sa.create_engine(database_url)
    try:
        with engine_pre.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO recurrings (
                        id,
                        merchant_id,
                        category_id,
                        schedule_type,
                        interval_n,
                        anchor_date,
                        tolerance_days,
                        active,
                        metadata_json
                    ) VALUES (
                        'rec-1',
                        NULL,
                        NULL,
                        'weekly',
                        1,
                        '2026-01-01',
                        1,
                        1,
                        '{}'
                    )
                    """
                )
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO recurring_events (
                        id,
                        recurring_id,
                        expected_date,
                        observed_transaction_id,
                        status
                    ) VALUES
                        ('re-1', 'rec-1', '2026-01-08', NULL, 'missed'),
                        ('re-2', 'rec-1', '2026-01-08', NULL, 'observed')
                    """
                )
            )
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
                    "id": "ri-1",
                    "ref_id": "re-1",
                    "created_at": datetime(2026, 2, 27, 4, 0, 0),
                },
            )
    finally:
        engine_pre.dispose()

    command.upgrade(config, "head")

    engine_post = sa.create_engine(database_url)
    try:
        with engine_post.begin() as connection:
            recurring_rows = connection.execute(
                sa.text(
                    """
                    SELECT id, status
                    FROM recurring_events
                    WHERE recurring_id = 'rec-1'
                      AND expected_date = '2026-01-08'
                    ORDER BY id ASC
                    """
                )
            ).all()
            remapped_ref_id = connection.execute(
                sa.text("SELECT ref_id FROM review_items WHERE id = 'ri-1'")
            ).scalar_one()

            assert recurring_rows == [("re-2", "observed")]
            assert remapped_ref_id == "re-2"

            with pytest.raises(sa.exc.IntegrityError):
                connection.execute(
                    sa.text(
                        """
                        INSERT INTO recurring_events (
                            id,
                            recurring_id,
                            expected_date,
                            observed_transaction_id,
                            status
                        ) VALUES (
                            're-3',
                            'rec-1',
                            '2026-01-08',
                            NULL,
                            'missed'
                        )
                        """
                    )
                )
    finally:
        engine_post.dispose()
