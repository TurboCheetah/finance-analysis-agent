from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_recurrings_active_key_shape_migration_deactivates_invalid_rows_and_enforces_check(tmp_path: Path) -> None:
    database_file = tmp_path / "tur45_recurrings_active_key_shape.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "1d4a7b9c2e6f")

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
                    ) VALUES
                        ('rec-invalid-both-null', NULL, NULL, 'weekly', 1, '2026-01-01', 1, 1, '{}'),
                        ('rec-invalid-both-set', 'mer-1', 'cat-1', 'weekly', 1, '2026-01-01', 1, 1, '{}'),
                        ('rec-valid-merchant', 'mer-2', NULL, 'weekly', 1, '2026-01-01', 1, 1, '{}')
                    """
                )
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
                    SELECT id, active
                    FROM recurrings
                    WHERE id IN ('rec-invalid-both-null', 'rec-invalid-both-set', 'rec-valid-merchant')
                    ORDER BY id ASC
                    """
                )
            ).all()
            assert rows == [
                ("rec-invalid-both-null", 0),
                ("rec-invalid-both-set", 0),
                ("rec-valid-merchant", 1),
            ]

            with pytest.raises(sa.exc.IntegrityError):
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
                            'rec-new-invalid',
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
    finally:
        engine_post.dispose()
