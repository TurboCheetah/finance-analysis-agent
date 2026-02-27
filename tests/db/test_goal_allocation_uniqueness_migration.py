from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_goal_allocation_unique_constraint_added_and_dedupes_existing_rows(tmp_path: Path) -> None:
    """
    Test that the migration adds a unique constraint to goal_allocations, deduplicates existing duplicate rows for the same (goal_id, account_id, period_month, allocation_type), and prevents future duplicate inserts.
    
    This test:
    - Sets up a temporary SQLite database.
    - Applies a baseline migration, seeds a goal, account, and two duplicate goal_allocations rows sharing the same composite key.
    - Applies the head migration which must remove duplicates and add the unique constraint.
    - Verifies exactly one allocation row remains for the composite key.
    - Verifies inserting another duplicate allocation now raises an IntegrityError.
    
    Parameters:
        tmp_path (Path): pytest-provided temporary directory for creating the test SQLite database file.
    """
    database_file = tmp_path / "tur45_goal_alloc_unique.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "a4c9e2f1b7d8")

    engine_pre = sa.create_engine(database_url)
    try:
        with engine_pre.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO accounts (id, name, type, currency) VALUES
                        ('acct-1', 'Checking', 'checking', 'USD')
                    """
                )
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO goals (
                        id,
                        name,
                        target_amount,
                        target_date,
                        monthly_contribution,
                        spending_reduces_progress,
                        status,
                        metadata_json
                    ) VALUES (
                        'goal-1',
                        'Vacation',
                        1000.00,
                        NULL,
                        NULL,
                        0,
                        'active',
                        NULL
                    )
                    """
                )
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO goal_allocations (
                        id,
                        goal_id,
                        account_id,
                        period_month,
                        amount,
                        allocation_type,
                        created_at
                    ) VALUES
                        ('ga-1', 'goal-1', 'acct-1', '2026-02', 50.00, 'manual', :created_at),
                        ('ga-2', 'goal-1', 'acct-1', '2026-02', 60.00, 'manual', :created_at)
                    """
                ),
                {"created_at": datetime(2026, 2, 27, 1, 0, 0)},
            )
    finally:
        engine_pre.dispose()

    command.upgrade(config, "head")

    engine_post = sa.create_engine(database_url)
    try:
        with engine_post.begin() as connection:
            allocation_rows = connection.execute(
                sa.text(
                    """
                    SELECT id, amount FROM goal_allocations
                    WHERE goal_id = 'goal-1'
                      AND account_id = 'acct-1'
                      AND period_month = '2026-02'
                      AND allocation_type = 'manual'
                    ORDER BY id ASC
                    """
                )
            ).all()

            # One duplicate should have been removed before unique constraint creation.
            assert len(allocation_rows) == 1

            with pytest.raises(sa.exc.IntegrityError):
                connection.execute(
                    sa.text(
                        """
                        INSERT INTO goal_allocations (
                            id,
                            goal_id,
                            account_id,
                            period_month,
                            amount,
                            allocation_type,
                            created_at
                        ) VALUES (
                            'ga-3',
                            'goal-1',
                            'acct-1',
                            '2026-02',
                            75.00,
                            'manual',
                            :created_at
                        )
                        """
                    ),
                    {"created_at": datetime(2026, 2, 27, 1, 1, 0)},
                )
    finally:
        engine_post.dispose()
