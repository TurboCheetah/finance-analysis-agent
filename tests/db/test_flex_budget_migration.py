from __future__ import annotations

from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_flex_budget_migration_adds_normalized_tables_and_backfills_buckets(tmp_path: Path) -> None:
    database_file = tmp_path / "tur44_flex_budget_migration.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "2f6c8a9d1e4b")

    engine = sa.create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO budgets (id, name, method, base_currency, active, created_at)
                    VALUES ('budget-flex', 'Household Flex', 'flex', 'USD', 1, :created_at)
                    """
                ),
                {"created_at": datetime(2026, 2, 26, 20, 0, 0)},
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO budget_buckets (
                        id,
                        budget_id,
                        period_month,
                        bucket_name,
                        planned_amount,
                        actual_amount,
                        rollover_policy
                    ) VALUES
                        ('bucket-1', 'budget-flex', '2026-02', 'Fixed', 1200.00, 1200.00, 'carry_positive'),
                        ('bucket-2', 'budget-flex', '2026-02', 'non-monthly', 300.00, 0.00, 'carry_both'),
                        ('bucket-3', 'budget-flex', '2026-02', 'flex', 600.00, 250.00, 'carry_negative'),
                        ('bucket-4', 'budget-flex', '2026-02', 'legacy_bucket', 10.00, 5.00, 'none')
                    """
                )
            )
    finally:
        engine.dispose()

    command.upgrade(config, "head")

    upgraded_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(upgraded_engine)
        tables = set(inspector.get_table_names())
        assert "budget_bucket_definitions" in tables
        assert "budget_bucket_category_mappings" in tables

        budget_category_columns = {column["name"] for column in inspector.get_columns("budget_categories")}
        budget_bucket_columns = {column["name"] for column in inspector.get_columns("budget_buckets")}
        assert "rollover_policy" in budget_category_columns
        assert "bucket_definition_id" in budget_bucket_columns

        with upgraded_engine.connect() as connection:
            definition_rows = connection.execute(
                sa.text(
                    """
                    SELECT id, bucket_key, rollover_policy
                    FROM budget_bucket_definitions
                    WHERE budget_id = 'budget-flex'
                    ORDER BY bucket_key
                    """
                )
            ).all()
            bucket_rows = connection.execute(
                sa.text(
                    """
                    SELECT id, bucket_name, bucket_definition_id
                    FROM budget_buckets
                    WHERE budget_id = 'budget-flex'
                    ORDER BY id
                    """
                )
            ).all()

        assert definition_rows == [
            ("bucketdef:budget-flex:fixed", "fixed", "carry_positive"),
            ("bucketdef:budget-flex:flex", "flex", "carry_negative"),
            ("bucketdef:budget-flex:non_monthly", "non_monthly", "carry_both"),
        ]
        assert bucket_rows == [
            ("bucket-1", "fixed", "bucketdef:budget-flex:fixed"),
            ("bucket-2", "non_monthly", "bucketdef:budget-flex:non_monthly"),
            ("bucket-3", "flex", "bucketdef:budget-flex:flex"),
            ("bucket-4", "legacy_bucket", None),
        ]
    finally:
        upgraded_engine.dispose()
