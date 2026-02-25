from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_reconciliation_migration_backfills_defaults_and_adds_indexes(tmp_path: Path) -> None:
    database_file = tmp_path / "tur42_reconciliation_migration.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "d5f0c7a1e9b2")

    engine = sa.create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    """
                    INSERT INTO accounts (id, name, type, currency)
                    VALUES ('acct-1', 'Checking', 'checking', 'USD')
                    """
                )
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO reconciliations (
                        id,
                        account_id,
                        period_start,
                        period_end,
                        expected_balance,
                        computed_balance,
                        delta,
                        match_rate,
                        trust_score,
                        status,
                        created_at
                    ) VALUES (
                        'rec-1',
                        'acct-1',
                        '2026-01-01',
                        '2026-01-31',
                        100.00,
                        99.50,
                        0.50,
                        0.75,
                        0.60,
                        'fail',
                        :created_at
                    )
                    """
                ),
                {"created_at": datetime(2026, 2, 25, 23, 0, 0)},
            )
    finally:
        engine.dispose()

    command.upgrade(config, "head")

    upgraded_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(upgraded_engine)
        columns = {column["name"] for column in inspector.get_columns("reconciliations")}
        assert {
            "statement_id",
            "unresolved_count",
            "adjustment_magnitude",
            "details_json",
            "approved_adjustment_txn_id",
            "approved_by",
            "approved_at",
        }.issubset(columns)

        indexes = {index["name"] for index in inspector.get_indexes("reconciliations")}
        assert "ix_reconciliations_statement_id" in indexes
        assert "ix_reconciliations_approved_adjustment_txn_id" in indexes

        with upgraded_engine.connect() as connection:
            row = connection.execute(
                sa.text(
                    """
                    SELECT unresolved_count, adjustment_magnitude, details_json, approved_adjustment_txn_id
                    FROM reconciliations
                    WHERE id = 'rec-1'
                    """
                )
            ).one()

        assert row.unresolved_count == 0
        assert Decimal(str(row.adjustment_magnitude)) == Decimal("0.00")
        assert row.details_json is None
        assert row.approved_adjustment_txn_id is None
    finally:
        upgraded_engine.dispose()
