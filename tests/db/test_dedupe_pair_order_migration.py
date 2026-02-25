from __future__ import annotations

from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_dedupe_pair_unique_migration_canonicalizes_reversed_rows(tmp_path: Path) -> None:
    database_file = tmp_path / "tur40_pair_order.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "6f4d9e3b2a10")

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
                    INSERT INTO transactions (
                        id,
                        account_id,
                        posted_date,
                        effective_date,
                        amount,
                        currency,
                        pending_status,
                        excluded,
                        source_kind,
                        created_at,
                        updated_at
                    ) VALUES
                        ('txn-1', 'acct-1', '2026-01-10', '2026-01-10', 12.34, 'USD', 'posted', 0, 'csv', :now, :now),
                        ('txn-2', 'acct-1', '2026-01-11', '2026-01-11', 12.34, 'USD', 'posted', 0, 'csv', :now, :now)
                    """
                ),
                {"now": datetime(2026, 2, 25, 1, 0, 0)},
            )
            connection.execute(
                sa.text(
                    """
                    INSERT INTO dedupe_candidates (
                        id,
                        txn_a_id,
                        txn_b_id,
                        score,
                        decision,
                        reason_json,
                        created_at,
                        decided_at
                    ) VALUES
                        ('dc-reversed-undecided', 'txn-2', 'txn-1', 0.81, NULL, '{}', :created_a, NULL),
                        ('dc-canonical-decided', 'txn-1', 'txn-2', 0.95, 'duplicate', '{}', :created_b, :decided_b)
                    """
                ),
                {
                    "created_a": datetime(2026, 2, 25, 1, 1, 0),
                    "created_b": datetime(2026, 2, 25, 1, 2, 0),
                    "decided_b": datetime(2026, 2, 25, 1, 3, 0),
                },
            )
    finally:
        engine.dispose()

    command.upgrade(config, "c3a1d7e4b9f0")

    upgraded_engine = sa.create_engine(database_url)
    try:
        with upgraded_engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    """
                    SELECT id, txn_a_id, txn_b_id, decision
                    FROM dedupe_candidates
                    ORDER BY id
                    """
                )
            ).all()

        assert rows == [("dc-canonical-decided", "txn-1", "txn-2", "duplicate")]
    finally:
        upgraded_engine.dispose()
