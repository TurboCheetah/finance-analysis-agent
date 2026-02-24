from __future__ import annotations

import sqlite3
from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _alembic_config(database_url: str) -> Config:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _assert_expected_indexes(inspector: sa.Inspector) -> None:
    expected_indexes = {
        "accounts": {("type",), ("currency",)},
        "statements": {("account_id", "period_end")},
        "import_batches": {
            ("source_type", "source_fingerprint", "received_at"),
            ("status",),
        },
        "import_batch_status_events": {
            ("batch_id", "changed_at"),
            ("to_status", "changed_at"),
        },
        "raw_transactions": {("import_batch_id",), ("parse_status",)},
        "transactions": {
            ("account_id", "posted_date"),
            ("merchant_id",),
            ("category_id",),
            ("pending_status",),
        },
        "transaction_events": {("transaction_id", "created_at"), ("event_type",)},
        "rules": {("enabled", "priority")},
        "rule_audits": {("transaction_id",), ("rule_run_id",)},
        "dedupe_candidates": {("decision",), ("score",)},
        "review_items": {("status", "item_type"), ("confidence",)},
        "reconciliations": {("account_id", "period_end"), ("status",)},
    }

    for table, expected in expected_indexes.items():
        actual = {tuple(index["column_names"]) for index in inspector.get_indexes(table)}
        for expected_columns in expected:
            assert expected_columns in actual, f"Missing index {table}{expected_columns}"


def test_alembic_upgrade_downgrade_smoke(tmp_path: Path) -> None:
    database_file = tmp_path / "tur31_smoke.db"
    database_url = f"sqlite:///{database_file}"
    config = _alembic_config(database_url)

    command.upgrade(config, "head")

    inspector = sa.inspect(sa.create_engine(database_url))
    assert "transactions" in inspector.get_table_names()

    command.downgrade(config, "base")

    with sqlite3.connect(database_file) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert tables <= {"alembic_version"}

    command.upgrade(config, "head")

    inspector = sa.inspect(sa.create_engine(database_url))
    assert "run_metadata" in inspector.get_table_names()


def test_baseline_schema_matches_prd_constraints_and_indexes(tmp_path: Path) -> None:
    database_file = tmp_path / "tur31_schema.db"
    database_url = f"sqlite:///{database_file}"
    config = _alembic_config(database_url)
    command.upgrade(config, "head")

    engine = sa.create_engine(database_url)
    inspector = sa.inspect(engine)

    expected_tables = {
        "accounts",
        "statements",
        "import_batches",
        "import_batch_status_events",
        "raw_transactions",
        "transactions",
        "transaction_events",
        "merchants",
        "merchant_aliases",
        "categories",
        "tags",
        "transaction_tags",
        "transaction_splits",
        "rules",
        "rule_runs",
        "rule_audits",
        "dedupe_candidates",
        "review_items",
        "balance_snapshots",
        "reconciliations",
        "budgets",
        "budget_periods",
        "budget_categories",
        "budget_targets",
        "budget_allocations",
        "budget_buckets",
        "budget_rollovers",
        "recurrings",
        "recurring_events",
        "goals",
        "goal_allocations",
        "goal_events",
        "reports",
        "run_metadata",
    }
    assert expected_tables.issubset(set(inspector.get_table_names()))

    _assert_expected_indexes(inspector)

    statements_uniques = inspector.get_unique_constraints("statements")
    assert ("source_fingerprint",) in {
        tuple(item["column_names"]) for item in statements_uniques
    }

    import_batch_uniques = inspector.get_unique_constraints("import_batches")
    assert ("source_fingerprint", "source_type") not in {
        tuple(item["column_names"]) for item in import_batch_uniques
    }

    balance_snapshot_uniques = inspector.get_unique_constraints("balance_snapshots")
    assert ("account_id", "snapshot_date", "source") in {
        tuple(item["column_names"]) for item in balance_snapshot_uniques
    }

    budget_period_uniques = inspector.get_unique_constraints("budget_periods")
    assert ("budget_id", "period_month") in {
        tuple(item["column_names"]) for item in budget_period_uniques
    }

    import_batch_columns = {column["name"] for column in inspector.get_columns("import_batches")}
    assert {
        "fingerprint_algo",
        "conflict_mode",
        "override_reason",
        "override_of_batch_id",
    }.issubset(import_batch_columns)

    with engine.connect() as connection:
        partial_index_sql = connection.execute(
            sa.text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'index' "
                "AND name = 'ux_transactions_account_source_kind_source_transaction_id_not_null'"
            )
        ).scalar_one()

    assert "WHERE source_transaction_id IS NOT NULL" in partial_index_sql
