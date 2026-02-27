from __future__ import annotations

import sqlite3
from pathlib import Path

import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


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
        "budget_buckets": {
            ("budget_id", "period_month", "bucket_definition_id"),
        },
        "budget_bucket_category_mappings": {
            ("bucket_definition_id",),
        },
        "categories": {("name",)},
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
        "dedupe_candidate_events": {
            ("dedupe_candidate_id", "created_at"),
            ("event_type", "created_at"),
        },
        "review_items": {
            ("status", "item_type"),
            ("confidence",),
            ("reason_code",),
            ("source",),
        },
        "review_item_events": {
            ("review_item_id", "created_at"),
            ("event_type", "created_at"),
        },
        "reconciliations": {
            ("account_id", "period_end"),
            ("status",),
            ("statement_id",),
            ("approved_adjustment_txn_id",),
        },
    }

    for table, expected in expected_indexes.items():
        actual = {tuple(index["column_names"]) for index in inspector.get_indexes(table)}
        for expected_columns in expected:
            assert expected_columns in actual, f"Missing index {table}{expected_columns}"


def test_alembic_upgrade_downgrade_smoke(tmp_path: Path) -> None:
    database_file = tmp_path / "tur31_smoke.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "head")

    first_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(first_engine)
        assert "transactions" in inspector.get_table_names()
    finally:
        first_engine.dispose()

    command.downgrade(config, "base")

    connection = sqlite3.connect(database_file)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    finally:
        connection.close()
    assert tables <= {"alembic_version"}

    command.upgrade(config, "head")

    second_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(second_engine)
        assert "run_metadata" in inspector.get_table_names()
    finally:
        second_engine.dispose()


def test_baseline_schema_matches_prd_constraints_and_indexes(tmp_path: Path) -> None:
    database_file = tmp_path / "tur31_schema.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)
    command.upgrade(config, "head")

    engine = sa.create_engine(database_url)
    try:
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
            "dedupe_candidate_events",
            "review_items",
            "review_item_events",
            "balance_snapshots",
            "reconciliations",
            "budgets",
            "budget_periods",
            "budget_categories",
            "budget_targets",
            "budget_allocations",
            "budget_buckets",
            "budget_bucket_definitions",
            "budget_bucket_category_mappings",
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
        budget_bucket_definition_uniques = inspector.get_unique_constraints("budget_bucket_definitions")
        assert ("budget_id", "bucket_key") in {
            tuple(item["column_names"]) for item in budget_bucket_definition_uniques
        }
        budget_bucket_mapping_uniques = inspector.get_unique_constraints("budget_bucket_category_mappings")
        assert ("budget_category_id",) in {
            tuple(item["column_names"]) for item in budget_bucket_mapping_uniques
        }
        goal_allocation_uniques = inspector.get_unique_constraints("goal_allocations")
        assert ("period_month", "goal_id", "account_id", "allocation_type") in {
            tuple(item["column_names"]) for item in goal_allocation_uniques
        }

        import_batch_columns = {column["name"] for column in inspector.get_columns("import_batches")}
        assert {
            "fingerprint_algo",
            "conflict_mode",
            "override_reason",
            "override_of_batch_id",
        }.issubset(import_batch_columns)
        review_item_columns = {
            column["name"]: column for column in inspector.get_columns("review_items")
        }
        assert "source" in review_item_columns
        assert review_item_columns["source"]["nullable"] is False

        reconciliation_columns = {
            column["name"]: column for column in inspector.get_columns("reconciliations")
        }
        assert {
            "statement_id",
            "unresolved_count",
            "adjustment_magnitude",
            "details_json",
            "approved_adjustment_txn_id",
            "approved_by",
            "approved_at",
        }.issubset(reconciliation_columns)

        with engine.connect() as connection:
            partial_index_sql = connection.execute(
                sa.text(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type = 'index' "
                    "AND name = 'ux_transactions_account_source_kind_source_transaction_id_not_null'"
                )
            ).scalar_one()
            root_category_index_sql = connection.execute(
                sa.text(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type = 'index' "
                    "AND name = 'ux_categories_root_name_parent_null'"
                )
            ).scalar_one()

        assert "WHERE source_transaction_id IS NOT NULL" in partial_index_sql
        assert "WHERE parent_id IS NULL" in root_category_index_sql
    finally:
        engine.dispose()
