from __future__ import annotations

import sqlalchemy as sa
from alembic import command

from tests.helpers import alembic_config


def test_quality_metrics_migration_adds_and_removes_metric_observations(tmp_path) -> None:
    database_file = tmp_path / "tur48_quality_metrics.db"
    database_url = f"sqlite:///{database_file}"
    config = alembic_config(database_url)

    command.upgrade(config, "4e8b1c7d2a9f")

    pre_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(pre_engine)
        assert "metric_observations" not in inspector.get_table_names()
    finally:
        pre_engine.dispose()

    command.upgrade(config, "head")

    upgraded_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(upgraded_engine)
        assert "metric_observations" in inspector.get_table_names()
        columns = {column["name"] for column in inspector.get_columns("metric_observations")}
        assert {
            "id",
            "run_id",
            "metric_group",
            "metric_key",
            "period_start",
            "period_end",
            "account_id",
            "template_key",
            "metric_value",
            "numerator",
            "denominator",
            "threshold_value",
            "threshold_operator",
            "alert_status",
            "dimensions_json",
            "created_at",
        }.issubset(columns)

        indexes = {index["name"] for index in inspector.get_indexes("metric_observations")}
        assert "ix_metric_observations_metric_key_period" in indexes
        assert "ix_metric_observations_account_id_period_end" in indexes
        assert "ix_metric_observations_template_key" in indexes
        assert "ix_metric_observations_alert_status" in indexes
    finally:
        upgraded_engine.dispose()

    command.downgrade(config, "4e8b1c7d2a9f")

    downgraded_engine = sa.create_engine(database_url)
    try:
        inspector = sa.inspect(downgraded_engine)
        assert "metric_observations" not in inspector.get_table_names()
    finally:
        downgraded_engine.dispose()
