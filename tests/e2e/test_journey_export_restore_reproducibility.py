from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from finance_analysis_agent.cli import app
from finance_analysis_agent.db.models import Category, MerchantAlias, Tag, TransactionSplit
from tests.backup.helpers import create_seeded_database
from tests.e2e.helpers import load_report_hashes, persist_artifact, session_for_database

pytestmark = pytest.mark.e2e


def _reporting_args(database_url: str, output_path: Path) -> list[str]:
    return [
        "reporting",
        "generate",
        "--database-url",
        database_url,
        "--period-month",
        "2026-02",
        "--report-type",
        "cash_flow",
        "--report-type",
        "category_trends",
        "--report-type",
        "net_worth",
        "--report-type",
        "budget_vs_actual",
        "--report-type",
        "goal_progress",
        "--budget-id",
        "budget-1",
        "--actor",
        "e2e-cli",
        "--reason",
        "round trip comparison",
        "--output",
        str(output_path),
    ]


def test_journey_export_restore_reproducibility(tmp_path: Path) -> None:
    source_database_url = create_seeded_database(tmp_path, filename="e2e-export-source.db")
    target_database_url = f"sqlite:///{tmp_path / 'e2e-export-target.db'}"
    bundle_dir = tmp_path / "bundle"
    source_report_path = tmp_path / "source-report.json"
    target_report_path = tmp_path / "target-report.json"
    export_output_path = tmp_path / "export-output.json"
    restore_output_path = tmp_path / "restore-output.json"

    runner = CliRunner()

    source_report = runner.invoke(app, _reporting_args(source_database_url, source_report_path))
    export_result = runner.invoke(
        app,
        [
            "backup",
            "export-bundle",
            "--database-url",
            source_database_url,
            "--output-dir",
            str(bundle_dir),
            "--actor",
            "e2e-cli",
            "--reason",
            "create round trip bundle",
            "--output",
            str(export_output_path),
        ],
    )
    restore_result = runner.invoke(
        app,
        [
            "backup",
            "restore-bundle",
            "--database-url",
            target_database_url,
            "--bundle-dir",
            str(bundle_dir),
            "--actor",
            "e2e-cli",
            "--reason",
            "restore round trip bundle",
            "--output",
            str(restore_output_path),
        ],
    )
    target_report = runner.invoke(app, _reporting_args(target_database_url, target_report_path))

    assert source_report.exit_code == 0, source_report.output
    assert export_result.exit_code == 0, export_result.output
    assert restore_result.exit_code == 0, restore_result.output
    assert target_report.exit_code == 0, target_report.output

    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "diagnostics.json").exists()

    assert load_report_hashes(source_report_path) == load_report_hashes(target_report_path)

    with session_for_database(target_database_url) as restored_session:
        alias_count = restored_session.scalar(select(func.count()).select_from(MerchantAlias))
        tag_count = restored_session.scalar(select(func.count()).select_from(Tag))
        split_count = restored_session.scalar(select(func.count()).select_from(TransactionSplit))
        restored_child = restored_session.get(Category, "cat-grocery")

        assert alias_count == 1
        assert tag_count == 1
        assert split_count == 1
        assert restored_child is not None
        assert restored_child.parent_id == "cat-root"

    persist_artifact("journey-export-restore-reproducibility/source-report.json", source_report_path)
    persist_artifact("journey-export-restore-reproducibility/target-report.json", target_report_path)
    persist_artifact("journey-export-restore-reproducibility/export-output.json", export_output_path)
    persist_artifact("journey-export-restore-reproducibility/restore-output.json", restore_output_path)
    persist_artifact("journey-export-restore-reproducibility/bundle", bundle_dir)
