from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from typer.testing import CliRunner

from finance_analysis_agent.cli import app
from finance_analysis_agent.db.models import Account, Transaction
from tests.backup.helpers import create_seeded_database


def test_backup_export_bundle_cli_writes_bundle_and_artifact(tmp_path: Path) -> None:
    database_url = create_seeded_database(tmp_path, filename="backup_cli_export.db")

    bundle_dir = tmp_path / "bundle"
    output_path = tmp_path / "backup-export-result.json"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "backup",
            "export-bundle",
            "--database-url",
            database_url,
            "--output-dir",
            str(bundle_dir),
            "--actor",
            "cli-tester",
            "--reason",
            "cli export",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "# Backup Export Summary" in result.stdout
    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "diagnostics.json").exists()
    assert (bundle_dir / "csv" / "transactions.csv").exists()
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["manifest_path"].endswith("manifest.json")
    assert payload["table_row_counts"]["accounts"] == 1


def test_backup_restore_bundle_cli_restores_rows(tmp_path: Path) -> None:
    source_database_url = create_seeded_database(tmp_path, filename="backup_cli_source.db")

    bundle_dir = tmp_path / "bundle"
    export_output = tmp_path / "backup-export-result.json"
    restore_output = tmp_path / "backup-restore-result.json"
    target_database_url = f"sqlite:///{tmp_path / 'backup_cli_restored.db'}"

    runner = CliRunner()
    export_result = runner.invoke(
        app,
        [
            "backup",
            "export-bundle",
            "--database-url",
            source_database_url,
            "--output-dir",
            str(bundle_dir),
            "--output",
            str(export_output),
        ],
    )
    assert export_result.exit_code == 0

    restore_result = runner.invoke(
        app,
        [
            "backup",
            "restore-bundle",
            "--database-url",
            target_database_url,
            "--bundle-dir",
            str(bundle_dir),
            "--output",
            str(restore_output),
        ],
    )
    assert restore_result.exit_code == 0
    assert "# Backup Restore Summary" in restore_result.stdout
    assert restore_output.exists()

    engine = create_engine(target_database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session: Session = session_factory()
    try:
        account_ids = session.execute(select(Account.id).order_by(Account.id.asc())).scalars().all()
        transaction_ids = session.execute(select(Transaction.id).order_by(Transaction.id.asc())).scalars().all()
    finally:
        session.close()
        engine.dispose()

    assert account_ids == ["acct-main"]
    assert transaction_ids == ["txn-1", "txn-2"]
