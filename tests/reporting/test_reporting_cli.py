from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from typer.testing import CliRunner

from finance_analysis_agent.cli import app
from finance_analysis_agent.db.models import Account, Transaction
from finance_analysis_agent.utils.time import utcnow
from tests.helpers import alembic_config


def _create_database(tmp_path: Path) -> str:
    database_file = tmp_path / "reporting_cli.db"
    database_url = f"sqlite:///{database_file}"
    command.upgrade(alembic_config(database_url), "head")
    return database_url


def _seed_cashflow_data(database_url: str) -> None:
    engine = create_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session: Session = session_factory()
    try:
        session.add(Account(id="acct-cli", name="CLI Checking", type="checking", currency="USD"))
        now = utcnow()
        session.add(
            Transaction(
                id="txn-cli-income",
                account_id="acct-cli",
                posted_date=date(2026, 2, 10),
                effective_date=date(2026, 2, 10),
                amount=Decimal("1000.00"),
                currency="USD",
                original_amount=Decimal("1000.00"),
                original_currency="USD",
                pending_status="posted",
                original_statement="seed",
                merchant_id=None,
                category_id=None,
                excluded=False,
                notes=None,
                source_kind="manual",
                source_transaction_id="src-txn-cli-income",
                import_batch_id=None,
                transfer_group_id=None,
                created_at=now,
                updated_at=now,
            )
        )
        session.commit()
    finally:
        session.close()
        engine.dispose()


def test_reporting_generate_cli_writes_markdown_and_json_artifact(tmp_path: Path) -> None:
    database_url = _create_database(tmp_path)
    _seed_cashflow_data(database_url)

    output_path = tmp_path / "reporting-output.json"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reporting",
            "generate",
            "--database-url",
            database_url,
            "--period-month",
            "2026-02",
            "--report-type",
            "cash_flow",
            "--actor",
            "cli-tester",
            "--reason",
            "cli test",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "# Reporting Run Summary" in result.stdout
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["report_types"] == ["cash_flow"]
    assert len(payload["reports"]) == 1
    assert payload["reports"][0]["report_type"] == "cash_flow"


def test_reporting_generate_cli_requires_budget_id_for_budget_vs_actual(tmp_path: Path) -> None:
    database_url = _create_database(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reporting",
            "generate",
            "--database-url",
            database_url,
            "--period-month",
            "2026-02",
            "--report-type",
            "budget_vs_actual",
        ],
    )

    assert result.exit_code == 1
    assert "budget_id is required when budget_vs_actual report is requested" in result.stderr
