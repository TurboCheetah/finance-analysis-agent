"""CLI entrypoints for finance-analysis-agent workflows."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any, cast

import typer

from finance_analysis_agent.backup import (
    ExportBundleRequest,
    ExportBundleResult,
    RestoreBundleRequest,
    RestoreBundleResult,
    export_bundle,
    restore_bundle,
)
from finance_analysis_agent.db.engine import get_session_factory
from finance_analysis_agent.reporting import (
    ReportType,
    ReportingGenerateRequest,
    ReportingGenerateResult,
    reporting_generate,
)

app = typer.Typer(help="Finance Analysis Agent CLI")
reporting_app = typer.Typer(help="Reporting workflows")
backup_app = typer.Typer(help="Backup/export workflows")
app.add_typer(reporting_app, name="reporting")
app.add_typer(backup_app, name="backup")


def _parse_iso_date(value: str, *, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must be in YYYY-MM-DD format") from exc


def _default_reporting_output_path(run_metadata_id: str) -> Path:
    return Path(f"reporting-run-{run_metadata_id}.json")


def _reporting_result_json_payload(result: ReportingGenerateResult) -> dict[str, object]:
    return {
        "run_metadata_id": result.run_metadata_id,
        "period_start": result.period_start.isoformat(),
        "period_end": result.period_end.isoformat(),
        "report_types": [item.value for item in result.report_types],
        "reports": [
            {
                "report_id": report.report_id,
                "report_type": report.report_type.value,
                "payload_hash": report.payload_hash,
                "payload_json": report.payload_json,
            }
            for report in result.reports
        ],
        "causes": [
            {
                "code": cause.code,
                "message": cause.message,
                "severity": cause.severity,
            }
            for cause in result.causes
        ],
    }


def _reporting_markdown_summary(result_payload: dict[str, object], artifact_path: Path) -> str:
    report_items = cast(list[dict[str, object]], result_payload["reports"])
    lines = [
        "# Reporting Run Summary",
        "",
        f"- Run Metadata ID: `{result_payload['run_metadata_id']}`",
        f"- Period: `{result_payload['period_start']}` to `{result_payload['period_end']}`",
        f"- Reports Generated: `{len(report_items)}`",
        f"- Artifact: `{artifact_path}`",
        "",
        "| Report Type | Report ID | Payload Hash |",
        "|---|---|---|",
    ]
    for item in report_items:
        lines.append(
            f"| `{item['report_type']}` | `{item['report_id']}` | `{item['payload_hash']}` |"
        )
    return "\n".join(lines)


def _default_backup_export_output_path() -> Path:
    return Path("backup-export-result.json")


def _default_backup_restore_output_path() -> Path:
    return Path("backup-restore-result.json")


def _backup_export_result_json_payload(result: ExportBundleResult) -> dict[str, object]:
    return {
        "output_dir": str(result.output_dir),
        "manifest_path": str(result.manifest_path),
        "diagnostics_path": str(result.diagnostics_path),
        "db_schema_revision": result.db_schema_revision,
        "table_row_counts": result.table_row_counts,
        "file_checksums": result.file_checksums,
    }


def _backup_restore_result_json_payload(result: RestoreBundleResult) -> dict[str, object]:
    return {
        "bundle_dir": str(result.bundle_dir),
        "manifest_path": str(result.manifest_path),
        "db_schema_revision": result.db_schema_revision,
        "restored_table_counts": result.restored_table_counts,
        "validated_files": result.validated_files,
    }


def _backup_export_markdown_summary(payload: dict[str, Any], artifact_path: Path) -> str:
    table_counts = cast(dict[str, int], payload["table_row_counts"])
    lines = [
        "# Backup Export Summary",
        "",
        f"- Bundle Directory: `{payload['output_dir']}`",
        f"- Manifest: `{payload['manifest_path']}`",
        f"- Diagnostics: `{payload['diagnostics_path']}`",
        f"- DB Schema Revision: `{payload['db_schema_revision']}`",
        f"- Tables Exported: `{len(table_counts)}`",
        f"- Artifact: `{artifact_path}`",
        "",
        "| Table | Row Count |",
        "|---|---|",
    ]
    for table_name in sorted(table_counts):
        lines.append(f"| `{table_name}` | `{table_counts[table_name]}` |")
    return "\n".join(lines)


def _backup_restore_markdown_summary(payload: dict[str, Any], artifact_path: Path) -> str:
    table_counts = cast(dict[str, int], payload["restored_table_counts"])
    lines = [
        "# Backup Restore Summary",
        "",
        f"- Bundle Directory: `{payload['bundle_dir']}`",
        f"- Manifest: `{payload['manifest_path']}`",
        f"- DB Schema Revision: `{payload['db_schema_revision']}`",
        f"- Tables Restored: `{len(table_counts)}`",
        f"- Artifact: `{artifact_path}`",
        "",
        "| Table | Restored Rows |",
        "|---|---|",
    ]
    for table_name in sorted(table_counts):
        lines.append(f"| `{table_name}` | `{table_counts[table_name]}` |")
    return "\n".join(lines)


@reporting_app.command("generate")
def reporting_generate_command(
    period_month: str | None = typer.Option(None, "--period-month", help="Reporting month in YYYY-MM format"),
    period_start: str | None = typer.Option(None, "--period-start", help="Start date in YYYY-MM-DD format"),
    period_end: str | None = typer.Option(None, "--period-end", help="End date in YYYY-MM-DD format"),
    report_type: list[ReportType] | None = typer.Option(None, "--report-type", help="Report type to generate"),
    account_id: list[str] | None = typer.Option(None, "--account-id", help="Account scope filter (repeatable)"),
    budget_id: str | None = typer.Option(None, "--budget-id", help="Budget id (required for budget_vs_actual)"),
    actor: str = typer.Option("cli", "--actor", help="Actor identifier for provenance"),
    reason: str = typer.Option("CLI reporting generate", "--reason", help="Reason for run metadata/audit trail"),
    output: Path | None = typer.Option(None, "--output", help="JSON artifact output path"),
    database_url: str | None = typer.Option(None, "--database-url", help="Override DATABASE_URL"),
) -> None:
    """Generate deterministic finance reports and persist report metadata."""

    parsed_period_start: date | None = None
    parsed_period_end: date | None = None
    if period_start is not None:
        parsed_period_start = _parse_iso_date(period_start, option_name="--period-start")
    if period_end is not None:
        parsed_period_end = _parse_iso_date(period_end, option_name="--period-end")

    session_factory = get_session_factory(database_url)
    session = session_factory()

    try:
        request = ReportingGenerateRequest(
            actor=actor,
            reason=reason,
            period_month=period_month,
            period_start=parsed_period_start,
            period_end=parsed_period_end,
            report_types=list(report_type or []),
            account_ids=list(account_id or []),
            budget_id=budget_id,
        )
        result = reporting_generate(request, session)
        session.commit()

        result_payload = _reporting_result_json_payload(result)
        artifact_path = output or _default_reporting_output_path(result.run_metadata_id)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(result_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        typer.echo(_reporting_markdown_summary(result_payload, artifact_path))
    except Exception as exc:
        session.rollback()
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    finally:
        session.close()


@backup_app.command("export-bundle")
def backup_export_bundle_command(
    output_dir: Path = typer.Option(Path("backup-bundle"), "--output-dir", help="Bundle output directory"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite output directory if it exists"),
    actor: str = typer.Option("cli", "--actor", help="Actor identifier for provenance"),
    reason: str = typer.Option("CLI backup export", "--reason", help="Reason for export"),
    output: Path | None = typer.Option(None, "--output", help="JSON artifact output path"),
    database_url: str | None = typer.Option(None, "--database-url", help="Override DATABASE_URL"),
) -> None:
    """Export a portable backup bundle with JSONL, CSV, manifest, and checksums."""

    session_factory = get_session_factory(database_url)
    session = session_factory()

    try:
        result = export_bundle(
            ExportBundleRequest(
                actor=actor,
                reason=reason,
                output_dir=output_dir,
                overwrite=overwrite,
            ),
            session,
        )
        session.commit()

        result_payload = _backup_export_result_json_payload(result)
        artifact_path = output or _default_backup_export_output_path()
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(result_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        typer.echo(_backup_export_markdown_summary(result_payload, artifact_path))
    except Exception as exc:
        session.rollback()
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    finally:
        session.close()


@backup_app.command("restore-bundle")
def backup_restore_bundle_command(
    bundle_dir: Path = typer.Option(..., "--bundle-dir", help="Bundle directory containing manifest.json"),
    allow_non_empty: bool = typer.Option(
        False,
        "--allow-non-empty",
        help="Allow restoring into a populated database by clearing existing rows first",
    ),
    actor: str = typer.Option("cli", "--actor", help="Actor identifier for provenance"),
    reason: str = typer.Option("CLI backup restore", "--reason", help="Reason for restore"),
    output: Path | None = typer.Option(None, "--output", help="JSON artifact output path"),
    database_url: str | None = typer.Option(None, "--database-url", help="Override DATABASE_URL"),
) -> None:
    """Restore a portable backup bundle into the configured database."""

    session_factory = get_session_factory(database_url)
    session = session_factory()

    try:
        result = restore_bundle(
            RestoreBundleRequest(
                actor=actor,
                reason=reason,
                bundle_dir=bundle_dir,
                allow_non_empty=allow_non_empty,
            ),
            session,
        )
        session.commit()

        result_payload = _backup_restore_result_json_payload(result)
        artifact_path = output or _default_backup_restore_output_path()
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(result_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        typer.echo(_backup_restore_markdown_summary(result_payload, artifact_path))
    except Exception as exc:
        session.rollback()
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    finally:
        session.close()


if __name__ == "__main__":
    app()
