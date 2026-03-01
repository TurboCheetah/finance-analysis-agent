"""CLI entrypoints for finance-analysis-agent workflows."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import cast

import typer

from finance_analysis_agent.db.engine import get_session_factory
from finance_analysis_agent.reporting import ReportType, ReportingGenerateRequest, reporting_generate
from finance_analysis_agent.reporting.types import ReportingGenerateResult

app = typer.Typer(help="Finance Analysis Agent CLI")
reporting_app = typer.Typer(help="Reporting workflows")
app.add_typer(reporting_app, name="reporting")


def _parse_iso_date(value: str, *, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must be in YYYY-MM-DD format") from exc


def _default_output_path(run_metadata_id: str) -> Path:
    return Path(f"reporting-run-{run_metadata_id}.json")


def _result_json_payload(result: object) -> dict[str, object]:
    typed_result = cast(ReportingGenerateResult, result)
    return {
        "run_metadata_id": typed_result.run_metadata_id,
        "period_start": typed_result.period_start.isoformat(),
        "period_end": typed_result.period_end.isoformat(),
        "report_types": [item.value for item in typed_result.report_types],
        "reports": [
            {
                "report_id": report.report_id,
                "report_type": report.report_type.value,
                "payload_hash": report.payload_hash,
                "payload_json": report.payload_json,
            }
            for report in typed_result.reports
        ],
        "causes": [
            {
                "code": cause.code,
                "message": cause.message,
                "severity": cause.severity,
            }
            for cause in typed_result.causes
        ],
    }


def _markdown_summary(result_payload: dict[str, object], artifact_path: Path) -> str:
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

        result_payload = _result_json_payload(result)
        artifact_path = output or _default_output_path(result.run_metadata_id)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(result_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        typer.echo(_markdown_summary(result_payload, artifact_path))
    except Exception as exc:
        session.rollback()
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    finally:
        session.close()


if __name__ == "__main__":
    app()
