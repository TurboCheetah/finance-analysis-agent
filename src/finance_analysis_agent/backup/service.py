"""Service-layer backup/export workflows for portable finance bundles."""

from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from time import perf_counter
import shutil
from typing import Any

from sqlalchemy import Date, DateTime, Float, Integer, Numeric, Boolean, String, Text, delete, func, select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy.sql.sqltypes import JSON as JsonType

from finance_analysis_agent.backup.types import (
    ExportBundleRequest,
    ExportBundleResult,
    RestoreBundleRequest,
    RestoreBundleResult,
)
from finance_analysis_agent.db.base import Base
from finance_analysis_agent.db.models import Category, Merchant, Tag, Transaction, TransactionTag
from finance_analysis_agent.utils.time import utcnow

BUNDLE_SCHEMA_VERSION = "1.0.0"
CHECKSUM_ALGORITHM = "sha256"

TRANSACTION_CSV_COLUMNS = [
    "transaction_id",
    "account_id",
    "posted_date",
    "effective_date",
    "amount",
    "currency",
    "original_amount",
    "original_currency",
    "pending_status",
    "merchant",
    "original_statement",
    "category",
    "parent_category",
    "tags",
    "excluded",
    "notes",
    "source_kind",
    "source_transaction_id",
    "import_batch_id",
    "created_at",
    "updated_at",
]


def _parse_non_empty(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _normalize_path(value: Path | str, *, field_name: str) -> Path:
    candidate = Path(value) if isinstance(value, str) else value
    if not isinstance(candidate, Path):
        raise ValueError(f"{field_name} must be a path")
    return candidate.resolve()


def _table_order() -> list[Table]:
    return list(Base.metadata.sorted_tables)


def _serialize_scalar(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _serialize_row(table: Table, row: dict[str, Any]) -> dict[str, object]:
    return {
        column.name: _serialize_scalar(row[column.name])
        for column in table.columns
    }


def _table_select_statement(table: Table):
    stmt = select(table)
    pk_columns = [column for column in table.primary_key.columns]
    if pk_columns:
        return stmt.order_by(*pk_columns)
    fallback_columns = [table.columns[column_name] for column_name in sorted(table.columns.keys())]
    if not fallback_columns:
        return stmt
    return stmt.order_by(*fallback_columns)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_db_schema_revision(session: Session) -> str | None:
    try:
        return session.execute(text("SELECT version_num FROM alembic_version LIMIT 1")).scalar_one_or_none()
    except SQLAlchemyError:
        return None


def _build_transaction_tags_lookup(session: Session) -> dict[str, list[str]]:
    rows = session.execute(
        select(TransactionTag.transaction_id, Tag.name)
        .join(Tag, Tag.id == TransactionTag.tag_id)
        .order_by(TransactionTag.transaction_id.asc(), Tag.name.asc())
    ).all()
    result: dict[str, list[str]] = {}
    for transaction_id, tag_name in rows:
        result.setdefault(transaction_id, []).append(tag_name)
    return result


def _build_transaction_csv_rows(session: Session) -> list[dict[str, object]]:
    parent_category = aliased(Category)
    tag_lookup = _build_transaction_tags_lookup(session)
    rows = session.execute(
        select(
            Transaction.id,
            Transaction.account_id,
            Transaction.posted_date,
            Transaction.effective_date,
            Transaction.amount,
            Transaction.currency,
            Transaction.original_amount,
            Transaction.original_currency,
            Transaction.pending_status,
            Merchant.canonical_name,
            Transaction.original_statement,
            Category.name,
            parent_category.name,
            Transaction.excluded,
            Transaction.notes,
            Transaction.source_kind,
            Transaction.source_transaction_id,
            Transaction.import_batch_id,
            Transaction.created_at,
            Transaction.updated_at,
        )
        .outerjoin(Merchant, Merchant.id == Transaction.merchant_id)
        .outerjoin(Category, Category.id == Transaction.category_id)
        .outerjoin(parent_category, parent_category.id == Category.parent_id)
        .order_by(Transaction.id.asc())
    ).all()

    csv_rows: list[dict[str, object]] = []
    for (
        transaction_id,
        account_id,
        posted_date,
        effective_date,
        amount,
        currency,
        original_amount,
        original_currency,
        pending_status,
        merchant_name,
        original_statement,
        category_name,
        parent_category_name,
        excluded,
        notes,
        source_kind,
        source_transaction_id,
        import_batch_id,
        created_at,
        updated_at,
    ) in rows:
        csv_rows.append(
            {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "posted_date": posted_date.isoformat(),
                "effective_date": effective_date.isoformat() if effective_date is not None else None,
                "amount": format(Decimal(amount), "f"),
                "currency": currency,
                "original_amount": format(Decimal(original_amount), "f") if original_amount is not None else None,
                "original_currency": original_currency,
                "pending_status": pending_status,
                "merchant": merchant_name,
                "original_statement": original_statement,
                "category": category_name,
                "parent_category": parent_category_name,
                "tags": ";".join(tag_lookup.get(transaction_id, [])),
                "excluded": "true" if excluded else "false",
                "notes": notes,
                "source_kind": source_kind,
                "source_transaction_id": source_transaction_id,
                "import_batch_id": import_batch_id,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
            }
        )
    return csv_rows


def _write_transactions_csv(path: Path, session: Session) -> None:
    rows = _build_transaction_csv_rows(session)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRANSACTION_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def export_bundle(request: ExportBundleRequest, session: Session) -> ExportBundleResult:
    """Export the canonical database state into a portable bundle."""

    _parse_non_empty(request.actor, field_name="actor")
    _parse_non_empty(request.reason, field_name="reason")
    output_dir = _normalize_path(request.output_dir, field_name="output_dir")

    if output_dir.exists():
        if output_dir.is_file():
            raise ValueError(f"output_dir must be a directory: {output_dir}")
        if not request.overwrite and any(output_dir.iterdir()):
            raise ValueError(f"output_dir already exists and is not empty: {output_dir}")
        if request.overwrite:
            shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_dir = output_dir / "json"
    csv_dir = output_dir / "csv"
    json_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    started = perf_counter()
    generated_at = utcnow()

    table_row_counts: dict[str, int] = {}
    artifact_paths: list[str] = []

    for table in _table_order():
        table_path = json_dir / f"{table.name}.jsonl"
        rows = session.execute(_table_select_statement(table)).mappings().all()
        with table_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                serialized = _serialize_row(table, dict(row))
                handle.write(json.dumps(serialized, sort_keys=True, separators=(",", ":")) + "\n")
        table_row_counts[table.name] = len(rows)
        artifact_paths.append(str(table_path.relative_to(output_dir).as_posix()))

    transaction_csv_path = csv_dir / "transactions.csv"
    _write_transactions_csv(transaction_csv_path, session)
    artifact_paths.append(str(transaction_csv_path.relative_to(output_dir).as_posix()))

    duration_ms = int((perf_counter() - started) * 1000)
    diagnostics_payload = {
        "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "export_duration_ms": duration_ms,
        "table_row_counts": table_row_counts,
        "warnings": [],
    }
    diagnostics_path = output_dir / "diagnostics.json"
    _write_json(diagnostics_path, diagnostics_payload)
    artifact_paths.append(str(diagnostics_path.relative_to(output_dir).as_posix()))

    checksum_by_path: dict[str, str] = {}
    size_by_path: dict[str, int] = {}
    for relative_path in sorted(artifact_paths):
        artifact_path = output_dir / relative_path
        checksum_by_path[relative_path] = _sha256_file(artifact_path)
        size_by_path[relative_path] = artifact_path.stat().st_size

    manifest_payload = {
        "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "checksum_algorithm": CHECKSUM_ALGORITHM,
        "db_schema_revision": _safe_db_schema_revision(session),
        "artifacts": [
            {
                "path": relative_path,
                "sha256": checksum_by_path[relative_path],
                "size_bytes": size_by_path[relative_path],
            }
            for relative_path in sorted(checksum_by_path)
        ],
        "diagnostics_path": "diagnostics.json",
        "table_order": [table.name for table in _table_order()],
    }
    manifest_path = output_dir / "manifest.json"
    _write_json(manifest_path, manifest_payload)

    return ExportBundleResult(
        output_dir=output_dir,
        manifest_path=manifest_path,
        diagnostics_path=diagnostics_path,
        db_schema_revision=manifest_payload["db_schema_revision"],
        table_row_counts=table_row_counts,
        file_checksums=checksum_by_path,
    )


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        raise ValueError(f"Missing required file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON file: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _coerce_value(value: object, column: Column[Any]) -> object:
    if value is None:
        return None
    column_type = column.type
    if isinstance(column_type, Numeric):
        return Decimal(str(value))
    if isinstance(column_type, Date):
        if not isinstance(value, str):
            raise ValueError(f"Expected ISO date string for {column.name}")
        return date.fromisoformat(value)
    if isinstance(column_type, DateTime):
        if not isinstance(value, str):
            raise ValueError(f"Expected ISO datetime string for {column.name}")
        return datetime.fromisoformat(value)
    if isinstance(column_type, JsonType):
        return value
    if isinstance(column_type, Boolean):
        return bool(value)
    if isinstance(column_type, Integer):
        return int(value)
    if isinstance(column_type, Float):
        return float(value)
    if isinstance(column_type, (String, Text)):
        return str(value)
    return value


def _read_jsonl_table_rows(path: Path, table: Table) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL row in {path} at line {line_number}") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"JSON object required in {path} at line {line_number}")
            row = {
                column.name: _coerce_value(payload.get(column.name), column)
                for column in table.columns
            }
            rows.append(row)
    return rows


def _current_table_counts(session: Session) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in _table_order():
        counts[table.name] = int(session.execute(select(func.count()).select_from(table)).scalar_one())
    return counts


def _clear_tables(session: Session) -> None:
    for table in reversed(_table_order()):
        session.execute(delete(table))
    session.flush()


def _restore_with_self_fk_retry(session: Session, table: Table, rows: list[dict[str, object]]) -> None:
    pending = list(rows)
    while pending:
        next_pending: list[dict[str, object]] = []
        progress = 0
        for row in pending:
            try:
                with session.begin_nested():
                    session.execute(table.insert().values(**row))
            except IntegrityError:
                next_pending.append(row)
            else:
                progress += 1
        if not next_pending:
            return
        if progress == 0:
            raise ValueError(f"Could not restore self-referential rows for table {table.name}")
        pending = next_pending


def _insert_rows(session: Session, table: Table, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    has_self_fk = any(
        fk.column.table.name == table.name
        for column in table.columns
        for fk in column.foreign_keys
    )
    if has_self_fk:
        _restore_with_self_fk_retry(session, table, rows)
    else:
        session.execute(table.insert(), rows)


def restore_bundle(request: RestoreBundleRequest, session: Session) -> RestoreBundleResult:
    """Restore a portable export bundle into the connected database."""

    _parse_non_empty(request.actor, field_name="actor")
    _parse_non_empty(request.reason, field_name="reason")
    bundle_dir = _normalize_path(request.bundle_dir, field_name="bundle_dir")
    if not bundle_dir.exists() or not bundle_dir.is_dir():
        raise ValueError(f"bundle_dir does not exist: {bundle_dir}")

    manifest_path = bundle_dir / "manifest.json"
    manifest = _load_json(manifest_path)
    if manifest.get("checksum_algorithm") != CHECKSUM_ALGORITHM:
        raise ValueError(f"Unsupported checksum algorithm: {manifest.get('checksum_algorithm')}")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("manifest.artifacts must be a non-empty list")

    artifact_map: dict[str, dict[str, object]] = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ValueError("manifest.artifacts entries must be objects")
        path = artifact.get("path")
        checksum = artifact.get("sha256")
        if not isinstance(path, str) or not path:
            raise ValueError("manifest artifact path is required")
        if not isinstance(checksum, str) or not checksum:
            raise ValueError(f"manifest artifact checksum missing for: {path}")
        artifact_map[path] = artifact

    diagnostics_rel_path = manifest.get("diagnostics_path")
    if not isinstance(diagnostics_rel_path, str) or not diagnostics_rel_path:
        raise ValueError("manifest.diagnostics_path is required")

    expected_paths = {f"json/{table.name}.jsonl" for table in _table_order()}
    expected_paths.add("csv/transactions.csv")
    expected_paths.add(diagnostics_rel_path)
    missing_from_manifest = sorted(expected_paths - set(artifact_map))
    if missing_from_manifest:
        raise ValueError(
            "Bundle is missing required artifact entries: "
            + ", ".join(missing_from_manifest)
        )

    for relative_path, artifact in artifact_map.items():
        artifact_path = bundle_dir / relative_path
        if not artifact_path.exists():
            raise ValueError(f"Missing bundle artifact file: {relative_path}")
        computed = _sha256_file(artifact_path)
        if computed != artifact["sha256"]:
            raise ValueError(f"Checksum mismatch for bundle artifact: {relative_path}")

    diagnostics_path = bundle_dir / diagnostics_rel_path
    diagnostics = _load_json(diagnostics_path)
    expected_counts = diagnostics.get("table_row_counts")
    if not isinstance(expected_counts, dict):
        raise ValueError("diagnostics.table_row_counts is required")

    bind = session.get_bind()
    Base.metadata.create_all(bind)

    current_counts = _current_table_counts(session)
    populated_tables = sorted([table_name for table_name, count in current_counts.items() if count > 0])
    if populated_tables and not request.allow_non_empty:
        raise ValueError(
            "Target database is not empty; set allow_non_empty=True to override. "
            f"Populated tables: {', '.join(populated_tables)}"
        )
    if populated_tables and request.allow_non_empty:
        _clear_tables(session)

    for table in _table_order():
        table_path = bundle_dir / "json" / f"{table.name}.jsonl"
        rows = _read_jsonl_table_rows(table_path, table)
        _insert_rows(session, table, rows)
    session.flush()

    restored_counts = _current_table_counts(session)
    for table_name, expected in expected_counts.items():
        if not isinstance(expected, int):
            raise ValueError(f"Invalid expected count for table {table_name}")
        actual = restored_counts.get(table_name)
        if actual != expected:
            raise ValueError(
                f"Row count mismatch after restore for {table_name}: expected={expected} actual={actual}"
            )

    return RestoreBundleResult(
        bundle_dir=bundle_dir,
        manifest_path=manifest_path,
        db_schema_revision=manifest.get("db_schema_revision")
        if isinstance(manifest.get("db_schema_revision"), str) or manifest.get("db_schema_revision") is None
        else None,
        restored_table_counts=restored_counts,
        validated_files=sorted(artifact_map.keys()),
    )
