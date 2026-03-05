from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from finance_analysis_agent.backup import ExportBundleRequest, RestoreBundleRequest, export_bundle, restore_bundle
from finance_analysis_agent.db.base import Base
from finance_analysis_agent.db.models import Account
from tests.backup.helpers import create_database, seed_backup_fixture


def _serialize_value(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _table_snapshot(session: Session, table_name: str) -> list[dict[str, object]]:
    table = Base.metadata.tables[table_name]
    rows = session.execute(select(table)).mappings().all()
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                column.name: _serialize_value(row[column.name])
                for column in table.columns
            }
        )
    normalized.sort(key=lambda item: json.dumps(item, sort_keys=True))
    return normalized


def _open_session(database_url: str) -> tuple[Session, object]:
    engine = create_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    return session_factory(), engine


def test_export_bundle_writes_expected_artifacts_and_canonical_csv(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_a = tmp_path / "bundle-a"
    result_a = export_bundle(
        ExportBundleRequest(
            actor="tester",
            reason="service export",
            output_dir=bundle_a,
        ),
        db_session,
    )
    bundle_b = tmp_path / "bundle-b"
    result_b = export_bundle(
        ExportBundleRequest(
            actor="tester",
            reason="service export repeat",
            output_dir=bundle_b,
        ),
        db_session,
    )

    assert (bundle_a / "manifest.json").exists()
    assert (bundle_a / "diagnostics.json").exists()
    assert (bundle_a / "csv" / "transactions.csv").exists()
    assert (bundle_a / "json" / "accounts.jsonl").exists()
    assert (bundle_a / "json" / "run_metadata.jsonl").exists()

    manifest = json.loads((bundle_a / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["checksum_algorithm"] == "sha256"
    artifact_paths = {item["path"] for item in manifest["artifacts"]}
    assert "csv/transactions.csv" in artifact_paths
    assert "json/accounts.jsonl" in artifact_paths
    assert "diagnostics.json" in artifact_paths

    with (bundle_a / "csv" / "transactions.csv").open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames is not None
        assert reader.fieldnames[0] == "transaction_id"
        assert reader.fieldnames[-1] == "updated_at"
        rows = list(reader)
    assert len(rows) == 2
    assert rows[0]["category"] == "Groceries"
    assert rows[0]["parent_category"] == "Food"
    assert rows[0]["tags"] == "Essential"

    stable_paths = sorted([path for path in result_a.file_checksums if path.startswith("json/") or path.startswith("csv/")])
    assert stable_paths
    assert {path: result_a.file_checksums[path] for path in stable_paths} == {
        path: result_b.file_checksums[path] for path in stable_paths
    }


def test_restore_bundle_detects_tampered_checksum(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_dir = tmp_path / "bundle"
    export_bundle(
        ExportBundleRequest(actor="tester", reason="tamper test", output_dir=bundle_dir),
        db_session,
    )
    with (bundle_dir / "json" / "accounts.jsonl").open("a", encoding="utf-8") as handle:
        handle.write("\n")

    restore_url = f"sqlite:///{tmp_path / 'restore_tamper.db'}"
    restore_session, restore_engine = _open_session(restore_url)
    try:
        with pytest.raises(ValueError, match="Checksum mismatch"):
            restore_bundle(
                RestoreBundleRequest(
                    actor="tester",
                    reason="restore tampered",
                    bundle_dir=bundle_dir,
                ),
                restore_session,
            )
    finally:
        restore_session.close()
        restore_engine.dispose()


def test_restore_bundle_requires_required_files(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_dir = tmp_path / "bundle"
    export_bundle(
        ExportBundleRequest(actor="tester", reason="missing file test", output_dir=bundle_dir),
        db_session,
    )
    (bundle_dir / "csv" / "transactions.csv").unlink()

    restore_url = f"sqlite:///{tmp_path / 'restore_missing.db'}"
    restore_session, restore_engine = _open_session(restore_url)
    try:
        with pytest.raises(ValueError, match="Missing bundle artifact file"):
            restore_bundle(
                RestoreBundleRequest(
                    actor="tester",
                    reason="restore missing",
                    bundle_dir=bundle_dir,
                ),
                restore_session,
            )
    finally:
        restore_session.close()
        restore_engine.dispose()


def test_restore_bundle_rejects_unsupported_schema_versions(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_dir = tmp_path / "bundle"
    export_bundle(
        ExportBundleRequest(actor="tester", reason="schema version test", output_dir=bundle_dir),
        db_session,
    )

    manifest_path = bundle_dir / "manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_payload["bundle_schema_version"] = "9.9.9"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    restore_url = f"sqlite:///{tmp_path / 'restore_schema.db'}"
    restore_session, restore_engine = _open_session(restore_url)
    try:
        with pytest.raises(ValueError, match="Unsupported bundle schema version"):
            restore_bundle(
                RestoreBundleRequest(
                    actor="tester",
                    reason="restore schema mismatch",
                    bundle_dir=bundle_dir,
                ),
                restore_session,
            )
    finally:
        restore_session.close()
        restore_engine.dispose()


def test_restore_bundle_rejects_path_traversal_artifact_entries(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_dir = tmp_path / "bundle"
    export_bundle(
        ExportBundleRequest(actor="tester", reason="path traversal test", output_dir=bundle_dir),
        db_session,
    )

    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("outside", encoding="utf-8")

    manifest_path = bundle_dir / "manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_payload["artifacts"].append(
        {
            "path": "../outside.txt",
            "sha256": hashlib.sha256(outside_file.read_bytes()).hexdigest(),
            "size_bytes": outside_file.stat().st_size,
        }
    )
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    restore_url = f"sqlite:///{tmp_path / 'restore_traversal.db'}"
    restore_session, restore_engine = _open_session(restore_url)
    try:
        with pytest.raises(ValueError, match="Invalid bundle artifact path"):
            restore_bundle(
                RestoreBundleRequest(
                    actor="tester",
                    reason="restore traversal",
                    bundle_dir=bundle_dir,
                ),
                restore_session,
            )
    finally:
        restore_session.close()
        restore_engine.dispose()


def test_restore_bundle_enforces_fresh_target_by_default(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_dir = tmp_path / "bundle"
    export_bundle(
        ExportBundleRequest(actor="tester", reason="fresh-target test", output_dir=bundle_dir),
        db_session,
    )

    populated_url = create_database(tmp_path, filename="restore_populated.db")
    populated_session, populated_engine = _open_session(populated_url)
    try:
        populated_session.add(Account(id="acct-existing", name="Existing", type="checking", currency="USD"))
        populated_session.commit()

        with pytest.raises(ValueError, match="not empty"):
            restore_bundle(
                RestoreBundleRequest(
                    actor="tester",
                    reason="should fail",
                    bundle_dir=bundle_dir,
                ),
                populated_session,
            )

        result = restore_bundle(
            RestoreBundleRequest(
                actor="tester",
                reason="override",
                bundle_dir=bundle_dir,
                allow_non_empty=True,
            ),
            populated_session,
        )
        populated_session.commit()

        assert result.restored_table_counts["accounts"] == 1
        restored_accounts = populated_session.execute(
            select(Account.id).order_by(Account.id.asc())
        ).scalars().all()
        assert restored_accounts == ["acct-main"]
    finally:
        populated_session.close()
        populated_engine.dispose()


def test_restore_bundle_round_trip_equivalence(db_session: Session, tmp_path: Path) -> None:
    seed_backup_fixture(db_session)
    db_session.commit()

    bundle_dir = tmp_path / "bundle"
    export_result = export_bundle(
        ExportBundleRequest(actor="tester", reason="round trip", output_dir=bundle_dir),
        db_session,
    )

    restore_url = f"sqlite:///{tmp_path / 'restore_roundtrip.db'}"
    restore_session, restore_engine = _open_session(restore_url)
    try:
        restore_result = restore_bundle(
            RestoreBundleRequest(
                actor="tester",
                reason="round trip",
                bundle_dir=bundle_dir,
            ),
            restore_session,
        )
        restore_session.commit()

        assert restore_result.db_schema_revision == export_result.db_schema_revision
        assert restore_result.restored_table_counts == export_result.table_row_counts

        for table_name in sorted(Base.metadata.tables.keys()):
            assert _table_snapshot(db_session, table_name) == _table_snapshot(restore_session, table_name)
    finally:
        restore_session.close()
        restore_engine.dispose()
