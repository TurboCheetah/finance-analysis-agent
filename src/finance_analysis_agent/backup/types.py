"""Typed contracts for backup export/restore workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class ExportBundleRequest:
    actor: str
    reason: str
    output_dir: Path | str
    overwrite: bool = False


@dataclass(slots=True)
class ExportBundleResult:
    output_dir: Path
    manifest_path: Path
    diagnostics_path: Path
    db_schema_revision: str | None
    table_row_counts: dict[str, int] = field(default_factory=dict)
    file_checksums: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class RestoreBundleRequest:
    actor: str
    reason: str
    bundle_dir: Path | str
    allow_non_empty: bool = False


@dataclass(slots=True)
class RestoreBundleResult:
    bundle_dir: Path
    manifest_path: Path
    db_schema_revision: str | None
    restored_table_counts: dict[str, int] = field(default_factory=dict)
    validated_files: list[str] = field(default_factory=list)
