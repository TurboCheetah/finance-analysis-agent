from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from finance_analysis_agent.db.models import (
    Account,
    BalanceSnapshot,
    Category,
    Merchant,
    Statement,
    Transaction,
)
from finance_analysis_agent.utils.time import utcnow
from tests.backup.helpers import create_database as create_database_with_migrations

ARTIFACTS_ENV_VAR = "E2E_ARTIFACTS_DIR"


def create_e2e_database(tmp_path: Path, *, filename: str) -> str:
    return create_database_with_migrations(tmp_path, filename=filename)


@contextmanager
def session_for_database(database_url: str) -> Iterator[Session]:
    engine = create_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session: Session = session_factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def persist_artifact(name: str, source_path: Path) -> None:
    artifact_root = os.environ.get(ARTIFACTS_ENV_VAR)
    if not artifact_root or not source_path.exists():
        return

    destination = Path(artifact_root) / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source_path.is_dir():
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source_path, destination)
        return

    shutil.copy2(source_path, destination)


def write_json_artifact(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_report_hashes(output_path: Path) -> dict[str, str]:
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    return {item["report_type"]: item["payload_hash"] for item in payload["reports"]}


def seed_account(
    session: Session,
    *,
    account_id: str,
    name: str,
    account_type: str = "checking",
    currency: str = "USD",
) -> None:
    session.add(Account(id=account_id, name=name, type=account_type, currency=currency))


def seed_category(
    session: Session,
    *,
    category_id: str,
    name: str,
    parent_id: str | None = None,
) -> None:
    session.add(
        Category(
            id=category_id,
            parent_id=parent_id,
            name=name,
            system_flag=False,
            active=True,
            created_at=utcnow(),
        )
    )


def seed_merchant(
    session: Session,
    *,
    merchant_id: str,
    canonical_name: str,
    confidence: float = 1.0,
) -> None:
    session.add(
        Merchant(
            id=merchant_id,
            canonical_name=canonical_name,
            confidence=confidence,
            created_at=utcnow(),
        )
    )


def seed_statement(
    session: Session,
    *,
    statement_id: str,
    account_id: str,
    period_start: date,
    period_end: date,
    ending_balance: Decimal | str,
    source_type: str = "pdf",
    source_fingerprint: str | None = None,
    status: str = "parsed",
) -> None:
    session.add(
        Statement(
            id=statement_id,
            account_id=account_id,
            source_type=source_type,
            source_fingerprint=source_fingerprint or f"{statement_id}-fingerprint",
            period_start=period_start,
            period_end=period_end,
            ending_balance=Decimal(str(ending_balance)),
            currency="USD",
            status=status,
            diagnostics_json={"seeded": True},
            created_at=utcnow(),
        )
    )


def seed_balance_snapshot(
    session: Session,
    *,
    snapshot_id: str,
    account_id: str,
    snapshot_date: date,
    balance: Decimal | str,
    source: str = "statement",
    statement_id: str | None = None,
) -> None:
    session.add(
        BalanceSnapshot(
            id=snapshot_id,
            account_id=account_id,
            snapshot_date=snapshot_date,
            balance=Decimal(str(balance)),
            source=source,
            statement_id=statement_id,
            created_at=utcnow(),
        )
    )


def seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    account_id: str,
    posted_date: date,
    amount: Decimal | str,
    currency: str = "USD",
    pending_status: str = "posted",
    original_statement: str = "seed transaction",
    source_kind: str = "manual",
    source_transaction_id: str | None = None,
    effective_date: date | None = None,
    original_amount: Decimal | str | None = None,
    original_currency: str | None = None,
    merchant_id: str | None = None,
    category_id: str | None = None,
    excluded: bool = False,
    notes: str | None = None,
) -> None:
    now = utcnow()
    decimal_amount = Decimal(str(amount))
    session.add(
        Transaction(
            id=transaction_id,
            account_id=account_id,
            posted_date=posted_date,
            effective_date=effective_date or posted_date,
            amount=decimal_amount,
            currency=currency,
            original_amount=(
                Decimal(str(original_amount)) if original_amount is not None else decimal_amount
            ),
            original_currency=original_currency or currency,
            pending_status=pending_status,
            original_statement=original_statement,
            merchant_id=merchant_id,
            category_id=category_id,
            excluded=excluded,
            notes=notes,
            source_kind=source_kind,
            source_transaction_id=source_transaction_id or f"src-{transaction_id}",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=now,
            updated_at=now,
        )
    )
