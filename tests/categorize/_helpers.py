from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import Account, Category, Merchant, Transaction
from finance_analysis_agent.utils.time import utcnow


def _seed_account(session: Session) -> None:
    session.add(Account(id="acct-1", name="Checking", type="checking", currency="USD"))


def _seed_category(session: Session, category_id: str, name: str) -> None:
    session.add(
        Category(
            id=category_id,
            parent_id=None,
            name=name,
            system_flag=False,
            active=True,
            created_at=utcnow(),
        )
    )


def _seed_merchant(session: Session, merchant_id: str, canonical_name: str) -> None:
    session.add(
        Merchant(
            id=merchant_id,
            canonical_name=canonical_name,
            confidence=1.0,
            created_at=utcnow(),
        )
    )


def _seed_transaction(
    session: Session,
    *,
    transaction_id: str,
    posted_date: date,
    original_statement: str,
    merchant_id: str | None,
    category_id: str | None,
    pending_status: str = "posted",
    amount: Decimal = Decimal("10.00"),
) -> None:
    created_at = utcnow()
    session.add(
        Transaction(
            id=transaction_id,
            account_id="acct-1",
            posted_date=posted_date,
            effective_date=posted_date,
            amount=amount,
            currency="USD",
            original_amount=amount,
            original_currency="USD",
            pending_status=pending_status,
            original_statement=original_statement,
            merchant_id=merchant_id,
            category_id=category_id,
            excluded=False,
            notes=None,
            source_kind="manual",
            source_transaction_id=f"src-{transaction_id}",
            import_batch_id=None,
            transfer_group_id=None,
            created_at=created_at,
            updated_at=created_at,
        )
    )
