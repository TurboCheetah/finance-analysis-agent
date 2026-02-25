from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

from sqlalchemy.orm import Session

from finance_analysis_agent.db.models import (
    Account,
    Category,
    Goal,
    Merchant,
    ReviewItem,
    Rule,
    Tag,
    Transaction,
    TransactionTag,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow


def seed_rules_baseline(session: Session) -> dict[str, str]:
    session.add_all(
        [
            Account(id="acct-1", name="Checking", type="checking", currency="USD"),
            Account(id="acct-2", name="Card", type="credit", currency="USD"),
            Category(
                id="cat-food",
                parent_id=None,
                name="Food",
                system_flag=False,
                active=True,
                created_at=utcnow(),
            ),
            Category(
                id="cat-coffee",
                parent_id=None,
                name="Coffee",
                system_flag=False,
                active=True,
                created_at=utcnow(),
            ),
            Category(
                id="cat-travel",
                parent_id=None,
                name="Travel",
                system_flag=False,
                active=True,
                created_at=utcnow(),
            ),
            Merchant(id="mer-coffee", canonical_name="Coffee Shop", confidence=1.0, created_at=utcnow()),
            Merchant(id="mer-market", canonical_name="Market", confidence=1.0, created_at=utcnow()),
            Goal(
                id="goal-1",
                name="Vacation",
                target_amount=Decimal("1000.00"),
                target_date=None,
                monthly_contribution=None,
                spending_reduces_progress=False,
                status="active",
                metadata_json=None,
            ),
            Tag(id="tag-existing", name="existing", created_at=utcnow()),
        ]
    )
    session.add_all(
        [
            Transaction(
                id="txn-posted-a",
                account_id="acct-1",
                posted_date=date(2026, 1, 5),
                effective_date=date(2026, 1, 5),
                amount=Decimal("5.50"),
                currency="USD",
                original_amount=Decimal("5.50"),
                original_currency="USD",
                pending_status="posted",
                original_statement="COFFEE SHOP #123",
                merchant_id="mer-coffee",
                category_id="cat-food",
                excluded=False,
                notes=None,
                source_kind="manual",
                source_transaction_id="seed-a",
                import_batch_id=None,
                transfer_group_id=None,
                created_at=utcnow(),
                updated_at=utcnow(),
            ),
            Transaction(
                id="txn-pending-a",
                account_id="acct-1",
                posted_date=date(2026, 1, 6),
                effective_date=date(2026, 1, 6),
                amount=Decimal("3.50"),
                currency="USD",
                original_amount=Decimal("3.50"),
                original_currency="USD",
                pending_status="pending",
                original_statement="COFFEE SHOP PENDING",
                merchant_id="mer-coffee",
                category_id="cat-food",
                excluded=False,
                notes=None,
                source_kind="manual",
                source_transaction_id="seed-b",
                import_batch_id=None,
                transfer_group_id=None,
                created_at=utcnow(),
                updated_at=utcnow(),
            ),
            Transaction(
                id="txn-posted-b",
                account_id="acct-2",
                posted_date=date(2026, 2, 10),
                effective_date=date(2026, 2, 10),
                amount=Decimal("100.00"),
                currency="USD",
                original_amount=Decimal("100.00"),
                original_currency="USD",
                pending_status="posted",
                original_statement="MARKET #987",
                merchant_id="mer-market",
                category_id=None,
                excluded=False,
                notes=None,
                source_kind="manual",
                source_transaction_id="seed-c",
                import_batch_id=None,
                transfer_group_id=None,
                created_at=utcnow(),
                updated_at=utcnow(),
            ),
        ]
    )
    session.add(TransactionTag(transaction_id="txn-posted-a", tag_id="tag-existing"))
    session.commit()
    return {
        "txn_posted_a": "txn-posted-a",
        "txn_pending_a": "txn-pending-a",
        "txn_posted_b": "txn-posted-b",
        "goal_1": "goal-1",
    }


def add_rule(
    session: Session,
    *,
    rule_id: str,
    priority: int,
    matcher_json: dict[str, object],
    action_json: dict[str, object],
    apply_to_pending: bool = False,
) -> Rule:
    rule = Rule(
        id=rule_id,
        name=f"Rule {rule_id}",
        priority=priority,
        enabled=True,
        apply_to_pending=apply_to_pending,
        matcher_json=matcher_json,
        action_json=action_json,
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    session.add(rule)
    session.commit()
    return rule


def add_open_rule_review(
    session: Session,
    *,
    transaction_id: str,
    review_id: str | None = None,
) -> None:
    resolved_review_id = review_id or str(uuid4())
    session.add(
        ReviewItem(
            id=resolved_review_id,
            item_type="transaction_rule",
            ref_table="transactions",
            ref_id=transaction_id,
            reason_code="rule.needs_review",
            confidence=None,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.RULES.value,
            assigned_to=None,
            payload_json={"seed": True},
            created_at=utcnow(),
            resolved_at=None,
        )
    )
    session.commit()
