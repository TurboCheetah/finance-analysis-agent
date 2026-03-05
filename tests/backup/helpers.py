from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from finance_analysis_agent.db.models import (
    Account,
    BalanceSnapshot,
    Budget,
    BudgetAllocation,
    BudgetBucket,
    BudgetBucketCategoryMapping,
    BudgetBucketDefinition,
    BudgetCategory,
    BudgetPeriod,
    BudgetRollover,
    BudgetTarget,
    Category,
    DedupeCandidate,
    DedupeCandidateEvent,
    Goal,
    GoalAllocation,
    GoalEvent,
    ImportBatch,
    ImportBatchStatusEvent,
    Merchant,
    MerchantAlias,
    RawTransaction,
    Reconciliation,
    Recurring,
    RecurringEvent,
    Report,
    ReviewItem,
    ReviewItemEvent,
    Rule,
    RuleAudit,
    RuleRun,
    RunMetadata,
    Statement,
    Tag,
    Transaction,
    TransactionEvent,
    TransactionSplit,
    TransactionTag,
)
from finance_analysis_agent.utils.time import utcnow
from tests.helpers import alembic_config


def create_database(tmp_path: Path, *, filename: str) -> str:
    database_file = tmp_path / filename
    database_url = f"sqlite:///{database_file}"
    command.upgrade(alembic_config(database_url), "head")
    return database_url


def seed_backup_fixture(session: Session) -> None:
    now = utcnow()

    session.add(
        Account(
            id="acct-main",
            name="Main Checking",
            type="checking",
            currency="USD",
            institution="Main Bank",
            opened_at=now,
            closed_at=None,
            metadata_json={"tier": "gold"},
        )
    )
    session.add(
        Statement(
            id="stmt-1",
            account_id="acct-main",
            source_type="pdf",
            source_fingerprint="stmt-fingerprint",
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            ending_balance=Decimal("974.50"),
            currency="USD",
            status="parsed",
            diagnostics_json={"pages": 2},
            created_at=now,
        )
    )
    session.add(
        ImportBatch(
            id="batch-1",
            source_type="pdf",
            source_ref="fixture.pdf",
            source_fingerprint="batch-fingerprint",
            fingerprint_algo="sha256",
            schema_version="1.0.0",
            conflict_mode="normal",
            override_reason=None,
            override_of_batch_id=None,
            status="finalized",
            received_at=now,
            finalized_at=now,
            error_summary=None,
        )
    )
    session.add(
        ImportBatchStatusEvent(
            id="batch-event-1",
            batch_id="batch-1",
            from_status=None,
            to_status="finalized",
            reason="fixture",
            actor="tester",
            changed_at=now,
        )
    )
    session.add(
        RawTransaction(
            id="raw-1",
            import_batch_id="batch-1",
            raw_payload_json={"line": "GROCERY STORE -25.50"},
            page_no=1,
            row_no=1,
            extraction_confidence=0.98,
            parse_status="parsed",
            error_code=None,
        )
    )

    session.add(
        Merchant(
            id="merchant-1",
            canonical_name="Neighborhood Grocery",
            confidence=0.99,
            created_at=now,
        )
    )
    session.add(
        MerchantAlias(
            id="merchant-alias-1",
            merchant_id="merchant-1",
            alias="NEIGHBORHOOD GROCERY",
            source_context="statement",
            confidence=0.92,
            created_at=now,
        )
    )
    session.add(
        Category(
            id="cat-root",
            parent_id=None,
            name="Food",
            system_flag=False,
            active=True,
            created_at=now,
        )
    )
    session.add(
        Category(
            id="cat-grocery",
            parent_id="cat-root",
            name="Groceries",
            system_flag=False,
            active=True,
            created_at=now,
        )
    )
    session.add(Tag(id="tag-essential", name="Essential", created_at=now))

    session.add(
        Goal(
            id="goal-emergency",
            name="Emergency Fund",
            target_amount=Decimal("10000.00"),
            target_date=date(2027, 12, 31),
            monthly_contribution=Decimal("500.00"),
            spending_reduces_progress=False,
            status="active",
            metadata_json={"priority": 1},
        )
    )

    session.add(
        Transaction(
            id="txn-1",
            account_id="acct-main",
            posted_date=date(2026, 2, 10),
            effective_date=date(2026, 2, 10),
            amount=Decimal("-25.50"),
            currency="USD",
            original_amount=Decimal("-25.50"),
            original_currency="USD",
            pending_status="posted",
            original_statement="NEIGHBORHOOD GROCERY",
            merchant_id="merchant-1",
            category_id="cat-grocery",
            excluded=False,
            notes="Weekly groceries",
            source_kind="statement",
            source_transaction_id="src-1",
            import_batch_id="batch-1",
            transfer_group_id=None,
            created_at=now,
            updated_at=now,
        )
    )
    session.add(
        Transaction(
            id="txn-2",
            account_id="acct-main",
            posted_date=date(2026, 2, 11),
            effective_date=date(2026, 2, 11),
            amount=Decimal("-25.50"),
            currency="USD",
            original_amount=Decimal("-25.50"),
            original_currency="USD",
            pending_status="posted",
            original_statement="NEIGHBORHOOD GROCERY DUP",
            merchant_id="merchant-1",
            category_id="cat-grocery",
            excluded=False,
            notes=None,
            source_kind="statement",
            source_transaction_id="src-2",
            import_batch_id="batch-1",
            transfer_group_id=None,
            created_at=now,
            updated_at=now,
        )
    )
    session.add(
        TransactionEvent(
            id="txn-event-1",
            transaction_id="txn-1",
            event_type="created",
            old_value_json=None,
            new_value_json={"category_id": "cat-grocery"},
            reason="fixture",
            actor="tester",
            provenance="manual",
            created_at=now,
        )
    )
    session.add(TransactionTag(transaction_id="txn-1", tag_id="tag-essential"))
    session.add(
        TransactionSplit(
            id="txn-split-1",
            transaction_id="txn-1",
            line_no=1,
            category_id="cat-grocery",
            amount=Decimal("-25.50"),
            memo="split memo",
            goal_id="goal-emergency",
        )
    )

    session.add(
        Rule(
            id="rule-1",
            name="Groceries rule",
            priority=1,
            enabled=True,
            apply_to_pending=False,
            matcher_json={"merchant_contains": "grocery"},
            action_json={"set_category_id": "cat-grocery"},
            created_at=now,
            updated_at=now,
        )
    )
    session.add(
        RuleRun(
            id="rule-run-1",
            rule_id="rule-1",
            run_mode="apply",
            dry_run=False,
            started_at=now,
            completed_at=now,
            summary_json={"matched": 1},
        )
    )
    session.add(
        RuleAudit(
            id="rule-audit-1",
            rule_run_id="rule-run-1",
            transaction_id="txn-1",
            matched=True,
            changes_json={"category_id": "cat-grocery"},
            confidence=0.97,
        )
    )

    session.add(
        DedupeCandidate(
            id="dedupe-1",
            txn_a_id="txn-1",
            txn_b_id="txn-2",
            score=0.95,
            decision="review",
            reason_json={"reason": "same amount/date"},
            created_at=now,
            decided_at=None,
        )
    )
    session.add(
        DedupeCandidateEvent(
            id="dedupe-event-1",
            dedupe_candidate_id="dedupe-1",
            event_type="created",
            old_value_json=None,
            new_value_json={"decision": "review"},
            reason="fixture",
            actor="tester",
            created_at=now,
        )
    )

    session.add(
        ReviewItem(
            id="review-1",
            item_type="dedupe_candidate_suggestion",
            ref_table="dedupe_candidates",
            ref_id="dedupe-1",
            reason_code="dedupe.needs_review",
            confidence=0.95,
            status="to_review",
            source="dedupe",
            assigned_to=None,
            payload_json={"candidate_id": "dedupe-1"},
            created_at=now,
            resolved_at=None,
        )
    )
    session.add(
        ReviewItemEvent(
            id="review-event-1",
            review_item_id="review-1",
            event_type="created",
            action="queue",
            from_status=None,
            to_status="to_review",
            actor="tester",
            reason="fixture",
            metadata_json={"source": "dedupe"},
            created_at=now,
        )
    )

    session.add(
        BalanceSnapshot(
            id="snap-1",
            account_id="acct-main",
            snapshot_date=date(2026, 2, 28),
            balance=Decimal("974.50"),
            source="statement",
            statement_id="stmt-1",
            created_at=now,
        )
    )
    session.add(
        Reconciliation(
            id="recon-1",
            account_id="acct-main",
            statement_id="stmt-1",
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            expected_balance=Decimal("974.50"),
            computed_balance=Decimal("974.50"),
            delta=Decimal("0.00"),
            match_rate=1.0,
            trust_score=0.99,
            unresolved_count=0,
            adjustment_magnitude=Decimal("0.00"),
            details_json={"matched_count": 1},
            approved_adjustment_txn_id=None,
            approved_by=None,
            approved_at=None,
            status="matched",
            created_at=now,
        )
    )

    session.add(
        Budget(
            id="budget-1",
            name="Main Budget",
            method="zero_based",
            base_currency="USD",
            active=True,
            created_at=now,
        )
    )
    session.add(
        BudgetPeriod(
            id="budget-period-1",
            budget_id="budget-1",
            period_month="2026-02",
            to_assign=Decimal("0.00"),
            assigned_total=Decimal("300.00"),
            spent_total=Decimal("25.50"),
            rollover_total=Decimal("20.00"),
            status="open",
        )
    )
    session.add(
        BudgetCategory(
            id="budget-category-1",
            budget_id="budget-1",
            category_id="cat-grocery",
            policy_json={"rollover": "carry_positive"},
            rollover_policy="carry_positive",
        )
    )
    session.add(
        BudgetTarget(
            id="budget-target-1",
            budget_category_id="budget-category-1",
            target_type="monthly",
            amount=Decimal("300.00"),
            cadence="monthly",
            top_up=True,
            snoozed_until=None,
            metadata_json={"priority": "high"},
        )
    )
    session.add(
        BudgetAllocation(
            id="budget-allocation-1",
            budget_period_id="budget-period-1",
            budget_category_id="budget-category-1",
            assigned_amount=Decimal("300.00"),
            source="manual",
        )
    )
    session.add(
        BudgetBucketDefinition(
            id="bucket-def-1",
            budget_id="budget-1",
            bucket_key="flex",
            name="Flex",
            rollover_policy="carry_positive",
        )
    )
    session.add(
        BudgetBucketCategoryMapping(
            id="bucket-map-1",
            bucket_definition_id="bucket-def-1",
            budget_category_id="budget-category-1",
        )
    )
    session.add(
        BudgetBucket(
            id="bucket-1",
            budget_id="budget-1",
            bucket_definition_id="bucket-def-1",
            period_month="2026-02",
            bucket_name="Flex",
            planned_amount=Decimal("300.00"),
            actual_amount=Decimal("25.50"),
            rollover_policy="carry_positive",
        )
    )
    session.add(
        BudgetRollover(
            id="rollover-1",
            budget_id="budget-1",
            dimension_type="category",
            dimension_id="budget-category-1",
            from_period="2026-01",
            to_period="2026-02",
            carry_amount=Decimal("20.00"),
            policy_applied="carry_positive",
        )
    )

    session.add(
        Recurring(
            id="recurring-1",
            merchant_id="merchant-1",
            category_id=None,
            schedule_type="monthly",
            interval_n=1,
            anchor_date=date(2026, 1, 1),
            tolerance_days=3,
            active=True,
            metadata_json={"kind": "subscription"},
        )
    )
    session.add(
        RecurringEvent(
            id="recurring-event-1",
            recurring_id="recurring-1",
            expected_date=date(2026, 2, 28),
            observed_transaction_id="txn-1",
            status="observed",
        )
    )

    session.add(
        GoalAllocation(
            id="goal-allocation-1",
            goal_id="goal-emergency",
            account_id="acct-main",
            period_month="2026-02",
            amount=Decimal("200.00"),
            allocation_type="manual",
            created_at=now,
        )
    )
    session.add(
        GoalEvent(
            id="goal-event-1",
            goal_id="goal-emergency",
            event_date=date(2026, 2, 10),
            event_type="allocation",
            amount=Decimal("200.00"),
            related_transaction_id="txn-1",
            metadata_json={"origin": "fixture"},
        )
    )

    session.add(
        RunMetadata(
            id="run-1",
            pipeline_name="reporting_generate",
            code_version="reporting-generate-v1",
            schema_version="1.0.0",
            config_hash="cfg-hash",
            started_at=now,
            completed_at=now,
            status="success",
            diagnostics_json={"report_count": 1},
        )
    )
    session.add(
        Report(
            id="report-1",
            report_type="cash_flow",
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            generated_at=now,
            payload_json={"summary": {"net": "974.50"}},
            run_id="run-1",
        )
    )


def create_seeded_database(tmp_path: Path, *, filename: str) -> str:
    database_url = create_database(tmp_path, filename=filename)
    engine = create_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session: Session = session_factory()
    try:
        seed_backup_fixture(session)
        session.commit()
    finally:
        session.close()
        engine.dispose()
    return database_url
