"""Canonical SQLite schema models for the Personal Finance OS ledger."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import JSON, Boolean, CheckConstraint, Date, DateTime, ForeignKey, Index, Numeric, String, Text
from sqlalchemy import UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from finance_analysis_agent.db.base import Base


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (
        Index("ix_accounts_type", "type"),
        Index("ix_accounts_currency", "currency"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)
    institution: Mapped[str | None] = mapped_column(String)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class Statement(Base):
    __tablename__ = "statements"
    __table_args__ = (
        UniqueConstraint("source_fingerprint", name="uq_statements_source_fingerprint"),
        Index("ix_statements_account_id_period_end", "account_id", "period_end"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    source_fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    ending_balance: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    diagnostics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ImportBatch(Base):
    __tablename__ = "import_batches"
    __table_args__ = (
        Index(
            "ix_import_batches_source_type_source_fingerprint_received_at",
            "source_type",
            "source_fingerprint",
            "received_at",
        ),
        Index("ix_import_batches_status", "status"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String)
    source_fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    fingerprint_algo: Mapped[str] = mapped_column(String, nullable=False, default="sha256")
    schema_version: Mapped[str] = mapped_column(String, nullable=False)
    conflict_mode: Mapped[str] = mapped_column(String, nullable=False, default="normal")
    override_reason: Mapped[str | None] = mapped_column(Text)
    override_of_batch_id: Mapped[str | None] = mapped_column(ForeignKey("import_batches.id"))
    status: Mapped[str] = mapped_column(String, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime)
    error_summary: Mapped[str | None] = mapped_column(Text)


class ImportBatchStatusEvent(Base):
    __tablename__ = "import_batch_status_events"
    __table_args__ = (
        Index("ix_import_batch_status_events_batch_id_changed_at", "batch_id", "changed_at"),
        Index("ix_import_batch_status_events_to_status_changed_at", "to_status", "changed_at"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    batch_id: Mapped[str] = mapped_column(ForeignKey("import_batches.id"), nullable=False)
    from_status: Mapped[str | None] = mapped_column(String)
    to_status: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    actor: Mapped[str | None] = mapped_column(String)
    changed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class RawTransaction(Base):
    __tablename__ = "raw_transactions"
    __table_args__ = (
        Index("ix_raw_transactions_import_batch_id", "import_batch_id"),
        Index("ix_raw_transactions_parse_status", "parse_status"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    import_batch_id: Mapped[str] = mapped_column(ForeignKey("import_batches.id"), nullable=False)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    page_no: Mapped[int | None] = mapped_column(nullable=True)
    row_no: Mapped[int | None] = mapped_column(nullable=True)
    extraction_confidence: Mapped[float | None] = mapped_column(nullable=True)
    parse_status: Mapped[str] = mapped_column(String, nullable=False)
    error_code: Mapped[str | None] = mapped_column(String)


class Transaction(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        Index("ix_transactions_account_id_posted_date", "account_id", "posted_date"),
        Index("ix_transactions_merchant_id", "merchant_id"),
        Index("ix_transactions_category_id", "category_id"),
        Index("ix_transactions_pending_status", "pending_status"),
        Index(
            "ux_transactions_account_source_kind_source_transaction_id_not_null",
            "account_id",
            "source_kind",
            "source_transaction_id",
            unique=True,
            sqlite_where=text("source_transaction_id IS NOT NULL"),
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    posted_date: Mapped[date] = mapped_column(Date, nullable=False)
    effective_date: Mapped[date | None] = mapped_column(Date)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2, asdecimal=True), nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)
    original_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    original_currency: Mapped[str | None] = mapped_column(String)
    pending_status: Mapped[str] = mapped_column(String, nullable=False)
    original_statement: Mapped[str | None] = mapped_column(Text)
    merchant_id: Mapped[str | None] = mapped_column(ForeignKey("merchants.id"))
    category_id: Mapped[str | None] = mapped_column(ForeignKey("categories.id"))
    excluded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[str | None] = mapped_column(Text)
    source_kind: Mapped[str] = mapped_column(String, nullable=False)
    source_transaction_id: Mapped[str | None] = mapped_column(String)
    import_batch_id: Mapped[str | None] = mapped_column(ForeignKey("import_batches.id"))
    transfer_group_id: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class TransactionEvent(Base):
    __tablename__ = "transaction_events"
    __table_args__ = (
        Index("ix_transaction_events_transaction_id_created_at", "transaction_id", "created_at"),
        Index("ix_transaction_events_event_type", "event_type"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    transaction_id: Mapped[str] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    old_value_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    new_value_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    reason: Mapped[str | None] = mapped_column(Text)
    actor: Mapped[str | None] = mapped_column(String)
    provenance: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Merchant(Base):
    __tablename__ = "merchants"
    __table_args__ = (UniqueConstraint("canonical_name", name="uq_merchants_canonical_name"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    canonical_name: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class MerchantAlias(Base):
    __tablename__ = "merchant_aliases"
    __table_args__ = (
        UniqueConstraint("alias", "source_context", name="uq_merchant_aliases_alias_source_context"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    merchant_id: Mapped[str] = mapped_column(ForeignKey("merchants.id"), nullable=False)
    alias: Mapped[str] = mapped_column(String, nullable=False)
    source_context: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Category(Base):
    __tablename__ = "categories"
    __table_args__ = (
        UniqueConstraint("parent_id", "name", name="uq_categories_parent_id_name"),
        Index(
            "ux_categories_root_name_parent_null",
            "name",
            unique=True,
            sqlite_where=text("parent_id IS NULL"),
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    parent_id: Mapped[str | None] = mapped_column(ForeignKey("categories.id"))
    name: Mapped[str] = mapped_column(String, nullable=False)
    system_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Tag(Base):
    __tablename__ = "tags"
    __table_args__ = (UniqueConstraint("name", name="uq_tags_name"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class TransactionTag(Base):
    __tablename__ = "transaction_tags"

    transaction_id: Mapped[str] = mapped_column(ForeignKey("transactions.id"), primary_key=True)
    tag_id: Mapped[str] = mapped_column(ForeignKey("tags.id"), primary_key=True)


class TransactionSplit(Base):
    __tablename__ = "transaction_splits"
    __table_args__ = (Index("ix_transaction_splits_transaction_id", "transaction_id"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    transaction_id: Mapped[str] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    line_no: Mapped[int] = mapped_column(nullable=False)
    category_id: Mapped[str | None] = mapped_column(ForeignKey("categories.id"))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    memo: Mapped[str | None] = mapped_column(Text)
    goal_id: Mapped[str | None] = mapped_column(ForeignKey("goals.id"))


class Rule(Base):
    __tablename__ = "rules"
    __table_args__ = (Index("ix_rules_enabled_priority", "enabled", "priority"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    priority: Mapped[int] = mapped_column(nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    apply_to_pending: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    matcher_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    action_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class RuleRun(Base):
    __tablename__ = "rule_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    rule_id: Mapped[str] = mapped_column(ForeignKey("rules.id"), nullable=False)
    run_mode: Mapped[str] = mapped_column(String, nullable=False)
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class RuleAudit(Base):
    __tablename__ = "rule_audits"
    __table_args__ = (
        Index("ix_rule_audits_transaction_id", "transaction_id"),
        Index("ix_rule_audits_rule_run_id", "rule_run_id"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    rule_run_id: Mapped[str] = mapped_column(ForeignKey("rule_runs.id"), nullable=False)
    transaction_id: Mapped[str] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    matched: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    changes_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    confidence: Mapped[float | None] = mapped_column(nullable=True)


class DedupeCandidate(Base):
    __tablename__ = "dedupe_candidates"
    __table_args__ = (
        Index("ix_dedupe_candidates_decision", "decision"),
        Index("ix_dedupe_candidates_score", "score"),
        Index("ux_dedupe_candidates_txn_pair", "txn_a_id", "txn_b_id", unique=True),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    txn_a_id: Mapped[str] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    txn_b_id: Mapped[str] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    score: Mapped[float] = mapped_column(nullable=False)
    decision: Mapped[str | None] = mapped_column(String)
    reason_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime)


class ReviewItem(Base):
    __tablename__ = "review_items"
    __table_args__ = (
        Index("ix_review_items_status_item_type", "status", "item_type"),
        Index("ix_review_items_confidence", "confidence"),
        Index("ix_review_items_reason_code", "reason_code"),
        Index("ix_review_items_source", "source"),
        Index(
            "ux_review_items_active_dedupe_candidate",
            "ref_table",
            "ref_id",
            "item_type",
            "source",
            unique=True,
            sqlite_where=text(
                "ref_table = 'dedupe_candidates' "
                "AND item_type = 'dedupe_candidate_suggestion' "
                "AND source = 'dedupe' "
                "AND status IN ('to_review', 'in_progress')"
            ),
        ),
        CheckConstraint(
            "status IN ('to_review', 'in_progress', 'resolved', 'rejected')",
            name="ck_review_items_status",
        ),
        CheckConstraint(
            "source IN ('pdf_extract', 'rules', 'dedupe', 'categorize', 'unknown')",
            name="ck_review_items_source",
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    item_type: Mapped[str] = mapped_column(String, nullable=False)
    ref_table: Mapped[str] = mapped_column(String, nullable=False)
    ref_id: Mapped[str] = mapped_column(String, nullable=False)
    reason_code: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    assigned_to: Mapped[str | None] = mapped_column(String)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime)


class ReviewItemEvent(Base):
    __tablename__ = "review_item_events"
    __table_args__ = (
        Index("ix_review_item_events_review_item_id_created_at", "review_item_id", "created_at"),
        Index("ix_review_item_events_event_type_created_at", "event_type", "created_at"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    review_item_id: Mapped[str] = mapped_column(ForeignKey("review_items.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    action: Mapped[str | None] = mapped_column(String)
    from_status: Mapped[str | None] = mapped_column(String)
    to_status: Mapped[str | None] = mapped_column(String)
    actor: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class BalanceSnapshot(Base):
    __tablename__ = "balance_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "snapshot_date",
            "source",
            name="uq_balance_snapshots_account_id_snapshot_date_source",
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    balance: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    statement_id: Mapped[str | None] = mapped_column(ForeignKey("statements.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Reconciliation(Base):
    __tablename__ = "reconciliations"
    __table_args__ = (
        Index("ix_reconciliations_account_id_period_end", "account_id", "period_end"),
        Index("ix_reconciliations_status", "status"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    expected_balance: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    computed_balance: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    delta: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    match_rate: Mapped[float | None] = mapped_column(nullable=True)
    trust_score: Mapped[float | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Budget(Base):
    __tablename__ = "budgets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    method: Mapped[str] = mapped_column(String, nullable=False)
    base_currency: Mapped[str] = mapped_column(String, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class BudgetPeriod(Base):
    __tablename__ = "budget_periods"
    __table_args__ = (
        UniqueConstraint("budget_id", "period_month", name="uq_budget_periods_budget_id_period_month"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_id: Mapped[str] = mapped_column(ForeignKey("budgets.id"), nullable=False)
    period_month: Mapped[str] = mapped_column(String, nullable=False)
    to_assign: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    assigned_total: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    spent_total: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    rollover_total: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)


class BudgetCategory(Base):
    __tablename__ = "budget_categories"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_id: Mapped[str] = mapped_column(ForeignKey("budgets.id"), nullable=False)
    category_id: Mapped[str] = mapped_column(ForeignKey("categories.id"), nullable=False)
    policy_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class BudgetTarget(Base):
    __tablename__ = "budget_targets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_category_id: Mapped[str] = mapped_column(ForeignKey("budget_categories.id"), nullable=False)
    target_type: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    cadence: Mapped[str | None] = mapped_column(String)
    top_up: Mapped[bool | None] = mapped_column(Boolean)
    snoozed_until: Mapped[date | None] = mapped_column(Date)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class BudgetAllocation(Base):
    __tablename__ = "budget_allocations"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_period_id: Mapped[str] = mapped_column(ForeignKey("budget_periods.id"), nullable=False)
    budget_category_id: Mapped[str] = mapped_column(ForeignKey("budget_categories.id"), nullable=False)
    assigned_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)


class BudgetBucket(Base):
    __tablename__ = "budget_buckets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_id: Mapped[str] = mapped_column(ForeignKey("budgets.id"), nullable=False)
    period_month: Mapped[str] = mapped_column(String, nullable=False)
    bucket_name: Mapped[str] = mapped_column(String, nullable=False)
    planned_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    actual_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    rollover_policy: Mapped[str | None] = mapped_column(String)


class BudgetRollover(Base):
    __tablename__ = "budget_rollovers"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_id: Mapped[str] = mapped_column(ForeignKey("budgets.id"), nullable=False)
    dimension_type: Mapped[str] = mapped_column(String, nullable=False)
    dimension_id: Mapped[str] = mapped_column(String, nullable=False)
    from_period: Mapped[str] = mapped_column(String, nullable=False)
    to_period: Mapped[str] = mapped_column(String, nullable=False)
    carry_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    policy_applied: Mapped[str] = mapped_column(String, nullable=False)


class Recurring(Base):
    __tablename__ = "recurrings"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    merchant_id: Mapped[str | None] = mapped_column(ForeignKey("merchants.id"))
    category_id: Mapped[str | None] = mapped_column(ForeignKey("categories.id"))
    schedule_type: Mapped[str] = mapped_column(String, nullable=False)
    interval_n: Mapped[int] = mapped_column(nullable=False)
    anchor_date: Mapped[date] = mapped_column(Date, nullable=False)
    tolerance_days: Mapped[int] = mapped_column(nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class RecurringEvent(Base):
    __tablename__ = "recurring_events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    recurring_id: Mapped[str] = mapped_column(ForeignKey("recurrings.id"), nullable=False)
    expected_date: Mapped[date] = mapped_column(Date, nullable=False)
    observed_transaction_id: Mapped[str | None] = mapped_column(ForeignKey("transactions.id"))
    status: Mapped[str] = mapped_column(String, nullable=False)


class Goal(Base):
    __tablename__ = "goals"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    target_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    target_date: Mapped[date | None] = mapped_column(Date)
    monthly_contribution: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    spending_reduces_progress: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class GoalAllocation(Base):
    __tablename__ = "goal_allocations"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    goal_id: Mapped[str] = mapped_column(ForeignKey("goals.id"), nullable=False)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    period_month: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    allocation_type: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class GoalEvent(Base):
    __tablename__ = "goal_events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    goal_id: Mapped[str] = mapped_column(ForeignKey("goals.id"), nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    related_transaction_id: Mapped[str | None] = mapped_column(ForeignKey("transactions.id"))
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    report_type: Mapped[str] = mapped_column(String, nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    run_id: Mapped[str | None] = mapped_column(String)


class RunMetadata(Base):
    __tablename__ = "run_metadata"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String, nullable=False)
    code_version: Mapped[str] = mapped_column(String, nullable=False)
    schema_version: Mapped[str] = mapped_column(String, nullable=False)
    config_hash: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String, nullable=False)
    diagnostics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
