"""add normalized flex budget bucket definitions and category mappings

Revision ID: 7e3b4c2d1f90
Revises: 2f6c8a9d1e4b
Create Date: 2026-02-26 19:10:00.000000

"""

from __future__ import annotations

from typing import Sequence

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "7e3b4c2d1f90"
down_revision: str | Sequence[str] | None = "2f6c8a9d1e4b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ALLOWED_BUCKET_KEYS = {"fixed", "non_monthly", "flex"}
_ALLOWED_ROLLOVER_POLICIES = {"none", "carry_positive", "carry_negative", "carry_both"}


def _canonical_bucket_key(raw_name: str | None) -> str | None:
    if raw_name is None:
        return None
    normalized = raw_name.strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    if normalized in _ALLOWED_BUCKET_KEYS:
        return normalized
    return None


def _bucket_display_name(bucket_key: str) -> str:
    if bucket_key == "non_monthly":
        return "Non-monthly"
    return bucket_key.replace("_", " ").title()


def _canonical_rollover_policy(raw_policy: str | None) -> str | None:
    if raw_policy is None:
        return None
    normalized = str(raw_policy).strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    if normalized in _ALLOWED_ROLLOVER_POLICIES:
        return normalized
    return None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "budget_bucket_definitions",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("budget_id", sa.String(), nullable=False),
        sa.Column("bucket_key", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("rollover_policy", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["budget_id"],
            ["budgets.id"],
            name=op.f("fk_budget_bucket_definitions_budget_id_budgets"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_budget_bucket_definitions")),
        sa.UniqueConstraint(
            "budget_id",
            "bucket_key",
            name="uq_budget_bucket_definitions_budget_id_bucket_key",
        ),
    )
    op.create_table(
        "budget_bucket_category_mappings",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("bucket_definition_id", sa.String(), nullable=False),
        sa.Column("budget_category_id", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["bucket_definition_id"],
            ["budget_bucket_definitions.id"],
            name=op.f(
                "fk_budget_bucket_category_mappings_bucket_definition_id_budget_bucket_definitions"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["budget_category_id"],
            ["budget_categories.id"],
            name=op.f("fk_budget_bucket_category_mappings_budget_category_id_budget_categories"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_budget_bucket_category_mappings")),
        sa.UniqueConstraint(
            "budget_category_id",
            name="uq_budget_bucket_category_mappings_budget_category_id",
        ),
    )
    op.create_index(
        "ix_budget_bucket_category_mappings_bucket_definition_id",
        "budget_bucket_category_mappings",
        ["bucket_definition_id"],
        unique=False,
    )

    with op.batch_alter_table("budget_categories", schema=None) as batch_op:
        batch_op.add_column(sa.Column("rollover_policy", sa.String(), nullable=True))

    with op.batch_alter_table("budget_buckets", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bucket_definition_id", sa.String(), nullable=True))
        batch_op.create_foreign_key(
            "fk_budget_buckets_bucket_definition_id_budget_bucket_definitions",
            "budget_bucket_definitions",
            ["bucket_definition_id"],
            ["id"],
        )
        batch_op.create_index(
            "ix_budget_buckets_budget_id_period_month_bucket_definition_id",
            ["budget_id", "period_month", "bucket_definition_id"],
            unique=False,
        )

    connection = op.get_bind()

    existing_buckets = connection.execute(
        sa.text(
            """
            SELECT id, budget_id, bucket_name, rollover_policy
            FROM budget_buckets
            ORDER BY budget_id ASC, id ASC
            """
        )
    ).mappings()
    definition_candidates: dict[tuple[str, str], str | None] = {}
    for row in existing_buckets:
        bucket_key = _canonical_bucket_key(row["bucket_name"])
        if bucket_key is None:
            continue
        canonical_policy = _canonical_rollover_policy(row["rollover_policy"])
        key = (row["budget_id"], bucket_key)
        existing_policy = definition_candidates.get(key)
        if existing_policy is None and canonical_policy is not None:
            definition_candidates[key] = canonical_policy
        elif key not in definition_candidates:
            definition_candidates[key] = None

    for (budget_id, bucket_key), rollover_policy in definition_candidates.items():
        definition_id = f"bucketdef:{budget_id}:{bucket_key}"
        connection.execute(
            sa.text(
                """
                INSERT OR IGNORE INTO budget_bucket_definitions (
                    id,
                    budget_id,
                    bucket_key,
                    name,
                    rollover_policy
                ) VALUES (
                    :id,
                    :budget_id,
                    :bucket_key,
                    :name,
                    :rollover_policy
                )
                """
            ),
            {
                "id": definition_id,
                "budget_id": budget_id,
                "bucket_key": bucket_key,
                "name": _bucket_display_name(bucket_key),
                "rollover_policy": rollover_policy or "none",
            },
        )

    bucket_rows = connection.execute(
        sa.text(
            """
            SELECT id, budget_id, bucket_name
            FROM budget_buckets
            """
        )
    ).mappings()
    for row in bucket_rows:
        bucket_key = _canonical_bucket_key(row["bucket_name"])
        if bucket_key is None:
            continue
        definition_id = f"bucketdef:{row['budget_id']}:{bucket_key}"
        connection.execute(
            sa.text(
                """
                UPDATE budget_buckets
                SET bucket_definition_id = :bucket_definition_id,
                    bucket_name = :bucket_name
                WHERE id = :id
                """
            ),
            {
                "bucket_definition_id": definition_id,
                "bucket_name": bucket_key,
                "id": row["id"],
            },
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("budget_buckets", schema=None) as batch_op:
        batch_op.drop_index("ix_budget_buckets_budget_id_period_month_bucket_definition_id")
        batch_op.drop_constraint(
            "fk_budget_buckets_bucket_definition_id_budget_bucket_definitions",
            type_="foreignkey",
        )
        batch_op.drop_column("bucket_definition_id")

    op.drop_index(
        "ix_budget_bucket_category_mappings_bucket_definition_id",
        table_name="budget_bucket_category_mappings",
    )
    op.drop_table("budget_bucket_category_mappings")

    with op.batch_alter_table("budget_categories", schema=None) as batch_op:
        batch_op.drop_column("rollover_policy")

    op.drop_table("budget_bucket_definitions")
