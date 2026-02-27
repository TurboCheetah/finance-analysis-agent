"""allow recurring source in review_items

Revision ID: a4c9e2f1b7d8
Revises: 7e3b4c2d1f90
Create Date: 2026-02-27 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a4c9e2f1b7d8"
down_revision: Union[str, Sequence[str], None] = "7e3b4c2d1f90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ACTIVE_DEDUPE_REVIEW_FILTER_SQL = """
    ref_table = 'dedupe_candidates'
    AND item_type = 'dedupe_candidate_suggestion'
    AND source = 'dedupe'
    AND status IN ('to_review', 'in_progress')
"""


def upgrade() -> None:
    """
    Modify the database schema to allow 'recurring' as a valid review_items.source and ensure the related partial unique index reflects the added source column.
    
    This migration updates the CHECK constraint on review_items.source to include 'recurring' and recreates the partial unique index ux_review_items_active_dedupe_candidate on (ref_table, ref_id, item_type, source) using the existing filter condition.
    """
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_review_items_source", type_="check")
        batch_op.create_check_constraint(
            "ck_review_items_source",
            "source IN ('pdf_extract', 'rules', 'dedupe', 'categorize', 'recurring', 'unknown')",
        )
    op.execute("PRAGMA foreign_keys=ON")
    op.drop_index("ux_review_items_active_dedupe_candidate", table_name="review_items")
    op.create_index(
        "ux_review_items_active_dedupe_candidate",
        "review_items",
        ["ref_table", "ref_id", "item_type", "source"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_DEDUPE_REVIEW_FILTER_SQL),
    )


def downgrade() -> None:
    """
    Revert the review_items schema to disallow the 'recurring' source and migrate existing values.
    
    Updates any review_items rows with source = 'recurring' to 'unknown', recreates the table constraint so source is limited to 'pdf_extract', 'rules', 'dedupe', 'categorize', and 'unknown' (excluding 'recurring'), and re-creates the partial unique index enforcing active dedupe candidate uniqueness.
    """
    op.execute("UPDATE review_items SET source = 'unknown' WHERE source = 'recurring'")
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_review_items_source", type_="check")
        batch_op.create_check_constraint(
            "ck_review_items_source",
            "source IN ('pdf_extract', 'rules', 'dedupe', 'categorize', 'unknown')",
        )
    op.execute("PRAGMA foreign_keys=ON")
    op.drop_index("ux_review_items_active_dedupe_candidate", table_name="review_items")
    op.create_index(
        "ux_review_items_active_dedupe_candidate",
        "review_items",
        ["ref_table", "ref_id", "item_type", "source"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_DEDUPE_REVIEW_FILTER_SQL),
    )
