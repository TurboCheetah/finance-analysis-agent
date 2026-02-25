"""add active dedupe review-item uniqueness guard

Revision ID: 8c1d7bf6e2a4
Revises: c3a1d7e4b9f0
Create Date: 2026-02-25 15:15:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "8c1d7bf6e2a4"
down_revision: Union[str, Sequence[str], None] = "c3a1d7e4b9f0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACTIVE_DEDUPE_REVIEW_FILTER_SQL = """
    ref_table = 'dedupe_candidates'
    AND item_type = 'dedupe_candidate_suggestion'
    AND source = 'dedupe'
    AND status IN ('to_review', 'in_progress')
"""

_ACTIVE_DEDUPE_REVIEW_RANKING_SQL = f"""
    WITH ranked AS (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY ref_table, ref_id, item_type, source
                ORDER BY
                    CASE status WHEN 'in_progress' THEN 0 WHEN 'to_review' THEN 1 ELSE 2 END ASC,
                    CASE WHEN assigned_to IS NULL OR TRIM(assigned_to) = '' THEN 1 ELSE 0 END ASC,
                    CASE WHEN created_at IS NULL THEN 1 ELSE 0 END ASC,
                    created_at ASC,
                    rowid ASC
            ) AS rn,
            FIRST_VALUE(id) OVER (
                PARTITION BY ref_table, ref_id, item_type, source
                ORDER BY
                    CASE status WHEN 'in_progress' THEN 0 WHEN 'to_review' THEN 1 ELSE 2 END ASC,
                    CASE WHEN assigned_to IS NULL OR TRIM(assigned_to) = '' THEN 1 ELSE 0 END ASC,
                    CASE WHEN created_at IS NULL THEN 1 ELSE 0 END ASC,
                    created_at ASC,
                    rowid ASC
            ) AS keep_id
        FROM review_items
        WHERE {_ACTIVE_DEDUPE_REVIEW_FILTER_SQL}
    ),
    to_remove AS (
        SELECT id, keep_id
        FROM ranked
        WHERE rn > 1
    )
"""


def upgrade() -> None:
    """Upgrade schema."""
    # Re-link review_item_events before deleting duplicate active review_items.
    op.execute(
        _ACTIVE_DEDUPE_REVIEW_RANKING_SQL
        + """
        UPDATE review_item_events
        SET review_item_id = (
            SELECT keep_id
            FROM to_remove
            WHERE to_remove.id = review_item_events.review_item_id
        )
        WHERE review_item_id IN (SELECT id FROM to_remove)
        """
    )
    op.execute(
        _ACTIVE_DEDUPE_REVIEW_RANKING_SQL
        + """
        DELETE FROM review_items
        WHERE id IN (SELECT id FROM to_remove)
        """
    )
    op.create_index(
        "ux_review_items_active_dedupe_candidate",
        "review_items",
        ["ref_table", "ref_id", "item_type", "source"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_DEDUPE_REVIEW_FILTER_SQL),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ux_review_items_active_dedupe_candidate", table_name="review_items")
