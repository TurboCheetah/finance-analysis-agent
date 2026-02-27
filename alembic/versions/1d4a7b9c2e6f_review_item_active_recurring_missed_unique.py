"""add active recurring-missed review item uniqueness guard

Revision ID: 1d4a7b9c2e6f
Revises: 6a7b3c1d9e2f
Create Date: 2026-02-27 12:20:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1d4a7b9c2e6f"
down_revision: Union[str, Sequence[str], None] = "6a7b3c1d9e2f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACTIVE_RECURRING_MISSED_FILTER_SQL = """
    ref_table = 'recurring_events'
    AND item_type = 'recurring_missed_event'
    AND source = 'recurring'
    AND status IN ('to_review', 'in_progress')
"""


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY ref_table, ref_id, item_type, source
                    ORDER BY
                        CASE
                            WHEN status = 'in_progress' THEN 0
                            WHEN status = 'to_review' THEN 1
                            ELSE 2
                        END ASC,
                        CASE WHEN created_at IS NULL THEN 1 ELSE 0 END ASC,
                        created_at ASC,
                        id ASC
                ) AS rn
            FROM review_items
            WHERE ref_table = 'recurring_events'
              AND item_type = 'recurring_missed_event'
              AND source = 'recurring'
              AND status IN ('to_review', 'in_progress')
        )
        UPDATE review_items
        SET status = 'resolved',
            resolved_at = COALESCE(resolved_at, CURRENT_TIMESTAMP)
        WHERE id IN (
            SELECT id
            FROM ranked
            WHERE rn > 1
        )
        """
    )

    op.create_index(
        "ux_review_items_active_recurring_missed_event",
        "review_items",
        ["ref_table", "ref_id", "item_type", "source"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_RECURRING_MISSED_FILTER_SQL),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ux_review_items_active_recurring_missed_event", table_name="review_items")
