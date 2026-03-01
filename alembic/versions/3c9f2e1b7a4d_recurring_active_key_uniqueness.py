"""enforce one active recurring per merchant/category key

Revision ID: 3c9f2e1b7a4d
Revises: f1b8a6029c3d
Create Date: 2026-02-27 03:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3c9f2e1b7a4d"
down_revision: Union[str, Sequence[str], None] = "f1b8a6029c3d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACTIVE_MERCHANT_FILTER = "active = 1 AND merchant_id IS NOT NULL AND category_id IS NULL"
_ACTIVE_CATEGORY_FILTER = "active = 1 AND category_id IS NOT NULL AND merchant_id IS NULL"


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        WITH ranked AS (
            SELECT
                rowid AS rid,
                ROW_NUMBER() OVER (
                    PARTITION BY merchant_id
                    ORDER BY rowid ASC
                ) AS rn
            FROM recurrings
            WHERE active = 1
              AND merchant_id IS NOT NULL
              AND category_id IS NULL
        )
        UPDATE recurrings
        SET active = 0
        WHERE rowid IN (SELECT rid FROM ranked WHERE rn > 1)
        """
    )
    op.execute(
        """
        WITH ranked AS (
            SELECT
                rowid AS rid,
                ROW_NUMBER() OVER (
                    PARTITION BY category_id
                    ORDER BY rowid ASC
                ) AS rn
            FROM recurrings
            WHERE active = 1
              AND category_id IS NOT NULL
              AND merchant_id IS NULL
        )
        UPDATE recurrings
        SET active = 0
        WHERE rowid IN (SELECT rid FROM ranked WHERE rn > 1)
        """
    )
    op.create_index(
        "ux_recurrings_active_merchant_id",
        "recurrings",
        ["merchant_id"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_MERCHANT_FILTER),
    )
    op.create_index(
        "ux_recurrings_active_category_id",
        "recurrings",
        ["category_id"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_CATEGORY_FILTER),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ux_recurrings_active_category_id", table_name="recurrings")
    op.drop_index("ux_recurrings_active_merchant_id", table_name="recurrings")
