"""enforce active recurring key shape

Revision ID: 4e8b1c7d2a9f
Revises: 1d4a7b9c2e6f
Create Date: 2026-02-27 12:50:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4e8b1c7d2a9f"
down_revision: Union[str, Sequence[str], None] = "1d4a7b9c2e6f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACTIVE_MERCHANT_FILTER_SQL = "active = 1 AND merchant_id IS NOT NULL AND category_id IS NULL"
_ACTIVE_CATEGORY_FILTER_SQL = "active = 1 AND category_id IS NOT NULL AND merchant_id IS NULL"
_ACTIVE_EXACTLY_ONE_KEY_CHECK_SQL = (
    "active = 0 OR "
    "((merchant_id IS NOT NULL AND category_id IS NULL) "
    "OR (merchant_id IS NULL AND category_id IS NOT NULL))"
)


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        UPDATE recurrings
        SET active = 0
        WHERE active = 1
          AND (
            (merchant_id IS NULL AND category_id IS NULL)
            OR (merchant_id IS NOT NULL AND category_id IS NOT NULL)
          )
        """
    )

    op.drop_index("ux_recurrings_active_merchant_id", table_name="recurrings")
    op.drop_index("ux_recurrings_active_category_id", table_name="recurrings")

    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("recurrings", recreate="always") as batch_op:
        batch_op.create_check_constraint(
            "ck_recurrings_active_exactly_one_key",
            _ACTIVE_EXACTLY_ONE_KEY_CHECK_SQL,
        )
    op.execute("PRAGMA foreign_keys=ON")

    op.create_index(
        "ux_recurrings_active_merchant_id",
        "recurrings",
        ["merchant_id"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_MERCHANT_FILTER_SQL),
    )
    op.create_index(
        "ux_recurrings_active_category_id",
        "recurrings",
        ["category_id"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_CATEGORY_FILTER_SQL),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ux_recurrings_active_merchant_id", table_name="recurrings")
    op.drop_index("ux_recurrings_active_category_id", table_name="recurrings")

    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("recurrings", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_recurrings_active_exactly_one_key", type_="check")
    op.execute("PRAGMA foreign_keys=ON")

    op.create_index(
        "ux_recurrings_active_merchant_id",
        "recurrings",
        ["merchant_id"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_MERCHANT_FILTER_SQL),
    )
    op.create_index(
        "ux_recurrings_active_category_id",
        "recurrings",
        ["category_id"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_CATEGORY_FILTER_SQL),
    )
