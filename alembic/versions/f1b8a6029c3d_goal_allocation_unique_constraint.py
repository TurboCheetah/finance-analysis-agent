"""enforce unique goal-allocation key per period

Revision ID: f1b8a6029c3d
Revises: a4c9e2f1b7d8
Create Date: 2026-02-27 02:30:00.000000

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "f1b8a6029c3d"
down_revision: Union[str, Sequence[str], None] = "a4c9e2f1b7d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        WITH ranked AS (
            SELECT
                rowid AS rid,
                ROW_NUMBER() OVER (
                    PARTITION BY period_month, goal_id, account_id, allocation_type
                    ORDER BY
                        CASE WHEN created_at IS NULL THEN 1 ELSE 0 END ASC,
                        created_at ASC,
                        rowid ASC
                ) AS rn
            FROM goal_allocations
        )
        DELETE FROM goal_allocations
        WHERE rowid IN (SELECT rid FROM ranked WHERE rn > 1)
        """
    )

    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("goal_allocations", recreate="always") as batch_op:
        batch_op.create_unique_constraint(
            "uq_goal_allocations_period_month_goal_id_account_id_allocation_type",
            ["period_month", "goal_id", "account_id", "allocation_type"],
        )
    op.execute("PRAGMA foreign_keys=ON")


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("goal_allocations", recreate="always") as batch_op:
        batch_op.drop_constraint(
            "uq_goal_allocations_period_month_goal_id_account_id_allocation_type",
            type_="unique",
        )
    op.execute("PRAGMA foreign_keys=ON")
