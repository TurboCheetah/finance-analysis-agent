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
    """
    Enforce a unique constraint on goal_allocations per period by removing duplicate rows and adding a composite unique key.
    
    Removes duplicate rows so that each (period_month, goal_id, account_id, allocation_type) combination is unique; when duplicates exist, keeps the row with the earliest non-null `created_at` (ties broken by `rowid`). Recreates the table to add the unique constraint named `uq_goal_allocations_period_month_goal_id_account_id_allocation_type`. Foreign key checks are temporarily disabled while the schema change is applied.
    """
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
    """
    Revert the migration that enforces a unique constraint on goal_allocations per period.
    
    Removes the unique constraint named
    `uq_goal_allocations_period_month_goal_id_account_id_allocation_type` from the
    goal_allocations table. Temporarily disables foreign key enforcement while the
    table is recreated and re-enables it afterwards.
    """
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("goal_allocations", recreate="always") as batch_op:
        batch_op.drop_constraint(
            "uq_goal_allocations_period_month_goal_id_account_id_allocation_type",
            type_="unique",
        )
    op.execute("PRAGMA foreign_keys=ON")
