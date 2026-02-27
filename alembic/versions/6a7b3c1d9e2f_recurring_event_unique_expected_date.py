"""enforce unique recurring event expected date per recurring

Revision ID: 6a7b3c1d9e2f
Revises: 3c9f2e1b7a4d
Create Date: 2026-02-27 04:00:00.000000

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "6a7b3c1d9e2f"
down_revision: Union[str, Sequence[str], None] = "3c9f2e1b7a4d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                recurring_id,
                expected_date,
                ROW_NUMBER() OVER (
                    PARTITION BY recurring_id, expected_date
                    ORDER BY
                        CASE WHEN status = 'observed' THEN 0 ELSE 1 END ASC,
                        CASE WHEN observed_transaction_id IS NULL THEN 1 ELSE 0 END ASC,
                        rowid ASC
                ) AS rn
            FROM recurring_events
        ),
        keepers AS (
            SELECT recurring_id, expected_date, id AS keep_id
            FROM ranked
            WHERE rn = 1
        ),
        duplicates AS (
            SELECT r.id AS duplicate_id, k.keep_id
            FROM ranked r
            JOIN keepers k
              ON k.recurring_id = r.recurring_id
             AND k.expected_date = r.expected_date
            WHERE r.rn > 1
        )
        UPDATE review_items
        SET ref_id = (
            SELECT keep_id
            FROM duplicates
            WHERE duplicate_id = review_items.ref_id
        )
        WHERE ref_table = 'recurring_events'
          AND ref_id IN (SELECT duplicate_id FROM duplicates)
        """
    )
    op.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                recurring_id,
                expected_date,
                ROW_NUMBER() OVER (
                    PARTITION BY recurring_id, expected_date
                    ORDER BY
                        CASE WHEN status = 'observed' THEN 0 ELSE 1 END ASC,
                        CASE WHEN observed_transaction_id IS NULL THEN 1 ELSE 0 END ASC,
                        rowid ASC
                ) AS rn
            FROM recurring_events
        )
        DELETE FROM recurring_events
        WHERE id IN (
            SELECT id
            FROM ranked
            WHERE rn > 1
        )
        """
    )

    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("recurring_events", recreate="always") as batch_op:
        batch_op.create_unique_constraint(
            "uq_recurring_events_recurring_id_expected_date",
            ["recurring_id", "expected_date"],
        )
    op.execute("PRAGMA foreign_keys=ON")


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("recurring_events", recreate="always") as batch_op:
        batch_op.drop_constraint(
            "uq_recurring_events_recurring_id_expected_date",
            type_="unique",
        )
    op.execute("PRAGMA foreign_keys=ON")
