"""add unique txn-pair index for dedupe candidates

Revision ID: c3a1d7e4b9f0
Revises: 6f4d9e3b2a10
Create Date: 2026-02-25 14:30:00.000000

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c3a1d7e4b9f0"
down_revision: Union[str, Sequence[str], None] = "6f4d9e3b2a10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Keep a single row per txn-pair before creating the unique index.
    op.execute(
        """
        DELETE FROM dedupe_candidates
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM dedupe_candidates
            GROUP BY txn_a_id, txn_b_id
        )
        """
    )
    op.create_index(
        "ux_dedupe_candidates_txn_pair",
        "dedupe_candidates",
        ["txn_a_id", "txn_b_id"],
        unique=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ux_dedupe_candidates_txn_pair", table_name="dedupe_candidates")
