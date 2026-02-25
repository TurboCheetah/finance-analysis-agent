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
    # Canonicalize historical rows so each logical pair uses a stable id order.
    # For swapped rows, also realign reason_json side-specific fields so txn_a/txn_b
    # snapshots and payee labels still match the persisted txn_a_id/txn_b_id.
    op.execute(
        """
        UPDATE dedupe_candidates
        SET txn_a_id = txn_b_id,
            txn_b_id = txn_a_id,
            reason_json = CASE
                WHEN reason_json IS NULL THEN NULL
                WHEN json_valid(reason_json) = 0 THEN reason_json
                WHEN json_type(reason_json, '$.score_breakdown.details.left_payee') IS NOT NULL
                    OR json_type(reason_json, '$.score_breakdown.details.right_payee') IS NOT NULL
                THEN json_set(
                    json_set(
                        reason_json,
                        '$.txn_a_snapshot',
                        json_extract(reason_json, '$.txn_b_snapshot'),
                        '$.txn_b_snapshot',
                        json_extract(reason_json, '$.txn_a_snapshot')
                    ),
                    '$.score_breakdown.details.left_payee',
                    json_extract(reason_json, '$.score_breakdown.details.right_payee'),
                    '$.score_breakdown.details.right_payee',
                    json_extract(reason_json, '$.score_breakdown.details.left_payee')
                )
                ELSE json_set(
                    reason_json,
                    '$.txn_a_snapshot',
                    json_extract(reason_json, '$.txn_b_snapshot'),
                    '$.txn_b_snapshot',
                    json_extract(reason_json, '$.txn_a_snapshot')
                )
            END
        WHERE txn_a_id > txn_b_id
        """
    )

    # Keep a single row per txn-pair before creating the unique index.
    # Prefer decided rows, then most recent decided_at, then stable rowid order.
    op.execute(
        """
        WITH ranked AS (
            SELECT
                rowid,
                ROW_NUMBER() OVER (
                    PARTITION BY txn_a_id, txn_b_id
                    ORDER BY
                        CASE WHEN decision IS NOT NULL THEN 0 ELSE 1 END ASC,
                        CASE WHEN decided_at IS NULL THEN 1 ELSE 0 END ASC,
                        decided_at DESC,
                        rowid ASC
                ) AS rn
            FROM dedupe_candidates
        )
        DELETE FROM dedupe_candidates
        WHERE rowid IN (SELECT rowid FROM ranked WHERE rn > 1)
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
