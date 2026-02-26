"""extend reconciliation checkpoints with trust details and adjustment approval

Revision ID: 2f6c8a9d1e4b
Revises: d5f0c7a1e9b2
Create Date: 2026-02-25 23:55:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "2f6c8a9d1e4b"
down_revision: Union[str, Sequence[str], None] = "d5f0c7a1e9b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("reconciliations", schema=None) as batch_op:
        batch_op.add_column(sa.Column("statement_id", sa.String(), nullable=True))
        batch_op.add_column(
            sa.Column("unresolved_count", sa.Integer(), nullable=False, server_default=sa.text("0"))
        )
        batch_op.add_column(
            sa.Column(
                "adjustment_magnitude",
                sa.Numeric(precision=18, scale=2),
                nullable=False,
                server_default=sa.text("0.00"),
            )
        )
        batch_op.add_column(sa.Column("details_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("approved_adjustment_txn_id", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("approved_by", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("approved_at", sa.DateTime(), nullable=True))
        batch_op.create_foreign_key(
            "fk_reconciliations_statement_id_statements",
            "statements",
            ["statement_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_reconciliations_approved_adjustment_txn_id_transactions",
            "transactions",
            ["approved_adjustment_txn_id"],
            ["id"],
        )
        batch_op.create_index("ix_reconciliations_statement_id", ["statement_id"], unique=False)
        batch_op.create_index(
            "ix_reconciliations_approved_adjustment_txn_id",
            ["approved_adjustment_txn_id"],
            unique=False,
        )

    with op.batch_alter_table("reconciliations", schema=None) as batch_op:
        batch_op.alter_column("unresolved_count", server_default=None)
        batch_op.alter_column("adjustment_magnitude", server_default=None)


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("reconciliations", schema=None) as batch_op:
        batch_op.drop_index("ix_reconciliations_approved_adjustment_txn_id")
        batch_op.drop_index("ix_reconciliations_statement_id")
        batch_op.drop_constraint(
            "fk_reconciliations_approved_adjustment_txn_id_transactions",
            type_="foreignkey",
        )
        batch_op.drop_constraint("fk_reconciliations_statement_id_statements", type_="foreignkey")
        batch_op.drop_column("approved_at")
        batch_op.drop_column("approved_by")
        batch_op.drop_column("approved_adjustment_txn_id")
        batch_op.drop_column("details_json")
        batch_op.drop_column("adjustment_magnitude")
        batch_op.drop_column("unresolved_count")
        batch_op.drop_column("statement_id")
