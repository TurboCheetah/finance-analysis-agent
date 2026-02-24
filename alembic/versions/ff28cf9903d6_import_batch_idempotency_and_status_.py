"""import batch idempotency and status events

Revision ID: ff28cf9903d6
Revises: b9023bb6dd2f
Create Date: 2026-02-24 13:32:58.332269

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ff28cf9903d6'
down_revision: Union[str, Sequence[str], None] = 'b9023bb6dd2f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "import_batch_status_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("batch_id", sa.String(), nullable=False),
        sa.Column("from_status", sa.String(), nullable=True),
        sa.Column("to_status", sa.String(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("actor", sa.String(), nullable=True),
        sa.Column("changed_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["import_batches.id"],
            name=op.f("fk_import_batch_status_events_batch_id_import_batches"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_import_batch_status_events")),
    )
    op.create_index(
        "ix_import_batch_status_events_batch_id_changed_at",
        "import_batch_status_events",
        ["batch_id", "changed_at"],
        unique=False,
    )
    op.create_index(
        "ix_import_batch_status_events_to_status_changed_at",
        "import_batch_status_events",
        ["to_status", "changed_at"],
        unique=False,
    )

    with op.batch_alter_table("import_batches", recreate="always") as batch_op:
        batch_op.add_column(
            sa.Column(
                "fingerprint_algo",
                sa.String(),
                nullable=False,
                server_default=sa.text("'sha256'"),
            )
        )
        batch_op.add_column(
            sa.Column(
                "conflict_mode",
                sa.String(),
                nullable=False,
                server_default=sa.text("'normal'"),
            )
        )
        batch_op.add_column(sa.Column("override_reason", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("override_of_batch_id", sa.String(), nullable=True))
        batch_op.drop_constraint(
            op.f("uq_import_batches_source_fingerprint_source_type"),
            type_="unique",
        )
        batch_op.create_index(
            "ix_import_batches_source_type_source_fingerprint_received_at",
            ["source_type", "source_fingerprint", "received_at"],
            unique=False,
        )
        batch_op.create_index("ix_import_batches_status", ["status"], unique=False)
        batch_op.create_foreign_key(
            op.f("fk_import_batches_override_of_batch_id_import_batches"),
            "import_batches",
            ["override_of_batch_id"],
            ["id"],
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("import_batches", recreate="always") as batch_op:
        batch_op.drop_constraint(
            op.f("fk_import_batches_override_of_batch_id_import_batches"),
            type_="foreignkey",
        )
        batch_op.drop_index("ix_import_batches_status")
        batch_op.drop_index("ix_import_batches_source_type_source_fingerprint_received_at")
        batch_op.drop_column("override_of_batch_id")
        batch_op.drop_column("override_reason")
        batch_op.drop_column("conflict_mode")
        batch_op.drop_column("fingerprint_algo")
        batch_op.create_unique_constraint(
            op.f("uq_import_batches_source_fingerprint_source_type"),
            ["source_fingerprint", "source_type"],
        )

    op.drop_index(
        "ix_import_batch_status_events_to_status_changed_at",
        table_name="import_batch_status_events",
    )
    op.drop_index(
        "ix_import_batch_status_events_batch_id_changed_at",
        table_name="import_batch_status_events",
    )
    op.drop_table("import_batch_status_events")
