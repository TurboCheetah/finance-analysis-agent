"""review queue states normalization and audit events

Revision ID: 6f4d9e3b2a10
Revises: 9d2c4a7f3b11
Create Date: 2026-02-25 10:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6f4d9e3b2a10"
down_revision: Union[str, Sequence[str], None] = "9d2c4a7f3b11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.add_column(
            sa.Column(
                "source",
                sa.String(),
                nullable=False,
                server_default=sa.text("'unknown'"),
            )
        )
        batch_op.create_index("ix_review_items_reason_code", ["reason_code"], unique=False)
        batch_op.create_index("ix_review_items_source", ["source"], unique=False)

    op.execute("UPDATE review_items SET status = 'to_review' WHERE status = 'open'")
    op.execute(
        "UPDATE review_items "
        "SET status = 'to_review' "
        "WHERE status NOT IN ('to_review', 'in_progress', 'resolved', 'rejected')"
    )

    op.execute("UPDATE review_items SET source = 'pdf_extract' WHERE ref_table = 'run_metadata'")
    op.execute(
        "UPDATE review_items "
        "SET source = 'rules' "
        "WHERE ref_table = 'transactions' "
        "AND reason_code = 'rule.needs_review'"
    )
    op.execute("UPDATE review_items SET source = 'unknown' WHERE source IS NULL OR TRIM(source) = ''")

    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.create_check_constraint(
            "ck_review_items_status",
            "status IN ('to_review', 'in_progress', 'resolved', 'rejected')",
        )
        batch_op.create_check_constraint(
            "ck_review_items_source",
            "source IN ('pdf_extract', 'rules', 'dedupe', 'categorize', 'unknown')",
        )

    op.create_table(
        "review_item_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("review_item_id", sa.String(), nullable=False),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=True),
        sa.Column("from_status", sa.String(), nullable=True),
        sa.Column("to_status", sa.String(), nullable=True),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["review_item_id"],
            ["review_items.id"],
            name=op.f("fk_review_item_events_review_item_id_review_items"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_review_item_events")),
    )
    op.create_index(
        "ix_review_item_events_review_item_id_created_at",
        "review_item_events",
        ["review_item_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_review_item_events_event_type_created_at",
        "review_item_events",
        ["event_type", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("UPDATE review_items SET status = 'open' WHERE status IN ('to_review', 'in_progress')")
    op.execute("UPDATE review_items SET status = 'resolved' WHERE status = 'rejected'")

    op.drop_index(
        "ix_review_item_events_event_type_created_at",
        table_name="review_item_events",
    )
    op.drop_index(
        "ix_review_item_events_review_item_id_created_at",
        table_name="review_item_events",
    )
    op.drop_table("review_item_events")

    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_review_items_source", type_="check")
        batch_op.drop_constraint("ck_review_items_status", type_="check")
        batch_op.drop_index("ix_review_items_source")
        batch_op.drop_index("ix_review_items_reason_code")
        batch_op.drop_column("source")
