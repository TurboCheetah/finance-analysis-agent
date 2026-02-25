"""add dedupe candidate event audit table

Revision ID: d5f0c7a1e9b2
Revises: 8c1d7bf6e2a4
Create Date: 2026-02-25 16:45:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "d5f0c7a1e9b2"
down_revision: Union[str, Sequence[str], None] = "8c1d7bf6e2a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "dedupe_candidate_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("dedupe_candidate_id", sa.String(), nullable=False),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("old_value_json", sa.JSON(), nullable=True),
        sa.Column("new_value_json", sa.JSON(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dedupe_candidate_id"],
            ["dedupe_candidates.id"],
            name=op.f("fk_dedupe_candidate_events_dedupe_candidate_id_dedupe_candidates"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dedupe_candidate_events")),
    )
    op.create_index(
        "ix_dedupe_candidate_events_candidate_id_created_at",
        "dedupe_candidate_events",
        ["dedupe_candidate_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_dedupe_candidate_events_event_type_created_at",
        "dedupe_candidate_events",
        ["event_type", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_dedupe_candidate_events_event_type_created_at",
        table_name="dedupe_candidate_events",
    )
    op.drop_index(
        "ix_dedupe_candidate_events_candidate_id_created_at",
        table_name="dedupe_candidate_events",
    )
    op.drop_table("dedupe_candidate_events")
