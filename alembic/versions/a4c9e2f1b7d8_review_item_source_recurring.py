"""allow recurring source in review_items

Revision ID: a4c9e2f1b7d8
Revises: 7e3b4c2d1f90
Create Date: 2026-02-27 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a4c9e2f1b7d8"
down_revision: Union[str, Sequence[str], None] = "7e3b4c2d1f90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ACTIVE_DEDUPE_REVIEW_FILTER_SQL = """
    ref_table = 'dedupe_candidates'
    AND item_type = 'dedupe_candidate_suggestion'
    AND source = 'dedupe'
    AND status IN ('to_review', 'in_progress')
"""


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_review_items_source", type_="check")
        batch_op.create_check_constraint(
            "ck_review_items_source",
            "source IN ('pdf_extract', 'rules', 'dedupe', 'categorize', 'recurring', 'unknown')",
        )
    op.execute("PRAGMA foreign_keys=ON")
    op.drop_index("ux_review_items_active_dedupe_candidate", table_name="review_items")
    op.create_index(
        "ux_review_items_active_dedupe_candidate",
        "review_items",
        ["ref_table", "ref_id", "item_type", "source"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_DEDUPE_REVIEW_FILTER_SQL),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("UPDATE review_items SET source = 'unknown' WHERE source = 'recurring'")
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("review_items", recreate="always") as batch_op:
        batch_op.drop_constraint("ck_review_items_source", type_="check")
        batch_op.create_check_constraint(
            "ck_review_items_source",
            "source IN ('pdf_extract', 'rules', 'dedupe', 'categorize', 'unknown')",
        )
    op.execute("PRAGMA foreign_keys=ON")
    op.drop_index("ux_review_items_active_dedupe_candidate", table_name="review_items")
    op.create_index(
        "ux_review_items_active_dedupe_candidate",
        "review_items",
        ["ref_table", "ref_id", "item_type", "source"],
        unique=True,
        sqlite_where=sa.text(_ACTIVE_DEDUPE_REVIEW_FILTER_SQL),
    )
