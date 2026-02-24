"""add partial unique index for root category names

Revision ID: 9d2c4a7f3b11
Revises: ff28cf9903d6
Create Date: 2026-02-24 16:35:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "9d2c4a7f3b11"
down_revision: Union[str, Sequence[str], None] = "ff28cf9903d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ux_categories_root_name_parent_null",
        "categories",
        ["name"],
        unique=True,
        sqlite_where=sa.text("parent_id IS NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ux_categories_root_name_parent_null", table_name="categories")
