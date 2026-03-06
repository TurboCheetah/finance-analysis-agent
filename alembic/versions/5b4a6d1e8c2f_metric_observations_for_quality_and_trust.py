"""add metric observations for quality and trust reporting

Revision ID: 5b4a6d1e8c2f
Revises: 4e8b1c7d2a9f
Create Date: 2026-03-06 11:20:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5b4a6d1e8c2f"
down_revision: Union[str, Sequence[str], None] = "4e8b1c7d2a9f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "metric_observations",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=True),
        sa.Column("metric_group", sa.String(), nullable=False),
        sa.Column("metric_key", sa.String(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=True),
        sa.Column("template_key", sa.String(), nullable=True),
        sa.Column("metric_value", sa.Float(), nullable=True),
        sa.Column("numerator", sa.Float(), nullable=True),
        sa.Column("denominator", sa.Float(), nullable=True),
        sa.Column("threshold_value", sa.Float(), nullable=True),
        sa.Column("threshold_operator", sa.String(), nullable=True),
        sa.Column("alert_status", sa.String(), nullable=False),
        sa.Column("dimensions_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_metric_observations_metric_key_period",
        "metric_observations",
        ["metric_key", "period_start", "period_end"],
        unique=False,
    )
    op.create_index(
        "ix_metric_observations_account_id_period_end",
        "metric_observations",
        ["account_id", "period_end"],
        unique=False,
    )
    op.create_index(
        "ix_metric_observations_template_key",
        "metric_observations",
        ["template_key"],
        unique=False,
    )
    op.create_index(
        "ix_metric_observations_alert_status",
        "metric_observations",
        ["alert_status"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_metric_observations_alert_status", table_name="metric_observations")
    op.drop_index("ix_metric_observations_template_key", table_name="metric_observations")
    op.drop_index("ix_metric_observations_account_id_period_end", table_name="metric_observations")
    op.drop_index("ix_metric_observations_metric_key_period", table_name="metric_observations")
    op.drop_table("metric_observations")
