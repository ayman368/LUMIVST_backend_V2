"""Merge multiple heads

Revision ID: c77f4f4ac74e
Revises: b3c4d5e6f7g8, f1a2b3c4d5e6, g1a2b3c4d5e6
Create Date: 2026-02-27 02:07:05.471630

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c77f4f4ac74e'
down_revision: Union[str, Sequence[str], None] = ('b3c4d5e6f7g8', 'f1a2b3c4d5e6', 'g1a2b3c4d5e6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
