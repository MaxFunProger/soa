"""bookings

Revision ID: 001
Revises:
Create Date: 2025-03-18

"""
from typing import Sequence, Union

from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE booking (
            id UUID PRIMARY KEY,
            user_id VARCHAR(128) NOT NULL,
            flight_id UUID NOT NULL,
            passenger_name VARCHAR(256) NOT NULL,
            passenger_email VARCHAR(256) NOT NULL,
            seat_count INTEGER NOT NULL,
            total_price NUMERIC(14, 2) NOT NULL,
            status VARCHAR(32) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_booking_seats CHECK (seat_count > 0),
            CONSTRAINT ck_booking_price CHECK (total_price > 0)
        );
        CREATE INDEX ix_booking_user ON booking (user_id);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS booking;")
