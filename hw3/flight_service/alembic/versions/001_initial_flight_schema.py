"""initial flight schema + seed

Revision ID: 001
Revises:
Create Date: 2025-03-18

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE airport (
            iata CHAR(3) PRIMARY KEY,
            name VARCHAR(128) NOT NULL
        );
        CREATE TABLE airline (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            code VARCHAR(3) NOT NULL UNIQUE,
            name VARCHAR(256) NOT NULL
        );
        CREATE TABLE flight (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            flight_number VARCHAR(16) NOT NULL,
            departure_date DATE NOT NULL,
            airline_id UUID NOT NULL REFERENCES airline(id) ON DELETE RESTRICT,
            origin_iata CHAR(3) NOT NULL REFERENCES airport(iata),
            destination_iata CHAR(3) NOT NULL REFERENCES airport(iata),
            scheduled_departure TIMESTAMPTZ NOT NULL,
            scheduled_arrival TIMESTAMPTZ NOT NULL,
            total_seats INTEGER NOT NULL,
            available_seats INTEGER NOT NULL,
            price NUMERIC(12, 2) NOT NULL,
            status VARCHAR(32) NOT NULL,
            CONSTRAINT ck_flight_total CHECK (total_seats > 0),
            CONSTRAINT ck_flight_avail CHECK (available_seats >= 0 AND available_seats <= total_seats),
            CONSTRAINT ck_flight_price CHECK (price > 0),
            CONSTRAINT uq_flight_number_date UNIQUE (flight_number, departure_date)
        );
        CREATE TABLE seat_reservation (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            flight_id UUID NOT NULL REFERENCES flight(id) ON DELETE RESTRICT,
            booking_id UUID NOT NULL UNIQUE,
            seat_count INTEGER NOT NULL,
            status VARCHAR(32) NOT NULL,
            CONSTRAINT ck_res_seats CHECK (seat_count > 0)
        );
        CREATE INDEX ix_flight_route ON flight (origin_iata, destination_iata, departure_date);
        CREATE INDEX ix_reservation_flight ON seat_reservation (flight_id);
    """)

    # Seed airports, airline, sample flights
    op.execute("""
        INSERT INTO airport (iata, name) VALUES
        ('SVO', 'Sheremetyevo'), ('VKO', 'Vnukovo'), ('LED', 'Pulkovo'),
        ('DME', 'Domodedovo'), ('AER', 'Sochi');
        INSERT INTO airline (code, name) VALUES ('SU', 'Aeroflot'), ('DP', 'Pobeda');
    """)
    op.execute("""
        INSERT INTO flight (
            id, flight_number, departure_date, airline_id, origin_iata, destination_iata,
            scheduled_departure, scheduled_arrival, total_seats, available_seats, price, status
        )
        SELECT
            gen_random_uuid(),
            'SU1234',
            DATE '2026-04-01',
            (SELECT id FROM airline WHERE code = 'SU' LIMIT 1),
            'SVO', 'LED',
            TIMESTAMPTZ '2026-04-01 10:00:00+03',
            TIMESTAMPTZ '2026-04-01 11:30:00+03',
            100, 100, 150.00, 'SCHEDULED'
        UNION ALL
        SELECT
            gen_random_uuid(),
            'SU5678',
            DATE '2026-04-01',
            (SELECT id FROM airline WHERE code = 'SU' LIMIT 1),
            'VKO', 'LED',
            TIMESTAMPTZ '2026-04-01 14:00:00+03',
            TIMESTAMPTZ '2026-04-01 15:20:00+03',
            50, 50, 99.99, 'SCHEDULED'
        UNION ALL
        SELECT
            gen_random_uuid(),
            'DP100',
            DATE '2026-04-02',
            (SELECT id FROM airline WHERE code = 'DP' LIMIT 1),
            'SVO', 'AER',
            TIMESTAMPTZ '2026-04-02 08:00:00+03',
            TIMESTAMPTZ '2026-04-02 10:30:00+03',
            180, 180, 45.00, 'SCHEDULED';
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS seat_reservation CASCADE;")
    op.execute("DROP TABLE IF EXISTS flight CASCADE;")
    op.execute("DROP TABLE IF EXISTS airline CASCADE;")
    op.execute("DROP TABLE IF EXISTS airport CASCADE;")
