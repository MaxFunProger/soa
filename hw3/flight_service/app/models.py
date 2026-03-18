import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    CheckConstraint,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class Airport(Base):
    __tablename__ = "airport"
    iata: Mapped[str] = mapped_column(CHAR(3), primary_key=True)
    name: Mapped[str] = mapped_column(String(128))


class Airline(Base):
    __tablename__ = "airline"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(String(3), unique=True)
    name: Mapped[str] = mapped_column(String(256))


class Flight(Base):
    __tablename__ = "flight"
    __table_args__ = (
        UniqueConstraint("flight_number", "departure_date", name="uq_flight_number_date"),
        CheckConstraint("total_seats > 0"),
        CheckConstraint("available_seats >= 0 AND available_seats <= total_seats"),
        CheckConstraint("price > 0"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    flight_number: Mapped[str] = mapped_column(String(16))
    departure_date: Mapped[date] = mapped_column(Date)
    airline_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("airline.id"))
    origin_iata: Mapped[str] = mapped_column(CHAR(3), ForeignKey("airport.iata"))
    destination_iata: Mapped[str] = mapped_column(CHAR(3), ForeignKey("airport.iata"))
    scheduled_departure: Mapped[datetime] = mapped_column()
    scheduled_arrival: Mapped[datetime] = mapped_column()
    total_seats: Mapped[int] = mapped_column(Integer)
    available_seats: Mapped[int] = mapped_column(Integer)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    status: Mapped[str] = mapped_column(String(32))

    airline = relationship("Airline")


class SeatReservation(Base):
    __tablename__ = "seat_reservation"
    __table_args__ = (CheckConstraint("seat_count > 0"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    flight_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("flight.id"))
    booking_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), unique=True)
    seat_count: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32))
