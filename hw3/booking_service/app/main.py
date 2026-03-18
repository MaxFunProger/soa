import logging
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import grpc
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.circuit_breaker import CircuitOpenError
from app.db import SessionLocal
from app.flight_client import FlightGrpcClient, get_flight_client
from app.models import Booking

logging.basicConfig(
    level=__import__("os").environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def grpc_to_http(exc: grpc.RpcError) -> HTTPException:
    code = exc.code()
    detail = exc.details() or code.name
    mapping = {
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 400,
        grpc.StatusCode.UNAUTHENTICATED: 502,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 504,
    }
    status = mapping.get(code, 502)
    return HTTPException(status_code=status, detail=detail)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    c = getattr(app.state, "flight_client", None)
    if c:
        c.close()


app = FastAPI(title="Booking Service", lifespan=lifespan)
app.state.flight_client = None


def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def flight_client_dep() -> FlightGrpcClient:
    return get_flight_client()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/flights")
def list_flights(
    origin: str = Query(..., min_length=3, max_length=3),
    destination: str = Query(..., min_length=3, max_length=3),
    date: Optional[date] = Query(None, description="YYYY-MM-DD"),
    fc: FlightGrpcClient = Depends(flight_client_dep),
):
    try:
        flights = fc.search_flights(origin.upper(), destination.upper(), date)
        return {"flights": flights}
    except CircuitOpenError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except grpc.RpcError as e:
        raise grpc_to_http(e)


@app.get("/flights/{flight_id}")
def get_flight(flight_id: str, fc: FlightGrpcClient = Depends(flight_client_dep)):
    try:
        return fc.get_flight(flight_id)
    except CircuitOpenError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except grpc.RpcError as e:
        raise grpc_to_http(e)


class BookingCreate(BaseModel):
    user_id: str = Field(..., min_length=1)
    flight_id: str
    passenger_name: str = Field(..., min_length=1)
    passenger_email: EmailStr
    seat_count: int = Field(..., ge=1)


@app.post("/bookings", status_code=201)
def create_booking(
    body: BookingCreate,
    db: Session = Depends(db_session),
    fc: FlightGrpcClient = Depends(flight_client_dep),
):
    try:
        fid = uuid.UUID(body.flight_id.strip())
    except ValueError:
        raise HTTPException(400, "invalid flight_id")

    booking_id = uuid.uuid4()

    try:
        try:
            flight = fc.get_flight(str(fid))
        except grpc.RpcError as e:
            raise grpc_to_http(e)
        if flight.get("status") != "SCHEDULED":
            raise HTTPException(400, "flight not available for booking")

        try:
            fc.reserve_seats(str(fid), body.seat_count, str(booking_id))
        except grpc.RpcError as e:
            raise grpc_to_http(e)

        unit = flight["price"]
        total = unit * body.seat_count
        row = Booking(
            id=booking_id,
            user_id=body.user_id,
            flight_id=fid,
            passenger_name=body.passenger_name,
            passenger_email=str(body.passenger_email),
            seat_count=body.seat_count,
            total_price=total,
            status="CONFIRMED",
        )
        db.add(row)
        try:
            db.commit()
        except Exception:
            db.rollback()
            try:
                fc.release_reservation(str(booking_id))
            except Exception as rel_err:
                log.error("compensate ReleaseReservation failed: %s", rel_err)
            raise
        db.refresh(row)
        return _booking_out(row)
    except CircuitOpenError as e:
        raise HTTPException(503, detail=str(e))


def _booking_out(b: Booking) -> dict:
    return {
        "id": str(b.id),
        "user_id": b.user_id,
        "flight_id": str(b.flight_id),
        "passenger_name": b.passenger_name,
        "passenger_email": b.passenger_email,
        "seat_count": b.seat_count,
        "total_price": str(b.total_price),
        "status": b.status,
        "created_at": b.created_at.isoformat() if b.created_at else None,
    }


@app.get("/bookings/{booking_id}")
def get_booking(booking_id: str, db: Session = Depends(db_session)):
    try:
        bid = uuid.UUID(booking_id)
    except ValueError:
        raise HTTPException(400, "invalid id")
    b = db.scalars(select(Booking).where(Booking.id == bid)).first()
    if not b:
        raise HTTPException(404, "booking not found")
    return _booking_out(b)


@app.get("/bookings")
def list_bookings(user_id: str = Query(...), db: Session = Depends(db_session)):
    rows = db.scalars(select(Booking).where(Booking.user_id == user_id).order_by(Booking.created_at.desc())).all()
    return {"bookings": [_booking_out(b) for b in rows]}


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(
    booking_id: str,
    db: Session = Depends(db_session),
    fc: FlightGrpcClient = Depends(flight_client_dep),
):
    try:
        bid = uuid.UUID(booking_id)
    except ValueError:
        raise HTTPException(400, "invalid id")
    b = db.scalars(select(Booking).where(Booking.id == bid).with_for_update()).first()
    if not b:
        raise HTTPException(404, "booking not found")
    if b.status != "CONFIRMED":
        raise HTTPException(400, "booking not cancellable")

    try:
        fc.release_reservation(str(bid))
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.NOT_FOUND:
            raise grpc_to_http(e)
    b.status = "CANCELLED"
    db.commit()
    db.refresh(b)
    return _booking_out(b)
