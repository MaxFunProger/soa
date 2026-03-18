import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from app.cache import (
    cache_get_json,
    cache_set_json,
    flight_key,
    get_redis,
    invalidate_flight_and_searches,
    search_key,
)
from app.db import SessionLocal
from app.models import Airline, Flight, SeatReservation

from flight.v1 import flight_pb2, flight_pb2_grpc

log = logging.getLogger(__name__)

STATUS_DB_TO_PROTO = {
    "SCHEDULED": flight_pb2.FLIGHT_STATUS_SCHEDULED,
    "DEPARTED": flight_pb2.FLIGHT_STATUS_DEPARTED,
    "CANCELLED": flight_pb2.FLIGHT_STATUS_CANCELLED,
    "COMPLETED": flight_pb2.FLIGHT_STATUS_COMPLETED,
}


def _ts(dt: datetime) -> Timestamp:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(dt.astimezone(timezone.utc).replace(tzinfo=None))
    return ts


def _flight_to_pb(f: Flight, airline: Airline) -> flight_pb2.Flight:
    return flight_pb2.Flight(
        id=str(f.id),
        flight_number=f.flight_number,
        departure_date=_ts(datetime.combine(f.departure_date, datetime.min.time(), tzinfo=timezone.utc)),
        airline_code=airline.code,
        airline_name=airline.name,
        origin_iata=f.origin_iata.strip(),
        destination_iata=f.destination_iata.strip(),
        scheduled_departure=_ts(f.scheduled_departure),
        scheduled_arrival=_ts(f.scheduled_arrival),
        total_seats=f.total_seats,
        available_seats=f.available_seats,
        price_amount=str(f.price),
        currency="RUB",
        status=STATUS_DB_TO_PROTO.get(f.status, flight_pb2.FLIGHT_STATUS_UNSPECIFIED),
    )


def _flight_to_dict(f: Flight, airline: Airline) -> dict:
    return {
        "id": str(f.id),
        "flight_number": f.flight_number,
        "departure_date": f.departure_date.isoformat(),
        "airline_code": airline.code,
        "airline_name": airline.name,
        "origin_iata": f.origin_iata.strip(),
        "destination_iata": f.destination_iata.strip(),
        "scheduled_departure": f.scheduled_departure.isoformat(),
        "scheduled_arrival": f.scheduled_arrival.isoformat(),
        "total_seats": f.total_seats,
        "available_seats": f.available_seats,
        "price_amount": str(f.price),
        "currency": "RUB",
        "status": f.status,
    }


def _dict_to_flight_pb(d: dict) -> flight_pb2.Flight:
    st = STATUS_DB_TO_PROTO.get(d.get("status", ""), flight_pb2.FLIGHT_STATUS_SCHEDULED)
    dep = datetime.fromisoformat(d["scheduled_departure"].replace("Z", "+00:00"))
    arr = datetime.fromisoformat(d["scheduled_arrival"].replace("Z", "+00:00"))
    dd = date.fromisoformat(d["departure_date"])
    return flight_pb2.Flight(
        id=d["id"],
        flight_number=d["flight_number"],
        departure_date=_ts(datetime.combine(dd, datetime.min.time(), tzinfo=timezone.utc)),
        airline_code=d["airline_code"],
        airline_name=d["airline_name"],
        origin_iata=d["origin_iata"],
        destination_iata=d["destination_iata"],
        scheduled_departure=_ts(dep),
        scheduled_arrival=_ts(arr),
        total_seats=d["total_seats"],
        available_seats=d["available_seats"],
        price_amount=d["price_amount"],
        currency=d.get("currency", "RUB"),
        status=st,
    )


class FlightServicer(flight_pb2_grpc.FlightServiceServicer):
    def SearchFlights(self, request, context):
        if not request.origin_iata or not request.destination_iata:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "origin_iata and destination_iata required")
        o, d = request.origin_iata.upper()[:3], request.destination_iata.upper()[:3]
        dep_date = None
        if request.HasField("departure_date"):
            dep_date = request.departure_date.ToDatetime().date()

        r = get_redis()
        sk = search_key(o, d, dep_date)
        hit, cached = cache_get_json(r, sk)
        if hit and cached is not None:
            return flight_pb2.SearchFlightsResponse(
                flights=[_dict_to_flight_pb(x) for x in cached.get("flights", [])]
            )

        session: Session = SessionLocal()
        try:
            q = (
                select(Flight)
                .options(joinedload(Flight.airline))
                .where(
                    Flight.origin_iata == o,
                    Flight.destination_iata == d,
                    Flight.status == "SCHEDULED",
                )
            )
            if dep_date is not None:
                q = q.where(Flight.departure_date == dep_date)
            rows = session.scalars(q).unique().all()
            lst = [_flight_to_dict(f, f.airline) for f in rows]
            cache_set_json(r, sk, {"flights": lst})
            return flight_pb2.SearchFlightsResponse(flights=[_dict_to_flight_pb(x) for x in lst])
        finally:
            session.close()

    def GetFlight(self, request, context):
        if not request.flight_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "flight_id required")
        fid = request.flight_id.strip()
        r = get_redis()
        fk = flight_key(fid)
        hit, cached = cache_get_json(r, fk)
        if hit and cached:
            return flight_pb2.GetFlightResponse(flight=_dict_to_flight_pb(cached))

        session: Session = SessionLocal()
        try:
            f = session.scalars(
                select(Flight).options(joinedload(Flight.airline)).where(Flight.id == uuid.UUID(fid))
            ).first()
            if not f:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            data = _flight_to_dict(f, f.airline)
            cache_set_json(r, fk, data)
            return flight_pb2.GetFlightResponse(flight=_flight_to_pb(f, f.airline))
        except ValueError:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid flight_id")
        finally:
            session.close()

    def ReserveSeats(self, request, context):
        if not request.flight_id or request.seat_count < 1:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "flight_id and seat_count>=1 required")
        try:
            bid = uuid.UUID(request.booking_id.strip())
        except ValueError:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid booking_id")

        fid = uuid.UUID(request.flight_id.strip())
        n = int(request.seat_count)
        r = get_redis()
        session: Session = SessionLocal()
        try:
            existing = session.scalars(
                select(SeatReservation).where(SeatReservation.booking_id == bid).with_for_update()
            ).first()
            if existing:
                if existing.status == "ACTIVE":
                    log.info("ReserveSeats idempotent hit booking_id=%s", bid)
                    return flight_pb2.ReserveSeatsResponse(
                        reservation_id=str(existing.id), reserved_seats=existing.seat_count
                    )
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "booking_id already used in a completed reservation",
                )

            fl = session.scalars(select(Flight).where(Flight.id == fid).with_for_update()).first()
            if not fl:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            if fl.status != "SCHEDULED":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "flight not open for booking")
            if fl.available_seats < n:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "not enough seats")

            fl.available_seats -= n
            res = SeatReservation(
                flight_id=fid, booking_id=bid, seat_count=n, status="ACTIVE"
            )
            session.add(res)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                session.expire_all()
                ex2 = session.scalars(select(SeatReservation).where(SeatReservation.booking_id == bid)).first()
                if ex2 and ex2.status == "ACTIVE":
                    return flight_pb2.ReserveSeatsResponse(
                        reservation_id=str(ex2.id), reserved_seats=ex2.seat_count
                    )
                context.abort(grpc.StatusCode.ABORTED, "concurrent reservation conflict")

            session.refresh(res)
            invalidate_flight_and_searches(r, str(fl.id), fl.origin_iata, fl.destination_iata)
            return flight_pb2.ReserveSeatsResponse(reservation_id=str(res.id), reserved_seats=n)
        finally:
            session.close()

    def ReleaseReservation(self, request, context):
        try:
            bid = uuid.UUID((request.booking_id or "").strip())
        except ValueError:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid booking_id")

        r = get_redis()
        session: Session = SessionLocal()
        try:
            res = session.scalars(
                select(SeatReservation).where(SeatReservation.booking_id == bid).with_for_update()
            ).first()
            if not res:
                context.abort(grpc.StatusCode.NOT_FOUND, "reservation not found")
            if res.status != "ACTIVE":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "reservation not active")

            fl = session.scalars(select(Flight).where(Flight.id == res.flight_id).with_for_update()).first()
            if not fl:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            fl.available_seats = min(fl.total_seats, fl.available_seats + res.seat_count)
            res.status = "RELEASED"
            session.commit()
            invalidate_flight_and_searches(r, str(fl.id), fl.origin_iata, fl.destination_iata)
            return flight_pb2.ReleaseReservationResponse(released_seats=res.seat_count)
        finally:
            session.close()

    def UpdateFlight(self, request, context):
        if not request.flight_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "flight_id required")
        try:
            fid = uuid.UUID(request.flight_id.strip())
        except ValueError:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid flight_id")

        r = get_redis()
        session: Session = SessionLocal()
        try:
            fl = session.scalars(select(Flight).where(Flight.id == fid).with_for_update()).first()
            if not fl:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            if request.HasField("status"):
                num = request.status
                if num == flight_pb2.FLIGHT_STATUS_SCHEDULED:
                    fl.status = "SCHEDULED"
                elif num == flight_pb2.FLIGHT_STATUS_DEPARTED:
                    fl.status = "DEPARTED"
                elif num == flight_pb2.FLIGHT_STATUS_CANCELLED:
                    fl.status = "CANCELLED"
                elif num == flight_pb2.FLIGHT_STATUS_COMPLETED:
                    fl.status = "COMPLETED"
            if request.HasField("total_seats") and request.total_seats > 0:
                delta = request.total_seats - fl.total_seats
                fl.total_seats = request.total_seats
                fl.available_seats = max(0, min(fl.total_seats, fl.available_seats + delta))
            session.commit()
            session.refresh(fl)
            fl = session.scalars(
                select(Flight).options(joinedload(Flight.airline)).where(Flight.id == fid)
            ).first()
            invalidate_flight_and_searches(r, str(fl.id), fl.origin_iata, fl.destination_iata)
            return flight_pb2.UpdateFlightResponse(flight=_flight_to_pb(fl, fl.airline))
        finally:
            session.close()
