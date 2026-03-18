"""Тесты gRPC FlightServicer"""
import uuid

import grpc
import pytest

from flight.v1 import flight_pb2


class AbortException(Exception):
    def __init__(self, code, details=""):
        self.code_val = code
        self.details_val = details
        super().__init__(details)


@pytest.fixture
def servicer_ctx_abort(db_session_factory, monkeypatch):
    """FlightServicer + SQLite; ctx.abort бросает AbortException"""
    from tests.conftest import FakeRedis
    import app.servicer as servicer_mod

    monkeypatch.setattr(servicer_mod, "SessionLocal", db_session_factory)
    monkeypatch.setattr(servicer_mod, "get_redis", lambda: FakeRedis())

    ctx = type("Ctx", (), {})()
    ctx.abort = lambda c, d="": (_ for _ in ()).throw(AbortException(c, d))

    from app.servicer import FlightServicer

    return FlightServicer(), ctx


def test_search_flights_scheduled_only(servicer_ctx_abort, seed_flight):
    """SearchFlights по маршруту возвращает только SCHEDULED-рейсы, включая сид"""
    svc, ctx = servicer_ctx_abort
    fid, _ = seed_flight
    req = flight_pb2.SearchFlightsRequest(origin_iata="SVO", destination_iata="LED")
    resp = svc.SearchFlights(req, ctx)
    assert len(resp.flights) >= 1
    ids = [f.id for f in resp.flights]
    assert str(fid) in ids


def test_search_flights_invalid_args(servicer_ctx_abort):
    """Пустой origin -> INVALID_ARGUMENT"""
    svc, ctx = servicer_ctx_abort
    req = flight_pb2.SearchFlightsRequest(origin_iata="", destination_iata="LED")
    with pytest.raises(AbortException) as ei:
        svc.SearchFlights(req, ctx)
    assert ei.value.code_val == grpc.StatusCode.INVALID_ARGUMENT


def test_get_flight_found(servicer_ctx_abort, seed_flight):
    """GetFlight по существующему id возвращает рейс и available_seats"""
    svc, ctx = servicer_ctx_abort
    fid, _ = seed_flight
    req = flight_pb2.GetFlightRequest(flight_id=str(fid))
    resp = svc.GetFlight(req, ctx)
    assert resp.flight.id == str(fid)
    assert resp.flight.available_seats == 10


def test_get_flight_not_found(servicer_ctx_abort):
    """Несуществующий flight_id -> NOT_FOUND"""
    svc, ctx = servicer_ctx_abort
    req = flight_pb2.GetFlightRequest(flight_id=str(uuid.uuid4()))
    with pytest.raises(AbortException) as ei:
        svc.GetFlight(req, ctx)
    assert ei.value.code_val == grpc.StatusCode.NOT_FOUND


def test_reserve_seats_success_and_decrements(servicer_ctx_abort, seed_flight, db_session_factory):
    """ReserveSeats уменьшает available_seats в БД на запрошенное число"""
    svc, ctx = servicer_ctx_abort
    fid, _ = seed_flight
    bid = str(uuid.uuid4())
    req = flight_pb2.ReserveSeatsRequest(flight_id=str(fid), seat_count=3, booking_id=bid)
    resp = svc.ReserveSeats(req, ctx)
    assert resp.reserved_seats == 3

    with db_session_factory() as s:
        from app.models import Flight
        from sqlalchemy import select

        fl = s.scalars(select(Flight).where(Flight.id == fid)).first()
        assert fl.available_seats == 7


def test_reserve_seats_idempotent(servicer_ctx_abort, seed_flight, db_session_factory):
    """Повторный ReserveSeats с тем же booking_id не списывает места дважды"""
    svc, ctx = servicer_ctx_abort
    fid, _ = seed_flight
    bid = str(uuid.uuid4())
    req = flight_pb2.ReserveSeatsRequest(flight_id=str(fid), seat_count=2, booking_id=bid)
    r1 = svc.ReserveSeats(req, ctx)
    r2 = svc.ReserveSeats(req, ctx)
    assert r1.reservation_id == r2.reservation_id
    assert r2.reserved_seats == 2
    with db_session_factory() as s:
        from app.models import Flight
        from sqlalchemy import select

        fl = s.scalars(select(Flight).where(Flight.id == fid)).first()
        assert fl.available_seats == 8


def test_reserve_seats_exhausted(servicer_ctx_abort, seed_flight):
    """Запрос мест больше available -> RESOURCE_EXHAUSTED"""
    svc, ctx = servicer_ctx_abort
    fid, _ = seed_flight
    req = flight_pb2.ReserveSeatsRequest(flight_id=str(fid), seat_count=11, booking_id=str(uuid.uuid4()))
    with pytest.raises(AbortException) as ei:
        svc.ReserveSeats(req, ctx)
    assert ei.value.code_val == grpc.StatusCode.RESOURCE_EXHAUSTED


def test_release_reservation(servicer_ctx_abort, seed_flight, db_session_factory):
    """ReleaseReservation возвращает места на рейс (available_seats восстановлены)"""
    svc, ctx = servicer_ctx_abort
    fid, _ = seed_flight
    bid = str(uuid.uuid4())
    svc.ReserveSeats(
        flight_pb2.ReserveSeatsRequest(flight_id=str(fid), seat_count=4, booking_id=bid),
        ctx,
    )
    resp = svc.ReleaseReservation(flight_pb2.ReleaseReservationRequest(booking_id=bid), ctx)
    assert resp.released_seats == 4
    with db_session_factory() as s:
        from app.models import Flight
        from sqlalchemy import select

        fl = s.scalars(select(Flight).where(Flight.id == fid)).first()
        assert fl.available_seats == 10


def test_release_reservation_not_found(servicer_ctx_abort):
    """ReleaseReservation по несуществующему booking_id -> NOT_FOUND"""
    svc, ctx = servicer_ctx_abort
    with pytest.raises(AbortException) as ei:
        svc.ReleaseReservation(
            flight_pb2.ReleaseReservationRequest(booking_id=str(uuid.uuid4())),
            ctx,
        )
    assert ei.value.code_val == grpc.StatusCode.NOT_FOUND
