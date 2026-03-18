import uuid

import grpc
import pytest


class GrpcNotFound(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.NOT_FOUND

    def details(self):
        return "not found"


def test_health(client):
    """GET /health возвращает 200 и статус ok"""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_list_flights_returns_mock_data(client, mock_flight_client):
    """GET /flights проксирует SearchFlights: ответ содержит рейсы из мока"""
    r = client.get("/flights?origin=SVO&destination=LED")
    assert r.status_code == 200
    data = r.json()
    assert "flights" in data
    assert len(data["flights"]) == 1
    assert data["flights"][0]["flight_number"] == "SU123"
    assert data["flights"][0]["origin"] == "SVO"
    assert data["flights"][0]["destination"] == "LED"


def test_list_flights_optional_date(client):
    """GET /flights с опциональным date - успешный ответ"""
    r = client.get("/flights?origin=SVO&destination=LED&date=2026-04-01")
    assert r.status_code == 200
    assert "flights" in r.json()


def test_list_flights_validation_origin(client):
    """Короткий origin (не 3 символа) - 422 валидация FastAPI"""
    r = client.get("/flights?origin=AB&destination=LED")
    assert r.status_code == 422


def test_get_flight_success(client, mock_flight_client):
    """GET /flights/{id} возвращает данные рейса от GetFlight"""
    fid = "11111111-1111-1111-1111-111111111111"
    r = client.get(f"/flights/{fid}")
    assert r.status_code == 200
    assert r.json()["id"] == fid
    assert r.json()["flight_number"] == "SU123"


def test_get_flight_not_found(client):
    """GetFlight с NOT_FOUND -> HTTP 404"""
    from app.flight_client import get_flight_client
    from app.main import app, flight_client_dep

    mock = type("Mock", (), {
        "search_flights": lambda *a, **k: [],
        "get_flight": lambda self, fid: (_ for _ in ()).throw(GrpcNotFound()),
        "reserve_seats": lambda *a, **k: None,
        "release_reservation": lambda *a, **k: None,
        "close": lambda: None,
        "reserve_calls": [],
        "release_calls": [],
    })()
    app.dependency_overrides[flight_client_dep] = lambda: mock
    r = client.get("/flights/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404
    app.dependency_overrides.pop(flight_client_dep, None)


def test_create_booking_success(client, mock_flight_client, booking_store):
    """POST /bookings: GetFlight + ReserveSeats, CONFIRMED, total_price = места x цена"""
    payload = {
        "user_id": "u1",
        "flight_id": "11111111-1111-1111-1111-111111111111",
        "passenger_name": "Ivan",
        "passenger_email": "ivan@test.ru",
        "seat_count": 2,
    }
    r = client.post("/bookings", json=payload)
    assert r.status_code == 201
    data = r.json()
    assert data["user_id"] == "u1"
    assert data["flight_id"] == "11111111-1111-1111-1111-111111111111"
    assert data["passenger_name"] == "Ivan"
    assert data["seat_count"] == 2
    assert data["status"] == "CONFIRMED"
    assert data["total_price"] == "200.00"
    assert "id" in data

    assert len(mock_flight_client.reserve_calls) == 1
    flight_id, seat_count, booking_id = mock_flight_client.reserve_calls[0]
    assert flight_id == "11111111-1111-1111-1111-111111111111"
    assert seat_count == 2
    assert data["id"] == booking_id


def test_create_booking_flight_not_found(client, booking_store):
    """Рейс не найден (GetFlight) -> 404, бронирование не создаётся"""
    from app.main import app, flight_client_dep
    from tests.conftest import MockFlightClient
    mock = MockFlightClient(get_flight_result=None, get_flight_error=None)  # NOT_FOUND
    app.dependency_overrides[flight_client_dep] = lambda: mock
    r = client.post("/bookings", json={
        "user_id": "u1",
        "flight_id": "00000000-0000-0000-0000-000000000000",
        "passenger_name": "I",
        "passenger_email": "a@b.ru",
        "seat_count": 1,
    })
    assert r.status_code == 404
    app.dependency_overrides.pop(flight_client_dep, None)


def test_get_booking_success(client, sample_booking):
    """GET /bookings/{id} возвращает сохранённую бронь"""
    r = client.get(f"/bookings/{sample_booking.id}")
    assert r.status_code == 200
    assert r.json()["id"] == str(sample_booking.id)
    assert r.json()["user_id"] == "user1"
    assert r.json()["status"] == "CONFIRMED"


def test_get_booking_not_found(client):
    """Несуществующий id брони -> 404"""
    r = client.get(f"/bookings/{uuid.uuid4()}")
    assert r.status_code == 404


def test_list_bookings(client, sample_booking):
    """GET /bookings?user_id=... возвращает брони пользователя"""
    r = client.get("/bookings?user_id=user1")
    assert r.status_code == 200
    assert len(r.json()["bookings"]) == 1
    assert r.json()["bookings"][0]["id"] == str(sample_booking.id)


def test_list_bookings_empty(client):
    """У пользователя без броней - пустой список"""
    r = client.get("/bookings?user_id=nobody")
    assert r.status_code == 200
    assert r.json()["bookings"] == []


def test_cancel_booking_success(client, sample_booking, mock_flight_client):
    """Отмена CONFIRMED: ReleaseReservation и статус CANCELLED"""
    r = client.post(f"/bookings/{sample_booking.id}/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "CANCELLED"
    assert len(mock_flight_client.release_calls) == 1
    assert mock_flight_client.release_calls[0][0] == str(sample_booking.id)


def test_cancel_booking_not_found(client):
    """Отмена несуществующей брони -> 404"""
    r = client.post(f"/bookings/{uuid.uuid4()}/cancel")
    assert r.status_code == 404


def test_cancel_booking_already_cancelled(client, sample_booking, booking_store):
    """Повторная отмена (уже CANCELLED) -> 400"""
    booking_store.update_status(sample_booking.id, "CANCELLED")
    r = client.post(f"/bookings/{sample_booking.id}/cancel")
    assert r.status_code == 400


