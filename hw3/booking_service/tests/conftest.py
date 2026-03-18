"""
Фикстуры: мок gRPC-клиента и фейковая сессия БД
"""
import os
import subprocess
import sys
import uuid
from pathlib import Path

# До импорта app: иначе app.db читает DATABASE_URL при загрузке
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("FLIGHT_GRPC_TARGET", "localhost:50051")
os.environ.setdefault("FLIGHT_GRPC_API_KEY", "test-key")

# Кодогенерация proto для импорта flight_client (один раз)
_hw3_root = Path(__file__).resolve().parents[2]
_gen = Path(__file__).resolve().parent / "_proto_gen"
if not (_gen / "flight" / "v1" / "flight_pb2.py").exists():
    _gen.mkdir(parents=True, exist_ok=True)
    import grpc_tools

    _proto_inc = Path(grpc_tools.__file__).resolve().parent / "_proto"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I" + str(_hw3_root / "proto"),
            "-I" + str(_proto_inc),
            f"--python_out={_gen}",
            f"--grpc_python_out={_gen}",
            str(_hw3_root / "proto" / "flight" / "v1" / "flight.proto"),
        ],
        check=True,
    )
    (_gen / "flight" / "__init__.py").touch()
    (_gen / "flight" / "v1" / "__init__.py").touch()
if str(_gen) not in sys.path:
    sys.path.insert(0, str(_gen))
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.flight_client import FlightGrpcClient, get_flight_client
from app.models import Booking


class FakeBookingStore:
    """хранилище бронирований для тестов"""

    def __init__(self):
        self._bookings: list[Booking] = []

    def add(self, row: Booking) -> None:
        self._bookings.append(row)

    def get(self, booking_id: uuid.UUID) -> Optional[Booking]:
        for b in self._bookings:
            if b.id == booking_id:
                return b
        return None

    def get_by_user(self, user_id: str) -> list[Booking]:
        return [b for b in self._bookings if b.user_id == user_id]

    def update_status(self, booking_id: uuid.UUID, status: str) -> None:
        for b in self._bookings:
            if b.id == booking_id:
                b.status = status
                return


class FakeDbSession:
    """Фейковая сессия БД: add/commit/refresh и запросы к FakeBookingStore"""

    def __init__(self, store: FakeBookingStore):
        self._store = store
        self._closed = False

    def add(self, instance: Any) -> None:
        self._store.add(instance)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def refresh(self, instance: Any) -> None:
        pass

    def close(self) -> None:
        self._closed = True

    def scalars(self, query):
        return _FakeResult(self._store, query)


def _extract_where_value(whereclause, column_key: str):
    """Достаёт значение из BinaryExpression (e.g. Booking.id == bid)."""
    if whereclause is None:
        return None
    if hasattr(whereclause, "clauses"):
        for c in whereclause.clauses:
            v = _extract_where_value(c, column_key)
            if v is not None:
                return v
        return None
    left = getattr(whereclause, "left", None)
    right = getattr(whereclause, "right", None)
    if left is None or right is None:
        return None
    if getattr(left, "key", None) == column_key or (hasattr(left, "name") and left.name == column_key):
        if hasattr(right, "value"):
            return right.value
        if hasattr(right, "effective_value"):
            return right.effective_value
        if hasattr(right, "val"):
            return right.val
        return right
    return None


class _FakeResult:
    def __init__(self, store: FakeBookingStore, query) -> None:
        self._store = store
        self._query = query

    def first(self) -> Optional[Booking]:
        try:
            w = getattr(self._query, "whereclause", None)
            bid = _extract_where_value(w, "id")
            if bid is not None:
                return self._store.get(bid)
            user_id = _extract_where_value(w, "user_id")
            if user_id is not None:
                rows = self._store.get_by_user(str(user_id))
                return rows[0] if rows else None
        except Exception:
            pass
        return None

    def all(self) -> list[Booking]:
        try:
            w = getattr(self._query, "whereclause", None)
            user_id = _extract_where_value(w, "user_id")
            if user_id is not None:
                return sorted(
                    self._store.get_by_user(str(user_id)),
                    key=lambda x: x.created_at or "",
                    reverse=True,
                )
        except Exception:
            pass
        return []

    def with_for_update(self):
        return self


class MockFlightClient(FlightGrpcClient):
    """Мок Flight gRPC: возвращает заданные данные или исключения"""

    def __init__(
        self,
        search_result: Optional[list[dict]] = None,
        get_flight_result: Optional[dict] = None,
        get_flight_error: Optional[Exception] = None,
        reserve_error: Optional[Exception] = None,
        release_error: Optional[Exception] = None,
    ):
        self._search_result = search_result if search_result is not None else []
        self._get_flight_result = get_flight_result
        self._get_flight_error = get_flight_error
        self._reserve_error = reserve_error
        self._release_error = release_error
        self.reserve_calls: list[tuple] = []
        self.release_calls: list[tuple] = []

    def search_flights(self, origin: str, destination: str, departure_date=None):
        return self._search_result

    def get_flight(self, flight_id: str) -> dict:
        if self._get_flight_error:
            raise self._get_flight_error
        if self._get_flight_result is None:
            import grpc
            from grpc import StatusCode
            e = grpc.RpcError()
            e.code = lambda: StatusCode.NOT_FOUND
            e.details = lambda: "flight not found"
            raise e
        return self._get_flight_result

    def reserve_seats(self, flight_id: str, seat_count: int, booking_id: str) -> None:
        self.reserve_calls.append((flight_id, seat_count, booking_id))
        if self._reserve_error:
            raise self._reserve_error

    def release_reservation(self, booking_id: str) -> None:
        self.release_calls.append((booking_id,))
        if self._release_error:
            raise self._release_error

    def close(self) -> None:
        pass


@pytest.fixture
def booking_store():
    return FakeBookingStore()


def _make_fake_session(store: FakeBookingStore):
    return FakeDbSession(store)


@pytest.fixture
def mock_flight_client():
    """Мок по умолчанию: один рейс, get_flight возвращает его"""
    from decimal import Decimal
    flight = {
        "id": "11111111-1111-1111-1111-111111111111",
        "flight_number": "SU123",
        "origin": "SVO",
        "destination": "LED",
        "price": Decimal("100.00"),
        "status": "SCHEDULED",
        "available_seats": 10,
    }
    return MockFlightClient(
        search_result=[flight],
        get_flight_result=flight,
    )


@pytest.fixture
def client(booking_store, mock_flight_client):
    """TestClient с подменой БД и gRPC-клиента"""

    def override_db():
        session = _make_fake_session(booking_store)
        try:
            yield session
        finally:
            session.close()

    def override_flight_client():
        return mock_flight_client

    from app.main import db_session, flight_client_dep
    app.dependency_overrides[db_session] = override_db
    app.dependency_overrides[flight_client_dep] = override_flight_client

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture
def sample_booking(booking_store):
    """Один сохранённый бронирование в store"""
    from decimal import Decimal
    from datetime import datetime, timezone
    bid = uuid.uuid4()
    b = Booking(
        id=bid,
        user_id="user1",
        flight_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        passenger_name="Test",
        passenger_email="test@test.com",
        seat_count=2,
        total_price=Decimal("200.00"),
        status="CONFIRMED",
        created_at=datetime.now(timezone.utc),
    )
    booking_store.add(b)
    return b
