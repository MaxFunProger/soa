import os
import subprocess
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

# proto для servicer
_hw3 = Path(__file__).resolve().parents[2]
_gen = Path(__file__).resolve().parent / "_proto_gen"
if not (_gen / "flight" / "v1" / "flight_pb2.py").exists():
    _gen.mkdir(parents=True, exist_ok=True)
    import grpc_tools

    _inc = Path(grpc_tools.__file__).resolve().parent / "_proto"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I" + str(_hw3 / "proto"),
            "-I" + str(_inc),
            f"--python_out={_gen}",
            f"--grpc_python_out={_gen}",
            str(_hw3 / "proto" / "flight" / "v1" / "flight.proto"),
        ],
        check=True,
    )
    (_gen / "flight" / "__init__.py").touch()
    (_gen / "flight" / "v1" / "__init__.py").touch()
sys.path.insert(0, str(_gen))


class FakeRedis:
    """Минимальный Redis для тестов: cache miss, инвалидация no-op"""

    def get(self, key):
        return None

    def setex(self, key, ttl, val):
        pass

    def delete(self, *keys):
        return 0

    def scan_iter(self, match=None, count=None):
        return iter([])

    def ping(self):
        return True


@pytest.fixture
def sqlite_engine():
    from app.models import Base

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session_factory(sqlite_engine):
    return sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)


@pytest.fixture
def seed_flight(db_session_factory):
    """Один рейс SVO-LED, 10 мест, SCHEDULED"""
    from app.models import Airline, Airport, Flight

    aid = uuid.uuid4()
    fid = uuid.uuid4()
    with db_session_factory() as s:
        s.add(Airport(iata="SVO", name="Sheremetyevo"))
        s.add(Airport(iata="LED", name="Pulkovo"))
        s.add(Airline(id=aid, code="SU", name="Aeroflot"))
        s.add(
            Flight(
                id=fid,
                flight_number="SU1",
                departure_date=date(2026, 4, 1),
                airline_id=aid,
                origin_iata="SVO",
                destination_iata="LED",
                scheduled_departure=datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc),
                scheduled_arrival=datetime(2026, 4, 1, 11, 0, tzinfo=timezone.utc),
                total_seats=10,
                available_seats=10,
                price=Decimal("100.00"),
                status="SCHEDULED",
            )
        )
        s.commit()
    return fid, aid


