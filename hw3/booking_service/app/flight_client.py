"""
gRPC-клиент Flight Service
"""
import logging
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Optional

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from app.circuit_breaker import CircuitOpenError, FlightCircuitBreaker

from flight.v1 import flight_pb2, flight_pb2_grpc

log = logging.getLogger(__name__)

RETRY_CODES = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
BACKOFF_SEC = (0.1, 0.2, 0.4)
MAX_ATTEMPTS = 3


def _ts_date(d: date) -> Timestamp:
    t = Timestamp()
    dt = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
    t.FromDatetime(dt.replace(tzinfo=None))
    return t


class FlightGrpcClient:
    def __init__(self):
        target = os.environ["FLIGHT_GRPC_TARGET"]
        self._deadline = float(os.environ.get("GRPC_DEADLINE_SEC", "10"))
        api = os.environ.get("FLIGHT_GRPC_API_KEY", "")
        self._metadata = (("x-api-key", api),)
        self._channel = grpc.insecure_channel(target)
        self._stub = flight_pb2_grpc.FlightServiceStub(self._channel)
        self._cb = FlightCircuitBreaker()

    def close(self) -> None:
        self._channel.close()

    def _call(
        self,
        op_name: str,
        fn: Callable[..., Any],
        *args,
        **kwargs,
    ):
        try:
            self._cb.before_call()
        except CircuitOpenError as e:
            log.warning("circuit blocked %s: %s", op_name, e)
            raise

        last_err: Optional[grpc.RpcError] = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                out = fn(
                    *args,
                    **kwargs,
                    metadata=self._metadata,
                    timeout=self._deadline,
                )
                self._cb.record_success()
                return out
            except grpc.RpcError as e:
                last_err = e
                code = e.code()
                if code in RETRY_CODES and attempt < MAX_ATTEMPTS - 1:
                    delay = BACKOFF_SEC[attempt]
                    log.info(
                        "grpc %s retry %s/%s after %s sleep=%s",
                        op_name,
                        attempt + 1,
                        MAX_ATTEMPTS,
                        code.name,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                retriable = code in RETRY_CODES
                self._cb.record_failure(retriable=retriable)
                raise
        assert last_err
        raise last_err

    def search_flights(
        self, origin: str, destination: str, departure_date: Optional[date] = None
    ) -> list[dict]:
        req = flight_pb2.SearchFlightsRequest(
            origin_iata=origin, destination_iata=destination
        )
        if departure_date:
            req.departure_date.CopyFrom(_ts_date(departure_date))
        resp = self._call("SearchFlights", self._stub.SearchFlights, req)
        out = []
        for f in resp.flights:
            if f.status != flight_pb2.FLIGHT_STATUS_SCHEDULED:
                continue
            out.append(self._flight_to_rest(f))
        return out

    def get_flight(self, flight_id: str) -> dict:
        req = flight_pb2.GetFlightRequest(flight_id=flight_id)
        resp = self._call("GetFlight", self._stub.GetFlight, req)
        return self._flight_to_rest(resp.flight)

    def reserve_seats(self, flight_id: str, seat_count: int, booking_id: str) -> None:
        req = flight_pb2.ReserveSeatsRequest(
            flight_id=flight_id, seat_count=seat_count, booking_id=booking_id
        )
        self._call("ReserveSeats", self._stub.ReserveSeats, req)

    def release_reservation(self, booking_id: str) -> None:
        req = flight_pb2.ReleaseReservationRequest(booking_id=booking_id)
        self._call("ReleaseReservation", self._stub.ReleaseReservation, req)

    @staticmethod
    def _flight_to_rest(f: flight_pb2.Flight) -> dict:
        dep = f.departure_date.ToDatetime().date().isoformat()
        return {
            "id": f.id,
            "flight_number": f.flight_number,
            "departure_date": dep,
            "airline_code": f.airline_code,
            "airline_name": f.airline_name,
            "origin": f.origin_iata,
            "destination": f.destination_iata,
            "scheduled_departure": f.scheduled_departure.ToDatetime().isoformat(),
            "scheduled_arrival": f.scheduled_arrival.ToDatetime().isoformat(),
            "total_seats": f.total_seats,
            "available_seats": f.available_seats,
            "price": Decimal(f.price_amount),
            "currency": f.currency or "RUB",
            "status": "SCHEDULED",
        }


_client: Optional[FlightGrpcClient] = None


def get_flight_client() -> FlightGrpcClient:
    global _client
    if _client is None:
        _client = FlightGrpcClient()
    return _client
