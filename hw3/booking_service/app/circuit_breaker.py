"""
Circuit Breaker для вызовов Flight Service
"""
import logging
import os
import threading
import time

log = logging.getLogger(__name__)

FAILURE_THRESHOLD = int(os.environ.get("CB_FAILURE_THRESHOLD", "5"))
OPEN_DURATION = float(os.environ.get("CB_OPEN_DURATION_SEC", "20"))
WINDOW_SEC = float(os.environ.get("CB_WINDOW_SEC", "60"))

class CircuitOpenError(Exception):
    """Сервис недоступен по circuit breaker"""
    pass


class FlightCircuitBreaker:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(self):
        self._state = self.CLOSED
        self._failures = 0
        self._failure_times: list[float] = []
        self._open_until = 0.0
        self._lock = threading.Lock()

    def _prune_window(self, now: float) -> None:
        self._failure_times = [t for t in self._failure_times if now - t <= WINDOW_SEC]

    def before_call(self) -> None:
        with self._lock:
            now = time.time()
            if self._state == self.OPEN:
                if now >= self._open_until:
                    log.info("circuit: %s -> %s (probe allowed)", self.OPEN, self.HALF_OPEN)
                    self._state = self.HALF_OPEN
                else:
                    raise CircuitOpenError(
                        f"circuit OPEN until {self._open_until:.0f}s (flight service unavailable)"
                    )

    def record_success(self) -> None:
        with self._lock:
            if self._state == self.HALF_OPEN:
                log.info("circuit: %s -> %s (probe OK)", self.HALF_OPEN, self.CLOSED)
            self._state = self.CLOSED
            self._failures = 0
            self._failure_times.clear()

    def record_failure(self, *, retriable: bool) -> None:
        """retriable: UNAVAILABLE / DEADLINE_EXCEEDED после исчерпания retry"""
        with self._lock:
            now = time.time()
            if not retriable:
                return

            self._failure_times.append(now)
            self._prune_window(now)
            count = len(self._failure_times)
            if count >= FAILURE_THRESHOLD:
                log.info(
                    "circuit: %s -> %s (failures in window: %s >= %s)",
                    self._state,
                    self.OPEN,
                    count,
                    FAILURE_THRESHOLD,
                )
                self._state = self.OPEN
                self._open_until = now + OPEN_DURATION
                self._failure_times.clear()

            if self._state == self.HALF_OPEN:
                log.info("circuit: %s -> %s (probe retriable fail)", self.HALF_OPEN, self.OPEN)
                self._state = self.OPEN
                self._open_until = now + OPEN_DURATION
