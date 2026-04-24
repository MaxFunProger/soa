"""Synthetic event generator producing realistic user sessions."""
from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone

from .kafka_producer import MovieEventProducer

log = logging.getLogger(__name__)

DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
MOVIES = [f"movie-{i:03d}" for i in range(1, 41)]
USER_POOL = [f"user-{i:04d}" for i in range(1, 201)]


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _build_event(
    *,
    user_id: str,
    movie_id: str,
    session_id: str,
    event_type: str,
    device_type: str,
    progress_seconds: int,
    timestamp_ms: int | None = None,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "timestamp": timestamp_ms if timestamp_ms is not None else _now_ms(),
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": progress_seconds,
    }


def _simulate_session(producer: MovieEventProducer) -> int:
    """Generate a realistic viewing session. Returns the number of events produced."""
    user_id = random.choice(USER_POOL)
    movie_id = random.choice(MOVIES)
    device_type = random.choice(DEVICES)
    session_id = str(uuid.uuid4())

    if random.random() < 0.1:
        producer.publish(
            _build_event(
                user_id=user_id,
                movie_id=movie_id,
                session_id=session_id,
                event_type="SEARCHED",
                device_type=device_type,
                progress_seconds=0,
            )
        )
        return 1

    events = 0
    progress = 0
    producer.publish(
        _build_event(
            user_id=user_id,
            movie_id=movie_id,
            session_id=session_id,
            event_type="VIEW_STARTED",
            device_type=device_type,
            progress_seconds=progress,
        )
    )
    events += 1

    pause_events = random.randint(0, 2)
    for _ in range(pause_events):
        progress += random.randint(30, 600)
        producer.publish(
            _build_event(
                user_id=user_id,
                movie_id=movie_id,
                session_id=session_id,
                event_type="VIEW_PAUSED",
                device_type=device_type,
                progress_seconds=progress,
            )
        )
        events += 1
        progress += random.randint(5, 60)
        producer.publish(
            _build_event(
                user_id=user_id,
                movie_id=movie_id,
                session_id=session_id,
                event_type="VIEW_RESUMED",
                device_type=device_type,
                progress_seconds=progress,
            )
        )
        events += 1

    if random.random() < 0.7:
        progress += random.randint(600, 5400)
        producer.publish(
            _build_event(
                user_id=user_id,
                movie_id=movie_id,
                session_id=session_id,
                event_type="VIEW_FINISHED",
                device_type=device_type,
                progress_seconds=progress,
            )
        )
        events += 1
        if random.random() < 0.4:
            producer.publish(
                _build_event(
                    user_id=user_id,
                    movie_id=movie_id,
                    session_id=session_id,
                    event_type="LIKED",
                    device_type=device_type,
                    progress_seconds=progress,
                )
            )
            events += 1
    return events


class GeneratorThread(threading.Thread):
    """Background thread driving the synthetic event generator."""

    def __init__(self, producer: MovieEventProducer, events_per_second: float) -> None:
        super().__init__(daemon=True, name="event-generator")
        self._producer = producer
        self._eps = max(events_per_second, 0.1)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        log.info("Event generator started (eps=%.2f)", self._eps)
        per_event_interval = 1.0 / self._eps
        while not self._stop.is_set():
            produced = _simulate_session(self._producer)
            time.sleep(per_event_interval * produced * random.uniform(0.8, 1.2))
