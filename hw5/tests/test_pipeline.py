"""End-to-end integration test: Producer → Kafka → ClickHouse."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

import requests


def _wait_for_event(client, event_id: str, timeout: float = 60.0) -> list[tuple]:
    deadline = time.time() + timeout
    last_rows: list[tuple] = []
    while time.time() < deadline:
        rows = client.query(
            "SELECT event_id, user_id, movie_id, event_type, device_type, session_id, progress_seconds "
            "FROM cinema.movie_events WHERE event_id = %(eid)s",
            parameters={"eid": event_id},
        ).result_rows
        if rows:
            return rows
        last_rows = rows
        time.sleep(1)
    return last_rows


def test_publish_event_flows_into_clickhouse(producer_url, clickhouse_client):
    unique_session = str(uuid.uuid4())
    user_id = f"test-user-{unique_session[:8]}"
    movie_id = f"test-movie-{unique_session[:8]}"

    payload = {
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP",
        "session_id": unique_session,
        "progress_seconds": 0,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }

    response = requests.post(f"{producer_url}/events", json=payload, timeout=10)
    assert response.status_code == 202, response.text
    event_id = response.json()["event_id"]
    assert uuid.UUID(event_id)

    rows = _wait_for_event(clickhouse_client, event_id, timeout=60.0)
    assert rows, f"Event {event_id} did not arrive in ClickHouse within the timeout"

    stored = rows[0]
    assert stored[0] == event_id
    assert stored[1] == user_id
    assert stored[2] == movie_id
    assert stored[3] == "VIEW_STARTED"
    assert stored[4] == "DESKTOP"
    assert stored[5] == unique_session
    assert stored[6] == 0


def test_validation_error_on_bad_payload(producer_url):
    bad_payload = {
        "user_id": "",
        "movie_id": "",
        "event_type": "UNKNOWN",
        "device_type": "DESKTOP",
        "session_id": "s",
    }
    response = requests.post(f"{producer_url}/events", json=bad_payload, timeout=10)
    assert response.status_code in (400, 422), response.text
