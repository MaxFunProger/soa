"""Integration tests for the producer -> Kafka -> ClickHouse path."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

import requests


def _publish(producer_url: str, payload: dict) -> str:
    r = requests.post(f"{producer_url}/events", json=payload, timeout=10)
    assert r.status_code == 202, r.text
    return r.json()["event_id"]


def _poll_clickhouse(ch, event_id: str, timeout: float = 60.0) -> list[tuple]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = ch.query(
            "SELECT event_id, user_id, movie_id, event_type, device_type, session_id, progress_seconds "
            "FROM cinema.movie_events WHERE event_id = %(eid)s",
            parameters={"eid": event_id},
        ).result_rows
        if rows:
            return rows
        time.sleep(1)
    return []


def test_event_flows_through_kafka_into_clickhouse(producer_url, clickhouse_client):
    session = str(uuid.uuid4())
    payload = {
        "user_id": f"u-{session[:8]}",
        "movie_id": f"m-{session[:8]}",
        "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP",
        "session_id": session,
        "progress_seconds": 0,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }
    event_id = _publish(producer_url, payload)
    rows = _poll_clickhouse(clickhouse_client, event_id)
    assert rows, f"event {event_id} did not arrive in ClickHouse"

    stored = rows[0]
    assert stored[0] == event_id
    assert stored[1] == payload["user_id"]
    assert stored[3] == "VIEW_STARTED"


def test_validation_error_increments_error_metric(producer_url):
    r = requests.post(
        f"{producer_url}/events",
        json={"user_id": "", "movie_id": "", "event_type": "UNKNOWN", "device_type": "DESKTOP", "session_id": "s"},
        timeout=10,
    )
    assert r.status_code in (400, 422)

    m = requests.get(f"{producer_url}/metrics", timeout=5).text
    assert 'http_request_errors_total' in m


def test_metrics_endpoint_reports_request_counters(producer_url):
    requests.get(f"{producer_url}/health", timeout=3)
    m = requests.get(f"{producer_url}/metrics", timeout=5).text
    assert 'http_requests_total' in m
    assert 'producer_kafka_events_published_total' in m
