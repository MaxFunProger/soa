"""Unit tests for the producer HTTP API (Kafka producer mocked)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api import build_app


@pytest.fixture()
def client():
    fake_producer = MagicMock()
    fake_producer.publish = MagicMock()
    app = build_app(fake_producer)
    with TestClient(app) as c:
        yield c, fake_producer


def test_health(client):
    c, _ = client
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_metrics_endpoint_exposes_prometheus_format(client):
    c, _ = client
    # warm up at least one request so http_requests_total has a value
    c.get("/health")
    r = c.get("/metrics")
    assert r.status_code == 200
    body = r.text
    assert "http_requests_total" in body
    assert "http_request_duration_seconds_bucket" in body


def test_publish_event_happy_path(client):
    c, fake = client
    payload = {
        "user_id": "user-1",
        "movie_id": "movie-1",
        "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP",
        "session_id": "sess-1",
        "progress_seconds": 0,
    }
    r = c.post("/events", json=payload)
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "accepted"
    assert body["event_id"]
    fake.publish.assert_called_once()

    sent = fake.publish.call_args.args[0]
    assert sent["user_id"] == "user-1"
    assert sent["event_type"] == "VIEW_STARTED"
    assert isinstance(sent["timestamp"], int)


def test_publish_event_rejects_unknown_event_type(client):
    c, _ = client
    r = c.post("/events", json={
        "user_id": "u", "movie_id": "m", "event_type": "DOWNLOADED",
        "device_type": "DESKTOP", "session_id": "s",
    })
    assert r.status_code == 422


def test_publish_event_rejects_empty_user(client):
    c, _ = client
    r = c.post("/events", json={
        "user_id": "", "movie_id": "m", "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP", "session_id": "s",
    })
    assert r.status_code == 422


def test_publish_event_returns_500_when_producer_raises(client):
    c, fake = client
    fake.publish.side_effect = RuntimeError("boom")
    r = c.post("/events", json={
        "user_id": "u", "movie_id": "m", "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP", "session_id": "s",
    })
    assert r.status_code == 500
