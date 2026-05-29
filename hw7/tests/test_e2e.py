"""End-to-end scenario: publish events, run recompute, assert metrics in Postgres."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

import psycopg
import requests


def _publish(producer_url: str, payload: dict) -> str:
    r = requests.post(f"{producer_url}/events", json=payload, timeout=10)
    assert r.status_code == 202, r.text
    return r.json()["event_id"]


def _wait_in_clickhouse(ch, event_ids: list[str], timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    pending = set(event_ids)
    while pending and time.time() < deadline:
        rows = ch.query(
            "SELECT event_id FROM cinema.movie_events WHERE event_id IN %(ids)s",
            parameters={"ids": list(pending)},
        ).result_rows
        for r in rows:
            pending.discard(r[0])
        if pending:
            time.sleep(1)
    assert not pending, f"events not visible in ClickHouse: {pending}"


def test_full_scenario_one_user_one_movie(producer_url, aggregation_url, clickhouse_client, postgres_dsn):
    user_id = f"e2e-{uuid.uuid4().hex[:8]}"
    movie_id = f"e2e-movie-{uuid.uuid4().hex[:6]}"
    session = str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc)

    events = [
        ("VIEW_STARTED", 0),
        ("VIEW_FINISHED", 1800),
        ("LIKED", 1800),
    ]
    ids: list[str] = []
    for ev, progress in events:
        ids.append(_publish(producer_url, {
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": ev,
            "device_type": "DESKTOP",
            "session_id": session,
            "progress_seconds": progress,
            "timestamp": now.isoformat(),
        }))

    _wait_in_clickhouse(clickhouse_client, ids)

    today = now.date().isoformat()
    r = requests.post(f"{aggregation_url}/recompute/{today}", timeout=60)
    assert r.status_code == 200, r.text
    result = r.json()["result"]
    assert result["dau"] >= 1
    assert result["views_started"] >= 1
    assert result["views_finished"] >= 1

    with psycopg.connect(postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metric_name, value FROM metrics WHERE metric_date = %s AND metric_name IN "
            "('dau', 'views_started', 'views_finished', 'view_conversion')",
            (now.date(),),
        )
        rows = {name: float(value) for name, value in cur.fetchall()}

    assert rows.get("dau", 0) >= 1
    assert rows.get("views_started", 0) >= 1
    assert rows.get("views_finished", 0) >= 1
    assert 0.0 <= rows.get("view_conversion", -1) <= 1.0


def test_aggregation_metrics_exposed(aggregation_url):
    m = requests.get(f"{aggregation_url}/metrics", timeout=5).text
    assert "aggregation_runs_total" in m
    assert "aggregation_run_duration_seconds_bucket" in m
    assert "aggregation_last_success_timestamp_seconds" in m
