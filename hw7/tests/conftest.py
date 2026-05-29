"""Shared fixtures: wait for the whole pipeline to become healthy."""
from __future__ import annotations

import os
import time

import clickhouse_connect
import psycopg
import pytest
import requests

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://localhost:8010")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
POSTGRES_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'analytics')} "
    f"user={os.getenv('POSTGRES_USER', 'analytics')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'analytics')}"
)


def _wait(name: str, check, timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            if check():
                return
        except Exception as exc:
            last_err = exc
        time.sleep(2)
    raise RuntimeError(f"{name} is not ready after {timeout}s: {last_err}")


@pytest.fixture(scope="session")
def producer_url() -> str:
    _wait("producer", lambda: requests.get(f"{PRODUCER_URL}/health", timeout=3).status_code == 200)
    return PRODUCER_URL


@pytest.fixture(scope="session")
def aggregation_url() -> str:
    _wait("aggregation", lambda: requests.get(f"{AGGREGATION_URL}/health", timeout=3).status_code == 200)
    return AGGREGATION_URL


@pytest.fixture(scope="session")
def clickhouse_client():
    def check() -> bool:
        c = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            username="default", password="", database="cinema",
        )
        c.query("SELECT 1")
        return True

    _wait("clickhouse", check)
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username="default", password="", database="cinema",
    )


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    def check() -> bool:
        with psycopg.connect(POSTGRES_DSN, connect_timeout=3) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True

    _wait("postgres", check)
    return POSTGRES_DSN
