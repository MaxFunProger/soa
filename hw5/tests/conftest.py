"""Pytest fixtures that wait for the whole pipeline to be healthy."""
from __future__ import annotations

import os
import time

import clickhouse_connect
import pytest
import requests

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))


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
    def check() -> bool:
        r = requests.get(f"{PRODUCER_URL}/health", timeout=3)
        return r.status_code == 200

    _wait("producer", check)
    return PRODUCER_URL


@pytest.fixture(scope="session")
def clickhouse_client():
    def check() -> bool:
        c = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username="default",
            password="",
            database="cinema",
        )
        c.query("SELECT 1")
        return True

    _wait("clickhouse", check)
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="",
        database="cinema",
    )
