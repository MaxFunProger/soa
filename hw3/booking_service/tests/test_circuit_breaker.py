import time

import pytest

import app.circuit_breaker as cbmod
from app.circuit_breaker import CircuitOpenError, FlightCircuitBreaker


def test_cb_success_resets(monkeypatch):
    """После успешного вызова circuit остаётся в CLOSED"""
    monkeypatch.setattr(cbmod, "FAILURE_THRESHOLD", 3)
    monkeypatch.setattr(cbmod, "OPEN_DURATION", 0.05)
    monkeypatch.setattr(cbmod, "WINDOW_SEC", 60.0)
    cb = FlightCircuitBreaker()
    cb.before_call()
    cb.record_success()
    assert cb._state == cb.CLOSED


def test_cb_open_blocks_then_half_open(monkeypatch):
    """Две retriable-ошибки -> OPEN; запросы блокируются; по таймауту -> HALF_OPEN"""
    monkeypatch.setattr(cbmod, "FAILURE_THRESHOLD", 2)
    monkeypatch.setattr(cbmod, "OPEN_DURATION", 0.05)
    monkeypatch.setattr(cbmod, "WINDOW_SEC", 60.0)
    cb = FlightCircuitBreaker()
    cb.record_failure(retriable=True)
    cb.record_failure(retriable=True)
    assert cb._state == cb.OPEN
    with pytest.raises(CircuitOpenError):
        cb.before_call()
    time.sleep(0.06)
    cb.before_call()
    assert cb._state == cb.HALF_OPEN


def test_cb_half_open_success_closes(monkeypatch):
    """Успех в HALF_OPEN возвращает circuit в CLOSED"""
    monkeypatch.setattr(cbmod, "OPEN_DURATION", 0.05)
    cb = FlightCircuitBreaker()
    cb._state = cb.HALF_OPEN
    cb.record_success()
    assert cb._state == cb.CLOSED


def test_cb_non_retriable_does_not_open(monkeypatch):
    """Ошибка не retriable (NOT_FOUND и т.п.) не открывает circuit"""
    monkeypatch.setattr(cbmod, "FAILURE_THRESHOLD", 1)
    cb = FlightCircuitBreaker()
    cb.record_failure(retriable=False)
    assert cb._state == cb.CLOSED
