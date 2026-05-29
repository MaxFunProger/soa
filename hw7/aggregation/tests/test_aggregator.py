"""Unit tests for the aggregation cycle with mocked storage clients."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from app.aggregator import AggregationCycle


@pytest.fixture()
def cycle():
    ch = MagicMock()
    pg = MagicMock()
    s3 = MagicMock()

    ch.dau.return_value = 42
    ch.avg_view_time.return_value = 1234.5
    ch.top_movies.return_value = [("movie-1", 10, 1), ("movie-2", 7, 2)]
    ch.view_conversion.return_value = (100, 80, 0.8)
    ch.retention_matrix.return_value = [
        (date(2026, 5, 1), 0, 50, 50, 1.0),
        (date(2026, 5, 1), 1, 50, 25, 0.5),
    ]
    s3.export_date.return_value = "daily/2026-05-29/aggregates.csv"

    return AggregationCycle(ch, pg, s3), ch, pg, s3


def test_run_for_date_writes_to_all_sinks(cycle):
    c, ch, pg, s3 = cycle
    result = c.run_for_date(date(2026, 5, 29))

    assert result["dau"] == 42
    assert result["views_started"] == 100
    assert result["views_finished"] == 80
    assert result["view_conversion"] == 0.8
    assert len(result["top_movies"]) == 2
    assert result["retention_rows"] == 2

    ch.write_agg_dau.assert_called_once_with(date(2026, 5, 29), 42)
    ch.write_agg_avg_view_time.assert_called_once()
    ch.write_agg_top_movies.assert_called_once()
    ch.write_agg_conversion.assert_called_once_with(date(2026, 5, 29), 100, 80, 0.8)
    ch.write_agg_retention.assert_called_once()

    pg.replace_top_movies.assert_called_once()
    pg.upsert_retention.assert_called_once()
    s3.export_date.assert_called_once_with(date(2026, 5, 29))


def test_run_for_date_marks_failure_on_exception(cycle):
    c, ch, _, _ = cycle
    ch.dau.side_effect = RuntimeError("ch down")

    from app.metrics import AGG_RUNS

    before = AGG_RUNS.labels(status="failure")._value.get()
    with pytest.raises(RuntimeError):
        c.run_for_date(date(2026, 5, 29))
    after = AGG_RUNS.labels(status="failure")._value.get()
    assert after == before + 1


def test_view_conversion_zero_started_returns_zero():
    """Sanity-check of business invariants on the ClickHouse helper itself."""
    from app.clickhouse_client import ClickHouseClient

    ch = ClickHouseClient.__new__(ClickHouseClient)
    ch._execute = MagicMock(return_value=[(0, 0)])  # type: ignore[attr-defined]
    started, finished, conv = ch.view_conversion(date(2026, 5, 29))
    assert (started, finished, conv) == (0, 0, 0.0)
