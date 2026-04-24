"""ClickHouse queries computing daily business metrics."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import clickhouse_connect
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Settings

log = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
            connect_timeout=10,
            send_receive_timeout=30,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def _execute(self, query: str, parameters: dict | None = None) -> list[tuple[Any, ...]]:
        return self._client.query(query, parameters=parameters or {}).result_rows

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def _command(self, query: str, parameters: dict | None = None) -> None:
        self._client.command(query, parameters=parameters or {})

    def dau(self, metric_date: date) -> int:
        rows = self._execute(
            """
            SELECT uniqExact(user_id)
            FROM cinema.movie_events
            WHERE event_date = %(d)s
            """,
            {"d": metric_date},
        )
        return int(rows[0][0]) if rows else 0

    def avg_view_time(self, metric_date: date) -> float:
        rows = self._execute(
            """
            SELECT avg(progress_seconds)
            FROM cinema.movie_events
            WHERE event_date = %(d)s AND event_type = 'VIEW_FINISHED'
            """,
            {"d": metric_date},
        )
        if not rows or rows[0][0] is None:
            return 0.0
        return float(rows[0][0])

    def top_movies(self, metric_date: date, limit: int = 10) -> list[tuple[str, int, int]]:
        rows = self._execute(
            """
            SELECT movie_id, count() AS views
            FROM cinema.movie_events
            WHERE event_date = %(d)s AND event_type = 'VIEW_STARTED'
            GROUP BY movie_id
            ORDER BY views DESC, movie_id ASC
            LIMIT %(lim)s
            """,
            {"d": metric_date, "lim": limit},
        )
        return [(str(movie_id), int(views), idx + 1) for idx, (movie_id, views) in enumerate(rows)]

    def view_conversion(self, metric_date: date) -> tuple[int, int, float]:
        rows = self._execute(
            """
            SELECT
                countIf(event_type = 'VIEW_STARTED')  AS started,
                countIf(event_type = 'VIEW_FINISHED') AS finished
            FROM cinema.movie_events
            WHERE event_date = %(d)s
            """,
            {"d": metric_date},
        )
        if not rows:
            return 0, 0, 0.0
        started = int(rows[0][0])
        finished = int(rows[0][1])
        conversion = finished / started if started > 0 else 0.0
        return started, finished, conversion

    def retention_matrix(self, reference_date: date, max_day: int = 7) -> list[tuple[date, int, int, int, float]]:
        """Build a retention matrix for cohorts from the last `max_day` days before reference_date."""
        rows = self._execute(
            """
            WITH cohorts AS (
                SELECT
                    user_id,
                    toDate(min(timestamp)) AS cohort_date
                FROM cinema.movie_events
                WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
                GROUP BY user_id
            ),
            activity AS (
                SELECT DISTINCT user_id, toDate(timestamp) AS active_date
                FROM cinema.movie_events
                WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
            ),
            sizes AS (
                SELECT cohort_date, count() AS cohort_size
                FROM cohorts
                WHERE cohort_date BETWEEN %(from)s AND %(to)s
                GROUP BY cohort_date
            )
            SELECT
                c.cohort_date                        AS cohort_date,
                dateDiff('day', c.cohort_date, a.active_date) AS day_offset,
                any(s.cohort_size)                   AS cohort_size,
                uniqExact(c.user_id)                 AS returned
            FROM cohorts c
            INNER JOIN activity a USING (user_id)
            INNER JOIN sizes s USING (cohort_date)
            WHERE c.cohort_date BETWEEN %(from)s AND %(to)s
              AND dateDiff('day', c.cohort_date, a.active_date) BETWEEN 0 AND %(max_day)s
            GROUP BY c.cohort_date, day_offset
            ORDER BY c.cohort_date, day_offset
            """,
            {
                "from": reference_date - timedelta(days=max_day * 4),
                "to": reference_date,
                "max_day": max_day,
            },
        )
        result: list[tuple[date, int, int, int, float]] = []
        for cohort_date, day_offset, cohort_size, returned in rows:
            size = int(cohort_size)
            ret = int(returned)
            retention = ret / size if size > 0 else 0.0
            result.append((cohort_date, int(day_offset), size, ret, retention))
        return result

    # Materialization into aggregate tables
    def write_agg_dau(self, metric_date: date, dau: int) -> None:
        self._command(
            "INSERT INTO cinema.agg_dau (metric_date, dau) VALUES (%(d)s, %(v)s)",
            {"d": metric_date, "v": dau},
        )

    def write_agg_avg_view_time(self, metric_date: date, avg_seconds: float) -> None:
        self._command(
            "INSERT INTO cinema.agg_avg_view_time (metric_date, avg_seconds) VALUES (%(d)s, %(v)s)",
            {"d": metric_date, "v": avg_seconds},
        )

    def write_agg_top_movies(self, metric_date: date, rows: list[tuple[str, int, int]]) -> None:
        if not rows:
            return
        computed_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        payload = [
            (metric_date, movie_id, views, rank, computed_at)
            for movie_id, views, rank in rows
        ]
        self._client.insert(
            "cinema.agg_top_movies",
            payload,
            column_names=["metric_date", "movie_id", "views", "rank", "computed_at"],
        )

    def write_agg_conversion(self, metric_date: date, started: int, finished: int, conversion: float) -> None:
        self._client.insert(
            "cinema.agg_conversion",
            [(metric_date, started, finished, conversion)],
            column_names=["metric_date", "views_started", "views_finished", "conversion"],
        )

    def write_agg_retention(self, rows: list[tuple[date, int, int, int, float]]) -> None:
        if not rows:
            return
        self._client.insert(
            "cinema.agg_retention",
            rows,
            column_names=["cohort_date", "day_offset", "cohort_size", "returned", "retention"],
        )

    def ping(self) -> bool:
        try:
            self._client.ping()
            return True
        except Exception:
            return False
