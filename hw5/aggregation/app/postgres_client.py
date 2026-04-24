"""PostgreSQL writer for pre-computed business metrics (idempotent upsert)."""
from __future__ import annotations

import logging
from datetime import date
from typing import Iterable

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Settings

log = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _dsn(self) -> str:
        return (
            f"host={self._settings.postgres_host} port={self._settings.postgres_port} "
            f"dbname={self._settings.postgres_db} user={self._settings.postgres_user} "
            f"password={self._settings.postgres_password}"
        )

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def upsert_metric(
        self,
        metric_date: date,
        metric_name: str,
        value: float,
        bucket_key: str = "",
    ) -> None:
        with psycopg.connect(self._dsn(), autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metrics (metric_date, metric_name, bucket_key, value, computed_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (metric_date, metric_name, bucket_key)
                DO UPDATE SET value = EXCLUDED.value, computed_at = NOW()
                """,
                (metric_date, metric_name, bucket_key, value),
            )

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def replace_top_movies(self, metric_date: date, rows: Iterable[tuple[str, int, int]]) -> None:
        with psycopg.connect(self._dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM metrics WHERE metric_date = %s AND metric_name = %s",
                (metric_date, "top_movie"),
            )
            for movie_id, views, rank in rows:
                cur.execute(
                    """
                    INSERT INTO metrics (metric_date, metric_name, bucket_key, value, computed_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (metric_date, metric_name, bucket_key)
                    DO UPDATE SET value = EXCLUDED.value, computed_at = NOW()
                    """,
                    (metric_date, "top_movie", f"{rank:02d}:{movie_id}", float(views)),
                )
            conn.commit()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def upsert_retention(
        self,
        rows: Iterable[tuple[date, int, int, int, float]],
    ) -> None:
        with psycopg.connect(self._dsn()) as conn, conn.cursor() as cur:
            for cohort_date, day_offset, cohort_size, returned, retention in rows:
                cur.execute(
                    """
                    INSERT INTO retention_matrix
                        (cohort_date, day_offset, cohort_size, returned, retention, computed_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (cohort_date, day_offset)
                    DO UPDATE SET
                        cohort_size = EXCLUDED.cohort_size,
                        returned    = EXCLUDED.returned,
                        retention   = EXCLUDED.retention,
                        computed_at = NOW()
                    """,
                    (cohort_date, day_offset, cohort_size, returned, retention),
                )
            conn.commit()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def fetch_aggregates_for_date(self, metric_date: date) -> list[dict]:
        with psycopg.connect(self._dsn(), row_factory=tuple_row) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_date, metric_name, bucket_key, value, computed_at
                FROM metrics
                WHERE metric_date = %s
                ORDER BY metric_name, bucket_key
                """,
                (metric_date,),
            )
            metrics = [
                {
                    "metric_date": r[0].isoformat(),
                    "metric_name": r[1],
                    "bucket_key": r[2],
                    "value": float(r[3]),
                    "computed_at": r[4].isoformat(),
                }
                for r in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT cohort_date, day_offset, cohort_size, returned, retention, computed_at
                FROM retention_matrix
                WHERE cohort_date = %s
                ORDER BY day_offset
                """,
                (metric_date,),
            )
            for r in cur.fetchall():
                metrics.append(
                    {
                        "metric_date": r[0].isoformat(),
                        "metric_name": "retention",
                        "bucket_key": f"d{r[1]}",
                        "value": float(r[4]),
                        "computed_at": r[5].isoformat(),
                    }
                )
        return metrics

    def ping(self) -> bool:
        try:
            with psycopg.connect(self._dsn()) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False
