"""Orchestration of business-metric aggregation cycles."""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone

from .clickhouse_client import ClickHouseClient
from .postgres_client import PostgresClient
from .s3_exporter import S3Exporter

log = logging.getLogger(__name__)


class AggregationCycle:
    def __init__(
        self,
        clickhouse: ClickHouseClient,
        postgres: PostgresClient,
        s3: S3Exporter,
    ) -> None:
        self._ch = clickhouse
        self._pg = postgres
        self._s3 = s3

    def run_for_date(self, metric_date: date) -> dict:
        start_ts = time.monotonic()
        log.info("aggregation: start metric_date=%s", metric_date)

        dau = self._ch.dau(metric_date)
        avg_view = self._ch.avg_view_time(metric_date)
        top = self._ch.top_movies(metric_date, limit=10)
        started, finished, conversion = self._ch.view_conversion(metric_date)
        retention = self._ch.retention_matrix(metric_date, max_day=7)

        self._ch.write_agg_dau(metric_date, dau)
        self._ch.write_agg_avg_view_time(metric_date, avg_view)
        self._ch.write_agg_top_movies(metric_date, top)
        self._ch.write_agg_conversion(metric_date, started, finished, conversion)
        self._ch.write_agg_retention(retention)

        self._pg.upsert_metric(metric_date, "dau", float(dau))
        self._pg.upsert_metric(metric_date, "avg_view_seconds", avg_view)
        self._pg.upsert_metric(metric_date, "views_started", float(started))
        self._pg.upsert_metric(metric_date, "views_finished", float(finished))
        self._pg.upsert_metric(metric_date, "view_conversion", conversion)
        self._pg.replace_top_movies(metric_date, top)
        self._pg.upsert_retention(retention)

        processed = dau + started + finished + len(top) + len(retention)
        self._s3.export_date(metric_date)

        elapsed = time.monotonic() - start_ts
        log.info(
            "aggregation: done metric_date=%s dau=%d started=%d finished=%d "
            "conversion=%.4f avg_view=%.2f top_movies=%d retention_rows=%d elapsed=%.3fs",
            metric_date,
            dau,
            started,
            finished,
            conversion,
            avg_view,
            len(top),
            len(retention),
            elapsed,
        )
        return {
            "metric_date": metric_date.isoformat(),
            "dau": dau,
            "avg_view_seconds": avg_view,
            "views_started": started,
            "views_finished": finished,
            "view_conversion": conversion,
            "top_movies": [{"movie_id": m, "views": v, "rank": r} for m, v, r in top],
            "retention_rows": len(retention),
            "processed_records": processed,
            "elapsed_seconds": round(elapsed, 3),
        }

    def run_today(self) -> dict:
        today = datetime.now(tz=timezone.utc).date()
        return self.run_for_date(today)
