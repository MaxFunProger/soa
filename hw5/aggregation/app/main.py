"""Entry point for the aggregation service."""
from __future__ import annotations

import logging
import threading

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler

from .aggregator import AggregationCycle
from .api import build_app
from .clickhouse_client import ClickHouseClient
from .config import load_settings
from .postgres_client import PostgresClient
from .s3_exporter import S3Exporter


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level)
    log = logging.getLogger("aggregation")

    ch = ClickHouseClient(settings)
    pg = PostgresClient(settings)
    s3 = S3Exporter(settings, pg)
    cycle = AggregationCycle(ch, pg, s3)

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        cycle.run_today,
        trigger="interval",
        seconds=settings.aggregation_interval_seconds,
        id="periodic-aggregation",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    log.info(
        "scheduler started: aggregation runs every %d seconds",
        settings.aggregation_interval_seconds,
    )

    def kickoff_initial_run() -> None:
        try:
            cycle.run_today()
        except Exception as exc:
            log.warning("initial aggregation cycle failed: %s", exc)

    threading.Thread(target=kickoff_initial_run, daemon=True).start()

    app = build_app(cycle, s3)
    try:
        uvicorn.run(app, host="0.0.0.0", port=8010, log_level=settings.log_level.lower())
    finally:
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
