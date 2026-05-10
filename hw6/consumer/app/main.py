"""Entry point: connects Cassandra, starts HTTP server and Kafka consumer loop."""
from __future__ import annotations

import logging
import os
import time

from . import metrics
from .cassandra_store import CassandraStore
from .config import load_settings
from .dlq import DLQProducer
from .handlers import EventHandlers
from .health import start_http_server
from .kafka_consumer import WarehouseConsumer, install_signal_handlers


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level)
    log = logging.getLogger("consumer")

    log.info("starting warehouse consumer (group=%s)", settings.consumer_group)

    store = CassandraStore(settings)
    store.connect()
    metrics.cassandra_connection_up.set(1)

    schemas_dir = os.environ.get("SCHEMAS_DIR", "/app/schemas")
    dlq = DLQProducer(settings, schemas_dir=schemas_dir)
    handlers = EventHandlers(store)
    consumer = WarehouseConsumer(settings, store, dlq, handlers)
    install_signal_handlers(consumer)

    def health_check() -> bool:
        h = consumer.health()
        ok_kafka = h["kafka_alive"]
        ok_cass = h["cassandra_alive"]
        metrics.cassandra_connection_up.set(1 if ok_cass else 0)
        metrics.kafka_connection_up.set(1 if ok_kafka else 0)
        return ok_kafka and ok_cass

    server = start_http_server(settings.metrics_port, health_check)

    consumer.start()
    try:
        consumer.run_forever()
    finally:
        log.info("shutting down")
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            dlq.close()
        except Exception:
            pass
        try:
            store.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
