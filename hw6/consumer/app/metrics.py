"""Prometheus-метрики consumer-сервиса."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Отставание consumer от HEAD топика, по партициям.
consumer_lag = Gauge(
    "consumer_lag",
    "Lag of consumer behind partition HEAD (latest_offset - committed_offset)",
    labelnames=("topic", "partition"),
)

# Счётчик обработанных событий по типам.
events_processed_total = Counter(
    "events_processed_total",
    "Total successfully processed events",
    labelnames=("event_type",),
)

# Счётчик пропущенных событий (idempotency / out-of-order).
events_skipped_total = Counter(
    "events_skipped_total",
    "Skipped events (duplicate or out-of-order)",
    labelnames=("reason",),
)

# Длительность обработки одного события.
event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing single event",
    labelnames=("event_type",),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# Ошибки записи в Cassandra.
cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Cassandra write errors during event processing",
    labelnames=("error_type",),
)

# Сообщения, отправленные в DLQ.
dlq_messages_total = Counter(
    "dlq_messages_total",
    "Messages routed to DLQ",
    labelnames=("error_code",),
)

# Состояние подключений (для health-probes).
kafka_connection_up = Gauge("kafka_connection_up", "1 if Kafka connection is healthy")
cassandra_connection_up = Gauge("cassandra_connection_up", "1 if Cassandra connection is healthy")
