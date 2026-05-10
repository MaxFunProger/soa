"""Kafka-consumer с at-least-once семантикой, идемпотентностью, DLQ и метриками."""
from __future__ import annotations

import json
import logging
import signal
import struct
import threading
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from . import metrics
from .cassandra_store import CassandraStore
from .config import Settings
from .dlq import DLQProducer
from .handlers import EventHandlers, ValidationError

log = logging.getLogger(__name__)

# Wire format: [magic_byte=0][schema_id big-endian uint32][avro_payload]
_WIRE_HEADER = struct.Struct(">bI")


def _record_name_from_full(name_with_namespace: str | None) -> str | None:
    if not name_with_namespace:
        return None
    return name_with_namespace.rsplit(".", 1)[-1]


class _SchemaIdResolver:
    """Достаёт record_name по schema_id из Schema Registry, кэшируя ответы."""

    def __init__(self, registry: SchemaRegistryClient) -> None:
        self._registry = registry
        self._cache: dict[int, str | None] = {}

    def resolve(self, schema_id: int) -> str | None:
        if schema_id in self._cache:
            return self._cache[schema_id]
        try:
            schema = self._registry.get_schema(schema_id)
            schema_dict = json.loads(schema.schema_str)
            record_name = schema_dict.get("name")
            self._cache[schema_id] = record_name
            return record_name
        except Exception as exc:
            log.warning("Failed to resolve schema_id=%d: %s", schema_id, exc)
            return None


class WarehouseConsumer:
    def __init__(
        self,
        settings: Settings,
        store: CassandraStore,
        dlq: DLQProducer,
        handlers: EventHandlers,
    ) -> None:
        self._settings = settings
        self._store = store
        self._dlq = dlq
        self._handlers = handlers

        self._registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        # schema_str=None → де-сериализатор использует writer's schema (читаем V1 и V2)
        self._avro_des = AvroDeserializer(
            schema_registry_client=self._registry,
            schema_str=None,
        )
        self._record_resolver = _SchemaIdResolver(self._registry)

        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.consumer_group,
                "client.id": f"{settings.consumer_group}-1",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 30000,
                "max.poll.interval.ms": 600000,
                "fetch.min.bytes": 1,
                # ленивее коммитить — после явной обработки делаем commit(message)
            }
        )
        self._stop = threading.Event()
        self._kafka_alive = True

        # Кэш TopicPartition для consumer_lag.
        self._lag_thread: Optional[threading.Thread] = None

    # ----------------------- Lifecycle ----------------------------------------

    def start(self) -> None:
        topic = self._settings.topic_name
        log.info("Subscribing to topic=%s as group=%s", topic, self._settings.consumer_group)
        self._consumer.subscribe([topic])
        self._lag_thread = threading.Thread(
            target=self._lag_loop, daemon=True, name="lag-poller",
        )
        self._lag_thread.start()

    def stop(self) -> None:
        self._stop.set()

    def health(self) -> dict:
        return {
            "kafka_alive": self._kafka_alive,
            "cassandra_alive": self._store.is_alive(),
        }

    # ----------------------- Main poll loop -----------------------------------

    def run_forever(self) -> None:
        ctx = SerializationContext(self._settings.topic_name, MessageField.VALUE)
        while not self._stop.is_set():
            try:
                msg = self._consumer.poll(timeout=1.0)
            except KafkaException as exc:
                log.exception("poll() raised KafkaException: %s", exc)
                self._kafka_alive = False
                metrics.kafka_connection_up.set(0)
                time.sleep(1)
                continue

            if msg is None:
                self._kafka_alive = True
                metrics.kafka_connection_up.set(1)
                continue

            if msg.error() is not None:
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", err)
                self._kafka_alive = False
                metrics.kafka_connection_up.set(0)
                time.sleep(1)
                continue

            self._kafka_alive = True
            metrics.kafka_connection_up.set(1)
            self._process_message(msg, ctx)

        log.info("Consumer loop stopped, closing.")
        self._consumer.close()

    # ----------------------- Per-message handling -----------------------------

    def _process_message(self, msg, ctx: SerializationContext) -> None:
        record_name: str | None = None
        value: dict | None = None
        try:
            raw = msg.value()
            # Парсим schema_id из wire-format и ищем record_name в Schema Registry.
            if raw is None or len(raw) < 5:
                raise ValueError("Avro wire format too short")
            magic, schema_id = _WIRE_HEADER.unpack_from(raw, 0)
            if magic != 0:
                raise ValueError(f"unexpected Avro magic byte: {magic}")
            record_name = self._record_resolver.resolve(schema_id)
            value = self._avro_des(raw, ctx)
        except Exception as exc:
            log.exception("Failed to deserialize message: %s", exc)
            self._send_to_dlq(
                msg, value=None, record_name=None,
                error_code="DESERIALIZATION_ERROR", reason=str(exc),
            )
            self._safe_commit(msg)
            return

        if record_name is None:
            self._send_to_dlq(
                msg, value=value, record_name=None,
                error_code="MISSING_RECORD_NAME",
                reason="Cannot determine record name from Avro payload",
            )
            self._safe_commit(msg)
            return

        log.info(
            "received event_id=%s type=%s partition=%d offset=%d ts=%s",
            (value or {}).get("event_id"),
            record_name,
            msg.partition(),
            msg.offset(),
            (value or {}).get("timestamp"),
        )

        start = time.perf_counter()
        try:
            applied = self._handlers.dispatch(record_name, value or {}, msg)
            elapsed = time.perf_counter() - start
            metrics.event_processing_duration_seconds.labels(record_name).observe(elapsed)
            if applied:
                metrics.events_processed_total.labels(record_name).inc()
            else:
                metrics.events_skipped_total.labels("duplicate_or_stale").inc()
            self._safe_commit(msg)
        except ValidationError as exc:
            elapsed = time.perf_counter() - start
            metrics.event_processing_duration_seconds.labels(record_name).observe(elapsed)
            log.warning(
                "Validation failed for event_id=%s code=%s reason=%s",
                (value or {}).get("event_id"), exc.code, exc.message,
            )
            self._send_to_dlq(
                msg, value=value, record_name=record_name,
                error_code=exc.code, reason=exc.message,
            )
            self._safe_commit(msg)
        except Exception as exc:
            elapsed = time.perf_counter() - start
            metrics.event_processing_duration_seconds.labels(record_name).observe(elapsed)
            log.exception(
                "Handler raised: event_id=%s type=%s err=%s",
                (value or {}).get("event_id"), record_name, exc,
            )
            metrics.cassandra_write_errors_total.labels(type(exc).__name__).inc()
            self._send_to_dlq(
                msg, value=value, record_name=record_name,
                error_code="HANDLER_ERROR", reason=f"{type(exc).__name__}: {exc}",
            )
            self._safe_commit(msg)

    # ----------------------- Commit & DLQ helpers -----------------------------

    def _safe_commit(self, msg) -> None:
        for attempt in range(5):
            try:
                self._consumer.commit(message=msg, asynchronous=False)
                return
            except KafkaException as exc:
                log.warning("commit() failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(0.2 * (attempt + 1))
        log.error(
            "Giving up commit() for partition=%d offset=%d (will reprocess on next start)",
            msg.partition(), msg.offset(),
        )

    def _send_to_dlq(
        self,
        msg,
        value: dict | None,
        record_name: str | None,
        error_code: str,
        reason: str,
    ) -> None:
        try:
            self._dlq.send(
                original_payload=value if value is not None else msg.value(),
                record_name=record_name,
                topic=self._settings.topic_name,
                partition=msg.partition(),
                offset=msg.offset(),
                error_reason=reason,
                error_code=error_code,
            )
            metrics.dlq_messages_total.labels(error_code).inc()
        except Exception as exc:
            log.exception("Failed to send to DLQ (continuing): %s", exc)

    # ----------------------- Consumer lag poller -----------------------------

    def _lag_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._update_lag_metrics()
            except Exception as exc:
                log.debug("lag update failed: %s", exc)
            self._stop.wait(5.0)

    def _update_lag_metrics(self) -> None:
        # Только если есть назначения партиций
        assignment = self._consumer.assignment()
        if not assignment:
            return
        committed = self._consumer.committed(assignment, timeout=10)
        for tp_committed in committed:
            try:
                low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(tp_committed.topic, tp_committed.partition),
                    timeout=5, cached=False,
                )
            except Exception:
                continue
            committed_offset = tp_committed.offset
            # OFFSET_INVALID = -1001 в librdkafka, у python-обёртки = -1001
            if committed_offset is None or committed_offset < 0:
                committed_offset = low
            lag = max(high - committed_offset, 0)
            metrics.consumer_lag.labels(
                topic=tp_committed.topic, partition=str(tp_committed.partition),
            ).set(lag)


# Подключение SIGTERM/SIGINT
def install_signal_handlers(consumer: WarehouseConsumer) -> None:
    def _handler(signum, frame):
        log.info("received signal %s, shutting down", signum)
        consumer.stop()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
