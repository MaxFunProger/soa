"""Avro-backed Kafka producer with retries and partition key support."""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from .config import Settings

log = logging.getLogger(__name__)


class MovieEventProducer:
    """Thin wrapper around confluent-kafka Producer with Avro serialization."""

    def __init__(self, settings: Settings, schema_str: str) -> None:
        self._settings = settings
        self._schema_registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._avro_serializer = AvroSerializer(
            schema_registry_client=self._schema_registry,
            schema_str=schema_str,
        )
        self._key_serializer = StringSerializer("utf-8")
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 10,
                "retry.backoff.ms": 200,
                "delivery.timeout.ms": 30000,
                "linger.ms": 20,
                "compression.type": "lz4",
                "client.id": "movie-events-producer",
            }
        )
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._stop = threading.Event()
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            log.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            log.info(
                "published event_id=%s partition=%s offset=%s",
                msg.key().decode() if msg.key() else None,
                msg.partition(),
                msg.offset(),
            )

    def publish(self, event: dict[str, Any]) -> None:
        """Publish event with retries on queue-full errors."""
        ctx = SerializationContext(self._settings.topic_name, MessageField.VALUE)
        value_bytes = self._avro_serializer(event, ctx)
        key_bytes = self._key_serializer(event["user_id"])

        log.info(
            "publishing event_id=%s event_type=%s user_id=%s timestamp=%s",
            event["event_id"],
            event["event_type"],
            event["user_id"],
            event["timestamp"],
        )

        attempt = 0
        while True:
            try:
                self._producer.produce(
                    topic=self._settings.topic_name,
                    key=key_bytes,
                    value=value_bytes,
                    on_delivery=self._delivery_callback,
                )
                return
            except BufferError:
                attempt += 1
                if attempt > 8:
                    raise
                backoff = min(2 ** attempt * 0.05, 5.0)
                log.warning(
                    "Producer queue is full, retrying after %.2fs (attempt %d)",
                    backoff,
                    attempt,
                )
                self._producer.poll(backoff)
            except KafkaException:
                raise

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._stop.set()
        self._producer.flush(10)
        self._poll_thread.join(timeout=2)
