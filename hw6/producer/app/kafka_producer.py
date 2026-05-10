"""Kafka-продюсер с Avro-сериализацией и TopicRecordNameStrategy.

В одном топике warehouse-events живут разные record-name (ProductReceived, ProductShipped, ...).
Каждый record name → своя schema в Schema Registry.
"""
from __future__ import annotations

import logging
import threading
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    topic_record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from .config import Settings

log = logging.getLogger(__name__)


class WMSProducer:
    def __init__(self, settings: Settings, schemas: dict[str, dict]) -> None:
        self._settings = settings
        self._registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._serializers: dict[str, AvroSerializer] = {}
        for record_name, info in schemas.items():
            self._serializers[record_name] = AvroSerializer(
                schema_registry_client=self._registry,
                schema_str=info["schema_str"],
                conf={"subject.name.strategy": topic_record_subject_name_strategy},
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
                "client.id": "wms-producer",
            }
        )
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            log.error("Delivery failed: %s", err)
        else:
            log.info(
                "published key=%s partition=%s offset=%s",
                msg.key().decode() if msg.key() else None,
                msg.partition(),
                msg.offset(),
            )

    def register_schema(self, record_name: str, schema_str: str) -> None:
        """Зарегистрировать новый сериализатор (для evolution V2)."""
        self._serializers[record_name] = AvroSerializer(
            schema_registry_client=self._registry,
            schema_str=schema_str,
            conf={"subject.name.strategy": topic_record_subject_name_strategy},
        )

    def publish(
        self,
        record_name: str,
        event: dict[str, Any],
        partition_key: str,
    ) -> None:
        if record_name not in self._serializers:
            raise ValueError(f"No serializer registered for record_name={record_name}")
        serializer = self._serializers[record_name]
        ctx = SerializationContext(self._settings.topic_name, MessageField.VALUE)
        value_bytes = serializer(event, ctx)
        key_bytes = self._key_serializer(partition_key)

        log.info(
            "publishing record=%s event_id=%s key=%s",
            record_name,
            event.get("event_id"),
            partition_key,
        )

        self._producer.produce(
            topic=self._settings.topic_name,
            key=key_bytes,
            value=value_bytes,
            on_delivery=self._delivery_callback,
        )

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._stop.set()
        self._producer.flush(10)
        self._poll_thread.join(timeout=2)
