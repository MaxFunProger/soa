"""Dead Letter Queue producer."""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from .config import Settings

log = logging.getLogger(__name__)


class DLQProducer:
    def __init__(self, settings: Settings, schemas_dir: str = "/app/schemas") -> None:
        self._settings = settings
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})

        schema_path = Path(schemas_dir) / "DLQEnvelope.avsc"
        if not schema_path.exists():
            # fallback: текущая директория
            schema_path = Path(__file__).parent.parent / "schemas" / "DLQEnvelope.avsc"
        schema_str = schema_path.read_text(encoding="utf-8")

        # DLQ subject = "<dlq_topic>-value" (TopicNameStrategy, по умолчанию).
        try:
            registry.register_schema(f"{settings.dlq_topic_name}-value",
                                     Schema(schema_str, schema_type="AVRO"))
        except Exception as exc:
            log.warning("DLQ schema registration failed (non-fatal): %s", exc)

        self._serializer = AvroSerializer(
            schema_registry_client=registry,
            schema_str=schema_str,
        )
        self._key_serializer = StringSerializer("utf-8")
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "client.id": "warehouse-consumer-dlq",
            }
        )
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def send(
        self,
        original_payload: bytes | dict | str | None,
        record_name: str | None,
        topic: str,
        partition: int,
        offset: int,
        error_reason: str,
        error_code: str,
        key: str | None = None,
    ) -> None:
        if isinstance(original_payload, bytes):
            try:
                original_str = original_payload.decode("utf-8", errors="replace")
            except Exception:
                original_str = repr(original_payload)
        elif isinstance(original_payload, (dict, list)):
            original_str = json.dumps(original_payload, ensure_ascii=False, default=str)
        elif original_payload is None:
            original_str = ""
        else:
            original_str = str(original_payload)

        envelope = {
            "original_event": original_str,
            "original_record_name": record_name,
            "original_topic": topic,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            "kafka_partition": partition,
            "kafka_offset": offset,
        }

        ctx = SerializationContext(self._settings.dlq_topic_name, MessageField.VALUE)
        value_bytes = self._serializer(envelope, ctx)

        key_bytes = self._key_serializer(key or f"{topic}:{partition}:{offset}")
        self._producer.produce(
            topic=self._settings.dlq_topic_name,
            key=key_bytes,
            value=value_bytes,
        )
        # ждём delivery — в DLQ важна гарантированная отправка
        self._producer.poll(0.1)
        self._producer.flush(5)
        log.info(
            "DLQ: sent record=%s code=%s reason=%s topic=%s partition=%d offset=%d",
            record_name, error_code, error_reason, topic, partition, offset,
        )

    def close(self) -> None:
        self._stop.set()
        self._producer.flush(10)
        self._poll_thread.join(timeout=2)
