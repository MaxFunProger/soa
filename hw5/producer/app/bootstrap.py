"""Bootstrap Kafka topic and Schema Registry subject at service startup."""
from __future__ import annotations

import logging
import time

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from .config import Settings

log = logging.getLogger(__name__)


def ensure_topic(settings: Settings, timeout_s: float = 60.0) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})

    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            md = admin.list_topics(timeout=5)
            if settings.topic_name in md.topics and md.topics[settings.topic_name].error is None:
                log.info("Kafka topic %s already exists", settings.topic_name)
                return
            break
        except Exception as exc:
            last_err = exc
            time.sleep(2)
    else:
        raise RuntimeError(f"Kafka is not available: {last_err}")

    new_topic = NewTopic(
        topic=settings.topic_name,
        num_partitions=settings.topic_partitions,
        replication_factor=settings.topic_replication_factor,
        config={"min.insync.replicas": str(settings.topic_min_insync_replicas)},
    )
    futures = admin.create_topics([new_topic])
    for name, future in futures.items():
        try:
            future.result(timeout=30)
            log.info(
                "Created Kafka topic %s (partitions=%d, rf=%d, min.isr=%d)",
                name,
                settings.topic_partitions,
                settings.topic_replication_factor,
                settings.topic_min_insync_replicas,
            )
        except Exception as exc:
            msg = str(exc)
            if "TOPIC_ALREADY_EXISTS" in msg or "already exists" in msg.lower():
                log.info("Kafka topic %s already exists", name)
            else:
                raise


def register_schema(settings: Settings, schema_str: str, timeout_s: float = 60.0) -> int:
    subject = f"{settings.topic_name}-value"
    client = SchemaRegistryClient({"url": settings.schema_registry_url})

    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            schema_id = client.register_schema(subject, Schema(schema_str, schema_type="AVRO"))
            log.info("Registered Avro schema for subject %s (id=%s)", subject, schema_id)
            return schema_id
        except Exception as exc:
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"Unable to register schema in Schema Registry: {last_err}")
