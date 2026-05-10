"""Создание Kafka-топиков и регистрация Avro-схем при старте продюсера."""
from __future__ import annotations

import logging
import time
from pathlib import Path

import requests
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from .config import Settings

log = logging.getLogger(__name__)


def _create_topic(admin: AdminClient, name: str, partitions: int, rf: int, min_isr: int) -> None:
    md = admin.list_topics(timeout=10)
    if name in md.topics and md.topics[name].error is None:
        log.info("Kafka topic %s already exists", name)
        return
    new_topic = NewTopic(
        topic=name,
        num_partitions=partitions,
        replication_factor=rf,
        config={"min.insync.replicas": str(min_isr)},
    )
    futures = admin.create_topics([new_topic])
    for n, future in futures.items():
        try:
            future.result(timeout=30)
            log.info(
                "Created Kafka topic %s (partitions=%d, rf=%d, min.isr=%d)",
                n, partitions, rf, min_isr,
            )
        except Exception as exc:
            msg = str(exc)
            if "TOPIC_ALREADY_EXISTS" in msg or "already exists" in msg.lower():
                log.info("Kafka topic %s already exists", n)
            else:
                raise


def ensure_topics(settings: Settings, timeout_s: float = 90.0) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=5)
            break
        except Exception as exc:
            last_err = exc
            time.sleep(2)
    else:
        raise RuntimeError(f"Kafka недоступен: {last_err}")

    _create_topic(
        admin,
        settings.topic_name,
        settings.topic_partitions,
        settings.topic_replication_factor,
        settings.topic_min_insync_replicas,
    )
    _create_topic(
        admin,
        settings.dlq_topic_name,
        partitions=1,
        rf=settings.topic_replication_factor,
        min_isr=settings.topic_min_insync_replicas,
    )


# Список схем для регистрации.
# Структура: (alias, filename_in_schemas_dir, record_name, set_compatibility)
# alias — ключ во внутреннем mapping продюсера (используется в API/генераторе)
# record_name — собственно имя record в Avro (определяет subject в TopicRecordNameStrategy)
# Версии регистрируются последовательно, более поздние — это эволюция (BACKWARD).
SCHEMA_LAYOUT: list[tuple[str, str, str]] = [
    # (alias, filename, record_name)
    ("ProductReceived__v1", "ProductReceived.v1.avsc", "ProductReceived"),
    ("ProductReceived__v2", "ProductReceived.v2.avsc", "ProductReceived"),
    ("ProductShipped",      "ProductShipped.avsc",      "ProductShipped"),
    ("ProductMoved",        "ProductMoved.avsc",        "ProductMoved"),
    ("ProductReserved",     "ProductReserved.avsc",     "ProductReserved"),
    ("ProductReleased",     "ProductReleased.avsc",     "ProductReleased"),
    ("InventoryCounted",    "InventoryCounted.avsc",    "InventoryCounted"),
    ("OrderCreated",        "OrderCreated.avsc",        "OrderCreated"),
    ("OrderCompleted",      "OrderCompleted.avsc",      "OrderCompleted"),
]


def _set_compatibility(settings: Settings, subject: str, level: str) -> None:
    """Используем REST API Schema Registry напрямую: PUT /config/{subject}."""
    # Берём первый URL из конфигурации (поддерживаем comma-separated).
    base = settings.schema_registry_url.split(",")[0].rstrip("/")
    url = f"{base}/config/{subject}"
    try:
        resp = requests.put(url, json={"compatibility": level}, timeout=10)
        resp.raise_for_status()
        log.info("Compatibility for subject=%s set to %s", subject, level)
    except Exception as exc:
        log.warning("Не удалось задать compatibility для %s: %s", subject, exc)


def register_schemas(settings: Settings, timeout_s: float = 120.0) -> dict[str, dict]:
    """Регистрируем все схемы в Schema Registry с TopicRecordNameStrategy.

    Возвращает map alias -> {"schema_str", "schema_id", "subject", "record_name"}.
    Для ProductReceived в одном subject окажутся обе версии (V1 и V2).
    """
    schemas_dir = Path(settings.schemas_dir)
    client = SchemaRegistryClient({"url": settings.schema_registry_url})

    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            client.get_subjects()
            break
        except Exception as exc:
            last_err = exc
            time.sleep(2)
    else:
        raise RuntimeError(f"Schema Registry недоступен: {last_err}")

    # Уникальные subjects (один subject на record_name)
    subjects_seen: set[str] = set()
    registered: dict[str, dict] = {}

    for alias, filename, record_name in SCHEMA_LAYOUT:
        path = schemas_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Avro schema not found: {path}")
        schema_str = path.read_text(encoding="utf-8")

        subject = f"{settings.topic_name}-{record_name}"
        if subject not in subjects_seen:
            _set_compatibility(settings, subject, "BACKWARD")
            subjects_seen.add(subject)
        schema_id = client.register_schema(subject, Schema(schema_str, schema_type="AVRO"))
        log.info(
            "Registered schema alias=%s file=%s subject=%s id=%s",
            alias, filename, subject, schema_id,
        )
        registered[alias] = {
            "schema_str": schema_str,
            "schema_id": schema_id,
            "subject": subject,
            "record_name": record_name,
            "filename": filename,
        }

    # Алиас "ProductReceived" — указывает на актуальную версию (v2).
    registered["ProductReceived"] = registered["ProductReceived__v2"]
    return registered
