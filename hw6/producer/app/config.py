"""Конфигурация WMS-продюсера."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    schema_registry_url: str
    topic_name: str
    dlq_topic_name: str
    topic_partitions: int
    topic_replication_factor: int
    topic_min_insync_replicas: int
    schemas_dir: str
    generator_enabled: bool
    generator_eps: float
    log_level: str


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    return Settings(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092"),
        schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        topic_name=os.environ.get("TOPIC_NAME", "warehouse-events"),
        dlq_topic_name=os.environ.get("DLQ_TOPIC_NAME", "warehouse-events-dlq"),
        topic_partitions=int(os.environ.get("TOPIC_PARTITIONS", "3")),
        topic_replication_factor=int(os.environ.get("TOPIC_REPLICATION_FACTOR", "2")),
        topic_min_insync_replicas=int(os.environ.get("TOPIC_MIN_INSYNC_REPLICAS", "1")),
        schemas_dir=os.environ.get("SCHEMAS_DIR", "/app/schemas"),
        generator_enabled=_bool(os.environ.get("GENERATOR_ENABLED"), default=False),
        generator_eps=float(os.environ.get("GENERATOR_EPS", "1")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
