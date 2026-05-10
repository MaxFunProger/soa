"""Конфигурация consumer-сервиса."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    schema_registry_url: str
    topic_name: str
    dlq_topic_name: str
    consumer_group: str
    cassandra_contact_points: tuple[str, ...]
    cassandra_port: int
    cassandra_keyspace: str
    cassandra_local_dc: str
    cassandra_write_consistency: str
    cassandra_read_consistency: str
    metrics_port: int
    log_level: str


def _list(value: str | None, default: list[str]) -> tuple[str, ...]:
    if not value:
        return tuple(default)
    return tuple(p.strip() for p in value.split(",") if p.strip())


def load_settings() -> Settings:
    return Settings(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092"),
        schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
        topic_name=os.environ.get("TOPIC_NAME", "warehouse-events"),
        dlq_topic_name=os.environ.get("DLQ_TOPIC_NAME", "warehouse-events-dlq"),
        consumer_group=os.environ.get("CONSUMER_GROUP", "warehouse-state-consumer"),
        cassandra_contact_points=_list(
            os.environ.get("CASSANDRA_CONTACT_POINTS"),
            ["cassandra-1", "cassandra-2", "cassandra-3"],
        ),
        cassandra_port=int(os.environ.get("CASSANDRA_PORT", "9042")),
        cassandra_keyspace=os.environ.get("CASSANDRA_KEYSPACE", "warehouse"),
        cassandra_local_dc=os.environ.get("CASSANDRA_LOCAL_DC", "datacenter1"),
        cassandra_write_consistency=os.environ.get("CASSANDRA_WRITE_CONSISTENCY", "QUORUM"),
        cassandra_read_consistency=os.environ.get("CASSANDRA_READ_CONSISTENCY", "ONE"),
        metrics_port=int(os.environ.get("METRICS_PORT", "9100")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
