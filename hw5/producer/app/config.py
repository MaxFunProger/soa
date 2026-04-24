"""Runtime configuration pulled from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    schema_registry_url: str
    topic_name: str
    topic_partitions: int
    topic_replication_factor: int
    topic_min_insync_replicas: int
    schema_path: str
    generator_enabled: bool
    generator_eps: float
    log_level: str


def load_settings() -> Settings:
    return Settings(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        topic_name=os.getenv("TOPIC_NAME", "movie-events"),
        topic_partitions=int(os.getenv("TOPIC_PARTITIONS", "3")),
        topic_replication_factor=int(os.getenv("TOPIC_REPLICATION_FACTOR", "2")),
        topic_min_insync_replicas=int(os.getenv("TOPIC_MIN_INSYNC_REPLICAS", "1")),
        schema_path=os.getenv("SCHEMA_PATH", "/app/schemas/movie_event.avsc"),
        generator_enabled=os.getenv("GENERATOR_ENABLED", "true").lower() == "true",
        generator_eps=float(os.getenv("GENERATOR_EPS", "5")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
