"""Runtime configuration pulled from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    aggregation_interval_seconds: int

    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str

    log_level: str


def load_settings() -> Settings:
    return Settings(
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        clickhouse_db=os.getenv("CLICKHOUSE_DB", "cinema"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "analytics"),
        postgres_user=os.getenv("POSTGRES_USER", "analytics"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "analytics"),
        aggregation_interval_seconds=int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300")),
        s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        s3_access_key=os.getenv("S3_ACCESS_KEY", "minio"),
        s3_secret_key=os.getenv("S3_SECRET_KEY", "minio12345"),
        s3_bucket=os.getenv("S3_BUCKET", "movie-analytics"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
