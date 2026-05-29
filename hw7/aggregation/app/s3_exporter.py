"""Daily aggregate export from PostgreSQL to S3-compatible storage."""
from __future__ import annotations

import csv
import io
import logging
from datetime import date

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Settings
from .postgres_client import PostgresClient

log = logging.getLogger(__name__)


class S3Exporter:
    def __init__(self, settings: Settings, postgres: PostgresClient) -> None:
        self._settings = settings
        self._pg = postgres
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self._settings.s3_bucket)
        except ClientError:
            try:
                self._client.create_bucket(Bucket=self._settings.s3_bucket)
                log.info("Created S3 bucket %s", self._settings.s3_bucket)
            except (ClientError, BotoCoreError) as exc:
                log.warning("Unable to ensure bucket %s: %s", self._settings.s3_bucket, exc)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def export_date(self, metric_date: date) -> str:
        metrics = self._pg.fetch_aggregates_for_date(metric_date)
        buffer = io.StringIO()
        writer = csv.DictWriter(
            buffer,
            fieldnames=["metric_date", "metric_name", "bucket_key", "value", "computed_at"],
        )
        writer.writeheader()
        for row in metrics:
            writer.writerow(row)

        key = f"daily/{metric_date.isoformat()}/aggregates.csv"
        try:
            self._client.put_object(
                Bucket=self._settings.s3_bucket,
                Key=key,
                Body=buffer.getvalue().encode("utf-8"),
                ContentType="text/csv",
            )
            log.info(
                "s3 export: wrote %d rows to s3://%s/%s",
                len(metrics),
                self._settings.s3_bucket,
                key,
            )
            return key
        except (ClientError, BotoCoreError) as exc:
            log.exception("s3 export failed for %s: %s", metric_date, exc)
            raise

    def ping(self) -> bool:
        try:
            self._client.head_bucket(Bucket=self._settings.s3_bucket)
            return True
        except Exception:
            return False
