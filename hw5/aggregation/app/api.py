"""HTTP API for the aggregation service."""
from __future__ import annotations

import logging
from datetime import date

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .aggregator import AggregationCycle
from .s3_exporter import S3Exporter

log = logging.getLogger(__name__)


class RecomputeResponse(BaseModel):
    status: str
    result: dict


def build_app(cycle: AggregationCycle, s3: S3Exporter) -> FastAPI:
    app = FastAPI(title="movie-analytics-aggregation", version="1.0.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/recompute/{metric_date}", response_model=RecomputeResponse)
    def recompute(metric_date: date) -> RecomputeResponse:
        try:
            result = cycle.run_for_date(metric_date)
            return RecomputeResponse(status="ok", result=result)
        except Exception as exc:
            log.exception("recompute failed for %s: %s", metric_date, exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/export/{metric_date}")
    def export_s3(metric_date: date) -> dict:
        try:
            key = s3.export_date(metric_date)
            return {"status": "ok", "key": key}
        except Exception as exc:
            log.exception("s3 export failed for %s: %s", metric_date, exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return app
