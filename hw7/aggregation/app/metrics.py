"""Prometheus metrics for the aggregation service."""
from __future__ import annotations

import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Number of HTTP requests handled by the service.",
    ["method", "endpoint", "status"],
)

HTTP_ERRORS = Counter(
    "http_request_errors_total",
    "Number of HTTP requests that resulted in a 4xx/5xx response.",
    ["method", "endpoint", "error_type"],
)

HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request handling latency in seconds.",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

AGG_RUNS = Counter(
    "aggregation_runs_total",
    "Aggregation cycles executed.",
    ["status"],  # success | failure
)

AGG_DURATION = Histogram(
    "aggregation_run_duration_seconds",
    "Duration of a full aggregation cycle (CH read + writes + PG upserts + S3 export).",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

AGG_RECORDS = Counter(
    "aggregation_records_processed_total",
    "Number of source records read from ClickHouse during aggregation.",
)

AGG_LAST_SUCCESS = Gauge(
    "aggregation_last_success_timestamp_seconds",
    "Unix timestamp of the last successful aggregation cycle.",
)


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    if route is not None and getattr(route, "path", None):
        return route.path
    return request.url.path


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)

        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            HTTP_ERRORS.labels(
                method=request.method,
                endpoint=_route_template(request),
                error_type="exception",
            ).inc()
            raise
        finally:
            elapsed = time.perf_counter() - start
            endpoint = _route_template(request)
            HTTP_LATENCY.labels(method=request.method, endpoint=endpoint).observe(elapsed)
            HTTP_REQUESTS.labels(
                method=request.method,
                endpoint=endpoint,
                status=str(status_code),
            ).inc()
            if status_code >= 400:
                HTTP_ERRORS.labels(
                    method=request.method,
                    endpoint=endpoint,
                    error_type=f"http_{status_code // 100}xx",
                ).inc()


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
