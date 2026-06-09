"""Query Prometheus and verify system-level SLI thresholds.

The script runs after the load test inside CI. Exits with code != 0 when any
SLI crosses its failure threshold; CI then marks the run as failed.

SLIs and thresholds are documented in README.md (section "SLI / SLO").
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlencode
from urllib.request import urlopen


@dataclass
class SLI:
    name: str
    query: str
    slo: float       # ok if value satisfies comparator(value, slo)
    fail_at: float   # CI fails if comparator(value, fail_at) is False
    unit: str
    comparator: Callable[[float, float], bool]
    description: str


# value <= threshold: latency / error rate
LE = lambda v, t: v <= t  # noqa: E731
# value >= threshold: availability
GE = lambda v, t: v >= t  # noqa: E731


SLIS: list[SLI] = [
    SLI(
        name="api_latency_p95",
        # End-to-end producer latency p95 over the last 5 minutes
        query='histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{service="producer"}[5m])))',
        slo=0.5,
        fail_at=1.0,
        unit="s",
        comparator=LE,
        description="Producer HTTP p95 latency over 5m",
    ),
    SLI(
        name="api_availability",
        # 1 - error_rate; SLO 99.5%, fail < 95%.
        # `or vector(0)` covers the common case when no errors were observed
        # over the window at all — Prometheus would otherwise return an empty
        # series for the numerator and the whole expression would evaluate to
        # "no data" instead of 1.0 (100% availability).
        query='1 - ((sum(rate(http_request_errors_total{service="producer"}[5m])) or vector(0)) '
              '/ clamp_min(sum(rate(http_requests_total{service="producer"}[5m])), 1e-9))',
        slo=0.995,
        fail_at=0.95,
        unit="ratio",
        comparator=GE,
        description="Producer HTTP availability over 5m",
    ),
    SLI(
        name="event_processing_freshness",
        # Seconds since the last successful aggregation cycle.
        # SLO: cycles run at least once a minute (interval=60s), so freshness < 120s.
        # Fail: > 600s, pipeline is visibly stalled.
        query="time() - aggregation_last_success_timestamp_seconds",
        slo=120.0,
        fail_at=600.0,
        unit="s",
        comparator=LE,
        description="Time since last successful aggregation cycle",
    ),
]


def query_prometheus(base_url: str, expr: str, timeout: float = 5.0) -> float:
    url = f"{base_url.rstrip('/')}/api/v1/query?{urlencode({'query': expr})}"
    with urlopen(url, timeout=timeout) as resp:
        payload = json.load(resp)
    if payload.get("status") != "success":
        raise RuntimeError(f"prometheus error: {payload}")
    results = payload["data"]["result"]
    if not results:
        return float("nan")
    return float(results[0]["value"][1])


def evaluate(base_url: str) -> tuple[list[dict], bool]:
    report = []
    ok_all = True
    for sli in SLIS:
        try:
            value = query_prometheus(base_url, sli.query)
        except Exception as exc:
            report.append({"name": sli.name, "error": str(exc), "ok": False})
            ok_all = False
            continue

        if value != value:  # NaN, no data yet
            report.append({
                "name": sli.name,
                "description": sli.description,
                "value": None,
                "slo": sli.slo,
                "fail_at": sli.fail_at,
                "unit": sli.unit,
                "meets_slo": False,
                "ok": False,
                "reason": "no data",
            })
            ok_all = False
            continue

        meets_slo = sli.comparator(value, sli.slo)
        meets_failure = sli.comparator(value, sli.fail_at)
        report.append({
            "name": sli.name,
            "description": sli.description,
            "value": round(value, 4),
            "slo": sli.slo,
            "fail_at": sli.fail_at,
            "unit": sli.unit,
            "meets_slo": meets_slo,
            "ok": meets_failure,
        })
        if not meets_failure:
            ok_all = False
    return report, ok_all


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify SLI thresholds against Prometheus.")
    parser.add_argument("--prometheus", default=os.getenv("PROMETHEUS_URL", "http://localhost:9090"))
    parser.add_argument("--wait", type=float, default=15.0,
                        help="Seconds to wait before evaluating (let load metrics propagate).")
    parser.add_argument("--out", default="sli-report.json")
    args = parser.parse_args()

    if args.wait > 0:
        print(f"sleeping {args.wait:.0f}s before SLI evaluation...", flush=True)
        time.sleep(args.wait)

    report, ok = evaluate(args.prometheus)
    with open(args.out, "w") as f:
        json.dump({"ok": ok, "results": report}, f, indent=2)

    width = max(len(r["name"]) for r in report)
    print("\nSLI report:")
    for r in report:
        if "error" in r:
            print(f"  {r['name']:<{width}}  ERROR: {r['error']}")
            continue
        status = "OK" if r["ok"] else "FAIL"
        if r.get("reason") == "no data":
            print(f"  {r['name']:<{width}}  value=<no data>  slo={r['slo']} "
                  f"fail_at={r['fail_at']}  [{status}]")
            continue
        slo_mark = "(ok)" if r.get("meets_slo") else "(over)"
        unit = r.get("unit", "")
        print(f"  {r['name']:<{width}}  value={r['value']} {unit:<6} "
              f"slo={r['slo']} {slo_mark} fail_at={r['fail_at']}  [{status}]")
    print(f"\noverall: {'OK' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
