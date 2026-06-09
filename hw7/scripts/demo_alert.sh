#!/usr/bin/env bash
# Force-fires the HighHTTPErrorRate alert for the producer:
#   - sends ~50 bad requests / sec for 6 minutes
#   - polls /api/v1/alerts until the alert flips to "firing"
#
# Use only against the local stack; this is a demonstration helper,
# not a real load source.
set -euo pipefail

PRODUCER_URL=${PRODUCER_URL:-http://localhost:8000}
PROM_URL=${PROMETHEUS_URL:-http://localhost:9090}
ALERT_NAME=${ALERT_NAME:-HighHTTPErrorRate}
DURATION_SECS=${DURATION_SECS:-360}

echo "spamming bad payloads at ${PRODUCER_URL}/events for ${DURATION_SECS}s ..."
END=$(( $(date +%s) + DURATION_SECS ))

(
  while [ "$(date +%s)" -lt "$END" ]; do
    for _ in $(seq 1 50); do
      curl -s -o /dev/null -X POST "${PRODUCER_URL}/events" \
        -H 'Content-Type: application/json' \
        -d '{"user_id":"","movie_id":"","event_type":"BAD","device_type":"DESKTOP","session_id":"x"}' &
    done
    wait
    sleep 1
  done
) &
SPAM_PID=$!

trap 'kill $SPAM_PID 2>/dev/null || true' EXIT

echo "polling Prometheus for alert state..."
while true; do
  state=$(curl -s "${PROM_URL}/api/v1/alerts" \
    | python3 -c "import json,sys;d=json.load(sys.stdin);a=[x for x in d['data']['alerts'] if x['labels'].get('alertname')=='${ALERT_NAME}'];print(a[0]['state'] if a else 'absent')")
  echo "  ${ALERT_NAME}: ${state}"
  if [ "${state}" = "firing" ]; then
    echo "alert is firing - demo complete"
    break
  fi
  sleep 5
done
