// k6 load test for the movie-events producer service.
// Drives ~POST /events with realistic payloads; fails CI if p95 latency or
// error rate exceeds the thresholds below.
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const PRODUCER_URL = __ENV.PRODUCER_URL || "http://localhost:8000";

export const publishErrors = new Counter("publish_errors");
export const publishOk = new Rate("publish_ok_ratio");

const EVENT_TYPES = ["VIEW_STARTED", "VIEW_PAUSED", "VIEW_RESUMED", "VIEW_FINISHED", "LIKED"];
const DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"];
const MOVIES = Array.from({ length: 40 }, (_, i) => `movie-${(i + 1).toString().padStart(3, "0")}`);
const USERS = Array.from({ length: 200 }, (_, i) => `load-user-${(i + 1).toString().padStart(4, "0")}`);

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export const options = {
  scenarios: {
    publish_events: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 15 },
        { duration: "30s", target: 15 },
        { duration: "5s", target: 0 },
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.02"],
    http_req_duration: ["p(95)<500"],
    publish_ok_ratio: ["rate>0.98"],
  },
};

export default function () {
  const payload = JSON.stringify({
    user_id: pick(USERS),
    movie_id: pick(MOVIES),
    event_type: pick(EVENT_TYPES),
    device_type: pick(DEVICES),
    session_id: uuidv4(),
    progress_seconds: Math.floor(Math.random() * 5400),
  });
  const res = http.post(`${PRODUCER_URL}/events`, payload, {
    headers: { "Content-Type": "application/json" },
    timeout: "10s",
  });

  const ok = check(res, {
    "accepted": (r) => r.status === 202,
    "has event_id": (r) => {
      try {
        return JSON.parse(r.body).event_id !== undefined;
      } catch (e) {
        return false;
      }
    },
  });

  publishOk.add(ok);
  if (!ok) publishErrors.add(1);

  sleep(Math.random() * 0.2);
}

export function handleSummary(data) {
  // путь должен совпадать с volume mount /results в docker-compose.load.yml
  return {
    "/results/summary.json": JSON.stringify(data, null, 2),
    stdout: textSummary(data),
  };
}

function textSummary(data) {
  const m = data.metrics;
  const p95 = m.http_req_duration?.values?.["p(95)"]?.toFixed(2);
  const errorRate = (m.http_req_failed?.values?.rate * 100).toFixed(3);
  const okRate = (m.publish_ok_ratio?.values?.rate * 100).toFixed(2);
  return `
=== Load summary ===
  p95 latency:      ${p95} ms
  http error rate:  ${errorRate} %
  publish_ok_ratio: ${okRate} %
=====================
`;
}
