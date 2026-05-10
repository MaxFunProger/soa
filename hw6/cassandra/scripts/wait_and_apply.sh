#!/usr/bin/env bash
# Ожидаем доступности кассандры и применяем все CQL-миграции из /migrations.
set -euo pipefail

HOST="${CASSANDRA_HOST:-cassandra-1}"
PORT="${CASSANDRA_PORT:-9042}"
MIG_DIR="${MIGRATIONS_DIR:-/migrations}"

echo "[init] waiting for ${HOST}:${PORT} to accept CQL..."
attempt=0
until cqlsh "${HOST}" "${PORT}" -e "SELECT cluster_name FROM system.local;" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "${attempt}" -gt 90 ]; then
    echo "[init] cassandra is not reachable, giving up" >&2
    exit 1
  fi
  sleep 5
done

echo "[init] cassandra is up; waiting for >=2 nodes to be UN..."
attempt=0
while true; do
  up_count=$(cqlsh "${HOST}" "${PORT}" -e "SELECT peer FROM system.peers;" 2>/dev/null \
    | awk '/^ [0-9a-fA-F]/ {n++} END {print n+0}')
  total=$((up_count + 1))
  if [ "${total}" -ge 2 ]; then
    echo "[init] cluster has ${total} reachable nodes"
    break
  fi
  attempt=$((attempt + 1))
  if [ "${attempt}" -gt 60 ]; then
    echo "[init] proceeding with ${total} node(s) — fallback (cluster may not be fully assembled yet)"
    break
  fi
  sleep 5
done

echo "[init] applying migrations from ${MIG_DIR}"
for f in "${MIG_DIR}"/*.cql; do
  echo "[init] -> ${f}"
  cqlsh "${HOST}" "${PORT}" -f "${f}"
done

echo "[init] migrations applied successfully"
