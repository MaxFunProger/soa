#!/bin/sh
set -e
echo "Waiting for PostgreSQL..."
until nc -z postgres-flight 5432 2>/dev/null; do sleep 1; done
echo "Running migrations..."
cd /app && alembic upgrade head
exec python -m app.main
