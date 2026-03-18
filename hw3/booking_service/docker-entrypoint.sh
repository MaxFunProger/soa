#!/bin/sh
set -e
echo "Waiting for PostgreSQL..."
until nc -z postgres-booking 5432 2>/dev/null; do sleep 1; done
echo "Running migrations..."
cd /app && alembic upgrade head
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
