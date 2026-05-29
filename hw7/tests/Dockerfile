FROM python:3.11-slim

WORKDIR /tests

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PRODUCER_URL=http://producer:8000 \
    AGGREGATION_URL=http://aggregation:8010 \
    CLICKHOUSE_HOST=clickhouse \
    CLICKHOUSE_PORT=8123 \
    POSTGRES_HOST=postgres \
    POSTGRES_PORT=5432 \
    POSTGRES_DB=analytics \
    POSTGRES_USER=analytics \
    POSTGRES_PASSWORD=analytics

CMD ["pytest", "-v", "--tb=short"]
