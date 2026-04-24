FROM python:3.11-slim

WORKDIR /tests

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PRODUCER_URL=http://producer:8000 \
    CLICKHOUSE_HOST=clickhouse \
    CLICKHOUSE_PORT=8123

CMD ["pytest", "-v", "-s"]
