# HW5 - Online Cinema: Event Streaming + Analytics Pipeline

Pipeline обработки событий онлайн-кинотеатра на базе Kafka, ClickHouse, PostgreSQL, MinIO и Grafana.
Язык сервисов — Python 3.11.

## Состав (маппинг на пункты задания)

| # | Пункт | Где реализовано |
|---|------|------------------|
| 1 | Avro-схема + Schema Registry + topic с 3 партициями, ключ `user_id` | `schemas/movie_event.avsc`, `schemas/README.md`, `producer/app/bootstrap.py` |
| 2 | Producer: HTTP API + генератор, Avro-сериализация, acks=all, retries, логирование | `producer/` (FastAPI + confluent-kafka) |
| 3 | ClickHouse: Kafka Engine → MaterializedView → MergeTree | `clickhouse/init/01-create-tables.sql` |
| 4 | Интеграционный тест pipeline | `tests/test_pipeline.py` |
| 5 | Aggregation-сервис: DAU, avg view time, top movies, view conversion, retention → PostgreSQL | `aggregation/` |
| 6 | Grafana-дашборд с Retention Cohort Heatmap + DAU / conversion / top movies / avg view time | `grafana/` |
| 7 | Экспорт в S3 (MinIO), партиционирование `daily/YYYY-MM-DD/aggregates.csv` | `aggregation/app/s3_exporter.py` |
| 8 | Kafka-кластер (2 брокера, KRaft), `replication.factor=2`, `min.insync.replicas=1`, Schema Registry, health checks у всех сервисов | `docker-compose.yml` |

## Запуск

```bash
cd hw5
docker compose up -d --build
```

Всё поднимется одной командой. Миграции ClickHouse и PostgreSQL применяются автоматически через
`/docker-entrypoint-initdb.d`. Topic `movie-events` и регистрация Avro-схемы выполняются сервисом
`producer` при старте.

### Адреса

| Сервис | URL |
|--------|-----|
| Producer HTTP API | http://localhost:8000 (Swagger: `/docs`) |
| Aggregation HTTP API | http://localhost:8010 (Swagger: `/docs`) |
| Kafka bootstrap | `localhost:9092`, `localhost:9093` |
| Schema Registry | http://localhost:8081 |
| ClickHouse HTTP | http://localhost:8123 |
| PostgreSQL | `localhost:5432` (db `analytics`, user `analytics`, pass `analytics`) |
| MinIO S3 | http://localhost:9001 (console: http://localhost:9002, user `minio` / pass `minio12345`) |
| Grafana | http://localhost:3000 (admin/admin; анонимный доступ тоже открыт) |

## Генератор событий

Продюсер поднимается с включённым по умолчанию генератором синтетических событий:
он создаёт реалистичные сессии (`VIEW_STARTED` → `VIEW_PAUSED` → `VIEW_RESUMED` → `VIEW_FINISHED` →
опционально `LIKED`; отдельно бросает `SEARCHED`). Скорость регулируется переменной `GENERATOR_EPS`.

Чтобы выключить генератор, нужно выставить `GENERATOR_ENABLED=false` в `docker-compose.yml`.

## Ручная публикация события

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "user-0001",
    "movie_id": "movie-042",
    "event_type": "VIEW_STARTED",
    "device_type": "DESKTOP",
    "session_id": "abc-123",
    "progress_seconds": 0
  }'
```

## Ручной пересчёт агрегатов и экспорт в S3

```bash
curl -X POST http://localhost:8010/recompute/2026-04-24
curl -X POST http://localhost:8010/export/2026-04-24
```

Интервал автоматического пересчёта задаётся переменной `AGGREGATION_INTERVAL_SECONDS`
(по умолчанию 60 секунд)

## Проверка данных

ClickHouse — raw-события:
```bash
docker exec -it clickhouse clickhouse-client --query \
  "SELECT event_type, count() FROM cinema.movie_events GROUP BY event_type ORDER BY event_type"
```

ClickHouse — агрегаты:
```bash
docker exec -it clickhouse clickhouse-client --query "SELECT * FROM cinema.agg_dau ORDER BY metric_date"
```

PostgreSQL — готовые метрики:
```bash
docker exec -it postgres psql -U analytics -d analytics -c \
  "SELECT metric_date, metric_name, bucket_key, value FROM metrics ORDER BY metric_date, metric_name LIMIT 50;"
```

MinIO — выгруженные агрегаты:
```bash
docker exec -it minio mc --help >/dev/null 2>&1 || true
# http://localhost:9002, movie-analytics/daily/YYYY-MM-DD/aggregates.csv
```

## Интеграционный тест

Тест публикует событие через Producer HTTP API, ждёт его появления в ClickHouse и проверяет поля:

```bash
make test
# или
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm --build tests
```

Тест запускается контейнером в общей сети, использует `depends_on: service_healthy`, идемпотентен
(каждый запуск публикует событие со свежим UUID)

## Бизнес-метрики

Считаются в ClickHouse:

- **DAU** — `uniqExact(user_id)` за день.
- **Среднее время просмотра** — `avg(progress_seconds)` на `VIEW_FINISHED`.
- **Топ фильмов** — `count() … GROUP BY movie_id ORDER BY views DESC LIMIT 10`.
- **Конверсия просмотра** — `countIf(VIEW_FINISHED) / countIf(VIEW_STARTED)`.
- **Retention D1…D7** — CTE с `min(timestamp)` для определения когорты + `dateDiff` + `uniqExact`.

Все метрики в ClickHouse (`cinema.agg_*`) и зеркалируются в PostgreSQL
(`metrics`, `retention_matrix`). Запись в Postgres идемпотентна (`ON CONFLIC ... DO UPDATE`)

## Замечания

- **Ключ партиционирования Kafka**: `user_id` (см. `schemas/README.md`).
- **ClickHouse Kafka Engine** использует `AvroConfluent` и `format_avro_schema_registry_url`, то есть
  десериализует те же сообщения, что шлёт Python-продьюсер через Confluent wire format (magic byte + schema id).
- **Идемпотентность продьюсера** включена (`enable.idempotence=true`, `acks=all`).
- **Отказоустойчивость Kafka**: 2 брокера в режиме KRaft, `replication.factor=2`, `min.insync.replicas=1`,
  health checks у всех сервисов (см. `docker-compose.yml`).
- **Retry + логирование**: клиенты ClickHouse/PostgreSQL/S3 используют `tenacity`;
  Kafka-продьюсер — встроенные retries + экспоненциальный backoff на `BufferError`.
