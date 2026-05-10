# HW6 - Smart Warehouse: Event-Driven State Management with Cassandra

Event-driven система управления складом на базе Kafka + Schema Registry + Cassandra (3 ноды) +
Prometheus + Grafana. Решение покрывает все 10 пунктов задания.

```
+-------------+    +----------------+    +-----------+
| WMS Service |--->|   Kafka (x2)   |--->| Consumer  |
| (FastAPI)   |    | + Schema Reg.  |    | service   |
+-------------+    +----------------+    +-----------+
                          |                    |
                          v                    v
                  warehouse-events       Cassandra
                  warehouse-events-dlq   (3 nodes, RF=3)

  Consumer экспортирует /metrics + /health -> Prometheus -> Grafana.
```

Все сервисы поднимаются одной командой `docker compose up`.

---

## Соответствие пунктам задания

| # | Пункт | Где реализовано |
|---|-------|------------------|
| 1 | Kafka consumer, group `warehouse-state-consumer`, **at-least-once**, ручной commit, лог `event_id/type/partition/offset` | `consumer/app/kafka_consumer.py` |
| 2 | Cassandra-схема: 3 денормализованные таблицы + processed_events + orders + events_log | `cassandra/init/01_schema.cql` |
| 3 | Обработчики событий, **детерминированная** запись состояния | `consumer/app/handlers.py` |
| 4 | Идемпотентность по `event_id` (проверка перед обработкой + запись в `processed_events` в той же BATCH) | `consumer/app/handlers.py`, `consumer/app/cassandra_store.py` |
| 5 | **LOGGED BATCH** атомарно обновляет 3 таблицы (`inventory_by_product_zone`, `inventory_by_product`, `inventory_by_zone`) + `processed_events` + `events_log` | `consumer/app/handlers.py::_apply_inventory_change` |
| 6 | Out-of-order: каждое событие сравнивается с `last_event_ts` затронутых (product, zone) и игнорируется, если устарело | `consumer/app/handlers.py` |
| 7 | DLQ (`warehouse-events-dlq`) c полным envelope (original_event, error_reason/code, kafka_metadata) | `consumer/app/dlq.py`, `schemas/DLQEnvelope.avsc` |
| 8 | Кластер Cassandra из 3 нод, NetworkTopologyStrategy, RF=3, `WRITE=QUORUM`, `READ=ONE` (с обоснованием) | `docker-compose.yml`, `cassandra/init/01_schema.cql`, `consumer/app/cassandra_store.py` |
| 9 | Prometheus-метрики (`/metrics`), `/health`, dashboard Grafana с lag/throughput/errors | `consumer/app/metrics.py`, `prometheus/`, `grafana/` |
| 10 | Schema Registry: V1 (без supplier_id) + V2 (BACKWARD compatible, default null) ProductReceived; consumer обрабатывает оба варианта | `schemas/ProductReceived.v{1,2}.avsc`, `producer/app/bootstrap.py`, `consumer/app/handlers.py` |

---

## Запуск

```bash
cd hw6
docker compose up -d --build
```

При первом запуске система разворачивается ~2–3 минуты (3 ноды Cassandra стартуют последовательно).
Готовность можно отследить:

```bash
docker compose ps
docker exec cassandra-1 nodetool status   # три ноды должны быть UN
```

### Адреса

| Сервис             | URL                                 |
|--------------------|-------------------------------------|
| Producer (HTTP)    | http://localhost:8000 (`/docs` Swagger) |
| Consumer health    | http://localhost:9100/health        |
| Consumer metrics   | http://localhost:9100/metrics       |
| Kafka brokers      | `localhost:9092`, `localhost:9093`  |
| Schema Registry    | http://localhost:8081               |
| Cassandra (CQL)    | `localhost:9042`                    |
| Prometheus         | http://localhost:9090               |
| Grafana            | http://localhost:3000 (admin/admin, доступен и анонимно) |

---

## Дизайн модели данных Cassandra

Все таблицы спроектированы **под запросы**, без вторичных индексов и JOIN.
Денормализация — намеренная: одни и те же данные дублируются под разные access-pattern'ы и
обновляются атомарно через `LOGGED BATCH`.

### `inventory_by_product_zone` — точечный лукап

```cql
PRIMARY KEY ((product_id, zone_id))
```

Композитный partition key даёт точечный доступ за один RPC: все строки одной (product, zone)
находятся на одном наборе реплик. Используется для горячего пути:

```cql
SELECT * FROM inventory_by_product_zone WHERE product_id=? AND zone_id=?;
```

### `inventory_by_product` — все остатки одного товара + агрегированные totals

```cql
PRIMARY KEY ((product_id), zone_id)
```

Партиционируется по товару, кластеризуется по зоне. Запрос `WHERE product_id=?` отдаёт все
зональные строки за один RPC. В каждую row дублируются `total_available/total_reserved` —
агрегированные значения по всем зонам товара, чтобы не считать `SUM` на стороне приложения.
Эти totals пересчитываются consumer'ом при каждой обработке события и обновляются в одной
BATCH-операции вместе с per-zone-полем.

### `inventory_by_zone` — все товары в одной зоне

```cql
PRIMARY KEY ((zone_id), product_id)
```

Зеркальная денормализация — для запроса «все SKU в зоне X».

### `processed_events` — идемпотентность

```cql
event_id TEXT PRIMARY KEY,
... TTL = 30 days
```

Перед обработкой consumer проверяет наличие event_id; в той же BATCH-операции записывается
отметка об успешной обработке. TTL 30 дней предотвращает безграничный рост таблицы.

### `orders` — состояние заказов

```cql
order_id TEXT PRIMARY KEY,
items list<frozen<tuple<text,text,bigint>>>
```

Заказ — это агрегат: один partition на заказ, items хранятся как frozen-collection (атомарная
запись).

### `events_log` — журнал событий (опциональный, для аудита)

Партиционируется по дате (yyyy-mm-dd), кластеризуется по `event_ts DESC`. Используется
исключительно для обзора недавних событий (например, в Grafana / cqlsh) и не участвует в
расчёте состояния.

### Consistency levels (пункт 8)

* **Запись:** `QUORUM` — гарантирует, что любое уведомление об успехе записи означает, что
  данные находятся как минимум на 2 из 3 реплик. При остановке одной ноды система продолжает
  принимать записи (2/3 — это QUORUM).
* **Чтение:** `ONE` — выбран **осознанно**:
  - В нашем случае consumer строго упорядочен по партиции Kafka (партиционирование по
    `product_id`), так что одно и то же `(product_id, zone_id)` всегда обрабатывается одним
    инстансом → нет конкурентных мутаций.
  - Перед каждым событием consumer всё равно делает read-modify-write: даже если read вернёт
    немного устаревшие данные, последующий QUORUM-write обновит ≥2 реплик; следующий read
    подхватит обновление с большой вероятностью с любой реплики.
  - QUORUM-чтение в этой схеме почти не даёт выгод (всё равно нет двух конкурирующих писателей
    по одной партиции), но удваивает latency и нагрузку на сеть.
  - Для строго чтения с гарантиями (например, бизнес-API на чтение) можно через переменную
    окружения `CASSANDRA_READ_CONSISTENCY=QUORUM` поднять уровень — поведение настраивается
    без правки кода.

---

## At-least-once + идемпотентность

* `enable.auto.commit=false` — offset коммитится **после** успешной записи в Cassandra.
* В любой ошибке обработчика событие отправляется в DLQ; offset коммитится, чтобы цикл не
  блокировался (см. пункт 7).
* `processed_events` хранит обработанные `event_id`. Перед обработкой делается
  `SELECT event_id FROM processed_events WHERE event_id=?` (CL=ONE — нам не страшно ложное
  «уже обработано», потому что write был QUORUM, и любое чтение с RF=3 с большой вероятностью
  его увидит). Если событие уже обработано, оно пропускается.
* Запись в `processed_events` входит в **ту же LOGGED BATCH** что и обновления денормализованных
  таблиц — то есть либо «состояние обновлено + событие отмечено как обработанное», либо ни то ни
  другое. Дубль из Kafka попадёт в идемпотентный путь и не приведёт к повторному обновлению.

---

## Атомарность денормализованных таблиц (пункт 5)

`consumer/app/handlers.py::_apply_inventory_change` собирает один
`BatchStatement(BatchType.LOGGED, consistency_level=QUORUM)`, в который попадают:

1. UPDATE `inventory_by_product_zone` (per-zone остатки + supplier_id)
2. UPDATE `inventory_by_zone` (зеркальная денормализация)
3. UPDATE `inventory_by_product` для каждой затронутой (product, zone) с пересчитанными
   `total_available/total_reserved`
4. UPDATE `orders` (если событие — OrderCreated/OrderCompleted)
5. INSERT `processed_events` (для идемпотентности)
6. INSERT `events_log` (audit)

LOGGED-batch в Cassandra **гарантирует**, что либо все мутации применятся, либо ни одна. Из этого
следует невозможность ситуации «inventory_by_zone обновлена, а inventory_by_product — нет».

---

## Out-of-order events (пункт 6)

В каждой строке `inventory_by_product_zone` и `inventory_by_product` хранится `last_event_ts`
(unix epoch ms). Перед применением события для каждой пары `(product_id, zone_id)` проверяется:

```
if event.timestamp <= state.last_event_ts: skip  # старое событие
```

Аналогично для `orders` — проверяется `last_event_ts` агрегата. Это работает в комбинации с
партиционированием по `product_id` в Kafka: события одного товара всегда идут через одного
consumer, и `last_event_ts` обновляется монотонно.

---

## DLQ (пункт 7)

Ошибки маршрутизируются в `warehouse-events-dlq` (Avro envelope в `schemas/DLQEnvelope.avsc`):

```json
{
  "original_event": "<JSON-сериализованное исходное событие или сырой base64>",
  "original_record_name": "ProductShipped",
  "original_topic": "warehouse-events",
  "error_reason": "Invalid quantity: -5 (must be positive)",
  "error_code": "VALIDATION_ERROR",
  "failed_at": 1762800000000,
  "kafka_partition": 2,
  "kafka_offset": 12345
}
```

Категории ошибок:
* `VALIDATION_ERROR`, `INSUFFICIENT_AVAILABLE`, `INSUFFICIENT_RESERVED`, `ORDER_NOT_FOUND` — бизнес-валидация.
* `DESERIALIZATION_ERROR`, `MISSING_RECORD_NAME` — некорректные сообщения.
* `HANDLER_ERROR` — любые runtime-ошибки.

Воспроизвести DLQ можно отправкой `PRODUCT_SHIPPED` с `quantity=-5` (см. сценарий ниже).

---

## Cassandra cluster (пункт 8)

`docker-compose.yml` поднимает три контейнера: `cassandra-1`, `cassandra-2`, `cassandra-3`,
объединённые в один кластер `warehouse-cluster` через seed=`cassandra-1`. Используется
`GossipingPropertyFileSnitch`, dc=`datacenter1`. Keyspace `warehouse` создаётся через
`NetworkTopologyStrategy` с `replication_factor = 3` (по одной реплике на ноду).

Демонстрация отказоустойчивости:

```bash
docker exec cassandra-1 nodetool status      # 3 ноды UN
docker stop cassandra-2                      # одна нода вне строя
# WRITE=QUORUM ещё работает (2 из 3); событие обрабатывается
# READ=ONE отдаёт ответ с любой из оставшихся нод
docker start cassandra-2                     # нода возвращается в кластер
```

Что покажет `WRITE=ALL` при упавшей ноде — таймаут (требует 3 реплики). Обоснование выбора
`READ=ONE` против `QUORUM` см. выше.

---

## Monitoring (пункт 9)

`/metrics` consumer'а отдаёт:

* `consumer_lag{topic, partition}` — gauge, вычисляется фоновым потоком каждые 5 секунд через
  `Consumer.committed()` + `Consumer.get_watermark_offsets()`. Это «отставание» от HEAD топика
  (latest_offset − committed_offset) по каждой партиции.
* `events_processed_total{event_type}` — counter, успешно обработанные события.
* `events_skipped_total{reason}` — counter, пропущенные события (duplicate / stale).
* `event_processing_duration_seconds{event_type}` — histogram.
* `cassandra_write_errors_total{error_type}` — counter, ошибки записи.
* `dlq_messages_total{error_code}` — counter, отправки в DLQ.
* `kafka_connection_up`, `cassandra_connection_up` — gauge для health-проб.

`/health` отдаёт **200 OK** только если Kafka жив и Cassandra-сессия отвечает на
`SELECT now() FROM system.local`. Иначе **503**.

Grafana provisioning поднимает datasource Prometheus и dashboard
`Warehouse Consumer` (`grafana/dashboards/consumer.json`) с панелями: Consumer Lag, Throughput,
Cassandra write errors, p95 latency, DLQ rate, Connections health (stat-плитка). Алерты на
`consumer_lag > 100` (warning) и `> 1000` (critical) определены в `prometheus/alerts.yml`.

---

## Schema Evolution (пункт 10)

В Schema Registry для одного субъекта `warehouse-events-ProductReceived` (TopicRecordNameStrategy)
зарегистрированы две версии:

* **v1** — `event_id, timestamp, product_id, zone_id, quantity`.
* **v2** — то же самое + `supplier_id: ["null", "string"], default=null`.

Стратегия совместимости — **BACKWARD** (`/config/<subject>` устанавливается продюсером при
старте). BACKWARD означает: новый ридер (читает по schema v2) сможет прочитать сообщения,
записанные старой схемой v1, потому что у `supplier_id` есть дефолт.

Producer публикует обе версии в один и тот же топик `warehouse-events`:

```bash
# V1 (без supplier_id):
curl -sS -X POST http://localhost:8000/events/product-received \
  -H 'Content-Type: application/json' \
  -d '{"product_id":"SKU-008","zone_id":"ZONE-A","quantity":100,"use_v2":false}'

# V2 (с supplier_id):
curl -sS -X POST http://localhost:8000/events/product-received \
  -H 'Content-Type: application/json' \
  -d '{"product_id":"SKU-008","zone_id":"ZONE-A","quantity":50,"supplier_id":"SUP-001"}'
```

Consumer-deserializer создан с `schema_str=None, return_record_name=True` — он использует
**writer's schema** (по schema_id из wire-format). `handle_product_received` универсально
работает с обеими версиями: если `supplier_id` отсутствует, в Cassandra пишется `NULL`; если
есть — записывается значение. Для V1-записей `inventory_by_product_zone.supplier_id` остаётся
`NULL` (default).

### Шаги по добавлению новой версии события (рецепт)

1. Добавить новый `.avsc` файл — например `ProductReceived.v3.avsc`. Новые поля **обязаны** иметь
   default value (например `["null", "string"]` с `default: null`), иначе уровень BACKWARD
   нарушается.
2. Зарегистрировать схему: либо вызвать `client.register_schema("warehouse-events-ProductReceived", ...)`
   из любого скрипта, либо добавить файл в `producer/app/bootstrap.py::SCHEMA_LAYOUT` (тогда
   регистрация выполнится при старте).
3. Если требуется новая колонка в Cassandra — добавить миграцию `cassandra/init/0X_*.cql` с
   `ALTER TABLE ... ADD column` и применить (для тестового окружения — `docker compose down -v`
   и заново `up`).
4. Обновить consumer-handler: считывать новое поле и записывать в Cassandra. V1/V2-сообщения
   остаются совместимыми благодаря дефолтам в Avro.
5. Прогнать тесты (`make test`) — V1, V2 и V3-события должны корректно обрабатываться.

---

### Полный pytest-прогон

Сборка и запуск интеграционных тестов в отдельном контейнере общей сети:

```bash
make test
```

Что проверяется (`tests/test_pipeline.py`):

* `/health` и `/metrics` consumer'а;
* базовый цикл склада + согласованность 3 таблиц;
* lifecycle заказа (CREATED → COMPLETED);
* идемпотентность (повторный event_id);
* out-of-order: старое событие игнорируется;
* DLQ для qty<=0 + восстановление pipeline после ошибки;
* schema evolution V1 + V2.

---

## Ручная проверка через CQL

```bash
docker exec -it cassandra-1 cqlsh -e "USE warehouse; DESCRIBE TABLES;"

docker exec -it cassandra-1 cqlsh -e \
  "SELECT * FROM warehouse.inventory_by_product_zone LIMIT 10;"

docker exec -it cassandra-1 cqlsh -e \
  "SELECT product_id, zone_id, total_available, total_reserved FROM warehouse.inventory_by_product LIMIT 10;"

docker exec -it cassandra-1 cqlsh -e \
  "SELECT * FROM warehouse.events_log LIMIT 10;"
```

Подсмотреть DLQ:

```bash
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 --topic warehouse-events-dlq \
  --from-beginning --max-messages 5 --timeout-ms 5000
```

---

## Что выключено в дефолтной поставке

* `GENERATOR_ENABLED=false` — синтетический генератор событий по умолчанию выключен, чтобы
  не зашумлять состояние Cassandra. Включить можно изменив переменную в `docker-compose.yml`
  у сервиса `producer`.

---
