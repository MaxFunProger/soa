# HW7 - CI/CD, Testing & Observability

База - сервисы из ДЗ-5 (Online Cinema). Здесь сверху накручены: метрики Prometheus
у producer и aggregation, дашборды Grafana отдельно для сервисов и для инфры,
unit / integration / e2e тесты, нагрузка k6, алерты Prometheus + Alertmanager,
проверка SLI из CI и сам GitHub Actions пайплайн, который всё это последовательно
прогоняет.

Всё поднимается одной командой:

```
cd hw7
docker compose up -d --build
```

## Что где лежит (по пунктам задания)

| п. | где |
|---|---|
| 1. CI pipeline | `.github/workflows/hw7.yml` |
| 2. Integration | `tests/test_integration.py` |
| 3. E2E | `tests/test_e2e.py` |
| 4. Prometheus + метрики сервисов | `producer/app/metrics.py`, `aggregation/app/metrics.py`, `prometheus/prometheus.yml` |
| 5. Grafana service dashboard | `grafana/dashboards/services.json` |
| 6. Grafana infra dashboard | `grafana/dashboards/infra.json` |
| 7. k6 нагрузка | `load/script.js` + `docker-compose.load.yml` |
| 8. e2e + нагрузка + проверка метрик в одном CI прогоне | job `load_and_sli` в workflow + `scripts/check_sli.py` |
| 9. Alert rules + Alertmanager | `prometheus/alerts.yml`, `alertmanager/alertmanager.yml`, демо: `scripts/demo_alert.sh` |
| 10. SLI/SLO | `scripts/check_sli.py` + секция ниже |

## Стек

Тот же что в ДЗ-5:

- producer (FastAPI, Python 3.11): принимает события по HTTP, кладёт их в Kafka
  (Avro через Schema Registry). Внутри есть генератор синтетических сессий,
  включается env'ом `GENERATOR_ENABLED`.
- aggregation (FastAPI + APScheduler): раз в минуту читает события из ClickHouse,
  считает DAU / avg view time / top movies / view conversion / retention,
  пишет в ClickHouse и Postgres и выгружает csv в MinIO.
- Kafka в KRaft, 2 брокера, `replication.factor=2`, `min.insync.replicas=1`.
- ClickHouse: Kafka engine -> Materialized view -> MergeTree.
- Postgres хранит готовые бизнес-метрики.
- MinIO - холодное хранилище агрегатов.

Новое в hw7: Prometheus, Alertmanager, kafka-exporter, postgres-exporter,
встроенные `/metrics` у ClickHouse, cAdvisor, и k6 как отдельный compose-сервис
для нагрузки.

## Адреса

| что | url |
|---|---|
| producer | http://localhost:8000 (`/health`, `/metrics`, `/events`, `/docs`) |
| aggregation | http://localhost:8010 (`/health`, `/metrics`, `/recompute/<date>`) |
| Prometheus | http://localhost:9090 (вкладка Alerts) |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 (admin/admin, доступен и анонимно) |
| Schema Registry | http://localhost:8081 |
| ClickHouse HTTP | http://localhost:8123 |
| MinIO console | http://localhost:9002 (minio / minio12345) |
| kafka-exporter | http://localhost:9308/metrics |
| cAdvisor | http://localhost:8085 |

## Запуск тестов

Unit (без docker, нужны только pip-зависимости сервиса + pytest):

```
cd producer    && PYTHONPATH=. python -m pytest tests -v
cd aggregation && PYTHONPATH=. python -m pytest tests -v
```

Integration + e2e (поднимают всю систему через compose и гоняют сценарии):

```
make test-int
# то же самое:
docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm --build tests
```

k6 (предварительно `make up`):

```
make load
```

Результаты k6 ложатся в `load/results/summary.json`.

Проверка SLI (с уже поднятым стеком):

```
make sli
# то же самое:
python3 scripts/check_sli.py --prometheus http://localhost:9090
```

## Метрики

Обязательные три есть у обоих сервисов:

| метрика | тип | labels |
|---|---|---|
| `http_requests_total` | counter | method, endpoint, status |
| `http_request_errors_total` | counter | method, endpoint, error_type |
| `http_request_duration_seconds` | histogram | method, endpoint |

`endpoint` берётся как route template (`/events`, не `/events/foo`) - смотрим в
`request.scope["route"].path`, иначе откатываемся на фактический url. Так
кардинальность ярлыков не взрывается. См. middleware в
`producer/app/metrics.py` и `aggregation/app/metrics.py`.

Плюс свои бизнес-метрики:

producer:
- `producer_kafka_events_published_total{event_type}` - сколько событий
  отдали в продьюсер (Avro encode + enqueue прошёл).
- `producer_kafka_events_delivered_total{result}` - сколько подтверждений
  пришло от брокеров (success / failure из delivery callback).
- `producer_kafka_publish_duration_seconds` - histogram, время от вызова
  publish() до постановки в очередь librdkafka.
- `producer_kafka_queue_length` - gauge, текущая длина outbound-очереди
  (опрашивается в poll-loop потоке).

aggregation:
- `aggregation_runs_total{status}` - success / failure циклов.
- `aggregation_run_duration_seconds` - histogram длительности цикла.
- `aggregation_records_processed_total` - суммарно обработано записей.
- `aggregation_last_success_timestamp_seconds` - unix-time последнего
  успешного цикла. Та же метрика используется в алерте AggregationStalled
  и в SLI freshness.

scrape_configs в `prometheus/prometheus.yml` - producer, aggregation,
kafka-exporter, postgres-exporter, ClickHouse `/metrics`, cAdvisor, и сам
Prometheus.

## Grafana

Datasource и provider дашбордов подкладываются автоматически через
`grafana/provisioning/...`.

Дашборды:

- **Cinema HW7 - Services** (`services.json`). Две секции, по сервису.
  Latency p50/p95/p99, error rate (5m), RPS по эндпоинтам, доля успешных
  delivery в Kafka, события по типам, длительность цикла агрегации,
  возраст последнего успешного цикла.
- **Cinema HW7 - Infrastructure** (`infra.json`). Kafka: consumer lag,
  количество брокеров, partitions у топика, throughput. Postgres: активные
  коннекты по базам и cache hit ratio. ClickHouse: insert rate, queries,
  memory tracking. cAdvisor: CPU по контейнерам. Должен отвечать на
  вопрос "где сейчас узко".
- **Movie Analytics** - бизнес-дашборд из ДЗ-5, оставил для полноты картины
  (retention heatmap, DAU, top movies).

JSON-ки лежат в репе, отдельный экспорт делать не надо.

## Alert rules

`prometheus/alerts.yml`. Подгружаются Prometheus'ом и шлются в Alertmanager
(`alertmanager/alertmanager.yml`).

- `ServiceDown` - `up{job=~"producer|aggregation"} == 0` за минуту, critical.
- `HighHTTPErrorRate` - errors/requests > 5% за 5 минут, warning.
- `HighLatencyP95` - p95 latency > 1s за 5 минут, warning. Тот же запрос
  используется в SLI.
- `KafkaConsumerLagHigh` - суммарный lag по consumergroup > 1000 за 5 минут.
- `AggregationStalled` - `time() - aggregation_last_success > 600` (10 минут).

Чтобы показать firing на защите:

```
./scripts/demo_alert.sh
```

Скрипт спамит сломанными запросами и опрашивает `/api/v1/alerts`, пока
`HighHTTPErrorRate` не перейдёт в firing. После этого алерт виден и в
Prometheus (`/alerts`), и в Alertmanager UI.

## SLI / SLO

Считаются из метрик Prometheus, не хардкод. Если порог отказа пересечён -
`scripts/check_sli.py` возвращает exit code 1 и роняет CI.

| SLI | что меряем | PromQL (упрощённо) | SLO | порог отказа |
|---|---|---|---|---|
| API latency p95 | producer p95 за 5m | `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{service="producer"}[5m])))` | < 500ms | > 1000ms |
| API availability | 1 - error_rate producer'а за 5m | `1 - errors/requests` | > 99.5% | < 95% |
| Event processing freshness | время с последнего успешного цикла агрегации | `time() - aggregation_last_success_timestamp_seconds` | < 120s | > 600s |

Откуда взялись пороги:

- Latency. Producer делает Avro encode и кидает в локальную очередь librdkafka.
  В норме это десятки миллисекунд. 500ms - это уже сильно медленнее обычного,
  но клиент ещё ждёт. 1s - очень плохо, реальный пользователь начнёт
  ретраить или таймаутиться.
- Availability. Производитель синтетических событий шлёт пачками, потеря
  больше 5% событий означает что либо Kafka недоступна, либо сериализатор
  падает - всё равно надо чинить срочно. 99.5% оставляю как нормальный SLO
  поскольку допускаю редкие 5xx при рестартах брокеров.
- Freshness. Cycle interval = 60 секунд (`AGGREGATION_INTERVAL_SECONDS=60`).
  120 секунд - один пропущенный цикл, переживём. 600 секунд (10 минут)
  без агрегации - что-то реально сломалось: либо ClickHouse, либо Postgres,
  либо сам сервис в loop'е падает.

Полная имплементация - в `scripts/check_sli.py`. Он же используется в CI
после k6: запускается, спит несколько секунд чтобы метрики прогрелись,
тянет три PromQL-запроса через `/api/v1/query`, пишет отчёт в
`load/results/sli-report.json` и валит шаг при превышении.

## CI

`.github/workflows/hw7.yml`. Триггер - push / PR в `hw-7` или `main`,
если затронут `hw7/**` или сам workflow; плюс `workflow_dispatch`.

Джобы идут последовательно:

1. **unit**. Ставит pip-зависимости producer и aggregation, гоняет pytest
   по их `tests/`. Запускается на чистом ubuntu, без docker.
2. **build**. Собирает docker-образы (producer, aggregation, tests).
   Зависит от unit - если юниты упали, сборку не делаем.
3. **integration**. `docker compose up -d --build --wait`, потом
   `docker compose run tests pytest test_integration.py test_e2e.py`.
   На failure печатает `docker compose logs --tail=200`.
4. **load_and_sli**. Поднимает стек, ждёт прогрева 30 секунд (генератор
   успевает накидать метрик), потом k6 через compose-override, потом
   `scripts/check_sli.py` против Prometheus. Артефакты (`load/results/`)
   складываются через `actions/upload-artifact`.

Если падает хоть один шаг - падает весь пайплайн.
