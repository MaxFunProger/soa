# Домашнее задание №3 — Flight Booking (gRPC + Redis)

## Запуск

```bash
cd hw3
docker compose up --build
```

- **Booking Service (REST):** http://localhost:8000  
- **Flight Service (gRPC):** `localhost:50051`  
- **PostgreSQL booking:** порт `15434` (с хоста)  
- **PostgreSQL flight:** порт `15435`  
- **Redis master:** `16379`, **Sentinel:** `26379`

Миграции Alembic выполняются при старте контейнеров `booking-service` и `flight-service`.

---

## Описание реализованных пунктов

### 1. gRPC-контракт Flight Service

Контракт описан в `proto/flight/v1/flight.proto`. Методы: **SearchFlights** (по маршруту и опционально дате), **GetFlight** (по id), **ReserveSeats** (flight_id, seat_count, booking_id), **ReleaseReservation** (booking_id), плюс **UpdateFlight** для инвалидации кеша. Для дат используется `google.protobuf.Timestamp`, для статусов - enum (FlightStatus, SeatReservationStatus). В комментариях зафиксированы gRPC-коды ошибок: NOT_FOUND, RESOURCE_EXHAUSTED, INVALID_ARGUMENT, FAILED_PRECONDITION, UNAUTHENTICATED. Код из proto генерируется в Dockerfile обоих сервисов через `grpc_tools.protoc`.

### 2. ER-диаграмма в 3NF

Схема в `docs/ER_DIAGRAM.md` в формате Mermaid. Flight Service (airport, airline, flight, seat_reservation) и Booking Service (booking). Ограничения целостности отражены в диаграмме и в миграциях: available_seats >= 0, total_seats и price > 0, уникальность пары (flight_number, departure_date), один booking_id на резервацию.

### 3. PostgreSQL и реализация обоих сервисов

Две отдельные БД в docker-compose: `postgres-booking` и `postgres-flight`. Миграции - Alembic, запускаются в `docker-entrypoint.sh` перед стартом приложения. Booking Service - FastAPI, REST по спецификации (поиск рейсов, рейс по id, создание/получение/список бронирований, отмена). Flight Service - gRPC-сервер на grpcio, реализация методов в `flight_service/app/servicer.py`.

### 4. Межсервисное взаимодействие по gRPC

Booking вызывает Flight при создании и отмене брони. Флоу создания: сначала **GetFlight** (получаем рейс и цену), затем **ReserveSeats** (атомарное резервирование мест с тем же booking_id, который потом станет id брони), затем считаем total_price = seat_count x price и пишем запись в свою БД со статусом CONFIRMED. При ошибке ReserveSeats бронирование не создаётся. При отмене вызывается **ReleaseReservation** по booking_id, затем статус брони ставится CANCELLED.

### 5. Транзакционная целостность

В Flight Service: резервирование - в одной транзакции уменьшаем available_seats и создаём запись в seat_reservation; отмена - в одной транзакции возвращаем места и обновляем статус резервации на RELEASED. Для устранения гонки при последнем месте используется **SELECT FOR UPDATE** по рейсу и по резервации по booking_id (в `servicer.py` — `with_for_update()`). В Booking Service: если после успешного ReserveSeats не удаётся сохранить бронирование в БД, делается компенсирующий вызов ReleaseReservation, чтобы не остаться в несогласованном состоянии.

### 6. Аутентификация межсервисных вызовов

Реализована проверка API Key в gRPC metadata. Booking передаёт ключ в metadata (заголовок `x-api-key`) при каждом вызове Flight Service. Flight проверяет его в **ApiKeyInterceptor** (`flight_service/app/interceptors.py`), который вешается на сервер до обработчиков; при отсутствии или неверном ключе возвращается gRPC-ошибка **UNAUTHENTICATED**. Ключ задаётся переменной окружения **FLIGHT_GRPC_API_KEY** в docker-compose для обоих сервисов.

### 7. Redis для кеширования

Redis поднимается в docker-compose (master, replica, sentinel - см. п.9). В Flight Service используется стратегия **Cache-Aside**: при GetFlight и SearchFlights сначала проверяем кеш; при промахе идём в БД и записываем результат в Redis с TTL 300 секунд. Ключи: `flight:{id}` для рейса, `search:{origin}:{destination}:{date}` (или `all` при отсутствии даты). У всех ключей TTL, бесконечного кеша нет. При изменении (ReserveSeats, ReleaseReservation, UpdateFlight) вызывается инвалидация: удаляются соответствующий flight-ключ и ключи поиска по этому маршруту. В логах пишутся cache hit и cache miss по ключу.

### 8. Retry при вызовах Flight Service

В Booking Service при вызове Flight используется retry: максимум 3 попытки с экспоненциальной задержкой 100, 200 и 400 мс. Повторяются только ошибки **UNAVAILABLE** и **DEADLINE_EXCEEDED**; для NOT_FOUND, INVALID_ARGUMENT, RESOURCE_EXHAUSTED retry не делается. Реализация - в `booking_service/app/flight_client.py` в методе `_call`. Для **ReserveSeats** обеспечена идемпотентность по booking_id: повторный вызов с тем же booking_id не создаёт второе бронирование и не списывает места дважды (в Flight Service при наличии активного бронирования по этому booking_id возвращается успех без изменений).

### 9. Redis в отказоустойчивой конфигурации

Реализован вариант с **Sentinel**: в docker-compose подняты redis-master, redis-replica и redis-sentinel с конфигом из `config/sentinel.conf`. Flight Service подключается к Redis через клиент с поддержкой Sentinel: `redis.sentinel.Sentinel` и `master_for("mymaster")` в `flight_service/app/cache.py`. Переменные окружения: REDIS_SENTINEL_HOSTS, REDIS_MASTER_NAME.

### 10. Circuit Breaker

Реализован в виде отдельного модуля и обёртки вокруг gRPC-клиента. Состояния: **CLOSED** (нормальная работа), **OPEN** (запросы к Flight не выполняются, сразу возвращается ошибка), **HALF_OPEN** (после таймаута пропускается один пробный запрос). При накоплении retriable-ошибок (UNAVAILABLE, DEADLINE_EXCEEDED после исчерпания retry) в скользящем окне circuit переходит в OPEN; после истечения таймаута — в HALF_OPEN; при успешном пробном запросе — обратно в CLOSED. Параметры задаются переменными окружения: CB_FAILURE_THRESHOLD, CB_OPEN_DURATION_SEC, CB_WINDOW_SEC. Переходы между состояниями логируются. В состоянии OPEN клиент получает HTTP 503 Service Unavailable.

## Примеры API

```bash
# Поиск рейсов (только SCHEDULED)
curl "http://localhost:8000/flights?origin=SVO&destination=LED&date=2026-04-01"

# Рейс по ID (из ответа поиска)
curl "http://localhost:8000/flights/<flight_id>"

# Бронирование
curl -X POST http://localhost:8000/bookings -H "Content-Type: application/json" \
  -d '{"user_id":"u1","flight_id":"<uuid>","passenger_name":"Ivan","passenger_email":"a@b.ru","seat_count":1}'

# Мои брони
curl "http://localhost:8000/bookings?user_id=u1"

# Отмена
curl -X POST http://localhost:8000/bookings/<booking_id>/cancel
```

## Проверка БД

```bash
docker exec -it hw3-postgres-booking-1 psql -U booking -d booking -c "SELECT * FROM booking;"
docker exec -it hw3-postgres-flight-1 psql -U flight -d flight -c "SELECT id, flight_number, available_seats FROM flight;"
docker exec -it hw3-postgres-flight-1 psql -U flight -d flight -c "SELECT * FROM seat_reservation;"
```

## Тесты

Из корня `hw3`:

```bash
cd booking_service && pip install -r requirements.txt && PYTHONPATH=. pytest tests/ -v
cd ../flight_service && pip install -r requirements.txt && PYTHONPATH=. pytest tests/ -v
```

При первом запуске в `tests/_proto_gen/` автоматически генерируется код из `../proto`.

## Генерация кода из .proto

```bash
PROTO_ROOT=$(python -c "import grpc_tools, os; print(os.path.join(os.path.dirname(grpc_tools.__file__), '_proto'))")
python -m grpc_tools.protoc -Iproto -I"$PROTO_ROOT" \
  --python_out=/tmp/gen --grpc_python_out=/tmp/gen proto/flight/v1/flight.proto
```

В Docker кодогенерация выполняется в Dockerfile обоих сервисов
