-- Smart Warehouse keyspace
-- 3-node cluster, NetworkTopologyStrategy, replication_factor = 3 (one replica per node).
-- Datacenter name = "datacenter1" (default for GossipingPropertyFileSnitch in cassandra image).

CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
} AND durable_writes = true;

USE warehouse;

-- Точечный лукап остатка товара в зоне.
-- Запросы:
--   SELECT * FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?;
-- Композитный partition key: (product_id, zone_id) даёт точечный доступ за один RPC.
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
  product_id    text,
  zone_id       text,
  available_quantity bigint,
  reserved_quantity  bigint,
  last_event_ts bigint,
  supplier_id   text,
  PRIMARY KEY ((product_id, zone_id))
);

-- Все остатки/зоны конкретного товара (агрегированные total + breakdown по зонам).
-- Запрос:
--   SELECT * FROM inventory_by_product WHERE product_id = ?;
-- Используется одна row-per-product, в которой поддерживаем total_available / total_reserved
-- (точная агрегированная цифра через всё количество зон).
-- Кроме того, для запроса разбиения "по всем зонам" используем clustering by zone_id.
-- Решение: храним агрегат и breakdown в одной таблице с zone_id как clustering key,
-- а агрегированные значения дублируем в отдельной "magic" zone_id = '__total__'.
CREATE TABLE IF NOT EXISTS inventory_by_product (
  product_id    text,
  zone_id       text,
  available_quantity bigint,
  reserved_quantity  bigint,
  total_available    bigint,
  total_reserved     bigint,
  last_event_ts bigint,
  PRIMARY KEY ((product_id), zone_id)
);

-- Все товары в конкретной зоне.
-- Запрос:
--   SELECT * FROM inventory_by_zone WHERE zone_id = ?;
CREATE TABLE IF NOT EXISTS inventory_by_zone (
  zone_id       text,
  product_id    text,
  available_quantity bigint,
  reserved_quantity  bigint,
  last_event_ts bigint,
  PRIMARY KEY ((zone_id), product_id)
);

-- Идемпотентность: храним обработанные event_id с TTL 30 дней.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id      text PRIMARY KEY,
  event_type    text,
  processed_at  timestamp,
  kafka_partition int,
  kafka_offset    bigint
) WITH default_time_to_live = 2592000;

-- Заказы.
CREATE TABLE IF NOT EXISTS orders (
  order_id   text PRIMARY KEY,
  status     text,
  items      list<frozen<tuple<text, text, bigint>>>,
  created_at timestamp,
  updated_at timestamp,
  last_event_ts bigint
);

-- Журнал всех событий (для аудита, опциональный).
-- Партицируется по дате (yyyy-mm-dd), кластеризуется по timestamp DESC + event_id.
CREATE TABLE IF NOT EXISTS events_log (
  bucket     text,
  event_ts   bigint,
  event_id   text,
  event_type text,
  payload    text,
  PRIMARY KEY ((bucket), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, event_id ASC);
