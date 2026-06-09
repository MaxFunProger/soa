-- Базовая схема: целевая MergeTree для событий + агрегатные таблицы.
-- Kafka engine + MaterializedView создаются отдельным сервисом
-- clickhouse-kafka-init после того, как producer уже создал topic
-- и зарегистрировал Avro-схему в Schema Registry.
-- Это нужно, потому что CREATE TABLE ... ENGINE = Kafka при init
-- блокирует открытие порта 8123 на retry'ях UNKNOWN_TOPIC_OR_PARTITION.

CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       Enum8(
        'VIEW_STARTED'  = 1,
        'VIEW_FINISHED' = 2,
        'VIEW_PAUSED'   = 3,
        'VIEW_RESUMED'  = 4,
        'LIKED'         = 5,
        'SEARCHED'      = 6),
    timestamp        DateTime64(3, 'UTC'),
    event_date       Date MATERIALIZED toDate(timestamp),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_date, user_id, timestamp)
TTL toDateTime(timestamp) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS cinema.agg_dau
(
    metric_date Date,
    dau         UInt64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.agg_avg_view_time
(
    metric_date Date,
    avg_seconds Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.agg_top_movies
(
    metric_date Date,
    movie_id    String,
    views       UInt64,
    rank        UInt32,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, movie_id);

CREATE TABLE IF NOT EXISTS cinema.agg_conversion
(
    metric_date Date,
    views_started  UInt64,
    views_finished UInt64,
    conversion     Float64,
    computed_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.agg_retention
(
    cohort_date Date,
    day_offset  UInt8,
    cohort_size UInt64,
    returned    UInt64,
    retention   Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_offset);
