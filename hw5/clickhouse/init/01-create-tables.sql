-- Database for raw events and aggregates
CREATE DATABASE IF NOT EXISTS cinema;

-- Raw events: Kafka Engine -> Materialized View -> MergeTree
-- Schema mirrors the Avro contract (schemas/movie_event.avsc)
CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
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
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka-1:29092,kafka-2:29092',
    kafka_topic_list         = 'movie-events',
    kafka_group_name         = 'clickhouse-movie-events',
    kafka_format             = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers      = 2,
    kafka_thread_per_consumer = 1;

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

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds
FROM cinema.movie_events_kafka;

-- Aggregate storage in ClickHouse
-- ReplacingMergeTree with computed_at as version to keep only latest
-- aggregate for a (metric_date, bucket) tuple
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
