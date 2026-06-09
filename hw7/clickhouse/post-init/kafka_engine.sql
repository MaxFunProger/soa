-- Kafka engine + MaterializedView. Применяется отдельным
-- sidecar'ом после того, как producer уже создал topic
-- movie-events и зарегистрировал Avro-схему в Schema Registry.

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
