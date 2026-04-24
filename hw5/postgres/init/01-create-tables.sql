-- Generic metric store (idempotent on metric_date + metric_name + bucket_key)
CREATE TABLE IF NOT EXISTS metrics (
    metric_date  DATE         NOT NULL,
    metric_name  TEXT         NOT NULL,
    bucket_key   TEXT         NOT NULL DEFAULT '',
    value        DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, metric_name, bucket_key)
);

CREATE INDEX IF NOT EXISTS metrics_name_date_idx ON metrics(metric_name, metric_date);

-- Retention matrix (cohort_date x day_offset)
CREATE TABLE IF NOT EXISTS retention_matrix (
    cohort_date DATE NOT NULL,
    day_offset  INT  NOT NULL,
    cohort_size BIGINT NOT NULL,
    returned    BIGINT NOT NULL,
    retention   DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cohort_date, day_offset)
);
