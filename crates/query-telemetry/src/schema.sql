CREATE TABLE IF NOT EXISTS query_failures
(
    event_id String,
    timestamp_unix_ms UInt64,
    received_at DateTime64(3) DEFAULT now64(3),
    node_id LowCardinality(String),
    namespace String,
    graph_id LowCardinality(String),
    cell_id LowCardinality(String),
    engine LowCardinality(String),
    stage LowCardinality(String),
    error_class LowCardinality(String),
    failure_reason LowCardinality(String),
    query String,
    elapsed_us UInt64,
    event String
)
ENGINE = ReplacingMergeTree
ORDER BY event_id
