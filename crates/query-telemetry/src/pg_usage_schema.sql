CREATE TABLE IF NOT EXISTS pg_usage_reports
(
    report_id String,
    received_at DateTime64(3) DEFAULT now64(3),
    sent_at_unix_ms UInt64,
    install_id String,
    server_started_at_unix_ms UInt64,
    extension_version LowCardinality(String),
    postgres_major UInt8,
    payload String
)
ENGINE = ReplacingMergeTree(sent_at_unix_ms)
ORDER BY report_id
TTL toDateTime(received_at) + INTERVAL 2 YEAR
