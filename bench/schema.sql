CREATE DATABASE IF NOT EXISTS bench;
DROP TABLE IF EXISTS bench.events;
CREATE TABLE bench.events
(
    event_id       UInt64,
    ts             DateTime64(3),
    user_id        UInt64,
    session_id     UInt64,
    game_id        LowCardinality(String),
    event          LowCardinality(String),
    platform       LowCardinality(String),
    country        LowCardinality(String),
    level          UInt16,
    session_ms     UInt32,
    revenue_cents  UInt64,
    is_payer       UInt8,
    props          String,
    event_hour     UInt8,
    is_revenue     Bool
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (game_id, event, ts);
