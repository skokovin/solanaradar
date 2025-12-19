-- =========================================================
-- 📡 Solana Radar: ClickHouse Schema Definition
-- Optimized for High-Throughput Blockchain Data
-- =========================================================

-- 1. Initialize Database
CREATE DATABASE IF NOT EXISTS solana_radar;
USE solana_radar;

-- =========================================================
-- TABLE 1: RAW TRANSACTIONS (Data Lake)
-- Stores full transaction logs. High write volume.
-- Retention: 24 Hours (Debug & Deep Dive only)
-- =========================================================
DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions
(
    `signature` String,
    `slot` UInt64,
    `block_time` DateTime64(3),     -- Millisecond precision
    `is_error` UInt8,               -- 0 = Success, 1 = Error
    `fee_lamports` UInt64,
    `compute_units` UInt32,         -- Resource consumption
    `program_ids` Array(String),    -- Invoked programs (used for filtering)
    `log_messages` Array(String) CODEC(ZSTD(1))
    -- OPTIMIZATION: Logs are verbose, ZSTD compression reduces disk usage by ~80%
)
    ENGINE = MergeTree
PARTITION BY toYYYYMMDD(block_time) -- Daily partitioning for efficient dropping
ORDER BY (slot, block_time)         -- Primary Key optimization
TTL block_time + INTERVAL 24 HOUR;  -- Automatic cleanup of old data

-- =========================================================
-- TABLE 2: DEFI TRADES (Business Data)
-- Stores only parsed swaps. "Clean" data for analytics.
-- Retention: Long-term (e.g., Months/Years)
-- =========================================================
DROP TABLE IF EXISTS defi_trades;

CREATE TABLE defi_trades
(
    `block_time` DateTime64(3),
    `slot` UInt64,
    `signature` String,
    `platform` LowCardinality(String), -- OPTIMIZATION: Dictionary encoding for repetitive strings (Raydium, Orca)
    `market` String,                   -- Liquidity Pool Address
    `signer` String,                   -- Wallet Address (Trader)
    `token_in` String,                 -- Mint Address (Sold)
    `token_out` String,                -- Mint Address (Bought)
    `amount_in` Float64,
    `amount_out` Float64,
    `is_bot` UInt8                     -- Bot detection flag
)
    ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)      -- Monthly partitioning
ORDER BY (platform, block_time);       -- Optimized for "Select * from ... where platform = 'Raydium'"

-- =========================================================
-- TABLE 3: AGGREGATED VOLUMES (Real-time Analytics)
-- Engine: SummingMergeTree
-- ClickHouse automatically aggregates data in the background.
-- API reads from here instantly (O(1) complexity) without scanning raw rows.
-- =========================================================
DROP TABLE IF EXISTS stats_volume_1m;

CREATE TABLE stats_volume_1m
(
    `minute` DateTime,
    `platform` LowCardinality(String),
    `tx_count` UInt64,        -- Aggregated count
    `volume_token_in` Float64 -- Aggregated volume
)
    ENGINE = SummingMergeTree
ORDER BY (minute, platform);

-- TRIGGER (Materialized View)
-- Automatically feeds the aggregation table as data arrives in `defi_trades`
DROP VIEW IF EXISTS mv_stats_volume_1m;

CREATE MATERIALIZED VIEW mv_stats_volume_1m TO stats_volume_1m
AS SELECT
              toStartOfMinute(block_time) as minute,
    platform,
    count() as tx_count,
    sum(amount_in) as volume_token_in
   FROM defi_trades
   GROUP BY minute, platform;

-- =========================================================
-- TABLE 4: NODE HEALTH (Infrastructure Monitoring)
-- Telemetry: CPU Temp, RAM, Disk I/O
-- =========================================================
DROP TABLE IF EXISTS node_health;

CREATE TABLE node_health
(
    `wall_time` DateTime64(3),
    `cpu_temp` Float32,
    `memory_used` UInt64,
    `disk_free` UInt64,
    `tps_observed` UInt32,
    `ingest_latency_ms` UInt32 -- Network latency tracking
)
    ENGINE = MergeTree
ORDER BY wall_time
TTL wall_time + INTERVAL 3 DAY; -- Keep telemetry for 72 hours