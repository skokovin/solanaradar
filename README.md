# 📡 Solana Radar: Backend Infrastructure

![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)
![ClickHouse](https://img.shields.io/badge/clickhouse-23.8%2B-yellow.svg)
![gRPC](https://img.shields.io/badge/transport-gRPC-blue.svg)
![Architecture](https://img.shields.io/badge/architecture-microservices-purple.svg)

**Solana Radar Backend** is the high-performance data processing core of the ecosystem. Organized as a Rust Workspace, it handles the ingestion, storage, and distribution of real-time blockchain metrics.

> **🔗 Integration Note:** This infrastructure is the downstream consumer for the **[Solana Geyser Plugin (skokovin/gp1)](https://github.com/skokovin/gp1)**. It accepts the raw stream directly from the validator's runtime.

---

## 🏗 System Architecture

The backend is split into two specialized microservices to separate **Write** (High Throughput) and **Read** (Low Latency) loads:

### [cite_start]1. Ingestor Service (`/ingestor`) — The Writer [cite: 3]
* **Role:** High-throughput gRPC Server.
* **Logic:** Accepts persistent streams, buffers data, performs "Smart Parsing" (Swap detection), and executes async batch inserts into ClickHouse.
* **Performance:** Capable of handling **5,000+ TPS** bursts with non-blocking I/O.

### [cite_start]2. API Server (`/api_server`) — The Reader [cite: 13]
* **Role:** Public Gateway for the Frontend (Angular).
* **Protocol:** Implements **gRPC-Web**, enabling browser clients to connect directly without a proxy.
* **Logic:** Queries pre-aggregated data from ClickHouse to serve real-time dashboards (TPS, Volume, Active Users).

---

## 💾 Database Schema & Optimization (ClickHouse)

We use **ClickHouse** (OLAP) instead of Postgres because traditional row-based DBs cannot sustain Solana's 400ms block time write load while serving analytics.

### Key Engineering Decisions:

1.  [cite_start]**Materialized Views (Auto-Aggregation):** [cite: 12, 13, 14, 15]
    * We don't calculate "Total Volume" by scanning millions of rows on every API request.
    * Instead, a `SummingMergeTree` engine aggregates data in the background as it is inserted. API queries are instant (O(1)).

2.  [cite_start]**Data Tiering (TTL):** [cite: 3, 5, 17, 18]
    * **Raw Transactions:** Stored for **24 hours** (Debug/Deep dive only). [cite_start]Compressed with `ZSTD` to save 80% disk space[cite: 5].
    * **Parsed Trades:** Stored permanently (Business value).
    * [cite_start]**Telemetry:** Stored for **3 days** to monitor node health[cite: 17, 18].

3.  [cite_start]**Dictionary Encoding:** [cite: 8]
    * Columns like `platform` (Raydium, Orca) use `LowCardinality(String)` to minimize storage footprint and speed up filtering.

---

## 💻 Code Implementation

### Swap Detection Logic (Ingestor)
The ingestor doesn't just blindly save data. It analyzes balance changes (`pre_token_balances` vs `post_token_balances`) to identify DeFi swaps:

```rust
// Logic: Identify Token IN/OUT based on Balance Changes (Delta)
// - Delta < 0: Token Sold (Sent from wallet)
// - Delta > 0: Token Bought (Received to wallet)
if delta < -0.000001 {
    token_sold = Some((mint, delta.abs()));
} else if delta > 0.000001 {
    token_bought = Some((mint, delta));
}