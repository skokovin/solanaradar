use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

// External Crates
use clickhouse::Client;
use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

// Internal Modules
use proto_types::{BatchTransaction, BatchUpdate, Empty, NodeHealth, TransactionInfo};
use proto_types::geyser_stats_server::{GeyserStats, GeyserStatsServer};

// === Configuration ===
// TODO: Load these from config.json or Environment Variables in production
const DB_URL: &str = "http://192.168.1.41:8123";
const DB_USER: &str = "default";
const DB_PASSWORD: &str = "";
const DB_DATABASE: &str = "solana_radar";
const LISTEN_ADDR: &str = "0.0.0.0:50051";

// Performance Tuning
const BATCH_SIZE: usize = 5_000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

// === DEX Program IDs (Mainnet) ===
const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const PUMP_FUN: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

// === Data Models (ClickHouse Mapping) ===

#[derive(clickhouse::Row, serde::Serialize)]
struct DbTransaction {
    signature: String,
    slot: u64,
    block_time: i64,
    is_error: u8,
    fee_lamports: u64,
    compute_units: u32,
    program_ids: Vec<String>,
    log_messages: Vec<String>,
}

#[derive(clickhouse::Row, serde::Serialize)]
struct DbNodeHealth {
    wall_time: i64,
    cpu_temp: f32,
    memory_used: u64,
    disk_free: u64,
    tps_observed: u32,
    ingest_latency_ms: u32,
}

#[derive(clickhouse::Row, serde::Serialize)]
struct DbDefiTrade {
    block_time: i64,
    slot: u64,
    signature: String,
    platform: String,
    market: String,
    signer: String,
    token_in: String,
    token_out: String,
    amount_in: f64,
    amount_out: f64,
    is_bot: u8,
}

// Unified enum to handle different data types in a single channel
enum IngestData {
    Raw(DbTransaction),
    Health(DbNodeHealth),
    Trade(DbDefiTrade),
}

// === gRPC Service Implementation ===

pub struct IngestService {
    tx_sender: mpsc::Sender<IngestData>,
}

#[tonic::async_trait]
impl GeyserStats for IngestService {

    // Handles Account Updates (Not stored in this MVP to save disk space)
    async fn send_updates(&self, _request: Request<BatchUpdate>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    // Handles Transactions Stream
    async fn send_transactions(&self, request: Request<BatchTransaction>) -> Result<Response<Empty>, Status> {
        let batch = request.into_inner();
        let wall_time = batch.wall_time as i64 * 1000;

        for tx in batch.transactions {
            let signature = bs58::encode(&tx.signature).into_string();
            let log_messages = tx.log_messages.clone();

            // Convert raw bytes to Base58 strings for analysis
            let account_keys_str: Vec<String> = tx.account_keys.iter()
                .map(|k| bs58::encode(k).into_string())
                .collect();

            // 1. Store RAW Transaction Data
            let db_row = DbTransaction {
                signature: signature.clone(),
                slot: 0, // Slot would be passed here in full implementation
                block_time: wall_time,
                is_error: if tx.err { 1 } else { 0 },
                fee_lamports: tx.fee,
                compute_units: tx.compute_units_consumed as u32,
                program_ids: account_keys_str.clone(),
                log_messages: log_messages.clone(),
            };

            // Non-blocking send to worker
            let _ = self.tx_sender.send(IngestData::Raw(db_row)).await;

            // 2. Intelligent DeFi Parser (Swap Detection)
            if !tx.err {
                if let Some(platform_name) = identify_platform(&account_keys_str) {

                    // Logic: Identify Token IN/OUT based on Balance Changes (Delta)
                    // - Delta < 0: Token Sold (Sent from wallet)
                    // - Delta > 0: Token Bought (Received to wallet)

                    let mut token_sold: Option<(String, f64)> = None;
                    let mut token_bought: Option<(String, f64)> = None;

                    // Map: (Owner, Mint) -> Change Amount
                    let mut changes: HashMap<(String, String), f64> = HashMap::new();

                    for b in &tx.pre_balances {
                        *changes.entry((b.owner.clone(), b.mint.clone())).or_default() -= b.ui_token_amount;
                    }
                    for b in &tx.post_balances {
                        *changes.entry((b.owner.clone(), b.mint.clone())).or_default() += b.ui_token_amount;
                    }

                    // Identify the Signer (User) - usually the first account
                    //let signer = account_keys_str.first().cloned().unwrap_or_default();
                    let signer = if let Some(addr) = account_keys_str.first() {
                        addr.clone()
                    } else {
                        "Unknown".to_string()
                    };

                    // Filter changes strictly for the signer to exclude DEX vault movements
                    for ((owner, mint), delta) in changes {
                        if owner == signer {
                            // Threshold 1e-6 to ignore dust/rent adjustments
                            if delta < -0.000001 {
                                token_sold = Some((mint, delta.abs()));
                            } else if delta > 0.000001 {
                                token_bought = Some((mint, delta));
                            }
                        }
                    }

                    // If we found both a Sell and a Buy action -> It's a Valid Swap
                    if let (Some((mint_in, raw_amt_in)), Some((mint_out, raw_amt_out))) = (token_sold, token_bought) {
                        let (sym_in, decimals_in) = get_token_metadata(&mint_in);
                        let (sym_out, decimals_out) = get_token_metadata(&mint_out);
                        let amount_in_real = raw_amt_in / 10f64.powi(decimals_in);
                        let amount_out_real = raw_amt_out / 10f64.powi(decimals_out);

                        let trade = DbDefiTrade {
                            block_time: wall_time,
                            slot: 0,
                            signature: signature.clone(),
                            platform: platform_name,
                            market: "RealData".to_string(), // Flag indicating verified parsing
                            signer: signer.clone(),
                            token_in: sym_in,
                            token_out: sym_out,
                            amount_in: amount_in_real,
                            amount_out: amount_out_real,
                            is_bot: 0, // Placeholder for bot detection logic
                        };

                        let _ = self.tx_sender.send(IngestData::Trade(trade)).await;
                    }
                }
            }
        }

        Ok(Response::new(Empty {}))
    }

    // Handles Node Health Telemetry
    async fn send_health(&self, request: Request<NodeHealth>) -> Result<Response<Empty>, Status> {
        let health = request.into_inner();
        info!("❤️ Node Health: {:.1}°C | Mem: {}MB", health.cpu_temp, health.memory_used / 1024 / 1024);

        let db_health = DbNodeHealth {
            wall_time: health.wall_time as i64 * 1000,
            cpu_temp: health.cpu_temp,
            memory_used: health.memory_used,
            disk_free: health.disk_ledger_free,
            tps_observed: health.cpu_load as u32,
            ingest_latency_ms: 6, // Hardcoded network latency baseline
        };

        let _ = self.tx_sender.send(IngestData::Health(db_health)).await;
        Ok(Response::new(Empty {}))
    }

    type SubscribeTransactionsStream = Pin<Box<dyn Stream<Item = Result<TransactionInfo, Status>> + Send + 'static>>;
    async fn subscribe_transactions(&self, _req: Request<Empty>) -> Result<Response<Self::SubscribeTransactionsStream>, Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

// === Background Worker (Batch Processor) ===

async fn spawn_processor(mut rx: mpsc::Receiver<IngestData>, db: Client) {
    let mut buf_txs = Vec::with_capacity(BATCH_SIZE);
    let mut buf_trades = Vec::with_capacity(BATCH_SIZE);
    let mut buf_health = Vec::with_capacity(100);

    let mut interval = tokio::time::interval(FLUSH_INTERVAL);

    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                match msg {
                    IngestData::Raw(row) => buf_txs.push(row),
                    IngestData::Trade(row) => buf_trades.push(row),
                    IngestData::Health(row) => buf_health.push(row),
                }

                // Optimization: Flush immediately if buffer is full
                if buf_txs.len() >= BATCH_SIZE {
                    flush_all(&db, &mut buf_txs, &mut buf_trades, &mut buf_health).await;
                }
            }
            _ = interval.tick() => {
                // Time-based flush to ensure low latency for dashboards
                flush_all(&db, &mut buf_txs, &mut buf_trades, &mut buf_health).await;
            }
        }
    }
}

async fn flush_all(
    db: &Client,
    txs: &mut Vec<DbTransaction>,
    trades: &mut Vec<DbDefiTrade>,
    health: &mut Vec<DbNodeHealth>
) {
    // 1. Bulk Insert Transactions
    if !txs.is_empty() {
        if let Ok(mut insert) = db.insert::<DbTransaction>("transactions").await {
            for row in txs.iter() { let _ = insert.write(row).await; }
            let _ = insert.end().await;
        }
        txs.clear();
    }

    // 2. Bulk Insert Trades
    if !trades.is_empty() {
        info!("📈 Flushing {} parsed trades to DB...", trades.len());
        if let Ok(mut insert) = db.insert::<DbDefiTrade>("defi_trades").await {
            for row in trades.iter() { let _ = insert.write(row).await; }
            match insert.end().await {
                Ok(_) => {},
                Err(e) => error!("Failed to write trades: {}", e),
            }
        }
        trades.clear();
    }

    // 3. Bulk Insert Health Metrics
    if !health.is_empty() {
        if let Ok(mut insert) = db.insert::<DbNodeHealth>("node_health").await {
            for row in health.iter() { let _ = insert.write(row).await; }
            let _ = insert.end().await;
        }
        health.clear();
    }
}

// === Helper Functions ===

fn identify_platform(account_keys: &[String]) -> Option<String> {
    for key in account_keys {
        if key == RAYDIUM_V4 { return Some("Raydium".to_string()); }
        if key == ORCA_WHIRLPOOL { return Some("Orca".to_string()); }
        if key == JUPITER_V6 { return Some("Jupiter".to_string()); }
        if key == PUMP_FUN { return Some("Pump.fun".to_string()); }
        if key == METEORA_DLMM { return Some("Meteora".to_string()); }
    }
    None
}

/// Simple Token Map for demonstration.
/// In production, this would query a Token Registry or Metadata Metaplex.
fn get_token_metadata(mint: &str) -> (String, i32) {
    match mint {
        // Native SOL always 9 decimals
        "So11111111111111111111111111111111111111112" => ("SOL".to_string(), 9),
        // Stablecoins (usually 6)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => ("USDC".to_string(), 6),
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => ("USDT".to_string(), 6),
        // Most SPL tokens (Bonk, Wif, Jup) are usually 6 or 9. Check Solscan!
        "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN" => ("JUP".to_string(), 6),
        "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263" => ("BONK".to_string(), 5), // BONK is weird! It has 5
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So" => ("mSOL".to_string(), 9),
        // Fallback for unknown tokens
        other => {
            let short_name = if other.len() > 4 { format!("{}..", &other[..4]) } else { "UNK".to_string() };
            // SAFETY: По умолчанию считаем 6, но для точности лучше бы иметь API
            (short_name, 6)
        }
    }
}
/*fn get_token_symbol(mint: &str) -> String {
    match mint {
        "So11111111111111111111111111111111111111112" => "SOL".to_string(),
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => "USDC".to_string(),
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => "USDT".to_string(),
        "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN" => "JUP".to_string(),
        "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263" => "BONK".to_string(),
        "HzwqbKZw8RnJC2SHW327Wt5rufWNmD7QSAv77uyTileX" => "PYTH".to_string(),
        "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm" => "WIF".to_string(),
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So" => "mSOL".to_string(),
        "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs" => "ETH".to_string(),
        other => {
            // Return shortened address for unknown tokens
            if other.len() > 4 {
                format!("{}..", &other[..4])
            } else {
                other.to_string()
            }
        }
    }
}
*/
// === Main Entry Point ===

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // 1. Establish Database Connection
    let db = Client::default()
        .with_url(DB_URL)
        .with_database(DB_DATABASE)
        .with_user(DB_USER)
        .with_password(DB_PASSWORD);

    // Health Check
    match db.query("SELECT 1").fetch_one::<u8>().await {
        Ok(_) => info!("✅ Ingestor connected to ClickHouse at {}", DB_URL),
        Err(e) => {
            error!("❌ Failed to connect to ClickHouse: {}", e);
            return Err(e.into());
        }
    }

    // 2. Start Background Processor (Producer-Consumer pattern)
    // Channel size is large to handle traffic spikes during batch flushes
    let (tx, rx) = mpsc::channel(100_000);
    tokio::spawn(spawn_processor(rx, db.clone()));

    // 3. Start gRPC Server
    let addr = LISTEN_ADDR.parse()?;
    let service = IngestService { tx_sender: tx };

    info!("🚀 Ingestor Service listening on {}", addr);
    Server::builder()
        .add_service(GeyserStatsServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}