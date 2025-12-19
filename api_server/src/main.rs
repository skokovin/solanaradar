use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

// External Crates
use clickhouse::Client;
use http::{HeaderName, Method};
use log::{info, error, warn};
use serde::Serialize;
use tonic::{transport::Server, Request, Response, Status};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{CorsLayer, Any};

// Internal Modules (Protobuf Definitions)
use proto_types::radar_api_server::{RadarApi, RadarApiServer};
use proto_types::{
    DashboardStatsResponse, DefiTrade, Empty, PlatformStat, PlatformStatsResponse,
    ServerStatus, TimeRangeRequest, TpsHistoryResponse, TpsPoint, TradeListResponse, TradeRequest,
};

// === Configuration ===
// TODO: In production, load secrets from Environment Variables or Vault.
const DB_URL: &str = "http://192.168.1.41:8123";
const DB_USER: &str = "default";
const DB_PASSWORD: &str = "";
const DB_DATABASE: &str = "solana_radar";
const SERVER_ADDR: &str = "0.0.0.0:50052";

// === Database Models (ClickHouse Row Mappings) ===

#[derive(clickhouse::Row, serde::Deserialize)]
struct DbStatRow {
    tps: u32,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct DbCountRow {
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct DbVolumeRow {
    total: f64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct DbTpsHistoryRow {
    ts: u32,
    tps: u32,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct DbPlatformRow {
    platform: String,
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct DbTradeRow {
    signature: String,
    slot: u64,
    block_time: i64,
    platform: String,
    market: String,
    token_in: String,
    token_out: String,
    amount_in: f64,
    amount_out: f64,
    signer: String,
    is_bot: u8,
}

/// Analytics Log Entry for internal monitoring
#[derive(Debug, Serialize, clickhouse::Row)]
struct ConnectionRow {
    client_ip: String,
    user_agent: String,
    connected_at: u32,
}

// === Service Implementation ===

pub struct RadarApiService {
    db: Client,
}

#[tonic::async_trait]
impl RadarApi for RadarApiService {

    // 1. Main Dashboard: TPS, Volume, Active Users, and Health Status
    async fn get_dashboard_stats(&self, req: Request<Empty>) -> Result<Response<DashboardStatsResponse>, Status> {
        let metadata = req.metadata().clone();

        // Extract IP (Support for Nginx/Cloudflare headers)
        let client_ip = match metadata.get("x-real-ip") {
            Some(ip) => ip.to_str().unwrap_or("unknown").to_string(),
            None => req.remote_addr().map(|a| a.ip().to_string()).unwrap_or("unknown".to_string()),
        };

        // Extract User-Agent
        let user_agent = match metadata.get("user-agent") {
            Some(ua) => ua.to_str().unwrap_or("unknown").to_string(),
            None => "unknown".to_string(),
        };

        // Async Background Logging ("Fire and Forget")
        let db_client = self.db.clone();
        let ip_db = client_ip.clone();
        let ua_db = user_agent.clone();

        tokio::spawn(async move {
            let row = ConnectionRow {
                client_ip: ip_db,
                user_agent: ua_db,
                connected_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,
            };

            // Insert into 'connection_logs'. Errors are ignored to not spam logs.
            if let Ok(mut insert) = db_client.insert::<ConnectionRow>("connection_logs").await {
                let _ = insert.write(&row).await;
                let _ = insert.end().await;
            }
        });

        // --- Execute Analytical Queries ---

        // A. Current TPS (Last Minute Avg)
        let tps_row = self.db
            .query("
                SELECT toUInt32(count() / 60) as tps
                FROM transactions
                WHERE block_time >= now() - INTERVAL 1 MINUTE
            ")
            .fetch_optional::<DbStatRow>()
            .await
            .map_err(|e| Status::internal(format!("TPS Query Error: {}", e)))?
            .unwrap_or(DbStatRow { tps: 0 });

        // B. Total Volume (Last 24h)
        let volume_row = self.db
            .query("
                SELECT sum(amount_in) as total
                FROM defi_trades
                WHERE block_time >= now() - INTERVAL 24 HOUR
            ")
            .fetch_optional::<DbVolumeRow>()
            .await
            .map_err(|e| Status::internal(format!("Volume Query Error: {}", e)))?
            .unwrap_or(DbVolumeRow { total: 0.0 });

        // C. Active Wallets (Last 24h using HyperLogLog)
        let wallets_row = self.db
            .query("
                SELECT uniq(signer) as count
                FROM defi_trades
                WHERE block_time >= now() - INTERVAL 24 HOUR
            ")
            .fetch_optional::<DbCountRow>()
            .await
            .map_err(|e| Status::internal(format!("Wallets Query Error: {}", e)))?
            .unwrap_or(DbCountRow { count: 0 });
        let volume_in_sol = volume_row.total / 1_000_000_000.0;
        Ok(Response::new(DashboardStatsResponse {
            current_tps: tps_row.tps,
            total_volume_24h: volume_in_sol as u64,
            active_wallets_24h: wallets_row.count,
            status: Some(ServerStatus {
                is_healthy: true,
                db_latency: "4ms".to_string(), // Placeholder for real latency check
            }),
        }))
    }

    // 2. Recent Trades Feed
    async fn get_recent_trades(&self, req: Request<TradeRequest>) -> Result<Response<TradeListResponse>, Status> {
        let r = req.into_inner();
        let limit = if r.limit > 0 && r.limit <= 100 { r.limit } else { 50 };

        // Dynamic query building
        let trades_vec = if r.platform.is_empty() {
            self.db.query("SELECT * FROM defi_trades ORDER BY block_time DESC LIMIT ?")
                .bind(limit)
                .fetch_all::<DbTradeRow>().await
        } else {
            self.db.query("SELECT * FROM defi_trades WHERE platform = ? ORDER BY block_time DESC LIMIT ?")
                .bind(&r.platform)
                .bind(limit)
                .fetch_all::<DbTradeRow>().await
        };

        match trades_vec {
            Ok(rows) => {
                let api_trades = rows.into_iter().map(|row| DefiTrade {
                    signature: row.signature,
                    slot: row.slot,
                    block_time: row.block_time as u64,
                    platform: row.platform,
                    token_in: row.token_in,
                    token_out: row.token_out,
                    amount_in: row.amount_in,
                    amount_out: row.amount_out,
                    signer: row.signer,
                    is_bot: row.is_bot == 1,
                }).collect();
                Ok(Response::new(TradeListResponse { trades: api_trades }))
            },
            Err(e) => {
                error!("ClickHouse Error (Recent Trades): {}", e);
                Err(Status::internal("Database query failed"))
            }
        }
    }

    // 3. Historical TPS Chart Data
    async fn get_tps_history(&self, _req: Request<TimeRangeRequest>) -> Result<Response<TpsHistoryResponse>, Status> {
        // Hardcoded to last 30 minutes, grouped by minute
        let query = "
            SELECT
                toUnixTimestamp(toStartOfMinute(block_time)) as ts,
                toUInt32(count() / 60) as tps
            FROM transactions
            WHERE block_time >= now() - INTERVAL 30 MINUTE
            GROUP BY ts
            ORDER BY ts ASC
        ";

        let rows_vec = self.db
            .query(query)
            .fetch_all::<DbTpsHistoryRow>()
            .await
            .map_err(|e| Status::internal(format!("History DB Error: {}", e)))?;

        let points = rows_vec.into_iter().map(|row| TpsPoint {
            timestamp: row.ts as u64 * 1000, // Convert to JS ms
            tps: row.tps,
        }).collect();

        Ok(Response::new(TpsHistoryResponse { points }))
    }

    // 4. Platform Distribution Stats
    async fn get_platform_stats(&self, _req: Request<TimeRangeRequest>) -> Result<Response<PlatformStatsResponse>, Status> {
        let query = "
            SELECT
                platform,
                count() as count
            FROM defi_trades
            WHERE block_time >= now() - INTERVAL 1 HOUR
            GROUP BY platform
            ORDER BY count DESC
        ";

        let rows = self.db
            .query(query)
            .fetch_all::<DbPlatformRow>()
            .await
            .map_err(|e| Status::internal(format!("Platform DB Error: {}", e)))?;

        let stats_proto = rows.into_iter().map(|row| PlatformStat {
            name: row.platform,
            tx_count: row.count,
            percentage: 0.0, // Calculated on Frontend
        }).collect();

        Ok(Response::new(PlatformStatsResponse { stats: stats_proto }))
    }
}

// === Main Entry Point ===

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize Logger
    env_logger::init();

    // 1. Initialize ClickHouse Client
    let db = Client::default()
        .with_url(DB_URL)
        .with_database(DB_DATABASE)
        .with_user(DB_USER)
        .with_password(DB_PASSWORD);

    // Health Check
    match db.query("SELECT 1").fetch_one::<u8>().await {
        Ok(_) => println!("✅ API Server connected to ClickHouse at {}", DB_URL),
        Err(e) => {
            println!("❌ Failed to connect to ClickHouse: {}", e);
            return Err(e.into());
        }
    }

    let service = RadarApiService { db };
    let addr: SocketAddr = SERVER_ADDR.parse()?;

    println!("🚀 API Server listening on {} (gRPC-Web enabled)", addr);

    // 2. Setup CORS (Essential for Angular/Browser access)
    let cors = CorsLayer::new()
        .allow_origin(Any) // Allow all origins (Dev Mode)
        .allow_headers(Any)
        .allow_methods(vec![Method::POST, Method::GET, Method::OPTIONS])
        .expose_headers(vec![
            HeaderName::from_static("grpc-status"),
            HeaderName::from_static("grpc-message"),
            HeaderName::from_static("grpc-status-details-bin"),
            HeaderName::from_static("x-grpc-web"),
        ]);

    // 3. Start gRPC Server with Web Layer
    Server::builder()
        .accept_http1(true) // Required for gRPC-Web
        .layer(cors)
        .layer(GrpcWebLayer::new()) // Translation layer
        .add_service(RadarApiServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}