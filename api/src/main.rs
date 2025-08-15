use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use simplelog::{CombinedLogger, LevelFilter, TermLogger, TerminalMode};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use std::{env::var, net::SocketAddr, str::FromStr, time::Duration};
use tokio::sync::mpsc::{self, UnboundedSender};
use uuid::Uuid;

use crate::processor::{Payment, Processor};

mod processor;
mod repository;

#[derive(Deserialize, Clone)]
pub struct ProcessorConfig {
    name: String,
    endpoint: String,
}

#[derive(Deserialize, Clone)]
pub struct Config {
    listen_port: u16,
    database_url: String,
    log_level: String,
    processors: Vec<ProcessorConfig>,
}

#[derive(Clone)]
pub struct App {
    pub targets: Vec<ProcessorConfig>,
    pub db: Pool<Postgres>,
    pub client: Client,
    pub sender: UnboundedSender<Payment>,
}

#[tokio::main]
async fn main() {
    let config = var("APP_CONFIG").unwrap();
    let config: Config = serde_json::from_str(&config).unwrap();

    let log_level = LevelFilter::from_str(&config.log_level).unwrap_or_else(|_| LevelFilter::Info);
    CombinedLogger::init(vec![TermLogger::new(
        log_level,
        simplelog::Config::default(),
        TerminalMode::Mixed,
        simplelog::ColorChoice::Always,
    )])
    .unwrap();

    let db = PgPoolOptions::new()
        .max_connections(32)
        .connect(&config.database_url)
        .await
        .unwrap();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(2))
        .pool_max_idle_per_host(500)
        .build()
        .unwrap();

    let (sender, receiver) = mpsc::unbounded_channel::<processor::Payment>();

    let state = App {
        db,
        targets: config.processors,
        client,
        sender,
    };

    let mut processor = Processor {
        state: state.clone(),
        receiver,
    };

    tokio::spawn(async move { processor.run_forever().await });

    let addr = SocketAddr::new([0, 0, 0, 0].into(), config.listen_port);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    let app = Router::new()
        .route("/payments", post(new_payment))
        .route("/payments-summary", get(summary))
        .with_state(state);

    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostPaymentDto {
    pub correlation_id: Uuid,
    pub amount: f64,
}

async fn new_payment(
    State(state): State<App>,
    Json(dto): Json<PostPaymentDto>,
) -> impl IntoResponse {
    let now = Utc::now();

    match state.sender.send(Payment {
        correlation_id: dto.correlation_id,
        amount: dto.amount,
        requested_at: now,
    }) {
        Ok(_) => StatusCode::CREATED,
        Err(error) => {
            log::error!("failed submitting to internal processor, {}", error);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[derive(Deserialize)]
struct SummaryParams {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

async fn summary(
    Query(window): Query<SummaryParams>,
    State(state): State<App>,
) -> impl IntoResponse {
    let from = window
        .from
        .unwrap_or_else(|| DateTime::<Utc>::from_str("0000-01-01T00:00:00.000Z").unwrap());
    let to = window
        .to
        .unwrap_or_else(|| DateTime::<Utc>::from_str("9999-12-31T23:59:59.999Z").unwrap());

    let processors = state.targets.iter().map(|p| p.name.clone()).collect();

    match repository::summary(&state.db, &processors, from, to).await {
        Ok(summary) => Ok((StatusCode::OK, Json(summary))),
        Err(error) => {
            log::error!("failed fetching summary, {}", error);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
