use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use simplelog::{CombinedLogger, LevelFilter, TermLogger, TerminalMode};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions, types::Decimal};
use std::{env::var, net::SocketAddr, str::FromStr, sync::OnceLock, time::Duration};
use tokio::sync::mpsc::{self, UnboundedSender};
use uuid::Uuid;

use crate::processor::Processor;

mod processor;
mod repository;

#[derive(Deserialize, Clone, Debug)]
pub struct ProcessorConfig {
    pub name: String,
    pub endpoint: String,
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub listen_port: u16,
    pub database_url: String,
    pub log_level: String,
    pub max_in_flight: usize,
    pub max_wait_millis: usize,
    pub external_processors: Vec<ProcessorConfig>,
}

pub static DB: OnceLock<Pool<Postgres>> = OnceLock::new();
pub static EXTERNAL_PROCESSORS: OnceLock<Vec<ProcessorConfig>> = OnceLock::new();
pub static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

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

    DB.set(
        PgPoolOptions::new()
            .max_connections(32)
            .connect(&config.database_url)
            .await
            .unwrap(),
    )
    .unwrap();

    EXTERNAL_PROCESSORS.set(config.external_processors).unwrap();

    HTTP_CLIENT
        .set(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .connect_timeout(Duration::from_secs(2))
                .pool_max_idle_per_host(500)
                .build()
                .unwrap(),
        )
        .unwrap();

    let (sender, receiver) = mpsc::unbounded_channel::<PostPaymentDto>();

    tokio::spawn(async move {
        let mut processor = Processor {
            receiver,
            max_in_flight: config.max_in_flight,
            max_wait_millis: config.max_wait_millis,
        };

        processor.run_forever().await
    });

    let addr = SocketAddr::new([0, 0, 0, 0].into(), config.listen_port);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    let app = Router::new()
        .route("/payments", post(new_payment))
        .route("/payments-summary", get(summary))
        .with_state(sender);

    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostPaymentDto {
    pub correlation_id: Uuid,
    pub amount: Decimal,
}

async fn new_payment(
    State(sender): State<UnboundedSender<PostPaymentDto>>,
    Json(dto): Json<PostPaymentDto>,
) -> impl IntoResponse {
    match sender.send(dto) {
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

async fn summary(Query(window): Query<SummaryParams>) -> impl IntoResponse {
    let from = window
        .from
        .unwrap_or_else(|| DateTime::<Utc>::from_str("0000-01-01T00:00:00.000Z").unwrap());
    let to = window
        .to
        .unwrap_or_else(|| DateTime::<Utc>::from_str("9999-12-31T23:59:59.999Z").unwrap());

    let processor_names = external_processors()
        .iter()
        .map(|p| p.name.clone())
        .collect();

    match repository::summary(db(), &processor_names, from, to).await {
        Ok(summary) => Ok((StatusCode::OK, Json(summary))),
        Err(error) => {
            log::error!("failed fetching summary, {}", error);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn db() -> &'static Pool<Postgres> {
    unsafe { DB.get().unwrap_unchecked() }
}

fn external_processors() -> &'static Vec<ProcessorConfig> {
    unsafe { EXTERNAL_PROCESSORS.get().unwrap_unchecked() }
}

fn http_client() -> &'static reqwest::Client {
    unsafe { HTTP_CLIENT.get().unwrap_unchecked() }
}
