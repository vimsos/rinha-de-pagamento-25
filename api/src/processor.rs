use chrono::{DateTime, Utc};
use flume::Receiver;
use rust_decimal::Decimal;
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
    time::Duration,
};
use uuid::Uuid;

use crate::{db, external_processors, http_client, repository};

#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct Payment {
    pub correlation_id: Uuid,
    pub amount: Decimal,
    pub requested_at: DateTime<Utc>,
}

pub struct Processor {
    pub receiver: Receiver<Payment>,
    pub max_in_flight: usize,
    pub max_wait_millis: usize,
}

impl Processor {
    pub async fn run_forever(&mut self) {
        let max_in_flight = self.max_in_flight;
        let max_wait = Duration::from_millis(self.max_wait_millis as u64);
        let in_flight = Arc::new(AtomicUsize::new(0));

        loop {
            for payment in self.receiver.drain() {
                let in_flight = in_flight.clone();
                tokio::spawn(async move {
                    while in_flight.load(atomic::Ordering::Relaxed) >= max_in_flight {
                        tokio::time::sleep(max_wait).await;
                    }

                    in_flight.fetch_add(1, atomic::Ordering::Relaxed);

                    if let Some(payment) = maybe_insert_into_db(payment).await {
                        let processed_by = submit_external_processor(payment, max_wait).await;
                        set_processed_by(payment, processed_by).await;
                    }

                    in_flight.fetch_sub(1, atomic::Ordering::Relaxed);
                });
            }

            tokio::time::sleep(max_wait).await;
        }
    }
}

async fn maybe_insert_into_db(payment: Payment) -> Option<Payment> {
    loop {
        match repository::insert(
            db(),
            payment.correlation_id,
            payment.amount,
            payment.requested_at,
        )
        .await
        {
            Ok(_) => return Some(payment),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                log::info!("{} already exists", payment.correlation_id);
                return None;
            }
            Err(error) => {
                log::error!(
                    "failed inserting {} into db, {}\nthis is really bad",
                    payment.correlation_id,
                    error
                );
            }
        }
    }
}

async fn submit_external_processor(
    payment: Payment,
    max_wait_between_attempts: Duration,
) -> String {
    let mut attempts = 0;

    loop {
        let external_processors = external_processors();
        let target = &external_processors[attempts % external_processors.len()];
        attempts += 1;
        let response_result = http_client()
            .post(&target.endpoint)
            .json(&payment)
            .send()
            .await;

        match response_result {
            Ok(response) if response.status().is_success() => {
                return target.name.clone();
            }
            Ok(response) => {
                log::error!(
                    "{} failed at {} with status {}, {} attempts",
                    payment.correlation_id,
                    target.name,
                    response.status(),
                    attempts
                );
            }
            Err(error) => {
                log::error!(
                    "{} failed at {} with error {}, {} attempts",
                    payment.correlation_id,
                    target.name,
                    error,
                    attempts
                );
            }
        };

        let wait_duration = Duration::from_millis(attempts as u64).max(max_wait_between_attempts);
        tokio::time::sleep(wait_duration).await;
    }
}

async fn set_processed_by(payment: Payment, processed_by: String) {
    loop {
        match repository::set_processed_by(db(), payment.correlation_id, &processed_by).await {
            Ok(_) => {
                log::info!("{} processed by {}", payment.correlation_id, &processed_by);
                return;
            }
            Err(error) => {
                log::error!(
                    "failed setting processed by for {} with {}\nthis is really bad",
                    payment.correlation_id,
                    error
                );
            }
        }
    }
}
