use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use crate::{PostPaymentDto, db, external_processors, http_client, repository};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Payment {
    pub correlation_id: Uuid,
    pub amount: f64,
    pub requested_at: DateTime<Utc>,
}

pub struct Processor {
    pub receiver: UnboundedReceiver<PostPaymentDto>,
}

impl Processor {
    pub async fn run_forever(&mut self) {
        loop {
            if let Some(dto) = self.receiver.recv().await {
                let now = Utc::now();
                let payment = Payment {
                    correlation_id: dto.correlation_id,
                    amount: dto.amount,
                    requested_at: now,
                };

                tokio::spawn(async move {
                    if let Some(payment) = insert_into_db(payment).await {
                        submit_external_processor(payment).await
                    }
                });
            } else {
                return;
            }
        }
    }
}

async fn insert_into_db(payment: Payment) -> Option<Payment> {
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

async fn submit_external_processor(payment: Payment) {
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
                _ = repository::set_processed_by(db(), payment.correlation_id, &target.name)
                    .await
                    .unwrap();

                log::info!("{} processed by {}", payment.correlation_id, target.name);

                return;
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
    }
}
