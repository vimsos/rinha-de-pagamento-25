use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use crate::{App, repository};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Payment {
    pub correlation_id: Uuid,
    pub amount: f64,
    pub requested_at: DateTime<Utc>,
}

pub struct Processor {
    pub state: App,
    pub receiver: UnboundedReceiver<Payment>,
}

impl Processor {
    pub async fn run_forever(&mut self) {
        loop {
            if let Some(payment) = self.receiver.recv().await {
                if let Some(payment) = self.try_insert_db(payment).await {
                    self.submit_external_processor(payment).await
                }
            } else {
                return;
            }
        }
    }

    async fn try_insert_db(&self, payment: Payment) -> Option<Payment> {
        loop {
            match repository::insert(
                &self.state.db,
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
                    log::error!("failed saving to db, {}", error);
                    log::error!("this is really bad btw");
                }
            }
        }
    }

    async fn submit_external_processor(&self, payment: Payment) {
        let mut attempts = 0;

        loop {
            let target = &self.state.targets[attempts % self.state.targets.len()];
            attempts += 1;
            let response_result = self
                .state
                .client
                .post(&target.endpoint)
                .json(&payment)
                .send()
                .await;

            match response_result {
                Ok(response) if response.status().is_success() => {
                    _ = repository::set_processed_by(
                        &self.state.db,
                        payment.correlation_id,
                        &target.name,
                    )
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
}
