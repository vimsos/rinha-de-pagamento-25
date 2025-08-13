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
                self.handle_payment(payment).await
            } else {
                return;
            }
        }
    }

    async fn handle_payment(&self, payment: Payment) {
        let mut attempts = 0;

        loop {
            let target_processor =
                &self.state.config.processors[attempts % self.state.config.processors.len()];
            attempts += 1;
            let response_result = self
                .state
                .client
                .post(&target_processor.endpoint)
                .json(&payment)
                .send()
                .await;

            match response_result {
                Ok(response) if response.status().is_success() => {
                    _ = repository::set_processed_by(
                        &self.state.db,
                        payment.correlation_id,
                        &target_processor.name,
                    )
                    .await
                    .unwrap();

                    log::info!(
                        "{} processed by {}",
                        payment.correlation_id,
                        target_processor.name
                    );

                    return;
                }
                Ok(response) => {
                    log::error!(
                        "{} failed at {} with status {}, {} attempts",
                        payment.correlation_id,
                        target_processor.name,
                        response.status(),
                        attempts
                    );
                }
                Err(error) => {
                    log::error!(
                        "{} failed at {} with error {}, {} attempts",
                        payment.correlation_id,
                        target_processor.name,
                        error,
                        attempts
                    );
                }
            };
        }
    }
}
