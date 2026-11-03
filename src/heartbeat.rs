use crate::api::AppState;
use serde_json::json;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

pub fn start(state: Arc<AppState>, shutdown: CancellationToken) {
    if !state.config.heartbeat.enabled {
        return;
    }

    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("HTTP client initialization failed");

        loop {
            let streams = state.db.streams().await;

            let (total, enabled) = match streams {
                Ok(items) => {
                    let total = items.len();
                    let enabled = items.iter().filter(|s| s.enabled == 1).count();
                    (total, enabled)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Heartbeat database query failed");
                    (0, 0)
                }
            };

            let database = sqlx::query("SELECT 1")
                .execute(&state.db.pool)
                .await
                .is_ok();

            let payload = json!({
                "node_id": state.config.node.id,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "status": if database { "healthy" } else { "degraded" },
                "streams": {
                    "total": total,
                    "enabled": enabled
                },
                "database": if database { "connected" } else { "unavailable" }
            });

            let mut request = client
                .post(&state.config.heartbeat.webhook_url)
                .json(&payload);

            if !state.config.heartbeat.token.is_empty() {
                request = request.bearer_auth(&state.config.heartbeat.token);
            }

            match request.send().await {
                Ok(response) if response.status().is_success() => {
                    tracing::debug!("Heartbeat delivered");
                }
                Ok(response) => {
                    tracing::warn!(
                        status = %response.status(),
                        "Heartbeat endpoint returned an error"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Heartbeat delivery failed"
                    );
                }
            }

            tokio::select! {
                _ = sleep(Duration::from_secs(
                    state.config.heartbeat.interval_seconds.max(10),
                )) => {}
                _ = shutdown.cancelled() => break,
            }
        }
    });
}
