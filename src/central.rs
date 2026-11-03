use crate::api::AppState;
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use rand::Rng;
use serde_json::json;
use std::{sync::Arc, time::Duration as StdDuration};
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

pub fn start(state: Arc<AppState>, shutdown: CancellationToken) {
    if !state.config.central.enabled {
        return;
    }

    let heartbeat_state = state.clone();
    let heartbeat_shutdown = shutdown.clone();
    tokio::spawn(async move {
        heartbeat_loop(heartbeat_state, heartbeat_shutdown).await;
    });

    tokio::spawn(async move {
        dispatcher_loop(state, shutdown).await;
    });
}

async fn heartbeat_loop(state: Arc<AppState>, shutdown: CancellationToken) {
    let mut timer = interval(StdDuration::from_secs(
        state.config.central.heartbeat_interval_seconds.max(10),
    ));

    loop {
        tokio::select! {
            _ = timer.tick() => {}
            _ = shutdown.cancelled() => break,
        }

        let enabled = state.db.active_stream_count().await.unwrap_or(0);
        let database = sqlx::query("SELECT 1")
            .execute(&state.db.pool)
            .await
            .is_ok();

        let payload = json!({
            "node_id": state.config.node.id,
            "timestamp": Utc::now().to_rfc3339(),
            "status": if database { "healthy" } else { "degraded" },
            "streams": {
                "enabled": enabled
            },
            "database": if database { "connected" } else { "unavailable" }
        });

        let url = format!(
            "{}/api/v1/nodes/{}/heartbeat",
            state.config.central.base_url.trim_end_matches('/'),
            state.config.node.id
        );

        if let Err(error) = post_json(&state, &url, &payload).await {
            warn!(error = %error, "Central heartbeat failed");
        }
    }
}

async fn dispatcher_loop(state: Arc<AppState>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            _ = sleep(StdDuration::from_secs(2)) => {}
            _ = shutdown.cancelled() => break,
        }

        let callback = match state.db.pending_callback().await {
            Ok(Some(callback)) => callback,
            Ok(None) => continue,
            Err(error) => {
                error!(error = %error, "Could not read callback outbox");
                continue;
            }
        };

        let payload: serde_json::Value = match serde_json::from_str(&callback.payload) {
            Ok(payload) => payload,
            Err(error) => {
                error!(callback_id = %callback.id, error = %error, "Invalid callback payload");
                let _ = state
                    .db
                    .reschedule_callback(
                        &callback.id,
                        callback.attempts + 1,
                        &(Utc::now() + Duration::hours(1)).to_rfc3339(),
                        "invalid payload",
                    )
                    .await;
                continue;
            }
        };

        let path = match callback.kind.as_str() {
            "archive" => format!("/api/v1/nodes/{}/archives", state.config.node.id),
            "event" => format!("/api/v1/nodes/{}/events", state.config.node.id),
            other => {
                warn!(callback_id = %callback.id, kind = other, "Unknown callback kind");
                let _ = state.db.mark_callback_delivered(&callback.id).await;
                continue;
            }
        };

        let url = format!(
            "{}{}",
            state.config.central.base_url.trim_end_matches('/'),
            path
        );

        match post_json(&state, &url, &payload).await {
            Ok(()) => {
                if let Err(error) = state.db.mark_callback_delivered(&callback.id).await {
                    error!(callback_id = %callback.id, error = %error, "Could not mark callback delivered");
                }
            }
            Err(error) => {
                let attempts = callback.attempts + 1;
                let delay = if attempts as u32 >= state.config.central.max_retries {
                    warn!(
                        callback_id = %callback.id,
                        attempts,
                        "Central callback retry budget exhausted; backing off for one hour"
                    );
                    3600
                } else {
                    callback_backoff(attempts)
                };
                let next = (Utc::now() + Duration::seconds(delay as i64)).to_rfc3339();
                warn!(
                    callback_id = %callback.id,
                    attempts,
                    retry_in_seconds = delay,
                    error = %error,
                    "Central callback failed; scheduling retry"
                );
                let _ = state
                    .db
                    .reschedule_callback(
                        &callback.id,
                        attempts,
                        &next,
                        &truncate(&format!("{:#}", error)),
                    )
                    .await;
            }
        }
    }
}

async fn post_json(state: &Arc<AppState>, url: &str, payload: &serde_json::Value) -> Result<()> {
    let response = client()?
        .post(url)
        .bearer_auth(&state.config.central.node_token)
        .json(payload)
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;

    if !response.status().is_success() {
        anyhow::bail!("central API returned {}", response.status());
    }

    Ok(())
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(StdDuration::from_secs(15))
        .build()
        .context("building central API client")
}

fn callback_backoff(attempts: i64) -> u64 {
    let exponent = attempts.clamp(0, 10) as u32;
    let base = 5u64.saturating_mul(2u64.saturating_pow(exponent)).min(3600);
    let jitter = rand::thread_rng().gen_range(0..=base / 2);
    base + jitter
}

fn truncate(value: &str) -> String {
    value.chars().take(500).collect()
}
