use axum::{extract::State, Json};
use serde_json::json;
use std::sync::Arc;

use crate::api::AppState;

#[utoipa::path(
    get,
    path = "/healthz",
    responses(
        (status = 200, description = "Process is alive", body = Object,
            example = json!({"status": "alive"}))
    )
)]
pub async fn live() -> Json<serde_json::Value> {
    Json(json!({
        "status": "alive"
    }))
}

#[utoipa::path(
    get,
    path = "/readyz",
    responses(
        (status = 200, description = "Service is ready", body = Object,
            example = json!({"status": "ready", "database": "connected", "node_id": "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b"})),
        (status = 503, description = "Service is not ready", body = Object,
            example = json!({"status": "not_ready", "database": "unavailable"}))
    )
)]
pub async fn ready(
    State(state): State<Arc<AppState>>,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    match sqlx::query("SELECT 1").execute(&state.db.pool).await {
        Ok(_) => (
            axum::http::StatusCode::OK,
            Json(json!({
                "status": "ready",
                "database": "connected",
                "node_id": state.config.node.id
            })),
        ),
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "database": "unavailable"
            })),
        ),
    }
}
