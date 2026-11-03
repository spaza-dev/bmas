use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use subtle::ConstantTimeEq;

use crate::api::AppState;

pub async fn require_token(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let expected = format!("Bearer {}", state.config.node.api_token);

    let valid = supplied.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1;

    if !valid {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}
