use axum::{
    extract::{Path, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    archive::ArchiveStore,
    auth,
    config::Config,
    db::{Database, NewStream, Stream, StreamUpdate},
    health,
};

pub struct AppState {
    pub config: Config,
    pub db: Database,
    pub store: Option<Arc<ArchiveStore>>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "Example Nightclub",
    "external_id": "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b",
    "source_type": "nightclub",
    "url": null,
    "input_type": "audio_device",
    "input": "default",
    "chunk_seconds": 300,
    "archive_audio": true,
    "enabled": false,
    "metadata": {"country": "KE", "city": "Nairobi"}
}))]
pub struct CreateStream {
    #[schema(example = "Example Nightclub")]
    pub name: String,
    #[schema(example = "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b", nullable)]
    pub external_id: Option<String>,
    #[schema(example = "nightclub")]
    pub source_type: Option<String>,
    #[schema(example = "https://stream.example.com/live", nullable)]
    pub url: Option<String>,
    #[schema(example = "nightclub", nullable)]
    pub description: Option<String>,
    #[schema(example = "audio_device")]
    pub input_type: Option<String>,
    #[schema(example = "default", nullable)]
    pub input: Option<String>,
    #[schema(example = 300, minimum = 30)]
    pub chunk_seconds: Option<i64>,
    #[schema(example = true)]
    pub archive_audio: Option<bool>,
    #[schema(example = true)]
    pub enabled: Option<bool>,
    #[schema(value_type = Option<Object>, example = json!({"country": "KE", "city": "Nairobi"}))]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "Example FM",
    "url": "https://stream.example.com/live",
    "chunk_seconds": 600,
    "archive_audio": false
}))]
pub struct UpdateStream {
    #[schema(example = "Example FM", nullable)]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_some")]
    #[schema(value_type = Option<String>, nullable)]
    pub url: Option<Option<String>>,
    #[schema(example = "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b", nullable)]
    pub external_id: Option<String>,
    #[schema(example = "internet_radio", nullable)]
    pub source_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_some")]
    #[schema(value_type = Option<String>, nullable)]
    pub description: Option<Option<String>>,
    #[schema(example = "url", nullable)]
    pub input_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_some")]
    #[schema(value_type = Option<String>, nullable)]
    pub input: Option<Option<String>>,
    #[schema(example = true, nullable)]
    pub enabled: Option<bool>,
    #[schema(example = 600, minimum = 30, nullable)]
    pub chunk_seconds: Option<i64>,
    #[schema(example = false, nullable)]
    pub archive_audio: Option<bool>,
    #[schema(value_type = Option<Object>, example = json!({"country": "KE"}))]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({"enabled_streams": 3}))]
pub struct MonitoringSummary {
    pub enabled_streams: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({"error": "Stream not found"}))]
pub struct ApiError {
    pub error: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NodeStatus {
    pub node_id: String,
    pub version: String,
    pub status: String,
    pub storage_enabled: bool,
    pub storage_backend: String,
    pub total_streams: i64,
    pub enabled_streams: i64,
    pub timestamp: String,
}

const MAX_METADATA_BYTES: usize = 64 * 1024;

fn deserialize_some<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

fn map_create_error(e: anyhow::Error) -> (StatusCode, Json<ApiError>) {
    if let Some(sqlx::Error::Database(database_error)) = e.downcast_ref::<sqlx::Error>() {
        if database_error.is_unique_violation() {
            return error(
                StatusCode::CONFLICT,
                "A stream with this external_id already exists",
            );
        }
    }

    internal(e)
}

fn error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            error: message.into(),
        }),
    )
}

fn internal(e: impl std::fmt::Display) -> (StatusCode, Json<ApiError>) {
    tracing::error!(error = %e, "API request failed");
    error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

fn valid_url(url: &str) -> bool {
    ["http://", "https://", "rtsp://"]
        .iter()
        .any(|scheme| url.starts_with(scheme))
}

fn valid_input_type(input_type: &str) -> bool {
    matches!(input_type, "url" | "audio_device")
}

fn validate_chunk_seconds(value: Option<i64>) -> Result<(), (StatusCode, Json<ApiError>)> {
    if value.is_some_and(|v| !(30..=86_400).contains(&v)) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Chunk duration must be between 30 and 86400 seconds",
        ));
    }

    Ok(())
}

fn metadata_string(
    metadata: &Option<serde_json::Value>,
) -> Result<String, (StatusCode, Json<ApiError>)> {
    match metadata {
        Some(value) if !value.is_object() => Err(error(
            StatusCode::BAD_REQUEST,
            "metadata must be a JSON object",
        )),
        Some(value) => {
            let serialized = serde_json::to_string(value)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "Invalid metadata JSON"))?;

            if serialized.len() > MAX_METADATA_BYTES {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "metadata must not exceed 64 KiB",
                ));
            }

            Ok(serialized)
        }
        None => Ok("{}".to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/streams",
    responses(
        (status = 200, description = "Stream list", body = [Stream]),
        (status = 401, description = "Unauthorized", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn list_streams(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Stream>>, (StatusCode, Json<ApiError>)> {
    let streams = state.db.streams().await.map_err(internal)?;
    Ok(Json(streams))
}

#[utoipa::path(
    post,
    path = "/api/v1/streams",
    request_body(
        content = CreateStream,
        description = "Create a monitoring stream. URL sources require `url`; physical sources use `input_type = \"audio_device\"` and `input`.",
        examples(
            ("audio_device" = (
                summary = "Physical audio source",
                value = json!({
                    "name": "Example Nightclub",
                    "external_id": "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b",
                    "source_type": "nightclub",
                    "url": null,
                    "input_type": "audio_device",
                    "input": "default",
                    "chunk_seconds": 300,
                    "archive_audio": true,
                    "enabled": false,
                    "metadata": {"country": "KE", "city": "Nairobi"}
                })
            )),
            ("internet_radio" = (
                summary = "HTTP audio stream",
                value = json!({
                    "name": "Example FM",
                    "external_id": "7f3c1e4a-9b2d-4c6f-8a1e-2d5b7c9f0a13",
                    "source_type": "internet_radio",
                    "url": "https://stream.example.com/live",
                    "input_type": "url",
                    "chunk_seconds": 300,
                    "archive_audio": true,
                    "enabled": false,
                    "metadata": {"country": "KE", "frequency": "98.4 FM"}
                })
            ))
        )
    ),
    responses(
        (status = 200, description = "Existing stream returned for idempotent external_id", body = Stream),
        (status = 201, description = "Stream created", body = Stream,
            examples(("created" = (
                summary = "Created stream",
                value = json!({
                    "id": "0b6c1c2e-7e49-4c8f-9e7f-6f6f2a0e4d11",
                    "name": "Example FM",
                    "url": "https://stream.example.com/live",
                    "enabled": 1,
                    "chunk_seconds": 300,
                    "archive_audio": 1,
                    "external_id": "7f3c1e4a-9b2d-4c6f-8a1e-2d5b7c9f0a13",
                    "source_type": "internet_radio",
                    "description": null,
                    "metadata": "{}",
                    "input_type": "url",
                    "input": "https://stream.example.com/live",
                    "deleted_at": null,
                    "created_at": "2026-11-03T09:00:00+00:00",
                    "updated_at": "2026-11-03T09:00:00+00:00"
                })
            )))),
        (status = 400, description = "Invalid input", body = ApiError,
            example = json!({"error": "A stream name and valid source are required"})),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 409, description = "Stream already exists", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateStream>,
) -> Result<(StatusCode, Json<Stream>), (StatusCode, Json<ApiError>)> {
    let name = payload.name.trim();

    if name.is_empty() || name.len() > 200 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "A stream name of 1-200 characters is required",
        ));
    }

    validate_chunk_seconds(payload.chunk_seconds)?;

    let source_type = payload
        .source_type
        .as_deref()
        .unwrap_or("internet_radio")
        .trim();

    if source_type.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "Invalid source_type"));
    }

    let input_type = payload.input_type.as_deref().unwrap_or("url").trim();

    if !valid_input_type(input_type) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "input_type must be 'url' or 'audio_device'",
        ));
    }

    let url = payload
        .url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let input = payload
        .input
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match input_type {
        "url" => {
            let candidate = url.or(input).unwrap_or("");
            if !valid_url(candidate) {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "A valid HTTP, HTTPS or RTSP URL is required for URL sources",
                ));
            }
        }
        "audio_device" => {
            if input.is_none() {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "An input device is required for audio_device sources",
                ));
            }
        }
        _ => unreachable!(),
    }

    let external_id = payload
        .external_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(external_id) = external_id {
        if let Some(existing) = state
            .db
            .get_stream_by_external_id(external_id)
            .await
            .map_err(internal)?
        {
            return Ok((StatusCode::OK, Json(existing)));
        }
    }

    let should_enable = payload.enabled.unwrap_or(true);

    if should_enable {
        let enabled = state.db.active_stream_count().await.map_err(internal)?;
        if enabled as usize >= state.config.max_enabled_streams() {
            return Err(error(
                StatusCode::CONFLICT,
                "Maximum enabled streams reached",
            ));
        }
    }

    let metadata = metadata_string(&payload.metadata)?;

    let stream = state
        .db
        .create_stream(NewStream {
            name,
            url,
            external_id,
            source_type,
            description: payload.description.as_deref(),
            metadata: &metadata,
            input_type,
            input,
            chunk_seconds: payload.chunk_seconds,
            archive_audio: payload.archive_audio,
            enabled: Some(should_enable),
        })
        .await
        .map_err(map_create_error)?;

    Ok((StatusCode::CREATED, Json(stream)))
}

#[utoipa::path(
    get,
    path = "/api/v1/streams/{id}",
    params(("id" = String, Path, description = "Stream ID")),
    responses(
        (status = 200, description = "Stream", body = Stream),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Stream not found", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Stream>, (StatusCode, Json<ApiError>)> {
    state
        .db
        .get_stream(&id)
        .await
        .map_err(internal)?
        .map(Json)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "Stream not found"))
}

#[utoipa::path(
    get,
    path = "/api/v1/streams/by-external-id/{external_id}",
    params(("external_id" = String, Path, description = "Central stream UUID")),
    responses(
        (status = 200, description = "Stream", body = Stream),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Stream not found", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn get_stream_by_external_id(
    State(state): State<Arc<AppState>>,
    Path(external_id): Path<String>,
) -> Result<Json<Stream>, (StatusCode, Json<ApiError>)> {
    state
        .db
        .get_stream_by_external_id(&external_id)
        .await
        .map_err(internal)?
        .map(Json)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "Stream not found"))
}

#[utoipa::path(
    patch,
    path = "/api/v1/streams/{id}",
    params(("id" = String, Path)),
    request_body(
        content = UpdateStream,
        description = "Update stream fields. Only supplied fields are changed.",
        example = json!({
            "name": "Example FM",
            "chunk_seconds": 600,
            "archive_audio": false
        })
    ),
    responses(
        (status = 204, description = "Stream updated"),
        (status = 400, description = "Invalid input", body = ApiError),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Stream not found", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn update_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateStream>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    if let Some(Some(url)) = payload.url.as_ref() {
        if !valid_url(url) {
            return Err(error(StatusCode::BAD_REQUEST, "Invalid stream URL"));
        }
    }

    if let Some(ref input_type) = payload.input_type {
        if !valid_input_type(input_type) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "input_type must be 'url' or 'audio_device'",
            ));
        }
    }

    validate_chunk_seconds(payload.chunk_seconds)?;

    if let Some(name) = payload.name.as_deref() {
        if name.trim().is_empty() || name.len() > 200 {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "A stream name of 1-200 characters is required",
            ));
        }
    }

    let external_id = payload.external_id.as_deref().map(str::trim);

    if let Some(external_id) = external_id {
        if external_id.is_empty() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "external_id cannot be empty",
            ));
        }
    }

    if let Some(enabled) = payload.enabled {
        if enabled {
            let current = state
                .db
                .get_stream(&id)
                .await
                .map_err(internal)?
                .ok_or_else(|| error(StatusCode::NOT_FOUND, "Stream not found"))?;

            if current.enabled == 0 {
                let count = state.db.active_stream_count().await.map_err(internal)?;
                if count as usize >= state.config.max_enabled_streams() {
                    return Err(error(
                        StatusCode::CONFLICT,
                        "Maximum enabled streams reached",
                    ));
                }
            }
        }

        if !state.db.set_enabled(&id, enabled).await.map_err(internal)? {
            return Err(error(StatusCode::NOT_FOUND, "Stream not found"));
        }
    }

    let metadata = match &payload.metadata {
        Some(value) => Some(metadata_string(&Some(value.clone()))?),
        None => None,
    };

    if payload.name.is_some()
        || payload.url.is_some()
        || payload.external_id.is_some()
        || payload.source_type.is_some()
        || payload.description.is_some()
        || payload.input_type.is_some()
        || payload.input.is_some()
        || payload.chunk_seconds.is_some()
        || payload.archive_audio.is_some()
        || metadata.is_some()
    {
        let updated = state
            .db
            .update_stream(
                &id,
                StreamUpdate {
                    name: payload.name.as_deref(),
                    url: payload.url.as_ref().map(|value| value.as_deref()),
                    external_id,
                    source_type: payload.source_type.as_deref(),
                    description: payload.description.as_ref().map(|value| value.as_deref()),
                    metadata: metadata.as_deref(),
                    input_type: payload.input_type.as_deref(),
                    input: payload.input.as_ref().map(|value| value.as_deref()),
                    chunk_seconds: payload.chunk_seconds,
                    archive_audio: payload.archive_audio,
                },
            )
            .await
            .map_err(internal)?;

        if !updated {
            return Err(error(StatusCode::NOT_FOUND, "Stream not found"));
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/api/v1/streams/{id}",
    params(("id" = String, Path)),
    responses(
        (status = 204, description = "Stream soft-deleted"),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Stream not found", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    if !state.db.delete_stream(&id).await.map_err(internal)? {
        return Err(error(StatusCode::NOT_FOUND, "Stream not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/v1/streams/{id}/start",
    params(("id" = String, Path)),
    responses(
        (status = 202, description = "Stream enabled"),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Stream not found", body = ApiError),
        (status = 409, description = "Maximum enabled streams reached", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn start_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let stream = state
        .db
        .get_stream(&id)
        .await
        .map_err(internal)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "Stream not found"))?;

    if stream.enabled == 0 {
        let enabled = state.db.active_stream_count().await.map_err(internal)?;
        if enabled as usize >= state.config.max_enabled_streams() {
            return Err(error(
                StatusCode::CONFLICT,
                "Maximum enabled streams reached",
            ));
        }
    }

    if !state.db.set_enabled(&id, true).await.map_err(internal)? {
        return Err(error(StatusCode::NOT_FOUND, "Stream not found"));
    }

    Ok(StatusCode::ACCEPTED)
}

#[utoipa::path(
    post,
    path = "/api/v1/streams/{id}/stop",
    params(("id" = String, Path)),
    responses(
        (status = 202, description = "Stream disabled"),
        (status = 401, description = "Unauthorized", body = ApiError),
        (status = 404, description = "Stream not found", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn stop_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    if !state.db.set_enabled(&id, false).await.map_err(internal)? {
        return Err(error(StatusCode::NOT_FOUND, "Stream not found"));
    }

    Ok(StatusCode::ACCEPTED)
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "BMAS API",
        version = "0.1.0",
        description = "Broadcast Monitoring & Archiving Service node API. All protected routes require `Authorization: Bearer <token>`."
    ),
    paths(
        list_streams,
        create_stream,
        get_stream,
        get_stream_by_external_id,
        update_stream,
        delete_stream,
        start_stream,
        stop_stream,
        monitoring_summary,
        node_status,
        health::live,
        health::ready,
    ),
    components(schemas(
        Stream,
        CreateStream,
        UpdateStream,
        MonitoringSummary,
        NodeStatus,
        ApiError,
    )),
    modifiers(&SecurityAddon)
)]
struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};

        let components = openapi.components.get_or_insert_with(Default::default);

        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    let protected = Router::new()
        .route("/api/v1/streams", get(list_streams).post(create_stream))
        .route(
            "/api/v1/streams/by-external-id/{external_id}",
            get(get_stream_by_external_id),
        )
        .route(
            "/api/v1/streams/{id}",
            get(get_stream).patch(update_stream).delete(delete_stream),
        )
        .route("/api/v1/streams/{id}/start", post(start_stream))
        .route("/api/v1/streams/{id}/stop", post(stop_stream))
        .route("/api/v1/monitoring/summary", get(monitoring_summary))
        .route("/api/v1/status", get(node_status))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_token,
        ));

    let mut app = Router::new()
        .route("/healthz", get(health::live))
        .route("/readyz", get(health::ready))
        .merge(protected)
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(TraceLayer::new_for_http());

    if !state.config.node.allowed_origins.is_empty() {
        let origins: Vec<HeaderValue> = state
            .config
            .node
            .allowed_origins
            .iter()
            .filter_map(|origin| origin.parse::<HeaderValue>().ok())
            .collect();

        if !origins.is_empty() {
            app = app.layer(
                CorsLayer::new()
                    .allow_origin(origins)
                    .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE])
                    .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
            );
        }
    }

    app.with_state(state)
}

#[utoipa::path(
    get,
    path = "/api/v1/monitoring/summary",
    responses(
        (status = 200, description = "Monitoring summary", body = MonitoringSummary),
        (status = 401, description = "Unauthorized", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn monitoring_summary(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MonitoringSummary>, (StatusCode, Json<ApiError>)> {
    let enabled_streams = state.db.active_stream_count().await.map_err(internal)?;

    Ok(Json(MonitoringSummary { enabled_streams }))
}

#[utoipa::path(
    get,
    path = "/api/v1/status",
    responses(
        (status = 200, description = "Node status", body = NodeStatus),
        (status = 401, description = "Unauthorized", body = ApiError)
    ),
    security(("bearer_auth" = []))
)]
async fn node_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<NodeStatus>, (StatusCode, Json<ApiError>)> {
    let total_streams = state.db.total_stream_count().await.map_err(internal)?;
    let enabled_streams = state.db.active_stream_count().await.map_err(internal)?;

    Ok(Json(NodeStatus {
        node_id: state.config.node.id.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        status: "healthy".to_string(),
        storage_enabled: state.config.storage.enabled,
        storage_backend: state.config.storage.backend.clone(),
        total_streams,
        enabled_streams,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        IndexingConfig, LimitsConfig, NodeConfig, RecordingConfig, StorageConfig,
    };
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_config(data_dir: String) -> Config {
        Config {
            node: NodeConfig {
                id: "test-node".to_string(),
                listen: "127.0.0.1:0".to_string(),
                data_dir,
                api_token: "0123456789abcdef0123456789abcdef".to_string(),
                max_concurrent_streams: 2,
                reconnect_delay_seconds: 1,
                keep_local_chunks: true,
                allowed_origins: Vec::new(),
            },
            recording: RecordingConfig {
                ffmpeg_path: "ffmpeg".to_string(),
                ffprobe_path: "ffprobe".to_string(),
                olaf_path: "olaf".to_string(),
                chunk_seconds: 30,
                sample_rate: 16000,
                archive_audio: false,
                ffprobe_timeout_seconds: 5,
                olaf_timeout_seconds: 5,
                stall_timeout_seconds: 60,
            },
            indexing: IndexingConfig {
                threads: 1,
                workers: 1,
                max_retries: 1,
            },
            storage: StorageConfig {
                enabled: false,
                backend: "s3".to_string(),
                endpoint_url: String::new(),
                region: "auto".to_string(),
                account_id: String::new(),
                bucket: String::new(),
                prefix: "bmas".to_string(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                force_path_style: None,
            },
            limits: LimitsConfig::default(),
        }
    }

    async fn test_state() -> Arc<AppState> {
        let data_dir = std::env::temp_dir().join(format!("bmas-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let config = test_config(data_dir.to_string_lossy().to_string());
        let db = Database::open(&config.node.data_dir).await.unwrap();
        Arc::new(AppState {
            config,
            db,
            store: None,
        })
    }

    #[tokio::test]
    async fn protected_routes_require_bearer_token() {
        let app = router(test_state().await);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/monitoring/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_routes_accept_valid_token() {
        let state = test_state().await;
        let app = router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/monitoring/summary")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {}", state.config.node.api_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_stream_is_idempotent_by_external_id() {
        let state = test_state().await;
        let app = router(state.clone());
        let payload = serde_json::json!({
            "name": "Example FM",
            "external_id": "external-test",
            "url": "https://stream.example.com/live",
            "chunk_seconds": 30,
            "enabled": false
        });

        let request = || {
            Request::builder()
                .method("POST")
                .uri("/api/v1/streams")
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {}", state.config.node.api_token),
                )
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap()
        };

        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(created["enabled"], 0);
        assert_eq!(created["external_id"], "external-test");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/by-external-id/external-test")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {}", state.config.node.api_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let found: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(found["id"], created["id"]);

        let response = app.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let existing: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(existing["id"], created["id"]);
    }

    #[test]
    fn metadata_must_be_an_object_and_bounded() {
        assert!(metadata_string(&Some(serde_json::json!(["not", "an", "object"]))).is_err());

        let oversized = serde_json::json!({
            "value": "x".repeat(MAX_METADATA_BYTES)
        });
        assert!(metadata_string(&Some(oversized)).is_err());
    }

    #[test]
    fn openapi_documents_post_stream_examples() {
        let document = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let request_body = &document["paths"]["/api/v1/streams"]["post"]["requestBody"]["content"]
            ["application/json"];

        assert!(request_body["examples"]["audio_device"].is_object());
        assert!(request_body["examples"]["internet_radio"].is_object());
        assert!(document["paths"]["/api/v1/streams"]["post"]["responses"]["200"].is_object());
        assert!(document["paths"]["/api/v1/streams"]["post"]["responses"]["201"].is_object());
        assert!(document["paths"]["/api/v1/streams/by-external-id/{external_id}"].is_object());
    }
}
