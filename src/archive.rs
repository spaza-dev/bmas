use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation},
    primitives::ByteStream,
    Client,
};
use serde::Serialize;
use std::path::Path;
use tracing::info;

use crate::config::R2Config;

#[derive(Debug, Clone, Serialize)]
pub struct IndexObject {
    pub object_key: String,
    pub format: String,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct Manifest {
    pub node_id: String,
    pub stream_id: String,
    pub external_stream_id: Option<String>,
    pub chunk_id: String,
    pub started_at: String,
    pub completed_at: String,
    pub duration_seconds: i64,
    pub audio_archived: bool,
    pub index_format: String,
    pub olaf_identifier: String,
    pub status: String,
    pub index_objects: Vec<IndexObject>,
    pub audio_object: Option<String>,
    pub metadata: serde_json::Value,
}

pub async fn write_manifest(directory: &Path, manifest: &Manifest) -> Result<()> {
    tokio::fs::create_dir_all(directory)
        .await
        .with_context(|| format!("Creating archive directory {}", directory.display()))?;

    let path = directory.join("manifest.json");

    let content = serde_json::to_vec_pretty(manifest).context("Serializing archive manifest")?;

    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("Writing manifest to {}", path.display()))?;

    info!(
        path = %path.display(),
        stream_id = %manifest.stream_id,
        chunk_id = %manifest.chunk_id,
        "Archive manifest written"
    );

    Ok(())
}

fn validate_r2_config(config: &R2Config) -> Result<()> {
    anyhow::ensure!(config.enabled, "R2 is disabled");

    anyhow::ensure!(
        !config.bucket.trim().is_empty(),
        "R2 bucket name is not configured"
    );

    anyhow::ensure!(
        !config.access_key_id.trim().is_empty(),
        "R2 access key ID is not configured"
    );

    anyhow::ensure!(
        !config.secret_access_key.trim().is_empty(),
        "R2 secret access key is not configured"
    );

    anyhow::ensure!(
        config.region.trim() == "auto",
        "Cloudflare R2 requires region = \"auto\"; configured region is '{}'",
        config.region
    );

    if config.endpoint_url.trim().is_empty() {
        anyhow::ensure!(
            !config.account_id.trim().is_empty(),
            "R2 account ID is not configured and no endpoint URL was provided"
        );
    }

    Ok(())
}

fn r2_endpoint(config: &R2Config) -> Result<String> {
    if !config.endpoint_url.trim().is_empty() {
        return Ok(config.endpoint_url.trim().trim_end_matches('/').to_string());
    }

    anyhow::ensure!(
        !config.account_id.trim().is_empty(),
        "R2 account ID is not configured"
    );

    Ok(format!(
        "https://{}.r2.cloudflarestorage.com",
        config.account_id.trim()
    ))
}

async fn create_r2_client(config: &R2Config) -> Result<Client> {
    validate_r2_config(config)?;

    let endpoint = r2_endpoint(config)?;

    let credentials = Credentials::new(
        config.access_key_id.trim(),
        config.secret_access_key.trim(),
        None,
        None,
        "R2",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("auto"))
        .credentials_provider(credentials)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();

    info!(
        endpoint = %endpoint,
        bucket = %config.bucket.trim(),
        region = "auto",
        "R2 client initialized"
    );

    Ok(Client::from_conf(s3_config))
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|value| value.to_str()) {
        Some("wav") => "audio/wav",
        Some("json") => "application/json",
        Some("mdb") => "application/octet-stream",
        Some("lock") => "application/octet-stream",
        _ => "application/octet-stream",
    }
}

async fn upload_file(client: &Client, bucket: &str, local_path: &Path, key: &str) -> Result<u64> {
    let metadata = tokio::fs::metadata(local_path)
        .await
        .with_context(|| format!("Reading metadata for {}", local_path.display()))?;

    let file_size = metadata.len();

    let body = ByteStream::from_path(local_path)
        .await
        .with_context(|| format!("Opening archive file {}", local_path.display()))?;

    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type(content_type(local_path))
        .body(body)
        .send()
        .await
        .with_context(|| {
            format!(
                "Uploading {} to R2 bucket '{}' with key '{}'",
                local_path.display(),
                bucket,
                key
            )
        })?;

    info!(
        bucket = %bucket,
        key = %key,
        bytes = file_size,
        etag = response.e_tag().unwrap_or("not-returned"),
        "R2 object uploaded successfully"
    );

    Ok(file_size)
}

pub async fn upload_directory(
    config: &R2Config,
    directory: &Path,
    prefix: &str,
) -> Result<Vec<String>> {
    validate_r2_config(config)?;

    let metadata = tokio::fs::metadata(directory)
        .await
        .with_context(|| format!("Reading archive directory {}", directory.display()))?;

    anyhow::ensure!(
        metadata.is_dir(),
        "Archive path is not a directory: {}",
        directory.display()
    );

    let client = create_r2_client(config).await?;
    let bucket = config.bucket.trim();
    let prefix = prefix.trim_matches('/');

    let mut directories = vec![directory.to_path_buf()];
    let mut files = Vec::new();

    while let Some(current_directory) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&current_directory)
            .await
            .with_context(|| format!("Reading directory {}", current_directory.display()))?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.is_dir() {
                directories.push(path);
                continue;
            }

            if !path.is_file() {
                continue;
            }

            let relative_path = path.strip_prefix(directory).with_context(|| {
                format!(
                    "Resolving path {} relative to {}",
                    path.display(),
                    directory.display()
                )
            })?;

            let relative_key = relative_path.to_string_lossy().replace('\\', "/");
            files.push((path, relative_key));
        }
    }

    files.sort_by(|left, right| {
        let left_manifest = left.1 == "manifest.json";
        let right_manifest = right.1 == "manifest.json";
        left_manifest
            .cmp(&right_manifest)
            .then_with(|| left.1.cmp(&right.1))
    });

    let mut uploaded_keys = Vec::with_capacity(files.len());
    let mut uploaded_bytes = 0u64;

    for (path, relative_key) in &files {
        let key = match prefix.is_empty() {
            true => relative_key.clone(),
            false => format!("{}/{}", prefix, relative_key),
        };

        let file_size = upload_file(&client, bucket, path, &key).await?;
        uploaded_bytes += file_size;
        uploaded_keys.push(key);
    }

    anyhow::ensure!(
        !uploaded_keys.is_empty(),
        "No files found to upload in archive directory {}",
        directory.display()
    );

    info!(
        bucket = %bucket,
        prefix = %prefix,
        uploaded_files = uploaded_keys.len(),
        uploaded_bytes,
        "R2 archive upload completed successfully"
    );

    Ok(uploaded_keys)
}
