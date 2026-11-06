use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation},
    primitives::ByteStream,
    Client,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::info;

use crate::config::StorageConfig;

pub const MANIFEST_VERSION: &str = "1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexObject {
    pub object_key: String,
    pub format: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_version: String,
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

pub async fn write_manifest(directory: &Path, manifest: &Manifest) -> Result<PathBuf> {
    tokio::fs::create_dir_all(directory)
        .await
        .with_context(|| format!("Creating archive directory {}", directory.display()))?;

    let path = directory.join("manifest.json");

    let content = serde_json::to_vec_pretty(manifest).context("Serializing archive manifest")?;

    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("Writing manifest to {}", path.display()))?;

    Ok(path)
}

pub fn manifest_object_key(prefix: &str, node_id: &str, day: &str, chunk_id: &str) -> String {
    let prefix = prefix.trim_matches('/');

    if prefix.is_empty() {
        format!("{}/manifests/{}/{}.json", node_id, day, chunk_id)
    } else {
        format!("{}/{}/manifests/{}/{}.json", prefix, node_id, day, chunk_id)
    }
}

pub fn manifest_day(started_at: &str) -> String {
    started_at
        .get(..10)
        .filter(|value| value.len() == 10)
        .unwrap_or("unknown")
        .to_string()
}

pub async fn index_object_keys(index_dir: &Path, archive_key: &str) -> Result<Vec<IndexObject>> {
    let mut directories = vec![index_dir.to_path_buf()];
    let mut objects = Vec::new();

    while let Some(current_directory) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&current_directory)
            .await
            .with_context(|| format!("Reading index directory {}", current_directory.display()))?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.is_dir() {
                directories.push(path);
                continue;
            }

            if !path.is_file() {
                continue;
            }

            let relative_path = path.strip_prefix(index_dir).with_context(|| {
                format!(
                    "Resolving index path {} relative to {}",
                    path.display(),
                    index_dir.display()
                )
            })?;

            let relative_key = relative_path.to_string_lossy().replace('\\', "/");

            objects.push(IndexObject {
                object_key: format!("{}/index/{}", archive_key, relative_key),
                format: "olaf-lmdb".to_string(),
                version: "1".to_string(),
            });
        }
    }

    objects.sort_by(|left, right| left.object_key.cmp(&right.object_key));

    anyhow::ensure!(
        !objects.is_empty(),
        "No index objects found in {}",
        index_dir.display()
    );

    Ok(objects)
}

pub struct ArchiveStore {
    client: Client,
    bucket: String,
    prefix: String,
}

impl ArchiveStore {
    pub async fn connect(config: &StorageConfig) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }

        validate_storage_config(config)?;

        let endpoint = storage_endpoint(config)?;
        let region = if config.backend.trim().eq_ignore_ascii_case("r2") {
            "auto".to_string()
        } else {
            config.region.trim().to_string()
        };

        let credentials = Credentials::new(
            config.access_key_id.trim(),
            config.secret_access_key.trim(),
            None,
            None,
            "bmas-storage",
        );

        let mut shared_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .credentials_provider(credentials);

        // Keep the AWS default endpoint chain for plain S3.
        if endpoint.is_some() {
            shared_builder = shared_builder.endpoint_url(endpoint.clone().unwrap());
        }

        let shared_config = shared_builder.load().await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);

        if let Some(endpoint) = endpoint {
            builder = builder.endpoint_url(endpoint);
        }

        let force_path_style = config
            .force_path_style
            .unwrap_or_else(|| config.backend.trim().eq_ignore_ascii_case("r2"));

        if force_path_style {
            builder = builder.force_path_style(true);
        }

        let client = Client::from_conf(builder.build());

        info!(
            backend = %config.backend,
            bucket = %config.bucket.trim(),
            region = %region,
            "Object storage client initialized"
        );

        Ok(Some(Self {
            client,
            bucket: config.bucket.trim().to_string(),
            prefix: config.prefix.trim().trim_matches('/').to_string(),
        }))
    }

    pub async fn upload_directory(&self, directory: &Path, key: &str) -> Result<Vec<String>> {
        let metadata = tokio::fs::metadata(directory)
            .await
            .with_context(|| format!("Reading archive directory {}", directory.display()))?;

        anyhow::ensure!(
            metadata.is_dir(),
            "Archive path is not a directory: {}",
            directory.display()
        );

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

        // Upload payloads first and the manifest last so a manifest is never
        // visible before the objects it references.
        files.sort_by(|left, right| {
            let left_manifest = left.1 == "manifest.json";
            let right_manifest = right.1 == "manifest.json";
            left_manifest
                .cmp(&right_manifest)
                .then_with(|| left.1.cmp(&right.1))
        });

        let mut uploaded_keys = Vec::with_capacity(files.len());

        for (path, relative_key) in &files {
            let object_key = format!("{}/{}", key.trim_matches('/'), relative_key);
            self.put_file(path, &object_key).await?;
            uploaded_keys.push(object_key);
        }

        anyhow::ensure!(
            !uploaded_keys.is_empty(),
            "No files found to upload in archive directory {}",
            directory.display()
        );

        Ok(uploaded_keys)
    }

    pub async fn upload_manifest(&self, manifest: &Manifest) -> Result<String> {
        let day = manifest_day(&manifest.started_at);
        let key = manifest_object_key(&self.prefix, &manifest.node_id, &day, &manifest.chunk_id);
        let body = serde_json::to_vec_pretty(manifest).context("Serializing archive manifest")?;
        self.put_bytes(body, &key, "application/json").await?;
        Ok(key)
    }

    async fn put_file(&self, local_path: &Path, key: &str) -> Result<u64> {
        let metadata = tokio::fs::metadata(local_path)
            .await
            .with_context(|| format!("Reading metadata for {}", local_path.display()))?;

        let file_size = metadata.len();

        let body = ByteStream::from_path(local_path)
            .await
            .with_context(|| format!("Opening archive file {}", local_path.display()))?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type(local_path))
            .body(body)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Uploading {} to bucket '{}' with key '{}'",
                    local_path.display(),
                    self.bucket,
                    key
                )
            })?;

        info!(
            bucket = %self.bucket,
            key = %key,
            bytes = file_size,
            "Object uploaded"
        );

        Ok(file_size)
    }

    async fn put_bytes(&self, body: Vec<u8>, key: &str, content_type: &str) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| {
                format!(
                    "Uploading manifest to bucket '{}' with key '{}'",
                    self.bucket, key
                )
            })?;

        Ok(())
    }
}

fn validate_storage_config(config: &StorageConfig) -> Result<()> {
    anyhow::ensure!(config.enabled, "Storage is disabled");

    anyhow::ensure!(
        !config.bucket.trim().is_empty(),
        "Storage bucket name is not configured"
    );

    anyhow::ensure!(
        !config.access_key_id.trim().is_empty(),
        "Storage access key ID is not configured"
    );

    anyhow::ensure!(
        !config.secret_access_key.trim().is_empty(),
        "Storage secret access key is not configured"
    );

    let backend = config.backend.trim().to_ascii_lowercase();
    anyhow::ensure!(
        backend == "s3" || backend == "r2",
        "Storage backend must be 's3' or 'r2'"
    );

    if backend == "r2" && config.endpoint_url.trim().is_empty() {
        anyhow::ensure!(
            !config.account_id.trim().is_empty(),
            "R2 account ID is not configured and no endpoint URL was provided"
        );
    }

    Ok(())
}

fn storage_endpoint(config: &StorageConfig) -> Result<Option<String>> {
    if !config.endpoint_url.trim().is_empty() {
        return Ok(Some(
            config.endpoint_url.trim().trim_end_matches('/').to_string(),
        ));
    }

    if config.backend.trim().eq_ignore_ascii_case("r2") {
        anyhow::ensure!(
            !config.account_id.trim().is_empty(),
            "R2 account ID is not configured"
        );

        return Ok(Some(format!(
            "https://{}.r2.cloudflarestorage.com",
            config.account_id.trim()
        )));
    }

    Ok(None)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_keys_are_under_manifests_prefix() {
        assert_eq!(
            manifest_object_key("archives", "node-1", "2026-11-03", "chunk-1"),
            "archives/node-1/manifests/2026-11-03/chunk-1.json"
        );
        assert_eq!(
            manifest_object_key("/archives/", "node-1", "2026-11-03", "chunk-1"),
            "archives/node-1/manifests/2026-11-03/chunk-1.json"
        );
        assert_eq!(
            manifest_object_key("", "node-1", "2026-11-03", "chunk-1"),
            "node-1/manifests/2026-11-03/chunk-1.json"
        );
    }

    #[tokio::test]
    async fn index_keys_include_actual_files() {
        let dir = std::env::temp_dir().join(format!("bmas-index-test-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(dir.join("db")).await.unwrap();
        tokio::fs::write(dir.join("db/data.mdb"), b"data")
            .await
            .unwrap();
        tokio::fs::write(dir.join("db/lock.mdb"), b"lock")
            .await
            .unwrap();

        let objects = index_object_keys(&dir, "archives/node/stream/day/chunk")
            .await
            .unwrap();

        let keys: Vec<_> = objects
            .iter()
            .map(|object| object.object_key.as_str())
            .collect();

        assert_eq!(
            keys,
            vec![
                "archives/node/stream/day/chunk/index/db/data.mdb",
                "archives/node/stream/day/chunk/index/db/lock.mdb"
            ]
        );
    }
}
