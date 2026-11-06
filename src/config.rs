use anyhow::{Context, Result};
use serde::Deserialize;
use std::{env, fs, path::Path};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    pub recording: RecordingConfig,
    pub indexing: IndexingConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub id: String,
    pub listen: String,
    pub data_dir: String,
    pub api_token: String,
    pub max_concurrent_streams: usize,
    pub reconnect_delay_seconds: u64,
    #[serde(default)]
    pub keep_local_chunks: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingConfig {
    pub ffmpeg_path: String,
    pub ffprobe_path: String,
    pub olaf_path: String,
    pub chunk_seconds: u64,
    pub sample_rate: u32,
    pub archive_audio: bool,
    #[serde(default = "default_ffprobe_timeout")]
    pub ffprobe_timeout_seconds: u64,
    #[serde(default = "default_olaf_timeout")]
    pub olaf_timeout_seconds: u64,
    #[serde(default = "default_stall_timeout")]
    pub stall_timeout_seconds: u64,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingConfig {
    pub threads: usize,
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

/// S3-compatible object storage used for archive payloads and manifests.
///
/// `backend = "r2"` derives the Cloudflare R2 endpoint from `account_id` when
/// `endpoint_url` is empty. `backend = "s3"` supports AWS S3 and any
/// S3-compatible service (MinIO, Backblaze B2, etc.).
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_storage_backend")]
    pub backend: String,
    #[serde(default)]
    pub endpoint_url: String,
    #[serde(default = "default_storage_region")]
    pub region: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default = "default_storage_prefix")]
    pub prefix: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    #[serde(default)]
    pub force_path_style: Option<bool>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    #[serde(default = "default_min_free_gb")]
    pub min_free_gb: u64,
    /// 0 means "same as node.max_concurrent_streams".
    #[serde(default)]
    pub max_enabled_streams: usize,
    #[serde(default = "default_work_retention")]
    pub work_retention_hours: u64,
    #[serde(default)]
    pub local_retention_days: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            min_free_gb: default_min_free_gb(),
            max_enabled_streams: 0,
            work_retention_hours: default_work_retention(),
            local_retention_days: 0,
        }
    }
}

fn default_ffprobe_timeout() -> u64 {
    60
}
fn default_olaf_timeout() -> u64 {
    900
}
fn default_stall_timeout() -> u64 {
    120
}
fn default_workers() -> usize {
    2
}
fn default_max_retries() -> u32 {
    5
}
fn default_min_free_gb() -> u64 {
    2
}
fn default_work_retention() -> u64 {
    24
}
fn default_storage_backend() -> String {
    "s3".to_string()
}
fn default_storage_region() -> String {
    "auto".to_string()
}
fn default_storage_prefix() -> String {
    "archives".to_string()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Cannot read {}", path.display()))?;

        let content = expand_env(&content)?;

        let config: Self = toml::from_str(&content).context("Invalid BMAS configuration")?;

        anyhow::ensure!(
            config.node.api_token.len() >= 32
                && config.node.api_token != "REPLACE_WITH_SECURE_TOKEN",
            "Configure a secure API token of at least 32 characters"
        );

        anyhow::ensure!(
            config.recording.chunk_seconds >= 30,
            "Chunk duration must be at least 30 seconds"
        );

        anyhow::ensure!(
            config.node.max_concurrent_streams > 0,
            "max_concurrent_streams must be greater than zero"
        );

        anyhow::ensure!(
            config.indexing.workers > 0,
            "indexing.workers must be greater than zero"
        );

        let backend = config.storage.backend.trim().to_ascii_lowercase();
        anyhow::ensure!(
            backend == "s3" || backend == "r2",
            "storage.backend must be 's3' or 'r2'"
        );

        if config.storage.enabled {
            anyhow::ensure!(
                !config.storage.bucket.trim().is_empty(),
                "storage bucket is required when storage is enabled"
            );
            anyhow::ensure!(
                !config.storage.access_key_id.trim().is_empty(),
                "storage access key is required when storage is enabled"
            );
            anyhow::ensure!(
                !config.storage.secret_access_key.trim().is_empty(),
                "storage secret is required when storage is enabled"
            );

            if backend == "r2" && config.storage.endpoint_url.trim().is_empty() {
                anyhow::ensure!(
                    !config.storage.account_id.trim().is_empty(),
                    "storage.account_id or storage.endpoint_url is required for R2"
                );
            }
        }

        Ok(config)
    }

    pub fn max_enabled_streams(&self) -> usize {
        if self.limits.max_enabled_streams == 0 {
            self.node.max_concurrent_streams
        } else {
            self.limits.max_enabled_streams
        }
    }
}

fn expand_env(content: &str) -> Result<String> {
    let mut output = String::with_capacity(content.len());
    let mut rest = content;

    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];

        let Some(end) = after.find('}') else {
            anyhow::bail!("Unterminated environment variable in configuration");
        };

        let name = &after[..end];

        anyhow::ensure!(
            !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "Invalid environment variable name '{}' in configuration",
            name
        );

        let value = env::var(name).with_context(|| {
            format!(
                "Missing environment variable '{}' referenced by configuration",
                name
            )
        })?;

        output.push_str(&value);
        rest = &after[end + 1..];
    }

    output.push_str(rest);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_environment_variables() {
        env::set_var("BMAS_TEST_TOKEN", "abc123");
        let expanded = expand_env("token = \"${BMAS_TEST_TOKEN}\"").unwrap();
        assert_eq!(expanded, "token = \"abc123\"");
    }

    #[test]
    fn rejects_unterminated_environment_variable() {
        assert!(expand_env("token = \"${NOPE\"").is_err());
    }
}
