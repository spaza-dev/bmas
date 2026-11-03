use anyhow::{Context, Result};
use serde::Deserialize;
use std::{env, fs, path::Path};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    pub recording: RecordingConfig,
    pub indexing: IndexingConfig,
    pub r2: R2Config,
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    #[serde(default)]
    pub central: CentralConfig,
    #[serde(default)]
    pub storage: StorageConfig,
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

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R2Config {
    pub enabled: bool,
    pub account_id: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub endpoint_url: String,
}

#[derive(Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default)]
    pub token: String,
    #[serde(default = "default_heartbeat_interval")]
    pub interval_seconds: u64,
}

#[derive(Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CentralConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub node_token: String,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "default_min_free_gb")]
    pub min_free_gb: u64,
    #[serde(default)]
    pub max_enabled_streams: usize,
    #[serde(default = "default_work_retention")]
    pub work_retention_hours: u64,
    #[serde(default)]
    pub local_retention_days: u64,
}

impl Default for StorageConfig {
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
fn default_heartbeat_interval() -> u64 {
    60
}
fn default_min_free_gb() -> u64 {
    2
}
fn default_work_retention() -> u64 {
    24
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

        if config.r2.enabled {
            anyhow::ensure!(!config.r2.bucket.is_empty(), "R2 bucket is required");
            anyhow::ensure!(
                !config.r2.account_id.is_empty(),
                "R2 account ID is required"
            );
            anyhow::ensure!(
                !config.r2.access_key_id.is_empty(),
                "R2 access key is required"
            );
            anyhow::ensure!(
                !config.r2.secret_access_key.is_empty(),
                "R2 secret is required"
            );
        }

        if config.heartbeat.enabled {
            anyhow::ensure!(
                config.heartbeat.webhook_url.starts_with("https://"),
                "Heartbeat webhook must use HTTPS"
            );
        }

        if config.central.enabled {
            anyhow::ensure!(
                config.central.base_url.starts_with("https://")
                    || config.central.base_url.starts_with("http://127.0.0.1")
                    || config.central.base_url.starts_with("http://localhost"),
                "Central API base URL must use HTTPS"
            );
            anyhow::ensure!(
                config.central.node_token.len() >= 32,
                "Central API node token must be at least 32 characters"
            );
            anyhow::ensure!(
                uuid::Uuid::parse_str(&config.node.id).is_ok(),
                "Node ID must be the UUID assigned by the central API"
            );

            if config.r2.enabled {
                anyhow::ensure!(
                    config.r2.prefix.trim_matches('/') == "archives",
                    "R2 prefix must be 'archives' when central integration is enabled"
                );
            }
        }

        Ok(config)
    }

    pub fn max_enabled_streams(&self) -> usize {
        if self.storage.max_enabled_streams == 0 {
            self.node.max_concurrent_streams
        } else {
            self.storage.max_enabled_streams
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
