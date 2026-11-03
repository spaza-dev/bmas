use anyhow::{Context, Result};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

pub async fn index_chunk(
    olaf_path: &str,
    audio_path: &Path,
    archive_dir: &Path,
    identifier: &str,
    threads: usize,
    timeout_seconds: u64,
) -> Result<PathBuf> {
    let home = archive_dir.join("olaf-home");

    if tokio::fs::try_exists(&home).await? {
        tokio::fs::remove_dir_all(&home).await?;
    }

    tokio::fs::create_dir_all(&home).await?;

    let mut command = Command::new(olaf_path);
    command
        .arg("store")
        .arg("--threads")
        .arg(threads.max(1).to_string())
        .arg("--with-ids")
        .arg(audio_path)
        .arg(identifier)
        .env("HOME", &home)
        .stdin(Stdio::null())
        .kill_on_drop(true);

    let output = tokio::time::timeout(Duration::from_secs(timeout_seconds), command.output())
        .await
        .context("OLAF indexing timed out")?
        .context("executing OLAF store")?;

    if !output.status.success() {
        anyhow::bail!(
            "OLAF indexing failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let source_db = home.join(".olaf");

    if !source_db.join("db").exists() {
        anyhow::bail!("OLAF did not create its expected database directory");
    }

    let index_path = archive_dir.join("index");

    if tokio::fs::try_exists(&index_path).await? {
        tokio::fs::remove_dir_all(&index_path).await?;
    }

    tokio::fs::rename(&source_db, &index_path).await?;

    Ok(index_path)
}
