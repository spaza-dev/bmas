mod api;
mod archive;
mod auth;
mod config;
mod db;
mod health;
mod indexer;
mod monitor;
mod recorder;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use db::{Database, NewStream};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "bmas",
    version,
    about = "Broadcast Monitoring & Archiving Service"
)]
struct Cli {
    #[arg(long, global = true, default_value = "/etc/bmas/config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the capture daemon and HTTP API.
    Serve,
    /// Inspect or validate configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Manage the local stream registry.
    Stream {
        #[command(subcommand)]
        action: StreamAction,
    },
    /// Print node status.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Check local binaries, directories and storage configuration.
    Doctor,
}

#[derive(Subcommand)]
enum ConfigAction {
    Validate,
}

#[derive(Subcommand)]
enum StreamAction {
    Add {
        #[arg(long)]
        name: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long, default_value = "url")]
        input_type: String,
        #[arg(long)]
        input: Option<String>,
        #[arg(long)]
        external_id: Option<String>,
        #[arg(long, default_value = "internet_radio")]
        source_type: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        chunk_seconds: Option<i64>,
        #[arg(long)]
        archive_audio: Option<bool>,
        #[arg(long)]
        disabled: bool,
        #[arg(long, default_value = "{}")]
        metadata: String,
    },
    List {
        #[arg(long)]
        json: bool,
    },
    Show {
        id_or_external_id: String,
    },
    Start {
        id_or_external_id: String,
    },
    Stop {
        id_or_external_id: String,
    },
    Remove {
        id_or_external_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = config::Config::load(&cli.config)?;

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => serve(config).await,
        Command::Config {
            action: ConfigAction::Validate,
        } => {
            println!("Configuration is valid.");
            Ok(())
        }
        Command::Stream { action } => stream_command(config, action).await,
        Command::Status { json } => status_command(config, json).await,
        Command::Doctor => doctor_command(config).await,
    }
}

async fn serve(config: config::Config) -> Result<()> {
    std::fs::create_dir_all(&config.node.data_dir).context("creating BMAS data directory")?;

    let db = Database::open(&config.node.data_dir).await?;
    let store = archive::ArchiveStore::connect(&config.storage).await?;
    let store = store.map(Arc::new);

    let state = Arc::new(api::AppState {
        config: config.clone(),
        db,
        store,
    });

    tracing::info!(
        node_id = %config.node.id,
        storage_enabled = config.storage.enabled,
        "Starting BMAS"
    );

    let shutdown = CancellationToken::new();

    let monitor_state = state.clone();
    let monitor_shutdown = shutdown.clone();
    let monitor_handle = tokio::spawn(async move {
        monitor::run(monitor_state, monitor_shutdown).await;
    });

    let app = api::router(state);

    let listener = tokio::net::TcpListener::bind(&config.node.listen)
        .await
        .context("binding HTTP listener")?;

    tracing::info!(
        address = %config.node.listen,
        "BMAS API listening"
    );

    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("Shutdown signal received");
        signal_shutdown.cancel();
    });

    let serve_shutdown = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            serve_shutdown.cancelled().await;
        })
        .await?;

    shutdown.cancel();

    let _ = tokio::time::timeout(Duration::from_secs(20), monitor_handle).await;

    Ok(())
}

async fn stream_command(config: config::Config, action: StreamAction) -> Result<()> {
    std::fs::create_dir_all(&config.node.data_dir).context("creating BMAS data directory")?;
    let db = Database::open(&config.node.data_dir).await?;

    match action {
        StreamAction::Add {
            name,
            url,
            input_type,
            input,
            external_id,
            source_type,
            description,
            chunk_seconds,
            archive_audio,
            disabled,
            metadata,
        } => {
            let name = name.trim();
            anyhow::ensure!(!name.is_empty(), "Stream name is required");
            anyhow::ensure!(name.len() <= 200, "Stream name must be at most 200 characters");
            anyhow::ensure!(
                input_type == "url" || input_type == "audio_device",
                "input_type must be 'url' or 'audio_device'"
            );

            if let Some(chunk_seconds) = chunk_seconds {
                anyhow::ensure!(
                    (30..=86_400).contains(&chunk_seconds),
                    "chunk_seconds must be between 30 and 86400"
                );
            }

            let metadata_value: serde_json::Value =
                serde_json::from_str(&metadata).context("metadata must be valid JSON")?;
            anyhow::ensure!(
                metadata_value.is_object(),
                "metadata must be a JSON object"
            );
            let metadata_string = serde_json::to_string(&metadata_value)?;

            let url_filtered = url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let input_filtered = input
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());

            if input_type == "url" {
                let candidate = url_filtered.or(input_filtered).unwrap_or("");
                anyhow::ensure!(
                    ["http://", "https://", "rtsp://"]
                        .iter()
                        .any(|scheme| candidate.starts_with(scheme)),
                    "A valid HTTP, HTTPS or RTSP URL is required for URL streams"
                );
            } else {
                anyhow::ensure!(
                    input_filtered.is_some(),
                    "An input device is required for audio_device streams"
                );
            }

            if let Some(external_id) = external_id.as_deref() {
                if let Some(existing) = db.get_stream_by_external_id(external_id).await? {
                    println!("{}", serde_json::to_string_pretty(&existing)?);
                    return Ok(());
                }
            }

            let stream = db
                .create_stream(NewStream {
                    name,
                    url: url_filtered,
                    external_id: external_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty()),
                    source_type: source_type.trim(),
                    description: description.as_deref(),
                    metadata: &metadata_string,
                    input_type: &input_type,
                    input: input_filtered,
                    chunk_seconds,
                    archive_audio,
                    enabled: Some(!disabled),
                })
                .await?;

            println!("{}", serde_json::to_string_pretty(&stream)?);
        }
        StreamAction::List { json } => {
            let streams = db.streams().await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&streams)?);
            } else {
                println!(
                    "{:<38} {:<8} {:<24} {:<16} {}",
                    "ID", "ENABLED", "NAME", "SOURCE", "EXTERNAL ID"
                );
                for stream in streams {
                    println!(
                        "{:<38} {:<8} {:<24} {:<16} {}",
                        stream.id,
                        if stream.enabled == 1 { "yes" } else { "no" },
                        stream.name,
                        stream.input_type,
                        stream.external_id.unwrap_or_default()
                    );
                }
            }
        }
        StreamAction::Show { id_or_external_id } => {
            let stream = require_stream(&db, &id_or_external_id).await?;
            println!("{}", serde_json::to_string_pretty(&stream)?);
        }
        StreamAction::Start { id_or_external_id } => {
            let stream = require_stream(&db, &id_or_external_id).await?;
            db.set_enabled(&stream.id, true).await?;
            println!("Started {}", stream.id);
        }
        StreamAction::Stop { id_or_external_id } => {
            let stream = require_stream(&db, &id_or_external_id).await?;
            db.set_enabled(&stream.id, false).await?;
            println!("Stopped {}", stream.id);
        }
        StreamAction::Remove { id_or_external_id } => {
            let stream = require_stream(&db, &id_or_external_id).await?;
            db.delete_stream(&stream.id).await?;
            println!("Removed {}", stream.id);
        }
    }

    Ok(())
}

async fn require_stream(db: &Database, id_or_external_id: &str) -> Result<db::Stream> {
    db.resolve_stream(id_or_external_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Stream '{}' not found", id_or_external_id))
}

async fn status_command(config: config::Config, json: bool) -> Result<()> {
    std::fs::create_dir_all(&config.node.data_dir).context("creating BMAS data directory")?;
    let db = Database::open(&config.node.data_dir).await?;

    let total_streams = db.total_stream_count().await?;
    let enabled_streams = db.active_stream_count().await?;
    let status = serde_json::json!({
        "node_id": config.node.id,
        "version": env!("CARGO_PKG_VERSION"),
        "status": "healthy",
        "storage_enabled": config.storage.enabled,
        "storage_backend": config.storage.backend,
        "total_streams": total_streams,
        "enabled_streams": enabled_streams,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!("Node:          {}", config.node.id);
        println!("Version:       {}", env!("CARGO_PKG_VERSION"));
        println!(
            "Storage:       {} ({})",
            if config.storage.enabled {
                "enabled"
            } else {
                "disabled"
            },
            config.storage.backend
        );
        println!("Streams:       {} total, {} enabled", total_streams, enabled_streams);
    }

    Ok(())
}

async fn doctor_command(config: config::Config) -> Result<()> {
    let mut failures = 0usize;

    for (label, path) in [
        ("ffmpeg", &config.recording.ffmpeg_path),
        ("ffprobe", &config.recording.ffprobe_path),
        ("olaf", &config.recording.olaf_path),
    ] {
        match tokio::process::Command::new(path)
            .arg("--version")
            .output()
            .await
        {
            Ok(output) if output.status.success() => println!("ok   {} -> {}", label, path),
            _ => {
                failures += 1;
                println!("FAIL {} -> {} (not executable)", label, path);
            }
        }
    }

    match std::fs::create_dir_all(&config.node.data_dir) {
        Ok(()) => println!("ok   data_dir -> {}", config.node.data_dir),
        Err(error) => {
            failures += 1;
            println!("FAIL data_dir -> {} ({})", config.node.data_dir, error);
        }
    }

    match archive::ArchiveStore::connect(&config.storage).await {
        Ok(Some(_)) => println!("ok   storage -> {} ({})", config.storage.bucket, config.storage.backend),
        Ok(None) => println!("warn storage disabled; archives stay local"),
        Err(error) => {
            failures += 1;
            println!("FAIL storage -> {}", error);
        }
    }

    anyhow::ensure!(failures == 0, "{} doctor check(s) failed", failures);
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
