mod api;
mod archive;
mod auth;
mod central;
mod config;
mod db;
mod health;
mod heartbeat;
mod indexer;
mod monitor;
mod recorder;

use anyhow::{Context, Result};
use clap::Parser;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "bmas",
    version,
    about = "Broadcast Monitoring & Archiving Service"
)]
struct Args {
    #[arg(long, default_value = "/etc/bmas/config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let config = config::Config::load(&args.config)?;

    std::fs::create_dir_all(&config.node.data_dir).context("creating BMAS data directory")?;

    let db = db::Database::open(&config.node.data_dir).await?;

    let state = Arc::new(api::AppState {
        config: config.clone(),
        db,
    });

    tracing::info!(
        node_id = %config.node.id,
        "Starting BMAS"
    );

    let shutdown = CancellationToken::new();

    let monitor_state = state.clone();
    let monitor_shutdown = shutdown.clone();
    let monitor_handle = tokio::spawn(async move {
        monitor::run(monitor_state, monitor_shutdown).await;
    });

    heartbeat::start(state.clone(), shutdown.clone());
    central::start(state.clone(), shutdown.clone());

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
