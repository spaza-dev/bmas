use crate::{
    api::AppState,
    archive::{self, Manifest},
    indexer,
    recorder::{Capture, CompletedSegment},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    process::Command,
    sync::{mpsc, Mutex, Semaphore},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArchiveJob {
    chunk_id: String,
    stream_id: String,
    stream_name: String,
    external_stream_id: Option<String>,
    archive_audio: bool,
    metadata: serde_json::Value,
    started_at: String,
    ended_at: String,
    segment_path: String,
    work_root: String,
    #[serde(default)]
    retry_count: u32,
}

fn sanitize_stream_name(name: &str) -> String {
    let sanitized = name
        .trim()
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();

    let sanitized = sanitized
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    if sanitized.is_empty() {
        "unknown-stream".to_string()
    } else {
        sanitized
    }
}

fn archive_name(stream_name: &str, started_at: DateTime<Utc>) -> String {
    let stream_name = sanitize_stream_name(stream_name);
    let timestamp = started_at.format("%Y-%m-%dT%H-%M-%SZ").to_string();
    format!("{}_{}", stream_name, timestamp)
}

fn archive_key(prefix: &str, node_id: &str, stream_id: &str, day: &str, chunk_id: &str) -> String {
    let prefix = prefix.trim_matches('/');

    if prefix.is_empty() {
        format!("{}/{}/{}/{}", node_id, stream_id, day, chunk_id)
    } else {
        format!("{}/{}/{}/{}/{}", prefix, node_id, stream_id, day, chunk_id)
    }
}

fn stream_input(stream: &crate::db::Stream) -> Option<&str> {
    stream
        .input
        .as_deref()
        .or_else(|| (!stream.url.is_empty()).then_some(stream.url.as_str()))
        .filter(|value| !value.is_empty())
}

fn stream_chunk_seconds(stream: &crate::db::Stream, default: u64) -> u64 {
    stream
        .chunk_seconds
        .map(|value| value as u64)
        .unwrap_or(default)
}

pub async fn run(state: Arc<AppState>, shutdown: CancellationToken) {
    cleanup_old_directories(
        PathBuf::from(&state.config.node.data_dir).join("work"),
        state.config.limits.work_retention_hours,
    )
    .await;

    if state.config.limits.local_retention_days > 0 {
        cleanup_old_directories(
            PathBuf::from(&state.config.node.data_dir).join("archives"),
            state.config.limits.local_retention_days * 24,
        )
        .await;
    }

    let (job_tx, job_rx) = mpsc::channel::<ArchiveJob>(100);
    let job_rx = Arc::new(Mutex::new(job_rx));

    let mut tasks = JoinSet::new();

    for worker in 0..state.config.indexing.workers {
        let state = state.clone();
        let shutdown = shutdown.clone();
        let job_rx = job_rx.clone();
        tasks.spawn(async move {
            archive_worker(state, job_rx, shutdown, format!("worker-{}", worker)).await;
        });
    }

    recover_pending_jobs(&state, &job_tx).await;

    let stream_slots = Arc::new(Semaphore::new(state.config.node.max_concurrent_streams));
    let active = Arc::new(Mutex::new(HashSet::new()));

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match state.db.streams().await {
            Ok(streams) => {
                for stream in streams.into_iter().filter(|stream| stream.enabled == 1) {
                    let mut active_set = active.lock().await;

                    if active_set.contains(&stream.id) {
                        continue;
                    }

                    active_set.insert(stream.id.clone());
                    drop(active_set);

                    let state = state.clone();
                    let slots = stream_slots.clone();
                    let shutdown = shutdown.clone();
                    let active = active.clone();
                    let job_tx = job_tx.clone();
                    let stream_id = stream.id.clone();

                    tasks.spawn(async move {
                        let permit = tokio::select! {
                            permit = slots.acquire_owned() => permit,
                            _ = shutdown.cancelled() => {
                                active.lock().await.remove(&stream_id);
                                return;
                            }
                        };

                        if permit.is_err() {
                            active.lock().await.remove(&stream_id);
                            return;
                        }

                        if let Err(error) = run_stream(state, stream, shutdown, job_tx).await {
                            error!(
                                stream_id = %stream_id,
                                error = %error,
                                "Stream worker failed"
                            );
                        }

                        drop(permit);
                        active.lock().await.remove(&stream_id);
                    });
                }
            }
            Err(error) => {
                error!(error = %error, "Cannot read stream registry");
            }
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(5)) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    // Stop producers first, then let workers drain what they can.
    drop(job_tx);

    let _ = timeout(Duration::from_secs(30), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
}

async fn run_stream(
    state: Arc<AppState>,
    mut stream: crate::db::Stream,
    shutdown: CancellationToken,
    job_tx: mpsc::Sender<ArchiveJob>,
) -> Result<()> {
    let stream_id = stream.id.clone();

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let current = state.db.get_stream(&stream_id).await?;

        match current {
            Some(current) if current.enabled == 1 => {
                stream = current;
            }
            _ => {
                info!(stream_id = %stream_id, "Stream disabled");
                break;
            }
        }

        let chunk_seconds = stream_chunk_seconds(&stream, state.config.recording.chunk_seconds);

        let Some(input) = stream_input(&stream) else {
            warn!(stream_id = %stream_id, "Stream has no input configured");
            sleep(Duration::from_secs(
                state.config.node.reconnect_delay_seconds,
            ))
            .await;
            continue;
        };

        if !has_free_space(
            &state.config.node.data_dir,
            state.config.limits.min_free_gb,
        )
        .await
        {
            warn!(
                stream_id = %stream_id,
                min_free_gb = state.config.limits.min_free_gb,
                "Insufficient disk space; pausing capture"
            );
            tokio::select! {
                _ = sleep(Duration::from_secs(30)) => {}
                _ = shutdown.cancelled() => break,
            }
            continue;
        }

        let run_id = Uuid::new_v4().to_string();
        let capture_dir = PathBuf::from(&state.config.node.data_dir)
            .join("work")
            .join("capture")
            .join(&stream_id)
            .join(&run_id);

        tokio::fs::create_dir_all(&capture_dir)
            .await
            .with_context(|| format!("Creating capture directory {}", capture_dir.display()))?;

        let mut capture = match Capture::start(
            &state.config.recording.ffmpeg_path,
            &stream.input_type,
            input,
            &capture_dir,
            chunk_seconds,
            state.config.recording.sample_rate,
        )
        .await
        {
            Ok(capture) => capture,
            Err(error) => {
                warn!(stream_id = %stream_id, error = %error, "FFmpeg startup failed");
                let _ = tokio::fs::remove_dir_all(&capture_dir).await;
                tokio::select! {
                    _ = sleep(Duration::from_secs(state.config.node.reconnect_delay_seconds)) => {}
                    _ = shutdown.cancelled() => break,
                }
                continue;
            }
        };

        info!(
            stream_id = %stream_id,
            chunk_seconds,
            input_type = %stream.input_type,
            "Stream capture started"
        );

        let stall_seconds = state
            .config
            .recording
            .stall_timeout_seconds
            .max(chunk_seconds + 60);
        let mut disabled = false;

        loop {
            if shutdown.is_cancelled() {
                let _ = capture.child.kill().await;
                break;
            }

            let completed = capture.completed_chunks().await?;

            for segment in completed {
                if let Err(error) = enqueue_segment(&state, &stream, segment, &job_tx).await {
                    error!(
                        stream_id = %stream_id,
                        error = %error,
                        "Could not enqueue completed segment"
                    );
                }
            }

            if let Some(status) = capture.child.try_wait()? {
                let stderr = capture.stderr_tail().await;
                warn!(stream_id = %stream_id, %status, stderr = %stderr, "FFmpeg exited");
                break;
            }

            match state.db.get_stream(&stream_id).await? {
                Some(current) if current.enabled == 1 => {
                    let current_chunk_seconds =
                        stream_chunk_seconds(&current, state.config.recording.chunk_seconds);
                    let current_input = stream_input(&current);

                    if current.input_type != stream.input_type
                        || current_input != Some(input)
                        || current_chunk_seconds != chunk_seconds
                    {
                        info!(
                            stream_id = %stream_id,
                            "Stream capture settings changed; restarting capture"
                        );
                        let _ = capture.child.kill().await;
                        break;
                    }
                }
                _ => {
                    disabled = true;
                    let _ = capture.child.kill().await;
                    break;
                }
            }

            if capture.last_activity.elapsed() > Duration::from_secs(stall_seconds) {
                let stderr = capture.stderr_tail().await;
                warn!(
                    stream_id = %stream_id,
                    stderr = %stderr,
                    "FFmpeg produced no segments within the stall timeout"
                );
                let _ = capture.child.kill().await;
                break;
            }

            tokio::select! {
                _ = sleep(Duration::from_secs(2)) => {}
                _ = shutdown.cancelled() => {
                    let _ = capture.child.kill().await;
                    break;
                }
            }
        }

        let _ = capture.child.kill().await;

        if let Ok(remaining) = capture.completed_chunks().await {
            for segment in remaining {
                if let Err(error) = enqueue_segment(&state, &stream, segment, &job_tx).await {
                    error!(
                        stream_id = %stream_id,
                        error = %error,
                        "Could not enqueue final segment"
                    );
                }
            }
        }

        let _ = tokio::fs::remove_file(&capture.list_path).await;
        let _ = tokio::fs::remove_dir_all(&capture_dir).await;

        if disabled || shutdown.is_cancelled() {
            break;
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(state.config.node.reconnect_delay_seconds)) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    Ok(())
}

async fn enqueue_segment(
    state: &Arc<AppState>,
    stream: &crate::db::Stream,
    segment: CompletedSegment,
    job_tx: &mpsc::Sender<ArchiveJob>,
) -> Result<()> {
    let chunk_id = Uuid::new_v4().to_string();
    let work_root = PathBuf::from(&state.config.node.data_dir)
        .join("work")
        .join("pending")
        .join(&chunk_id);

    tokio::fs::create_dir_all(&work_root)
        .await
        .with_context(|| format!("Creating work directory {}", work_root.display()))?;

    let segment_path = work_root.join("segment.wav");

    if let Err(rename_error) = tokio::fs::rename(&segment.path, &segment_path).await {
        tokio::fs::copy(&segment.path, &segment_path)
            .await
            .with_context(|| {
                format!(
                    "Moving segment {} into work directory (rename failed: {})",
                    segment.path.display(),
                    rename_error
                )
            })?;
        let _ = tokio::fs::remove_file(&segment.path).await;
    }

    let metadata = serde_json::from_str::<serde_json::Value>(&stream.metadata)
        .unwrap_or_else(|_| json!({}));

    let archive_audio = stream
        .archive_audio
        .map(|value| value == 1)
        .unwrap_or(state.config.recording.archive_audio);

    let job = ArchiveJob {
        chunk_id,
        stream_id: stream.id.clone(),
        stream_name: stream.name.clone(),
        external_stream_id: stream.external_id.clone(),
        archive_audio,
        metadata,
        started_at: segment.started_at.to_rfc3339(),
        ended_at: segment.ended_at.to_rfc3339(),
        segment_path: segment_path.to_string_lossy().to_string(),
        work_root: work_root.to_string_lossy().to_string(),
        retry_count: 0,
    };

    write_job(&work_root, &job).await?;

    job_tx
        .send(job)
        .await
        .context("Archive queue is closed; segment retained on disk for recovery")
}

async fn write_job(work_root: &Path, job: &ArchiveJob) -> Result<()> {
    let path = work_root.join("job.json");
    let content = serde_json::to_vec_pretty(job).context("Serializing archive job")?;
    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("Writing job file {}", path.display()))?;
    Ok(())
}

async fn recover_pending_jobs(state: &Arc<AppState>, job_tx: &mpsc::Sender<ArchiveJob>) {
    let root = PathBuf::from(&state.config.node.data_dir)
        .join("work")
        .join("pending");

    let Ok(mut entries) = tokio::fs::read_dir(&root).await else {
        return;
    };

    let mut recovered = 0usize;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let work_root = entry.path();

        if !work_root.is_dir() {
            continue;
        }

        let job_path = work_root.join("job.json");
        let Ok(content) = tokio::fs::read(&job_path).await else {
            let _ = tokio::fs::remove_dir_all(&work_root).await;
            continue;
        };

        let Ok(job) = serde_json::from_slice::<ArchiveJob>(&content) else {
            warn!(path = %job_path.display(), "Ignoring unreadable archive job");
            continue;
        };

        if !tokio::fs::try_exists(&job.segment_path).await.unwrap_or(false) {
            warn!(
                chunk_id = %job.chunk_id,
                "Dropping archive job whose segment is missing"
            );
            let _ = tokio::fs::remove_dir_all(&work_root).await;
            continue;
        }

        if job_tx.send(job).await.is_ok() {
            recovered += 1;
        }
    }

    if recovered > 0 {
        info!(recovered, "Recovered in-flight archive jobs");
    }
}

async fn archive_worker(
    state: Arc<AppState>,
    job_rx: Arc<Mutex<mpsc::Receiver<ArchiveJob>>>,
    shutdown: CancellationToken,
    worker_id: String,
) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let job = {
            let mut receiver = job_rx.lock().await;
            tokio::select! {
                job = receiver.recv() => job,
                _ = shutdown.cancelled() => None,
            }
        };

        let Some(mut job) = job else {
            if shutdown.is_cancelled() {
                break;
            }
            // Channel closed by the producer.
            break;
        };

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            match process_job(&state, &job).await {
                Ok(()) => break,
                Err(error) => {
                    let message = truncate_error(&format!("{:#}", error));
                    job.retry_count += 1;

                    if job.retry_count >= state.config.indexing.max_retries {
                        error!(
                            worker_id = %worker_id,
                            chunk_id = %job.chunk_id,
                            error = %message,
                            "Archive job failed permanently"
                        );
                        cleanup_failed_job(&state, &job).await;
                        break;
                    }

                    let delay = backoff_seconds(job.retry_count);
                    warn!(
                        worker_id = %worker_id,
                        chunk_id = %job.chunk_id,
                        error = %message,
                        retry_in_seconds = delay,
                        "Archive job failed; retrying"
                    );
                    let _ = write_job(Path::new(&job.work_root), &job).await;

                    tokio::select! {
                        _ = sleep(Duration::from_secs(delay)) => {}
                        _ = shutdown.cancelled() => break,
                    }
                }
            }
        }
    }
}

async fn cleanup_failed_job(state: &AppState, job: &ArchiveJob) {
    if state.config.node.keep_local_chunks {
        return;
    }

    let _ = tokio::fs::remove_dir_all(&job.work_root).await;
}

async fn process_job(state: &Arc<AppState>, job: &ArchiveJob) -> Result<()> {
    let segment_path = PathBuf::from(&job.segment_path);
    let work_root = PathBuf::from(&job.work_root);

    anyhow::ensure!(
        tokio::fs::try_exists(&segment_path).await.unwrap_or(false),
        "Segment is missing: {}",
        segment_path.display()
    );

    let started_at = DateTime::parse_from_rfc3339(&job.started_at)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let day = started_at.format("%Y-%m-%d").to_string();

    tokio::fs::create_dir_all(&work_root).await?;

    let identifier = format!("{}/{}/{}", job.stream_id, day, job.chunk_id);

    let index_path = indexer::index_chunk(
        &state.config.recording.olaf_path,
        &segment_path,
        &work_root,
        &identifier,
        state.config.indexing.threads,
        state.config.recording.olaf_timeout_seconds,
    )
    .await?;

    let _ = tokio::fs::remove_dir_all(work_root.join("olaf-home")).await;

    let audio_name = archive_name(&job.stream_name, started_at);
    let audio_filename = format!("{}.wav", audio_name);
    let stored_audio = if job.archive_audio {
        tokio::fs::copy(&segment_path, work_root.join(&audio_filename))
            .await
            .with_context(|| format!("Archiving audio {}", segment_path.display()))?;
        true
    } else {
        false
    };

    let duration = probe_duration(
        &state.config.recording.ffprobe_path,
        &segment_path,
        state.config.recording.ffprobe_timeout_seconds,
    )
    .await?;

    let key = archive_key(
        &state.config.storage.prefix,
        &state.config.node.id,
        &job.stream_id,
        &day,
        &job.chunk_id,
    );

    let index_objects = archive::index_object_keys(&index_path, &key).await?;
    let audio_object = stored_audio.then(|| format!("{}/{}", key, audio_filename));

    let manifest = Manifest {
        manifest_version: archive::MANIFEST_VERSION.to_string(),
        node_id: state.config.node.id.clone(),
        stream_id: job.stream_id.clone(),
        external_stream_id: job.external_stream_id.clone(),
        chunk_id: job.chunk_id.clone(),
        started_at: started_at.to_rfc3339(),
        completed_at: Utc::now().to_rfc3339(),
        duration_seconds: duration,
        audio_archived: stored_audio,
        index_format: "olaf-lmdb".to_string(),
        olaf_identifier: identifier,
        status: "complete".to_string(),
        index_objects,
        audio_object,
        metadata: job.metadata.clone(),
    };

    archive::write_manifest(&work_root, &manifest).await?;

    if let Some(store) = state.store.as_ref() {
        let uploaded_keys = store.upload_directory(&work_root, &key).await?;
        info!(
            chunk_id = %job.chunk_id,
            uploaded_files = uploaded_keys.len(),
            "Archive payload uploaded"
        );

        let manifest_key = store.upload_manifest(&manifest).await?;
        info!(
            chunk_id = %job.chunk_id,
            manifest_key = %manifest_key,
            "Archive manifest published"
        );
    } else {
        info!(
            chunk_id = %job.chunk_id,
            "Storage disabled; archive retained locally"
        );
    }

    let _ = tokio::fs::remove_file(&segment_path).await;

    if !state.config.node.keep_local_chunks && state.store.is_some() {
        let _ = tokio::fs::remove_dir_all(&work_root).await;
    }

    info!(
        stream_id = %job.stream_id,
        chunk_id = %job.chunk_id,
        key = %key,
        duration_seconds = duration,
        ended_at = %job.ended_at,
        "Chunk archived successfully"
    );

    Ok(())
}

async fn probe_duration(ffprobe: &str, audio_path: &Path, timeout_seconds: u64) -> Result<i64> {
    let mut command = Command::new(ffprobe);
    command
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(audio_path)
        .kill_on_drop(true);

    let output = timeout(Duration::from_secs(timeout_seconds), command.output())
        .await
        .context("FFprobe timed out")?
        .context("Running FFprobe")?;

    anyhow::ensure!(
        output.status.success(),
        "FFprobe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let value = String::from_utf8(output.stdout).context("Invalid FFprobe output")?;
    let duration: f64 = value
        .trim()
        .parse()
        .context("Cannot parse audio duration")?;

    anyhow::ensure!(
        duration.is_finite() && duration > 0.0,
        "Invalid audio duration: {}",
        duration
    );

    Ok(duration.round() as i64)
}

fn backoff_seconds(retry_count: u32) -> u64 {
    let exponent = retry_count.min(10);
    let base = 5u64.saturating_mul(2u64.saturating_pow(exponent)).min(3600);
    let jitter = rand::thread_rng().gen_range(0..=base / 2);
    base + jitter
}

fn truncate_error(error: &str) -> String {
    error.chars().take(500).collect()
}

async fn cleanup_old_directories(root: PathBuf, max_age_hours: u64) {
    if max_age_hours == 0 {
        return;
    }

    let _ = tokio::task::spawn_blocking(move || {
        let max_age = std::time::Duration::from_secs(max_age_hours * 3600);
        cleanup_directory_sync(&root, max_age, true);
    })
    .await;
}

fn cleanup_directory_sync(root: &Path, max_age: std::time::Duration, remove_empty: bool) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        cleanup_directory_sync(&path, max_age, remove_empty);

        let age = entry
            .metadata()
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| std::time::SystemTime::now().duration_since(modified).ok());

        if age.is_some_and(|age| age > max_age) {
            let _ = std::fs::remove_dir_all(&path);
        } else if remove_empty {
            let _ = std::fs::remove_dir(&path);
        }
    }
}

async fn has_free_space(data_dir: &str, min_free_gb: u64) -> bool {
    if min_free_gb == 0 {
        return true;
    }

    let output = Command::new("df")
        .args(["-Pk"])
        .arg(data_dir)
        .output()
        .await;

    let Ok(output) = output else {
        return true;
    };

    if !output.status.success() {
        return true;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let Some(line) = text.lines().nth(1) else {
        return true;
    };
    let Some(available_kb) = line.split_whitespace().nth(3) else {
        return true;
    };
    let Ok(available_kb) = available_kb.parse::<u64>() else {
        return true;
    };

    available_kb / 1024 / 1024 >= min_free_gb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_stream_names() {
        assert_eq!(sanitize_stream_name("Classic 105"), "classic-105");
        assert_eq!(sanitize_stream_name("NRG Radio Kenya!"), "nrg-radio-kenya");
        assert_eq!(sanitize_stream_name("!!!"), "unknown-stream");
    }

    #[test]
    fn archive_keys_include_node_and_prefix() {
        let key = archive_key(
            "archives",
            "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b",
            "stream-1",
            "2026-11-03",
            "chunk-1",
        );

        assert_eq!(
            key,
            "archives/9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b/stream-1/2026-11-03/chunk-1"
        );
    }

    #[test]
    fn archive_keys_trim_prefix_slashes() {
        let key = archive_key(
            "/archives/",
            "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b",
            "stream-1",
            "2026-11-03",
            "chunk-1",
        );

        assert_eq!(
            key,
            "archives/9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b/stream-1/2026-11-03/chunk-1"
        );
    }

    #[test]
    fn backoff_is_bounded_and_jittered() {
        for retry in 0..20 {
            let delay = backoff_seconds(retry);
            assert!(delay >= 5);
            assert!(delay <= 5400);
        }
    }

    #[test]
    fn archive_job_round_trips_through_json() {
        let job = ArchiveJob {
            chunk_id: "chunk-1".to_string(),
            stream_id: "stream-1".to_string(),
            stream_name: "Example FM".to_string(),
            external_stream_id: None,
            archive_audio: true,
            metadata: json!({"city": "Nairobi"}),
            started_at: "2026-11-03T09:00:00+00:00".to_string(),
            ended_at: "2026-11-03T09:05:00+00:00".to_string(),
            segment_path: "/tmp/segment.wav".to_string(),
            work_root: "/tmp/work".to_string(),
            retry_count: 0,
        };

        let encoded = serde_json::to_string(&job).unwrap();
        let decoded: ArchiveJob = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.chunk_id, job.chunk_id);
    }
}
