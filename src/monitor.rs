use crate::{
    api::AppState,
    archive::{self, IndexObject, Manifest},
    indexer,
    recorder::{Capture, CompletedSegment},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use serde_json::json;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    process::Command,
    sync::{Mutex, Semaphore},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

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
        format!("archives/{}/{}/{}/{}", node_id, stream_id, day, chunk_id)
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
    if let Err(error) = state.db.recover_interrupted_chunks().await {
        error!(error = %error, "Could not recover interrupted chunks");
    }

    cleanup_old_directories(
        PathBuf::from(&state.config.node.data_dir).join("work"),
        state.config.storage.work_retention_hours,
    )
    .await;

    if state.config.storage.local_retention_days > 0 {
        cleanup_old_directories(
            PathBuf::from(&state.config.node.data_dir).join("archives"),
            state.config.storage.local_retention_days * 24,
        )
        .await;
    }

    let stream_slots = Arc::new(Semaphore::new(state.config.node.max_concurrent_streams));
    let active = Arc::new(Mutex::new(HashSet::new()));
    let mut tasks = JoinSet::new();

    for worker in 0..state.config.indexing.workers {
        let state = state.clone();
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            worker_loop(state, shutdown, format!("worker-{}", worker)).await;
        });
    }

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

                        if let Err(error) = run_stream(state, stream, shutdown).await {
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

    let _ = timeout(Duration::from_secs(15), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
}

async fn run_stream(
    state: Arc<AppState>,
    mut stream: crate::db::Stream,
    shutdown: CancellationToken,
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
            state.config.storage.min_free_gb,
        )
        .await
        {
            warn!(
                stream_id = %stream_id,
                min_free_gb = state.config.storage.min_free_gb,
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
                if let Err(error) = enqueue_segment(&state, &stream, segment).await {
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
                if let Err(error) = enqueue_segment(&state, &stream, segment).await {
                    error!(
                        stream_id = %stream_id,
                        error = %error,
                        "Could not enqueue final segment"
                    );
                }
            }
        }

        let _ = tokio::fs::remove_file(&capture.list_path).await;
        let _ = tokio::fs::remove_dir(&capture_dir).await;

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
) -> Result<()> {
    let chunk_id = Uuid::new_v4().to_string();
    let day = segment.started_at.format("%Y-%m-%d").to_string();
    let work_root = PathBuf::from(&state.config.node.data_dir)
        .join("archives")
        .join(&stream.id)
        .join(&day)
        .join(&chunk_id);

    tokio::fs::create_dir_all(&work_root)
        .await
        .with_context(|| format!("Creating archive directory {}", work_root.display()))?;

    state
        .db
        .create_chunk(
            &chunk_id,
            &stream.id,
            &segment.started_at.to_rfc3339(),
            &work_root.to_string_lossy(),
            &segment.path.to_string_lossy(),
        )
        .await?;

    let _ = segment.ended_at;
    Ok(())
}

async fn worker_loop(state: Arc<AppState>, shutdown: CancellationToken, worker_id: String) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match state.db.claim_chunk(&worker_id, 3600).await {
            Ok(Some(chunk)) => {
                if let Err(error) = process_chunk(&state, &chunk).await {
                    let message = truncate_error(&format!("{:#}", error));

                    if chunk.retry_count + 1 >= state.config.indexing.max_retries as i64 {
                        error!(
                            chunk_id = %chunk.id,
                            error = %message,
                            "Chunk processing failed permanently"
                        );
                        if let Err(update_error) =
                            state.db.mark_chunk_failed(&chunk.id, &message).await
                        {
                            error!(chunk_id = %chunk.id, error = %update_error, "Could not mark chunk failed");
                        }
                    } else {
                        let delay = backoff_seconds(chunk.retry_count);
                        let next_retry = Utc::now() + ChronoDuration::seconds(delay as i64);
                        warn!(
                            chunk_id = %chunk.id,
                            error = %message,
                            retry_in_seconds = delay,
                            "Chunk processing failed; scheduling retry"
                        );
                        if let Err(update_error) = state
                            .db
                            .schedule_chunk_retry(&chunk.id, &message, &next_retry.to_rfc3339())
                            .await
                        {
                            error!(chunk_id = %chunk.id, error = %update_error, "Could not schedule chunk retry");
                        }
                    }
                }
            }
            Ok(None) => {
                tokio::select! {
                    _ = sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.cancelled() => break,
                }
            }
            Err(error) => {
                error!(worker_id = %worker_id, error = %error, "Could not claim chunk");
                tokio::select! {
                    _ = sleep(Duration::from_secs(5)) => {}
                    _ = shutdown.cancelled() => break,
                }
            }
        }
    }
}

async fn process_chunk(state: &Arc<AppState>, chunk: &crate::db::Chunk) -> Result<()> {
    let stream = state.db.get_stream(&chunk.stream_id).await?;

    let stream_name = stream
        .as_ref()
        .map(|stream| stream.name.clone())
        .unwrap_or_else(|| "unknown-stream".to_string());
    let external_id = stream
        .as_ref()
        .and_then(|stream| stream.external_id.clone());
    let archive_audio = stream
        .as_ref()
        .and_then(|stream| stream.archive_audio.map(|value| value == 1))
        .unwrap_or(state.config.recording.archive_audio);
    let metadata = stream
        .as_ref()
        .and_then(|stream| serde_json::from_str::<serde_json::Value>(&stream.metadata).ok())
        .unwrap_or_else(|| json!({}));

    let segment_path = chunk
        .segment_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("Chunk has no segment path"))?;

    let started_at = DateTime::parse_from_rfc3339(&chunk.started_at)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let day = started_at.format("%Y-%m-%d").to_string();

    let work_root = chunk
        .local_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(&state.config.node.data_dir)
                .join("archives")
                .join(&chunk.stream_id)
                .join(&day)
                .join(&chunk.id)
        });

    tokio::fs::create_dir_all(&work_root).await?;

    let identifier = format!("{}/{}/{}", chunk.stream_id, day, chunk.id);

    state
        .db
        .update_chunk_status(&chunk.id, "indexing", None)
        .await?;

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

    let audio_name = archive_name(&stream_name, started_at);
    let audio_filename = format!("{}.wav", audio_name);
    let stored_audio = if archive_audio {
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
    let ended_at = started_at + ChronoDuration::seconds(duration);

    let key = archive_key(
        &state.config.r2.prefix,
        &state.config.node.id,
        &chunk.stream_id,
        &day,
        &chunk.id,
    );

    if state.config.central.enabled {
        anyhow::ensure!(
            key.starts_with(&format!("archives/{}/", state.config.node.id)),
            "Archive key '{}' is outside archives/{}",
            key,
            state.config.node.id
        );
    }

    let index_objects = vec![IndexObject {
        object_key: format!("{}/index", key),
        format: "olaf-lmdb".to_string(),
        version: "1".to_string(),
    }];
    let audio_object = stored_audio.then(|| format!("{}/{}", key, audio_filename));

    let manifest = Manifest {
        node_id: state.config.node.id.clone(),
        stream_id: chunk.stream_id.clone(),
        external_stream_id: external_id.clone(),
        chunk_id: chunk.id.clone(),
        started_at: started_at.to_rfc3339(),
        completed_at: Utc::now().to_rfc3339(),
        duration_seconds: duration,
        audio_archived: stored_audio,
        index_format: "olaf-lmdb".to_string(),
        olaf_identifier: identifier,
        status: "complete".to_string(),
        index_objects: index_objects.clone(),
        audio_object: audio_object.clone(),
        metadata: metadata.clone(),
    };

    archive::write_manifest(&work_root, &manifest).await?;

    let mut index_uploaded = false;
    let mut audio_uploaded = false;

    if state.config.r2.enabled {
        state
            .db
            .update_chunk_status(&chunk.id, "uploading", None)
            .await?;

        let uploaded_keys = archive::upload_directory(&state.config.r2, &work_root, &key).await?;
        index_uploaded = uploaded_keys
            .iter()
            .any(|uploaded| uploaded.contains("/index/"));
        audio_uploaded = stored_audio
            && uploaded_keys
                .iter()
                .any(|uploaded| uploaded.ends_with(&audio_filename));
    }

    if state.config.central.enabled {
        let metadata_size = serde_json::to_vec(&metadata)
            .map(|value| value.len())
            .unwrap_or(usize::MAX);

        if !state.config.r2.enabled {
            warn!(
                chunk_id = %chunk.id,
                "Skipping central archive callback because R2 is disabled"
            );
        } else if external_id.is_none() {
            warn!(
                chunk_id = %chunk.id,
                stream_id = %chunk.stream_id,
                "Skipping central archive callback because the stream has no external_id"
            );
        } else if Uuid::parse_str(external_id.as_deref().unwrap_or("")).is_err() {
            warn!(
                chunk_id = %chunk.id,
                stream_id = %chunk.stream_id,
                external_id = %external_id.as_deref().unwrap_or(""),
                "Skipping central archive callback because external_id is not a UUID"
            );
        } else if metadata_size > 64 * 1024 {
            warn!(
                chunk_id = %chunk.id,
                stream_id = %chunk.stream_id,
                metadata_size,
                "Skipping central archive callback because metadata exceeds 64 KiB"
            );
        } else {
            let external_stream_id = external_id.as_deref().unwrap_or("");
            let payload = json!({
                "node_chunk_id": chunk.id,
                "external_stream_id": external_stream_id,
                "started_at": started_at.to_rfc3339(),
                "ended_at": ended_at.to_rfc3339(),
                "duration_seconds": duration,
                "status": "complete",
                "index_archived": index_uploaded,
                "audio_archived": audio_uploaded,
                "r2_prefix": key,
                "manifest": {
                    "index_objects": index_objects,
                    "audio_object": audio_object
                },
                "metadata": metadata
            });

            state
                .db
                .enqueue_callback("archive", &payload.to_string())
                .await?;
        }
    }

    let status = if state.config.r2.enabled {
        "complete"
    } else {
        "complete_local"
    };

    state
        .db
        .complete_chunk(
            &chunk.id,
            crate::db::ChunkCompletion {
                ended_at: &ended_at.to_rfc3339(),
                duration,
                r2_prefix: state.config.r2.enabled.then_some(key.as_str()),
                status,
                index_archived: index_path.exists(),
                audio_archived: stored_audio,
                index_uploaded,
                audio_uploaded,
            },
        )
        .await?;

    let _ = tokio::fs::remove_file(&segment_path).await;
    if let Some(parent) = segment_path.parent() {
        let _ = tokio::fs::remove_dir(parent).await;
    }

    if state.config.r2.enabled && !state.config.node.keep_local_chunks {
        let _ = tokio::fs::remove_dir_all(&work_root).await;
    }

    info!(
        stream_id = %chunk.stream_id,
        chunk_id = %chunk.id,
        r2_key = %key,
        duration_seconds = duration,
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

fn backoff_seconds(retry_count: i64) -> u64 {
    let exponent = retry_count.clamp(0, 10) as u32;
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
}
