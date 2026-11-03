use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Instant,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::Mutex,
};

#[derive(Debug, Clone)]
pub struct CompletedSegment {
    pub path: PathBuf,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

pub struct Capture {
    pub child: Child,
    pub list_path: PathBuf,
    pub output_dir: PathBuf,
    pub capture_started_at: DateTime<Utc>,
    read_offset: u64,
    pub last_activity: Instant,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
}

impl Capture {
    pub async fn start(
        ffmpeg: &str,
        input_type: &str,
        input: &str,
        output_dir: &Path,
        chunk_seconds: u64,
        sample_rate: u32,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(output_dir).await?;

        let list_path = output_dir.join("segments.csv");
        let pattern = output_dir.join("chunk-%Y%m%dT%H%M%S.wav");

        let mut command = Command::new(ffmpeg);
        command.args(["-hide_banner", "-nostdin", "-loglevel", "warning"]);

        if input_type == "audio_device" {
            command.args(["-f", "alsa", "-i", input]);
        } else {
            if input.starts_with("http://") || input.starts_with("https://") {
                command.args([
                    "-reconnect",
                    "1",
                    "-reconnect_streamed",
                    "1",
                    "-reconnect_delay_max",
                    "5",
                ]);
            }
            command.args(["-i", input]);
        }

        command
            .args([
                "-vn",
                "-ac",
                "1",
                "-ar",
                &sample_rate.to_string(),
                "-c:a",
                "pcm_s16le",
                "-f",
                "segment",
                "-segment_time",
                &chunk_seconds.to_string(),
                "-segment_format",
                "wav",
                "-reset_timestamps",
                "1",
                "-strftime",
                "1",
                "-segment_list_type",
                "csv",
                "-segment_list_flags",
                "+live",
            ])
            .arg("-segment_list")
            .arg(&list_path)
            .arg(pattern)
            .env("TZ", "UTC")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().context("starting FFmpeg")?;
        let stderr_tail = Arc::new(Mutex::new(VecDeque::new()));

        if let Some(stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let mut tail = tail.lock().await;
                    if tail.len() >= 50 {
                        tail.pop_front();
                    }
                    tail.push_back(line);
                }
            });
        }

        Ok(Self {
            child,
            list_path,
            output_dir: output_dir.to_path_buf(),
            capture_started_at: Utc::now(),
            read_offset: 0,
            last_activity: Instant::now(),
            stderr_tail,
        })
    }

    pub async fn completed_chunks(&mut self) -> Result<Vec<CompletedSegment>> {
        let mut file = match tokio::fs::File::open(&self.list_path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e.into()),
        };

        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        file.seek(std::io::SeekFrom::Start(self.read_offset))
            .await?;

        let mut content = String::new();
        file.read_to_string(&mut content).await?;

        if content.is_empty() {
            return Ok(Vec::new());
        }

        let consumed = content.rfind('\n').map(|index| index + 1).unwrap_or(0);
        if consumed == 0 {
            return Ok(Vec::new());
        }

        let complete = &content[..consumed];
        self.read_offset += consumed as u64;

        let mut completed = Vec::new();

        for line in complete.lines() {
            let mut columns = line.split(',');
            let Some(filename) = columns.next() else {
                continue;
            };

            let filename = filename.trim().trim_matches('"');
            if filename.is_empty() {
                continue;
            }

            let start_seconds = columns
                .next()
                .and_then(|value| value.trim().parse::<f64>().ok());
            let end_seconds = columns
                .next()
                .and_then(|value| value.trim().parse::<f64>().ok());

            let path = PathBuf::from(filename);
            let path = if path.is_absolute() {
                path
            } else {
                self.output_dir.join(path)
            };

            if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
                continue;
            }

            let metadata = tokio::fs::metadata(&path).await?;
            if metadata.len() == 0 {
                continue;
            }

            let started_at = match start_seconds {
                Some(seconds) if seconds.is_finite() && seconds >= 0.0 => {
                    self.capture_started_at
                        + ChronoDuration::milliseconds((seconds * 1000.0) as i64)
                }
                _ => filename_timestamp(&path).unwrap_or_else(Utc::now),
            };

            let ended_at = match end_seconds {
                Some(seconds) if seconds.is_finite() && seconds >= 0.0 => {
                    self.capture_started_at
                        + ChronoDuration::milliseconds((seconds * 1000.0) as i64)
                }
                _ => started_at,
            };

            self.last_activity = Instant::now();
            completed.push(CompletedSegment {
                path,
                started_at,
                ended_at,
            });
        }

        Ok(completed)
    }

    pub async fn stderr_tail(&self) -> String {
        let tail = self.stderr_tail.lock().await;
        tail.iter().cloned().collect::<Vec<_>>().join("\n")
    }
}

fn filename_timestamp(path: &Path) -> Option<DateTime<Utc>> {
    let stem = path.file_stem()?.to_str()?;
    let timestamp = stem.strip_prefix("chunk-")?;
    let naive = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y%m%dT%H%M%S").ok()?;
    Some(Utc.from_utc_datetime(&naive))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filename_timestamp() {
        let path = Path::new("chunk-20261103T092408.wav");
        let timestamp = filename_timestamp(path).unwrap();
        assert_eq!(timestamp.to_rfc3339(), "2026-11-03T09:24:08+00:00");
    }
}
