use anyhow::Result;
use chrono::{Duration, Utc};
use serde::Serialize;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use std::{path::Path, str::FromStr};

#[derive(Clone)]
pub struct Database {
    pub pool: SqlitePool,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct Stream {
    pub id: String,
    pub name: String,
    pub url: String,
    pub enabled: i64,
    pub chunk_seconds: Option<i64>,
    pub archive_audio: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
    pub external_id: Option<String>,
    pub source_type: String,
    pub description: Option<String>,
    pub metadata: String,
    pub input_type: String,
    pub input: Option<String>,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct Chunk {
    pub id: String,
    pub stream_id: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_seconds: Option<i64>,
    pub status: String,
    pub local_path: Option<String>,
    pub r2_prefix: Option<String>,
    pub audio_archived: i64,
    pub index_archived: i64,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub segment_path: Option<String>,
    pub retry_count: i64,
    pub next_retry_at: Option<String>,
    pub lease_until: Option<String>,
    pub claimed_by: Option<String>,
    pub last_error: Option<String>,
    pub index_uploaded: i64,
    pub audio_uploaded: i64,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct Callback {
    pub id: String,
    pub kind: String,
    pub payload: String,
    pub attempts: i64,
    pub next_attempt_at: String,
    pub last_error: Option<String>,
    pub created_at: String,
    pub delivered_at: Option<String>,
}

pub struct NewStream<'a> {
    pub name: &'a str,
    pub url: Option<&'a str>,
    pub external_id: Option<&'a str>,
    pub source_type: &'a str,
    pub description: Option<&'a str>,
    pub metadata: &'a str,
    pub input_type: &'a str,
    pub input: Option<&'a str>,
    pub chunk_seconds: Option<i64>,
    pub archive_audio: Option<bool>,
    pub enabled: Option<bool>,
}

pub struct StreamUpdate<'a> {
    pub name: Option<&'a str>,
    pub url: Option<Option<&'a str>>,
    pub external_id: Option<&'a str>,
    pub source_type: Option<&'a str>,
    pub description: Option<Option<&'a str>>,
    pub metadata: Option<&'a str>,
    pub input_type: Option<&'a str>,
    pub input: Option<Option<&'a str>>,
    pub chunk_seconds: Option<i64>,
    pub archive_audio: Option<bool>,
}

pub struct ChunkCompletion<'a> {
    pub ended_at: &'a str,
    pub duration: i64,
    pub r2_prefix: Option<&'a str>,
    pub status: &'a str,
    pub index_archived: bool,
    pub audio_archived: bool,
    pub index_uploaded: bool,
    pub audio_uploaded: bool,
}

impl Database {
    pub async fn open(data_dir: &str) -> Result<Self> {
        let path = Path::new(data_dir).join("bmas.sqlite3");

        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}?mode=rwc", path.display()))?
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(10));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    pub async fn streams(&self) -> Result<Vec<Stream>> {
        Ok(sqlx::query_as::<_, Stream>(
            "SELECT * FROM streams WHERE deleted_at IS NULL ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_stream(&self, id: &str) -> Result<Option<Stream>> {
        Ok(
            sqlx::query_as::<_, Stream>(
                "SELECT * FROM streams WHERE id = ? AND deleted_at IS NULL",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?,
        )
    }

    pub async fn get_stream_by_external_id(&self, external_id: &str) -> Result<Option<Stream>> {
        Ok(sqlx::query_as::<_, Stream>(
            "SELECT * FROM streams WHERE external_id = ? AND deleted_at IS NULL",
        )
        .bind(external_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn create_stream(&self, stream: NewStream<'_>) -> Result<Stream> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO streams
            (id, name, url, enabled, chunk_seconds, archive_audio,
             external_id, source_type, description, metadata,
             input_type, input, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(stream.name)
        .bind(stream.url.unwrap_or(""))
        .bind(i64::from(stream.enabled.unwrap_or(true)))
        .bind(stream.chunk_seconds)
        .bind(stream.archive_audio.map(i64::from))
        .bind(stream.external_id)
        .bind(stream.source_type)
        .bind(stream.description)
        .bind(stream.metadata)
        .bind(stream.input_type)
        .bind(stream.input)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        self.get_stream(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Stream creation failed"))
    }

    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE streams SET enabled = ?, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(i64::from(enabled))
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn update_stream(&self, id: &str, update: StreamUpdate<'_>) -> Result<bool> {
        let mut builder = sqlx::QueryBuilder::new("UPDATE streams SET updated_at = ");
        builder.push_bind(Utc::now().to_rfc3339());

        if let Some(name) = update.name {
            builder.push(", name = ").push_bind(name);
        }

        if let Some(url) = update.url {
            builder.push(", url = ");
            match url {
                Some(url) => {
                    builder.push_bind(url);
                }
                None => {
                    builder.push_bind("");
                }
            }
        }

        if let Some(external_id) = update.external_id {
            builder.push(", external_id = ").push_bind(external_id);
        }

        if let Some(source_type) = update.source_type {
            builder.push(", source_type = ").push_bind(source_type);
        }

        if let Some(description) = update.description {
            builder.push(", description = ");
            match description {
                Some(description) => {
                    builder.push_bind(description);
                }
                None => {
                    builder.push("NULL");
                }
            }
        }

        if let Some(metadata) = update.metadata {
            builder.push(", metadata = ").push_bind(metadata);
        }

        if let Some(input_type) = update.input_type {
            builder.push(", input_type = ").push_bind(input_type);
        }

        if let Some(input) = update.input {
            builder.push(", input = ");
            match input {
                Some(input) => {
                    builder.push_bind(input);
                }
                None => {
                    builder.push("NULL");
                }
            }
        }

        if let Some(chunk_seconds) = update.chunk_seconds {
            builder.push(", chunk_seconds = ").push_bind(chunk_seconds);
        }

        if let Some(archive_audio) = update.archive_audio {
            builder
                .push(", archive_audio = ")
                .push_bind(i64::from(archive_audio));
        }

        builder
            .push(" WHERE id = ")
            .push_bind(id)
            .push(" AND deleted_at IS NULL");

        let result = builder.build().execute(&self.pool).await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_stream(&self, id: &str) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE streams SET deleted_at = ?, enabled = 0, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(&now)
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn create_chunk(
        &self,
        id: &str,
        stream_id: &str,
        started_at: &str,
        local_path: &str,
        segment_path: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO chunks
            (id, stream_id, started_at, status, local_path, segment_path,
             created_at, updated_at)
             VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(stream_id)
        .bind(started_at)
        .bind(local_path)
        .bind(segment_path)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn claim_chunk(&self, worker_id: &str, lease_seconds: i64) -> Result<Option<Chunk>> {
        let now = Utc::now();
        let now_string = now.to_rfc3339();
        let lease_until = (now + Duration::seconds(lease_seconds)).to_rfc3339();

        Ok(sqlx::query_as::<_, Chunk>(
            "UPDATE chunks SET
                status = 'indexing',
                claimed_by = ?,
                lease_until = ?,
                updated_at = ?
             WHERE id = (
                SELECT id FROM chunks
                WHERE status IN ('pending', 'retrying', 'indexing', 'uploading')
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                  AND (lease_until IS NULL OR lease_until <= ?)
                ORDER BY started_at
                LIMIT 1
             )
             RETURNING *",
        )
        .bind(worker_id)
        .bind(&lease_until)
        .bind(&now_string)
        .bind(&now_string)
        .bind(&now_string)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn update_chunk_status(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE chunks SET status = ?, error = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(status)
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn complete_chunk(&self, id: &str, completion: ChunkCompletion<'_>) -> Result<()> {
        sqlx::query(
            "UPDATE chunks SET
                status = ?,
                ended_at = ?,
                duration_seconds = ?,
                r2_prefix = ?,
                audio_archived = ?,
                index_archived = ?,
                index_uploaded = ?,
                audio_uploaded = ?,
                error = NULL,
                last_error = NULL,
                lease_until = NULL,
                claimed_by = NULL,
                updated_at = ?
             WHERE id = ?",
        )
        .bind(completion.status)
        .bind(completion.ended_at)
        .bind(completion.duration)
        .bind(completion.r2_prefix)
        .bind(i64::from(completion.audio_archived))
        .bind(i64::from(completion.index_archived))
        .bind(i64::from(completion.index_uploaded))
        .bind(i64::from(completion.audio_uploaded))
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn schedule_chunk_retry(
        &self,
        id: &str,
        error: &str,
        next_retry_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE chunks SET
                status = 'retrying',
                retry_count = retry_count + 1,
                next_retry_at = ?,
                last_error = ?,
                lease_until = NULL,
                claimed_by = NULL,
                updated_at = ?
             WHERE id = ?",
        )
        .bind(next_retry_at)
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_chunk_failed(&self, id: &str, error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE chunks SET
                status = 'failed',
                last_error = ?,
                error = ?,
                lease_until = NULL,
                claimed_by = NULL,
                updated_at = ?
             WHERE id = ?",
        )
        .bind(error)
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn recover_interrupted_chunks(&self) -> Result<u64> {
        let now = Utc::now().to_rfc3339();

        let result = sqlx::query(
            "UPDATE chunks SET
                status = CASE
                    WHEN segment_path IS NULL THEN 'failed'
                    ELSE 'retrying'
                END,
                next_retry_at = ?,
                lease_until = NULL,
                claimed_by = NULL,
                last_error = COALESCE(last_error, 'recovered after restart'),
                error = COALESCE(error, 'recovered after restart'),
                updated_at = ?
             WHERE status IN ('recording', 'indexing', 'uploading')",
        )
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn active_stream_count(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM streams WHERE enabled = 1 AND deleted_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn failed_chunks(&self) -> Result<Vec<Chunk>> {
        Ok(sqlx::query_as::<_, Chunk>(
            "SELECT * FROM chunks
                 WHERE status = 'failed'
                 ORDER BY updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn list_chunks(
        &self,
        status: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Chunk>> {
        match status {
            Some(status) => Ok(sqlx::query_as::<_, Chunk>(
                "SELECT * FROM chunks WHERE status = ?
                 ORDER BY started_at DESC LIMIT ? OFFSET ?",
            )
            .bind(status)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?),
            None => Ok(sqlx::query_as::<_, Chunk>(
                "SELECT * FROM chunks ORDER BY started_at DESC LIMIT ? OFFSET ?",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?),
        }
    }

    pub async fn enqueue_callback(&self, kind: &str, payload: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO callbacks (id, kind, payload, attempts, next_attempt_at, created_at)
             VALUES (?, ?, ?, 0, ?, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(kind)
        .bind(payload)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn pending_callback(&self) -> Result<Option<Callback>> {
        let now = Utc::now().to_rfc3339();
        Ok(sqlx::query_as::<_, Callback>(
            "SELECT * FROM callbacks
             WHERE delivered_at IS NULL AND next_attempt_at <= ?
             ORDER BY next_attempt_at LIMIT 1",
        )
        .bind(&now)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn reschedule_callback(
        &self,
        id: &str,
        attempts: i64,
        next_attempt_at: &str,
        error: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE callbacks SET attempts = ?, next_attempt_at = ?, last_error = ?
             WHERE id = ?",
        )
        .bind(attempts)
        .bind(next_attempt_at)
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_callback_delivered(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE callbacks SET delivered_at = ?, last_error = NULL WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> Database {
        let dir = std::env::temp_dir().join(format!("bmas-db-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Database::open(dir.to_str().unwrap()).await.unwrap()
    }

    async fn test_stream(db: &Database) -> Stream {
        db.create_stream(NewStream {
            name: "Test Stream",
            url: Some("https://example.com/live"),
            external_id: Some("external-test"),
            source_type: "internet_radio",
            description: None,
            metadata: "{}",
            input_type: "url",
            input: Some("https://example.com/live"),
            chunk_seconds: Some(30),
            archive_audio: Some(true),
            enabled: None,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn soft_delete_retains_chunk_history() {
        let db = test_db().await;
        let stream = test_stream(&db).await;

        db.create_chunk(
            "chunk-1",
            &stream.id,
            "2026-11-03T09:00:00+00:00",
            "/tmp/archive",
            "/tmp/segment.wav",
        )
        .await
        .unwrap();

        assert!(db.delete_stream(&stream.id).await.unwrap());
        assert!(db.get_stream(&stream.id).await.unwrap().is_none());
        assert!(db
            .get_stream_by_external_id("external-test")
            .await
            .unwrap()
            .is_none());
        assert_eq!(db.list_chunks(None, 10, 0).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn soft_deleted_external_id_can_be_reused() {
        let db = test_db().await;
        let first = test_stream(&db).await;

        assert!(db.delete_stream(&first.id).await.unwrap());

        let second = db
            .create_stream(NewStream {
                name: "Replacement Stream",
                url: Some("https://example.com/other"),
                external_id: Some("external-test"),
                source_type: "internet_radio",
                description: None,
                metadata: "{}",
                input_type: "url",
                input: Some("https://example.com/other"),
                chunk_seconds: Some(30),
                archive_audio: Some(true),
                enabled: Some(false),
            })
            .await
            .unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(
            db.get_stream_by_external_id("external-test")
                .await
                .unwrap()
                .unwrap()
                .id,
            second.id
        );
    }

    #[tokio::test]
    async fn update_stream_clears_nullable_fields() {
        let db = test_db().await;
        let stream = test_stream(&db).await;

        let updated = db
            .update_stream(
                &stream.id,
                StreamUpdate {
                    name: None,
                    url: Some(None),
                    external_id: None,
                    source_type: None,
                    description: Some(None),
                    metadata: None,
                    input_type: None,
                    input: Some(None),
                    chunk_seconds: None,
                    archive_audio: None,
                },
            )
            .await
            .unwrap();

        assert!(updated);
        let stream = db.get_stream(&stream.id).await.unwrap().unwrap();
        assert!(stream.url.is_empty());
        assert!(stream.input.is_none());
        assert!(stream.description.is_none());
    }

    #[tokio::test]
    async fn chunk_claims_are_leased_and_recoverable() {
        let db = test_db().await;
        let stream = test_stream(&db).await;

        db.create_chunk(
            "chunk-1",
            &stream.id,
            "2026-11-03T09:00:00+00:00",
            "/tmp/archive",
            "/tmp/segment.wav",
        )
        .await
        .unwrap();

        let claimed = db.claim_chunk("worker-1", 60).await.unwrap().unwrap();
        assert_eq!(claimed.id, "chunk-1");
        assert_eq!(claimed.status, "indexing");

        assert!(db.claim_chunk("worker-2", 60).await.unwrap().is_none());

        db.schedule_chunk_retry(
            "chunk-1",
            "transient",
            &(Utc::now() - Duration::seconds(1)).to_rfc3339(),
        )
        .await
        .unwrap();

        let retried = db.claim_chunk("worker-2", 60).await.unwrap().unwrap();
        assert_eq!(retried.retry_count, 1);
        assert_eq!(retried.status, "indexing");

        db.mark_chunk_failed("chunk-1", "permanent").await.unwrap();
        assert_eq!(db.failed_chunks().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn interrupted_chunks_are_requeued() {
        let db = test_db().await;
        let stream = test_stream(&db).await;

        db.create_chunk(
            "chunk-1",
            &stream.id,
            "2026-11-03T09:00:00+00:00",
            "/tmp/archive",
            "/tmp/segment.wav",
        )
        .await
        .unwrap();
        db.claim_chunk("worker-1", 60).await.unwrap().unwrap();

        assert_eq!(db.recover_interrupted_chunks().await.unwrap(), 1);
        let recovered = db.claim_chunk("worker-2", 60).await.unwrap().unwrap();
        assert_eq!(recovered.status, "indexing");
    }
}
