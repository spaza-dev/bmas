use anyhow::Result;
use chrono::Utc;
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

    pub async fn resolve_stream(&self, id_or_external_id: &str) -> Result<Option<Stream>> {
        if let Some(stream) = self.get_stream(id_or_external_id).await? {
            return Ok(Some(stream));
        }

        self.get_stream_by_external_id(id_or_external_id).await
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

    pub async fn active_stream_count(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM streams WHERE enabled = 1 AND deleted_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn total_stream_count(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM streams WHERE deleted_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await?)
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
    async fn ephemeral_database_only_has_streams() {
        let db = test_db().await;

        for table in ["chunks", "callbacks"] {
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .bind(table)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            assert_eq!(exists, 0, "table {} should not exist", table);
        }
    }

    #[tokio::test]
    async fn soft_delete_retains_history_but_hides_stream() {
        let db = test_db().await;
        let stream = test_stream(&db).await;

        assert!(db.delete_stream(&stream.id).await.unwrap());
        assert!(db.get_stream(&stream.id).await.unwrap().is_none());
        assert!(db
            .get_stream_by_external_id("external-test")
            .await
            .unwrap()
            .is_none());
        assert_eq!(db.total_stream_count().await.unwrap(), 0);
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
    async fn resolves_stream_by_id_or_external_id() {
        let db = test_db().await;
        let stream = test_stream(&db).await;

        assert_eq!(
            db.resolve_stream(&stream.id).await.unwrap().unwrap().id,
            stream.id
        );
        assert_eq!(
            db.resolve_stream("external-test")
                .await
                .unwrap()
                .unwrap()
                .id,
            stream.id
        );
    }
}
