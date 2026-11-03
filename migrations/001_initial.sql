
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS streams (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    chunk_seconds INTEGER,
    archive_audio INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    id TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    duration_seconds INTEGER,
    status TEXT NOT NULL DEFAULT 'recording',
    local_path TEXT,
    r2_prefix TEXT,
    audio_archived INTEGER NOT NULL DEFAULT 0,
    index_archived INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    FOREIGN KEY (stream_id)
        REFERENCES streams(id)
);

CREATE INDEX IF NOT EXISTS idx_streams_enabled
ON streams(enabled);

CREATE INDEX IF NOT EXISTS idx_chunks_stream_date
ON chunks(stream_id, started_at);

CREATE INDEX IF NOT EXISTS idx_chunks_status
ON chunks(status);
