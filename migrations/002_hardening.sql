PRAGMA foreign_keys = ON;

ALTER TABLE streams ADD COLUMN external_id TEXT;
ALTER TABLE streams ADD COLUMN source_type TEXT NOT NULL DEFAULT 'internet_radio';
ALTER TABLE streams ADD COLUMN description TEXT;
ALTER TABLE streams ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}';
ALTER TABLE streams ADD COLUMN input_type TEXT NOT NULL DEFAULT 'url';
ALTER TABLE streams ADD COLUMN input TEXT;
ALTER TABLE streams ADD COLUMN deleted_at TEXT;

UPDATE streams SET input = url WHERE input IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_external_id
ON streams(external_id)
WHERE external_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_streams_active
ON streams(enabled, deleted_at);

ALTER TABLE chunks ADD COLUMN segment_path TEXT;
ALTER TABLE chunks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunks ADD COLUMN next_retry_at TEXT;
ALTER TABLE chunks ADD COLUMN lease_until TEXT;
ALTER TABLE chunks ADD COLUMN claimed_by TEXT;
ALTER TABLE chunks ADD COLUMN last_error TEXT;
ALTER TABLE chunks ADD COLUMN index_uploaded INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunks ADD COLUMN audio_uploaded INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_chunks_queue
ON chunks(status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_chunks_lease
ON chunks(lease_until);

CREATE TABLE IF NOT EXISTS callbacks (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_callbacks_pending
ON callbacks(delivered_at, next_attempt_at);
