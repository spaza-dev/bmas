PRAGMA foreign_keys = ON;

DROP INDEX IF EXISTS idx_streams_external_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_external_id
ON streams(external_id)
WHERE external_id IS NOT NULL AND deleted_at IS NULL;
