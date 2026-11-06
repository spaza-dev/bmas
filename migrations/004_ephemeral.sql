-- BMAS is ephemeral: only the stream registry is durable.
-- Archive/recording state lives in object storage manifests; in-flight
-- work is kept in the local work directory and recovered on startup.
PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS callbacks;
DROP TABLE IF EXISTS chunks;

PRAGMA foreign_keys = ON;
