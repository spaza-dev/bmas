# BMAS

Broadcast Monitoring & Archiving Service.

A Rust/Tokio node agent for concurrent audio capture, OLAF fingerprinting,
and distributed audio archiving.

## Architecture

- Rust / Tokio: service and concurrency
- Axum: REST API
- SQLite: stream registry
- FFmpeg: audio capture
- OLAF: acoustic fingerprinting (LMDB index)
- S3-compatible storage: Cloudflare R2, AWS S3, MinIO, Backblaze B2
- systemd: service management

## Installation

Supported platforms:

- Ubuntu 22.04
- Ubuntu 24.04

Install:

    sudo ./scripts/install.sh

The installer checks whether BMAS is already installed before doing any
installation work. It installs FFmpeg/FFprobe, Zig, OLAF, Rust if needed,
BMAS, a systemd service, and `/etc/bmas/bmas.env`.

Update:

    sudo ./scripts/update.sh

The updater preserves the installed configuration by default. Use
`--sync-config` only when you intentionally want to replace it.

## Configuration

Configuration:

    /etc/bmas/config.toml

Secrets and environment overrides:

    /etc/bmas/bmas.env

Data:

    /var/lib/bmas/

Configuration values support `${ENV_VAR}` expansion. The installed
`config.toml` contains only placeholders; real credentials belong in
`bmas.env` (mode `0600`, owned by root). Never commit `config.toml`,
`bmas.env`, or `.env`. Unknown config keys are rejected. The updater
migrates legacy `[r2]`, `[heartbeat]`, and old `[storage]` limits
configuration, backing it up to `/etc/bmas/config.toml.legacy.bak`, and
seeds a missing `bmas.env` from literal values in the installed config.

Required environment variables:

    BMAS_API_TOKEN
    BMAS_NODE_ID

Required when object storage is enabled:

    BMAS_STORAGE_ACCOUNT_ID
    BMAS_STORAGE_ACCESS_KEY_ID
    BMAS_STORAGE_SECRET_ACCESS_KEY
    BMAS_STORAGE_BUCKET
    BMAS_STORAGE_ENDPOINT_URL

`BMAS_STORAGE_ACCOUNT_ID` is required only for the R2 backend when no
explicit endpoint is configured. `BMAS_NODE_ID` must be the node UUID
returned by the central API. The installer generates a UUID for
standalone installs; replace it with the central UUID when connecting the
node. Storage is disabled by the installer until credentials are
provisioned.

CORS is disabled unless `node.allowed_origins` is non-empty. The API
accepts JSON bodies up to 1 MiB.

## API

Base URL:

    http://127.0.0.1:8787

Authentication:

    Authorization: Bearer YOUR_API_TOKEN

Endpoints:

    GET    /api/v1/streams
    POST   /api/v1/streams
    GET    /api/v1/streams/{id}
    GET    /api/v1/streams/by-external-id/{external_id}
    PATCH  /api/v1/streams/{id}
    DELETE /api/v1/streams/{id}

    POST   /api/v1/streams/{id}/start
    POST   /api/v1/streams/{id}/stop

    GET    /api/v1/monitoring/summary
    GET    /api/v1/status

Health:

    GET /healthz
    GET /readyz

Documentation:

    GET /docs
    GET /openapi.json

All protected routes require the bearer token. `POST /api/v1/streams`
supports URL sources and physical `audio_device` sources. `enabled`
defaults to `true`; central should create streams with `"enabled": false`
and call `/start` explicitly. Creating a stream with an existing
`external_id` returns the existing stream with `200 OK` instead of
creating a duplicate.

```json
{
  "name": "Example Nightclub",
  "external_id": "9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b",
  "source_type": "nightclub",
  "url": null,
  "input_type": "audio_device",
  "input": "default",
  "chunk_seconds": 300,
  "archive_audio": true,
  "enabled": false,
  "metadata": {
    "country": "KE",
    "city": "Nairobi"
  }
}
```

`metadata` must be a JSON object and is limited to 64 KiB serialized.
`PATCH` treats an explicit `null` for `url`, `description`, or `input`
as a request to clear the field; clearing `url` stores an empty string.

Stream deletion is a soft delete. Deleted streams do not reserve their
`external_id`, so a central stream UUID can be reused after soft deletion.

## Archive model

Each completed recording chunk is independently indexed and archived.

Local layout:

    /var/lib/bmas/archives/<stream-id>/YYYY-MM-DD/<chunk-id>/
      manifest.json
      <stream-name>_<timestamp>.wav
      index/

Object-storage payload layout:

    <prefix>/<node-id>/<stream-id>/YYYY-MM-DD/<chunk-id>/
      manifest.json
      <stream-name>_<timestamp>.wav
      index/

Central discovery manifests are published separately, after their
payloads:

    <prefix>/<node-id>/manifests/YYYY-MM-DD/<chunk-id>.json

With the required central prefix `archives`, manifest discovery uses:

    archives/<node-id>/manifests/YYYY-MM-DD/<chunk-id>.json

Audio recordings are optional. Each index is an independent OLAF LMDB
database. The canonical discovery manifest is uploaded last so central
never treats a partial upload as complete.

Completed chunks are processed by an ephemeral, work-directory-backed
queue with exponential backoff and jitter. Interrupted work is recovered
on restart; local work retention is controlled by
`limits.work_retention_hours`.

## Central integration

The central API provisions nodes through `POST /api/v1/nodes`. It returns
a one-time node token and stores only its SHA-256 hash. BMAS does not
self-register. Set `BMAS_NODE_ID` to the UUID returned by central.

Central stores each node's BMAS API token encrypted at rest and uses it
to poll `GET /api/v1/status` and `GET /api/v1/streams`. Do not share one
global BMAS token through central environment variables.

When connected to central:

- `node.id` must be the central node UUID.
- `external_id` is required and must be the central stream UUID.
- `storage.prefix` must be `archives`, producing manifest keys under
  `archives/<node-id>/manifests/`.

BMAS does not call central. Central ingests archives by scanning the
manifest prefix and resolving `external_stream_id` or `stream_id`.

## Service management

Status:

    sudo systemctl status bmas

Logs:

    sudo journalctl -u bmas -f

Restart:

    sudo systemctl restart bmas

Stop:

    sudo systemctl stop bmas

The service handles SIGTERM/SIGINT, stops capture, and drains in-flight
work before exiting.

## Development

Build:

    cargo build

Release:

    cargo build --release

Run:

    export BMAS_API_TOKEN=...
    export BMAS_NODE_ID=9b2b1f7a-1c2d-4e5f-8a9b-0c1d2e3f4a5b
    # and BMAS_STORAGE_* variables if storage is enabled
    cargo run -- --config config.toml serve

Inspect:

    cargo run -- --config config.toml config validate
    cargo run -- --config config.toml status --json
    cargo run -- --config config.toml doctor
    cargo run -- --config config.toml stream list --json

Test and lint:

    cargo fmt --all -- --check
    cargo clippy --all-targets -- -D warnings
    cargo test
