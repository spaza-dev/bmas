# BMAS

Broadcast Monitoring & Archiving Service.

A Rust/Tokio node agent for concurrent audio capture, OLAF fingerprinting,
and distributed audio archiving.

## Architecture

- Rust / Tokio: service and concurrency
- Axum: REST API
- SQLite: stream registry and durable chunk job queue
- FFmpeg: audio capture
- OLAF: acoustic fingerprinting (LMDB index)
- Cloudflare R2: archive storage
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
automatically removes a legacy `[r2] token` entry (backing it up to
`/etc/bmas/config.toml.legacy-token.bak`) and seeds a missing
`bmas.env` from literal values in the installed config.

Required environment variables:

    BMAS_API_TOKEN
    BMAS_NODE_ID

Required when R2 is enabled:

    BMAS_R2_ACCOUNT_ID
    BMAS_R2_ACCESS_KEY_ID
    BMAS_R2_SECRET_ACCESS_KEY
    BMAS_R2_BUCKET
    BMAS_R2_ENDPOINT_URL

Required when central integration is enabled:

    BMAS_CENTRAL_NODE_TOKEN

`BMAS_NODE_ID` must be the node UUID returned by the central API. The
installer generates a UUID for standalone installs; replace it with the
central UUID when connecting the node. R2 is disabled by the installer
until credentials are provisioned.

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

    GET    /api/v1/chunks
    GET    /api/v1/chunks/failed
    GET    /api/v1/monitoring/summary

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

Stream deletion is a soft delete; chunk history is retained for
reconciliation. Deleted streams do not reserve their `external_id`, so a
central stream UUID can be reused after soft deletion.

## Archive model

Each completed recording chunk is independently indexed and archived.

Local layout:

    /var/lib/bmas/archives/<stream-id>/YYYY-MM-DD/<chunk-id>/
      manifest.json
      <stream-name>_<timestamp>.wav
      index/

R2 layout:

    <prefix>/<node-id>/<stream-id>/YYYY-MM-DD/<chunk-id>/
      manifest.json
      <stream-name>_<timestamp>.wav
      index/

When central integration is enabled, `<prefix>` must be `archives`, so
the R2 layout is:

    archives/<node-id>/<stream-id>/YYYY-MM-DD/<chunk-id>/

Central validates archive callbacks against this node-prefixed layout.
Audio recordings are optional. Each index is an independent OLAF LMDB
database. `manifest.json` is uploaded last so a partial upload is never
treated as complete.

Chunks are processed by a durable SQLite-backed queue. Failed chunks are
retried with exponential backoff and jitter, and remain visible through
`GET /api/v1/chunks/failed`. Interrupted work is recovered on restart.

## Central integration

The central API provisions nodes through `POST /api/v1/nodes`. It returns
a one-time node token and stores only its SHA-256 hash. BMAS does not
self-register. Store the raw token as `BMAS_CENTRAL_NODE_TOKEN` in
`/etc/bmas/bmas.env` and set `BMAS_NODE_ID` to the UUID returned by
central.

The BMAS API token used by central to call this node must be stored in
central's database, encrypted per node. Do not share one global BMAS
token through central environment variables.

Set `[central] enabled = true`, `base_url`, and `node_token` to send:

- node heartbeats
- archive completion callbacks through a durable outbox

When central integration is enabled:

- `node.id` must be the central node UUID.
- `external_id` is required and must be the central stream UUID.
- `[r2].prefix` must be `archives`, producing keys under
  `archives/<node-id>/`.

Archive callbacks use `external_id` as the central stream identifier and
are idempotent by `(node_id, node_chunk_id)` on the central side.

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
    # and R2/central variables if enabled
    cargo run -- --config config.toml

Test and lint:

    cargo fmt --all -- --check
    cargo clippy --all-targets -- -D warnings
    cargo test
