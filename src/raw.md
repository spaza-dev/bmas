just give me the docs here
Below is the complete specification, organized as Markdown documents. You can copy these into the corresponding files in your project.

Recommended folder structure

spaza-airplay-api-spec/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── api.md
│   ├── security.md
│   ├── query-report-execution.md
│   ├── bmas-integration.md
│   └── decisions.md
├── db/
│   ├── schema.md
│   └── migrations/
│       └── 0001_initial.sql
└── agent/
    ├── AGENTS.md
    └── implementation-plan.md
1. README.md
# Spaza Airplay API & Database Specification

This repository contains the implementation specification for the
Spaza Airplay API Service and its D1 database.

## Technology

- Runtime: Cloudflare Workers
- Framework: Hono
- Database: Cloudflare D1
- Object storage: Cloudflare R2
- Authentication: Bearer JWT
- API documentation: OpenAPI 3.1 + Swagger UI
- Query execution: Separate OLAF Query Service
- Monitoring nodes: BMAS

## Deployment model

One shared API codebase deployed in two modes:

1. Operations
2. Customer

Each deployment has its own D1 database.

The same API routes, validation, services and database schema
are used in both modes.

## Documentation

- docs/architecture.md: System architecture and deployment model.
- docs/api.md: API endpoints and conventions.
- docs/security.md: Authentication, authorization and rate limits.
- docs/query-report-execution.md: Query and report execution.
- docs/bmas-integration.md: BMAS integration.
- docs/decisions.md: Agreed architectural decisions.
- db/schema.md: D1 schema.
- db/migrations/0001_initial.sql: Initial database migration.
- agent/AGENTS.md: Instructions for AI coding agents.
- agent/implementation-plan.md: Implementation phases.

## Scope

This project implements:

- Hono API service.
- Operations and Customer deployment modes.
- D1 database and migrations.
- Node and stream management.
- Catalog management.
- Archive registry.
- Query orchestration.
- Report configuration and generation.
- Authentication and authorization.
- Rate limiting.
- BMAS integration.
- Export management.

## Out of scope

- OLAF Query Service implementation.
- OLAF query algorithms.
- OLAF archive processing.
- BMAS implementation.
- Frontend implementation.
- Rights management.
- Royalty administration.

## Important principles

1. A stream belongs to one BMAS node at a time.
2. Stream reassignment is supported.
3. No monitoring_assignments table.
4. streams.node_id identifies the current node.
5. Streams may or may not have URLs.
6. Custom attributes use JSON metadata.
7. Query results are ephemeral.
8. Report data is generated at runtime.
9. D1 stores query/report definitions, not their results.
10. Exported files are temporary and expire.
11. Cloudflare Access handles user identity.
12. No application users, roles or memberships tables.
2. docs/architecture.md
# Spaza Airplay Architecture

## 1. Overview

Spaza Airplay is a broadcast monitoring, archiving and intelligence
platform.

It captures media from multiple source types, archives OLAF indexes,
searches those archives against recording catalogs and generates
monitoring reports.

The platform is designed for radio, television, digital media,
nightclubs, public transport, concerts, venues and other sources.

It is a monitoring and intelligence platform, not a rights-management
or royalty-administration platform.

## 2. Main components

### API Service

A shared Hono API deployed on Cloudflare Workers.

Responsibilities:

- Authentication and authorization.
- Node management.
- Stream management.
- Catalog management.
- Archive registry.
- Query orchestration.
- Report generation.
- Export management.
- Audit logging.
- BMAS communication.

### D1

Each deployment has its own D1 database.

D1 stores:

- Nodes.
- Streams.
- Catalog metadata.
- Archive metadata.
- Saved query definitions.
- Saved report definitions.
- Audit logs.

D1 does not store audio, OLAF index files or generated query results.

### R2

Stores:

- OLAF index archives.
- Optional audio archives.
- Temporary query exports.
- Temporary report exports.

### BMAS

Broadcast Monitoring & Archiving Service.

BMAS is deployed on monitoring nodes and is responsible for:

- Capturing media.
- Recording streams.
- Splitting recordings into chunks.
- Running OLAF indexing.
- Uploading completed index chunks to R2.
- Optionally archiving audio.
- Reporting archive metadata to the central API.

### OLAF Query Service

A separate Cloudflare Container running the OLAF query engine.

Responsibilities:

- Download archived OLAF indexes from R2.
- Execute OLAF queries.
- Match against supplied catalog fingerprints.
- Return matches, timestamps, durations and confidence values.

The Query Service is developed separately.

The API project only implements the integration client and orchestration layer.

## 3. Deployment model

There is one API codebase deployed multiple times.

Deployment mode is controlled through configuration.

### Operations deployment

```toml
[vars]
MODE = "operations"
DATA_ACCESS_MODE = "direct"
Operations deployment owns:

Spaza BMAS nodes.

Spaza streams.

Global Spaza Catalog.

Spaza archives.

Operations queries and reports.

Customer deployment
[vars]
MODE = "customer"
DATA_ACCESS_MODE = "api"
Each customer has:

Its own API deployment.

Its own D1 database.

Its own private catalog.

Its own BMAS nodes.

Its own streams.

Its own archives.

Its own query definitions.

Its own report definitions.

4. Data access
Each deployment has a primary D1 database.

In customer mode, approved Spaza-owned data can also be accessed through:

Direct binding to Operations D1.

HTTP requests to the Operations API.

The implementation must hide this distinction behind a data-access layer.

Example:

interface DataSources {
  primary: D1Database;
  operations?: D1Database;

  mode: "operations" | "customer";

  operationsAccess: "direct" | "api";
}

The primary database is always the current deployment's database.

Direct D1 access is not inherently read-only. Customer-mode code must strictly restrict Operations database access to approved reads.

Cross-database joins and transactions are not supported. Data must be combined in the application service layer.

5. Resource ownership
Resource

Operations

Customer

Spaza nodes

Full access

No node details

Spaza streams

Full access

Approved read-only access

Global catalog

Full access

Read-only access

Spaza archives

Full access

Authorized query access

Customer nodes

No access

Own resources

Customer streams

No access

Own resources

Private catalog

No access

Own resources

Customer archives

No access

Own resources

Queries

Own definitions

Own definitions

Reports

Own definitions

Own definitions

Customers must never see internal Spaza node details, including:

Node IDs.

Hostnames.

Endpoints.

Credentials.

Health information.

Internal configuration.

Operations must never access customer-private data.

6. Stream-to-node relationship
A stream can belong to only one BMAS node at a time.

The relationship is represented directly in:

streams.node_id

There is no assignments table.

A stream may have a null node_id when it has not yet been assigned.

Reassignment
Reassignment is an orchestrated operation:

Validate the destination node.

Create the stream on the destination BMAS node.

Confirm successful creation.

Gracefully stop the stream on the current node.

Remove the stream from the current node.

Update streams.node_id.

Record the operation in audit_logs.

If destination creation fails, leave the original node untouched.

If the old node cannot be stopped or the stream cannot be removed,
do not update node_id. Flag the operation for reconciliation.

The API must prevent simultaneous active ownership.

7. Browser communication
The browser communicates only with the central API.

It must never call BMAS node APIs directly.

The API proxies authorized operations to BMAS.

8. Query execution
Client submits a query.

API authenticates and authorizes the request.

API resolves catalog scope.

API resolves authorized archives.

API resolves exact R2 object references.

API sends a request to the OLAF Query Service.

Query Service downloads and processes the indexes.

Query Service returns matching results.

API returns results or prepares an export.

Results are discarded after execution.

No query results are persisted in D1.

9. Report generation
Reports are saved configurations.

When a report is executed:

API loads its configuration.

API resolves the relevant sources, archives and catalog.

API invokes the Query Service where required.

API generates report data.

API returns a preview or generates an export.

Generated data is discarded.

No report-run or report-result tables are maintained.


---

# 3. `db/schema.md`

```markdown
# Spaza Airplay D1 Database Schema

## 1. Conventions

- IDs: UUID strings stored as TEXT.
- Timestamps: UTC Unix milliseconds stored as INTEGER.
- Flexible attributes: JSON serialized into TEXT.
- Foreign keys apply within the same D1 database.
- R2 object keys are stored instead of public URLs.
- D1 stores metadata and registries, not audio or OLAF index files.

The same schema is deployed to Operations and Customer databases.

## 2. Tables

### nodes

BMAS node registry.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| name | TEXT |
| type | TEXT |
| endpoint | TEXT |
| status | TEXT |
| metadata | TEXT JSON |
| api_token_hash | TEXT |
| created_at | INTEGER |
| updated_at | INTEGER |

Node types may include:

- stream_monitor
- web_capture
- iot

The raw node bearer token must never be stored.

### streams

All monitored source types.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| name | TEXT |
| type | TEXT |
| description | TEXT nullable |
| url | TEXT nullable |
| node_id | TEXT nullable FK |
| status | TEXT |
| metadata | TEXT JSON |
| created_at | INTEGER |
| updated_at | INTEGER |

Supported source types include:

- internet_radio
- terrestrial_radio
- television
- youtube
- nightclub
- public_transport
- concert
- venue
- other

The URL is nullable because physical sources do not necessarily have URLs.

Example metadata:

{
  "country": "KE",
  "region": "Nairobi",
  "city": "Nairobi",
  "frequency": "98.4 FM",
  "station_callsign": "Example FM",
  "language": "en"
}

### catalog_items

Songs, advertisements, jingles, station IDs and other recordings.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| name | TEXT |
| type | TEXT |
| description | TEXT nullable |
| status | TEXT |
| metadata | TEXT JSON |
| created_at | INTEGER |
| updated_at | INTEGER |

Types:

- song
- advertisement
- jingle
- station_id
- promotional_audio
- other

Example song metadata:

{
  "artist": "Artist A",
  "album": "Album A",
  "isrc": "KEABC2600001",
  "label": "Example Records",
  "genre": "Afrobeats"
}

Advertisement metadata may include:

- brand
- campaign
- agency
- product
- duration_seconds

Do not create separate artist, album, label or brand tables in the initial version.

### archives

Registry of completed BMAS chunks.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| node_id | TEXT FK |
| node_chunk_id | TEXT |
| stream_id | TEXT FK |
| started_at | INTEGER |
| ended_at | INTEGER |
| duration_seconds | INTEGER |
| status | TEXT |
| index_archived | INTEGER |
| audio_archived | INTEGER |
| r2_prefix | TEXT |
| metadata | TEXT JSON |
| created_at | INTEGER |

Unique constraint:

(node_id, node_chunk_id)

The archive metadata or manifest must identify the exact OLAF index objects and optional audio object.

### queries

Saved query definitions only.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| name | TEXT |
| filters | TEXT JSON |
| created_at | INTEGER |
| updated_at | INTEGER |

### reports

Saved report configurations only.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| name | TEXT |
| description | TEXT nullable |
| configuration | TEXT JSON |
| created_at | INTEGER |
| updated_at | INTEGER |

### audit_logs

Audit events.

| Field | Type |
|---|---|
| id | TEXT PRIMARY KEY |
| action | TEXT |
| resource_type | TEXT |
| resource_id | TEXT nullable |
| details | TEXT JSON |
| created_at | INTEGER |

Audit logs must not contain query result rows, report datasets, credentials or raw tokens.

## 3. Tables that must not exist

Do not create:

- users
- roles
- memberships
- monitoring_assignments
- query_jobs
- query_matches
- report_runs
- query_result_snapshots
- report_result_snapshots

Cloudflare Access handles user identity.

streams.node_id handles node assignment.

Query and report results are ephemeral.

## 4. Recommended indexes

- nodes(status)
- streams(node_id)
- streams(type, status)
- streams(status)
- catalog_items(type, status)
- archives(stream_id, started_at)
- archives(node_id, created_at)
- archives(status, started_at)
- queries(updated_at)
- reports(updated_at)
- audit_logs(created_at)
- audit_logs(resource_type, resource_id, created_at)
4. db/migrations/0001_initial.sql
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    metadata TEXT NOT NULL DEFAULT '{}',
    api_token_hash TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS streams (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    description TEXT,
    url TEXT,
    node_id TEXT REFERENCES nodes(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS catalog_items (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS archives (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    node_chunk_id TEXT NOT NULL,
    stream_id TEXT NOT NULL REFERENCES streams(id),
    started_at INTEGER NOT NULL,
    ended_at INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'processing',
    index_archived INTEGER NOT NULL DEFAULT 0
        CHECK (index_archived IN (0, 1)),
    audio_archived INTEGER NOT NULL DEFAULT 0
        CHECK (audio_archived IN (0, 1)),
    r2_prefix TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    UNIQUE (node_id, node_chunk_id)
);

CREATE TABLE IF NOT EXISTS queries (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    filters TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    configuration TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    details TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_status
    ON nodes(status);

CREATE INDEX IF NOT EXISTS idx_streams_node_id
    ON streams(node_id);

CREATE INDEX IF NOT EXISTS idx_streams_type_status
    ON streams(type, status);

CREATE INDEX IF NOT EXISTS idx_streams_status
    ON streams(status);

CREATE INDEX IF NOT EXISTS idx_catalog_items_type_status
    ON catalog_items(type, status);

CREATE INDEX IF NOT EXISTS idx_archives_stream_started
    ON archives(stream_id, started_at);

CREATE INDEX IF NOT EXISTS idx_archives_node_created
    ON archives(node_id, created_at);

CREATE INDEX IF NOT EXISTS idx_archives_status_started
    ON archives(status, started_at);

CREATE INDEX IF NOT EXISTS idx_queries_updated
    ON queries(updated_at);

CREATE INDEX IF NOT EXISTS idx_reports_updated
    ON reports(updated_at);

CREATE INDEX IF NOT EXISTS idx_audit_created
    ON audit_logs(created_at);

CREATE INDEX IF NOT EXISTS idx_audit_resource_created
    ON audit_logs(resource_type, resource_id, created_at);

5. docs/api.md
# Spaza Airplay API Specification

## 1. General

Base path:

/api/v1

Framework: Hono.

Runtime: Cloudflare Workers.

Authentication:

Authorization: Bearer <token>

User-facing routes require a valid bearer JWT.

BMAS callbacks use a separate node-specific bearer token.

Internal service-to-service requests use a separate service credential
or Cloudflare Service Binding.

## 2. Response format

Successful response:

{
  "data": {},
  "meta": {}
}

List response:

{
  "data": [],
  "meta": {
    "limit": 50,
    "next_cursor": null
  }
}

Error response:

{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "The request is invalid.",
    "details": []
  },
  "request_id": "uuid"
}

## 3. HTTP status codes

| Status | Meaning |
|---|---|
| 200 | Successful request |
| 201 | Resource created |
| 202 | Accepted asynchronous operation |
| 204 | Successful deletion |
| 400 | Invalid request |
| 401 | Missing or invalid authentication |
| 403 | Insufficient permissions |
| 404 | Resource not found |
| 409 | Resource/state conflict |
| 413 | Request too large |
| 422 | Semantic validation failure |
| 429 | Rate limit exceeded |
| 502 | Downstream service failure |
| 503 | Downstream unavailable |
| 504 | Downstream timeout |

## 4. Pagination

List endpoints should support:

- limit (default 50, maximum 200)
- cursor

Use cursor pagination for large datasets.

## 5. Overview

| Method | Endpoint |
|---|---|
| GET | /overview |
| GET | /overview/activity |
| GET | /overview/coverage |

## 6. Nodes

| Method | Endpoint |
|---|---|
| GET | /nodes |
| POST | /nodes |
| GET | /nodes/:id |
| PATCH | /nodes/:id |
| DELETE | /nodes/:id |
| GET | /nodes/:id/status |
| POST | /nodes/:id/commands |

Node creation provisions a BMAS node record and generates a one-time bearer token.

The raw token is returned only once.

## 7. Streams

| Method | Endpoint |
|---|---|
| GET | /streams |
| POST | /streams |
| GET | /streams/:id |
| PATCH | /streams/:id |
| DELETE | /streams/:id |
| POST | /streams/:id/start |
| POST | /streams/:id/stop |
| POST | /streams/:id/reassign |

Stream creation supports URL and non-URL sources.

Example:

{
  "name": "Example Nightclub",
  "type": "nightclub",
  "url": null,
  "node_id": "node-uuid",
  "metadata": {
    "country": "KE",
    "city": "Nairobi",
    "venue": "Example Club"
  }
}

### Reassignment

Request:

POST /streams/:id/reassign

{
  "destination_node_id": "destination-node-uuid"
}

The API must:

1. Validate destination node.
2. Create stream on destination BMAS node.
3. Confirm creation.
4. Gracefully stop old node stream.
5. Remove old node stream.
6. Update central streams.node_id.
7. Record audit event.

Do not update node_id before the old node has been successfully stopped and removed.

If any step fails, retain the original node assignment and report the failure for reconciliation.

## 8. BMAS integration

| Method | Endpoint |
|---|---|
| POST | /nodes/register |
| POST | /nodes/:id/heartbeat |
| POST | /nodes/:id/events |
| POST | /nodes/:id/archives |

These routes use node bearer authentication.

The node identity in the request must match the authenticated node.

Archive registration must be idempotent using:

(node_id, node_chunk_id)

## 9. Catalog

| Method | Endpoint |
|---|---|
| GET | /catalog/items |
| POST | /catalog/items |
| GET | /catalog/items/:id |
| PATCH | /catalog/items/:id |
| DELETE | /catalog/items/:id |
| POST | /catalog/import |

Catalog scope:

- private
- global
- all

Operations mode manages Global Catalog.

Customer mode manages its private catalog and may read authorized Global Catalog records.

Customers cannot modify Global Catalog records.

## 10. Archives

| Method | Endpoint |
|---|---|
| GET | /archives |
| GET | /archives/:id |
| GET | /archives/coverage |
| GET | /archives/coverage/:streamId |

These endpoints return archive metadata and coverage information.

They do not expose public R2 URLs.

## 11. Query Engine

| Method | Endpoint | Purpose |
|---|---|---|
| GET | /queries | List saved query definitions |
| POST | /queries | Save query definition |
| GET | /queries/:id | Retrieve query definition |
| PATCH | /queries/:id | Update query definition |
| DELETE | /queries/:id | Delete query definition |
| POST | /queries/:id/run | Execute saved query |
| POST | /query | Execute ad-hoc query |
| POST | /query/export | Execute and export results |

Query filters should support:

- Date/time range.
- Source/stream IDs.
- Source type.
- Country, region and city.
- Stream metadata.
- Recording/catalog metadata.
- Artist, album, ISRC, label.
- Brand and campaign.
- Catalog type.
- Private/global catalog scope.
- Match confidence threshold.

Example:

{
  "catalog": {
    "scope": "all",
    "item_types": ["song"]
  },
  "recording": {
    "metadata": {
      "artist": {
        "eq": "Artist A"
      }
    }
  },
  "sources": {
    "metadata": {
      "country": {
        "in": ["KE", "UG"]
      }
    }
  },
  "time": {
    "from": "2026-01-01T00:00:00Z",
    "to": "2026-01-31T23:59:59Z"
  },
  "match": {
    "minimum_confidence": 0.8
  }
}

The API validates filters, resolves catalog and archive references,
and submits a normalized request to the separate OLAF Query Service.

Query results are returned at runtime and never persisted in D1.

## 12. Reports

| Method | Endpoint |
|---|---|
| GET | /reports |
| POST | /reports |
| GET | /reports/:id |
| PATCH | /reports/:id |
| DELETE | /reports/:id |
| POST | /reports/:id/run |
| POST | /reports/:id/export |

Reports are saved configurations.

Report data is generated at runtime.

No report run or report result is stored in D1.

## 13. Administration

| Method | Endpoint |
|---|---|
| GET | /settings |
| PATCH | /settings |
| GET | /audit-logs |

No user-management endpoints.

## 14. Health and documentation

| Method | Endpoint |
|---|---|
| GET | /health |
| GET | /ready |
| GET | /openapi.json |
| GET | /docs |

Swagger UI should be available at /docs.
6. docs/security.md
# Security Specification

## 1. Bearer authentication

All user-facing API requests require:

Authorization: Bearer <JWT>

The API must validate the JWT signature, not merely decode it.

Validation requirements:

- Signature verification using trusted JWKS.
- Issuer validation.
- Audience validation.
- Expiration validation.
- Not-before validation where present.
- Explicit signing algorithm allowlist.
- Required subject claim.

JWT configuration:

- AUTH_ISSUER
- AUTH_AUDIENCE
- AUTH_JWKS_URL

Cache JWKS keys with a bounded TTL and support key rotation.

Cloudflare Access is the identity boundary. The API does not create
or manage application users.

## 2. Authorization

Authorization is enforced in the API service layer.

Operations mode:

- Manage Spaza-owned resources.
- Access Global Catalog.
- Access Spaza archives.

Customer mode:

- Manage customer-owned resources.
- Access private catalog.
- Read authorized Global Catalog data.
- Query authorized Spaza archives.
- Never access another customer's private data.

Never trust browser-supplied ownership or tenant identifiers.

## 3. BMAS authentication

BMAS nodes use separate bearer tokens.

- Generate high-entropy random tokens.
- Minimum 32 bytes of entropy.
- Store only a secure hash/HMAC.
- Display token once during provisioning.
- Support token rotation and revocation.
- Restrict each token to its node.

Node tokens must never authenticate as user tokens.

## 4. Internal service authentication

Use Cloudflare Service Bindings where possible.

Otherwise use dedicated service bearer credentials stored as Worker
secrets.

Do not forward user tokens to internal services unnecessarily.

## 5. Rate limits

Initial recommended limits:

| Traffic | Limit |
|---|---|
| Authenticated reads | 120 requests/minute/user |
| Authenticated writes | 30 requests/minute/user |
| Query execution | 5 requests/minute/user |
| Query exports | 2 requests/10 minutes/user |
| Report generation/export | 3 requests/10 minutes/user |
| Catalog imports | 5 requests/10 minutes/user |
| BMAS heartbeat | 120 requests/minute/node |
| BMAS events/archive callbacks | 60 requests/minute/node |
| Failed authentication | 20 attempts/minute/IP |

These are initial defaults and should be configurable.

Use Cloudflare Rate Limiting bindings or an equivalent shared
rate-limiting service. Do not rely on in-memory Worker counters.

Expensive query and report operations should also have concurrency
limits, maximum date ranges and execution timeouts.

On rate-limit breach:

HTTP 429

Retry-After: 30

Return:

{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Too many requests."
  }
}

## 6. Request limits

Initial limits:

- JSON request body: 1 MiB.
- Query filter payload: 64 KiB.
- Metadata JSON per record: 64 KiB.
- Maximum list page size: 200.
- Catalog import: bounded file size and row count.
- Export generation: bounded output size and execution time.

## 7. CORS

- Allow configured frontend origins only.
- Do not use wildcard origins with credentials.
- Allow Authorization and Content-Type headers.
- Expose Retry-After and Content-Disposition when needed.
- Enforce HTTPS.

## 8. Secrets

Never:

- Store raw bearer tokens in D1.
- Log Authorization headers.
- Put credentials in URLs.
- Return node credentials to customers.
- Expose internal R2 keys to browsers.
- Include secrets in audit logs.

Use Worker secrets for credentials and encryption keys.

## 9. Audit logging

Audit:

- Node registration and token rotation.
- Stream creation, update, deletion and reassignment.
- Catalog changes and imports.
- Query/report definition changes.
- Query/report exports.
- Settings changes.

Do not store query results or report datasets in audit logs.
7. docs/query-report-execution.md
# Query and Report Execution

## 1. Core principle

Queries and reports are definitions, not stored result sets.

D1 stores:

- Saved query definitions.
- Saved report configurations.

D1 does not store:

- Query execution history.
- Query matches.
- Report runs.
- Report datasets.
- Result snapshots.
- Persistent result caches.

## 2. OLAF Query Service

The OLAF Query Service is a separate Cloudflare Container.

It is outside the scope of this API project.

Its responsibilities:

1. Download OLAF index archives from R2.
2. Run OLAF queries.
3. Match against supplied catalog fingerprints.
4. Return matching recordings.
5. Return match timestamps.
6. Return durations.
7. Return confidence and other matching metrics.

The API must not implement OLAF internals.

## 3. API responsibilities

The API:

1. Authenticates the request.
2. Validates query filters.
3. Resolves private/global catalog scope.
4. Resolves eligible archives.
5. Applies authorization.
6. Resolves exact R2 object references.
7. Calls the Query Service.
8. Returns results or generates an export.
9. Discards generated results.

The Query Service must not make customer authorization decisions.

It must not accept arbitrary browser-provided R2 keys.

## 4. Integration

Prefer a Cloudflare Service Binding.

Otherwise use authenticated private HTTPS.

Implement an adapter:

interface QueryServiceClient {
  execute(
    request: QueryExecutionRequest
  ): Promise<QueryExecutionResult>;
}

Keep the API independent of the Query Service's internal routes,
container implementation and OLAF version.

## 5. Contract versioning

Use a versioned request/response contract.

Example request:

{
  "contract_version": "1",
  "request_id": "uuid",
  "catalog": [],
  "archives": [],
  "options": {
    "minimum_confidence": 0.8
  }
}

Each catalog item should include:

- Central catalog item ID.
- Name and type.
- Relevant metadata.
- Server-resolved fingerprint reference.

Each archive should include:

- Central archive ID.
- Stream ID.
- Start/end timestamps.
- Exact server-resolved index object references.

Example response:

{
  "contract_version": "1",
  "request_id": "uuid",
  "status": "completed",
  "matches": [
    {
      "catalog_item_id": "uuid",
      "archive_id": "uuid",
      "stream_id": "uuid",
      "matched_at": 1790000123000,
      "duration_seconds": 24.6,
      "confidence": 0.94,
      "metadata": {}
    }
  ],
  "summary": {
    "archives_scanned": 10,
    "matches_found": 1,
    "duration_seconds": 18.2
  }
}

This is an initial integration contract. Final details should be
agreed with the separately developed Query Service.

## 6. Metadata filtering

The API resolves metadata filters against D1 records before
submitting the normalized query.

Recording metadata examples:

- artist
- album
- isrc
- label
- brand
- campaign

Stream metadata examples:

- country
- region
- city
- frequency
- station
- venue
- vehicle_id
- platform

Metadata filtering must use a constrained filter grammar.

Never allow arbitrary SQL or unrestricted expressions from clients.

## 7. Execution lifecycle

For the initial implementation, support bounded synchronous execution
where Worker and Query Service execution limits permit.

Enforce:

- Maximum date range.
- Maximum archive count.
- Maximum execution time.
- Maximum result size.

Handle downstream failures safely.

- Timeout: 504.
- Downstream unavailable: 503.
- Downstream failure: 502.

Do not persist partial results.

If asynchronous execution is required later, use temporary state
with TTL. Do not introduce persistent query job or result tables.

## 8. Exports

Supported formats:

- CSV
- XLSX
- PDF

Export flow:

1. Execute query/report.
2. Generate export.
3. Stream small files directly.
4. Store large files temporarily in private R2.
5. Provide a short-lived download mechanism.
6. Expire/delete the object.

Recommended temporary export expiry: 24 hours.

Use a dedicated temporary export prefix and R2 lifecycle policy.

Do not create a permanent export history table.

## 9. Reports

A report definition stores:

- Source filters.
- Catalog scope.
- Selected columns and metrics.
- Grouping.
- Sorting.
- Date range policy.
- Output preferences.
- Visualization/layout configuration.

At runtime:

1. Load report configuration.
2. Resolve permitted data.
3. Execute relevant queries.
4. Aggregate results.
5. Generate preview or export.
6. Discard generated data.

No report-run or report-result persistence.
8. docs/bmas-integration.md
# BMAS Integration Specification

## 1. Overview

BMAS (Broadcast Monitoring & Archiving Service) is the monitoring
node software.

The central API manages BMAS nodes and streams.

BMAS handles capture, recording, chunking, OLAF indexing and archive
uploads.

## 2. Stream identity

The central API owns the canonical stream UUID.

BMAS maintains its own local stream ID.

When creating a stream on BMAS, send the central stream ID as:

external_id

Example:

{
  "name": "Example Nightclub",
  "source_type": "nightclub",
  "external_id": "central-stream-uuid",
  "url": null,
  "config": {
    "input_type": "audio_device"
  },
  "archive_audio": true,
  "chunk_seconds": 300
}

URL is optional because physical sources may not use URLs.

## 3. BMAS endpoints

The API may call:

| Method | BMAS endpoint |
|---|---|
| GET | /api/v1/streams |
| POST | /api/v1/streams |
| GET | /api/v1/streams/{id} |
| PATCH | /api/v1/streams/{id} |
| DELETE | /api/v1/streams/{id} |
| POST | /api/v1/streams/{id}/start |
| POST | /api/v1/streams/{id}/stop |
| GET | /api/v1/monitoring/summary |
| GET | /api/v1/chunks/failed |

All calls use the node-specific bearer token.

## 4. Archive completion

When a chunk is uploaded and indexed, BMAS calls:

POST /api/v1/nodes/{node_id}/archives

Example:

{
  "node_chunk_id": "bmas-local-chunk-id",
  "external_stream_id": "central-stream-uuid",
  "started_at": "2026-11-02T10:00:00Z",
  "ended_at": "2026-11-02T10:05:00Z",
  "duration_seconds": 300,
  "status": "complete",
  "index_archived": true,
  "audio_archived": false,
  "r2_prefix": "archives/node/stream/date/chunk",
  "manifest": {
    "index_objects": [
      {
        "object_key": "archives/.../index.lmdb",
        "format": "olaf-lmdb",
        "version": "1"
      }
    ],
    "audio_object": null
  },
  "metadata": {}
}

The API must verify:

- Authenticated node identity.
- Stream exists.
- Stream is assigned to the authenticated node.
- Object keys belong to the node's permitted archive prefix.
- Chunk identity is valid.

## 5. Reliability

BMAS callbacks may be retried.

The API must deduplicate using:

(node_id, node_chunk_id)

BMAS should retry transient failures using exponential backoff
and jitter.

The API should support reconciliation against BMAS chunk status.

A completed-chunks listing endpoint is recommended for BMAS,
because a failed-chunks endpoint alone is insufficient for
full archive reconciliation.
9. docs/decisions.md
# Architecture Decisions

These decisions are agreed and should not be changed without approval.

| Area | Decision |
|---|---|
| API framework | Hono |
| Runtime | Cloudflare Workers |
| API codebase | One shared codebase |
| Deployment modes | Operations and Customer |
| Database | Separate D1 per deployment |
| Database schema | Same migrations everywhere |
| Operations data access | Direct D1 or Operations API |
| Authentication | Bearer JWT |
| Identity provider | Cloudflare Access |
| Application user tables | None |
| Roles/memberships | None |
| BMAS authentication | Separate node bearer token |
| Node assignment | One node per stream |
| Assignment table | None |
| Current node field | streams.node_id |
| Reassignment | Create destination, stop/remove old, update node_id |
| Source URL | Nullable |
| Custom metadata | JSON TEXT |
| Catalog | Songs, ads, jingles, station IDs, other |
| D1 storage | Metadata and registries |
| Audio/index storage | R2 |
| Query definitions | Persisted in D1 |
| Query results | Ephemeral |
| Report definitions | Persisted in D1 |
| Report data | Generated at runtime |
| Result tables | None |
| Export storage | Temporary R2 objects |
| Export expiry | 24 hours initially |
| Query Service | Separate Cloudflare Container |
| OLAF implementation | Out of scope |
| BMAS implementation | Separate project |
| Browser-to-BMAS calls | Not allowed |
| API documentation | OpenAPI 3.1 + Swagger UI |
| Rate limiting | Required |
10. agent/AGENTS.md
This is the most important document for guiding AI coding agents. Place it at the repository root as well if your coding agent expects root-level AGENTS.md.

# AI Coding Agent Instructions

You are implementing the Spaza Airplay API Service.

Read the specification documents before making changes.

## 1. Required technology

- TypeScript.
- Hono.
- Cloudflare Workers.
- Cloudflare D1.
- Cloudflare R2.
- OpenAPI 3.1.
- Swagger UI.

Do not introduce another backend framework.

## 2. Read these documents first

1. README.md
2. docs/architecture.md
3. docs/decisions.md
4. db/schema.md
5. docs/api.md
6. docs/security.md
7. docs/query-report-execution.md
8. docs/bmas-integration.md
9. agent/implementation-plan.md

## 3. Architecture rules

There is one API codebase.

It is deployed in two modes:

- operations
- customer

The same API routes, services, schemas and migrations are used
in both modes.

Each deployment has its own D1 database.

Customer mode may access approved Operations data through
a direct D1 binding or Operations API.

Do not create separate API implementations for Operations
and Customer.

## 4. Database rules

- IDs are UUID strings.
- Timestamps are UTC Unix milliseconds.
- Custom metadata is JSON TEXT.
- Audio and OLAF indexes are stored in R2.
- D1 stores metadata and registries.
- streams.node_id identifies the current BMAS node.
- There is no monitoring_assignments table.
- A source belongs to one BMAS node at a time.
- Stream URL is nullable.
- Do not normalize arbitrary metadata into new tables without approval.

## 5. Query and report rules

This is critical.

D1 stores query definitions and report configurations only.

Never create:

- query_jobs
- query_matches
- report_runs
- query result tables
- report result tables
- persistent result caches
- result snapshots

Query results are ephemeral.

Report data is generated at runtime.

Exports may use temporary R2 storage with automatic expiry.

Audit logs may record execution metadata but never result rows.

## 6. Query Service boundary

The OLAF Query Service is a separate Cloudflare Container.

Do not implement OLAF processing.

Implement only a typed client/adapter that:

- Sends validated catalog references.
- Sends authorized archive references.
- Sends query options.
- Receives matching results.
- Handles timeouts and errors.

The API is responsible for authorization and resolving R2 references.

The Query Service must not decide customer authorization.

## 7. Security rules

- All user-facing endpoints require bearer JWT authentication.
- Validate signature, issuer, audience and expiration.
- Never merely decode JWTs.
- BMAS node bearer tokens are separate.
- Never store raw tokens.
- Never log credentials.
- Enforce resource ownership in the service layer.
- Never trust browser-supplied R2 keys or node endpoints.
- Operations must not access customer-private data.
- Customer mode must not expose internal Spaza node details.
- Use parameterized SQL.
- Apply rate limits.
- Return safe errors.

## 8. Hono implementation

Suggested structure:

src/
  index.ts
  app.ts
  env.ts

  middleware/
    auth.ts
    node-auth.ts
    rate-limit.ts
    request-id.ts
    error-handler.ts
    mode.ts

  routes/
    overview.ts
    nodes.ts
    streams.ts
    catalog.ts
    archives.ts
    queries.ts
    reports.ts
    settings.ts
    audit-logs.ts
    bmas-callbacks.ts

  services/
    node-service.ts
    stream-service.ts
    reassignment-service.ts
    catalog-service.ts
    archive-service.ts
    query-service-client.ts
    query-execution-service.ts
    report-service.ts
    export-service.ts
    audit-service.ts

  repositories/
    nodes-repository.ts
    streams-repository.ts
    catalog-repository.ts
    archives-repository.ts
    definitions-repository.ts
    audit-repository.ts

  data-access/
    sources.ts
    operations-direct.ts
    operations-api.ts

  schemas/
    common.ts
    streams.ts
    catalog.ts
    queries.ts
    reports.ts

  types/
    api.ts
    query-service.ts

Keep the implementation cohesive. Do not create unnecessary
abstractions or empty modules.

## 9. Testing requirements

Test:

- Operations and Customer mode behavior.
- Customer data isolation.
- Operations data isolation.
- JWT validation.
- BMAS token validation.
- Rate limiting.
- Stream CRUD.
- Node CRUD.
- Reassignment success.
- Reassignment failures at every stage.
- Archive callback idempotency.
- Metadata filtering.
- SQL injection resistance.
- Query Service timeouts.
- Query Service errors.
- Ephemeral query results.
- Ephemeral report data.
- Export expiry.
- No secrets in logs.

## 10. Implementation discipline

For every task:

1. Read relevant documentation.
2. Inspect existing implementation.
3. Identify affected modules.
4. Implement the smallest coherent change.
5. Add tests.
6. Run formatting.
7. Run type checking.
8. Run tests.
9. Verify migrations.
10. Report changed files and test results.

Do not rewrite unrelated modules.

Do not silently change agreed architecture.

Do not add tables or endpoints that contradict this specification.

If a requirement is ambiguous or conflicts with the architecture,
identify the conflict before implementation.
11. agent/implementation-plan.md
# API Implementation Plan

Implement the API incrementally.

## Phase 1 — Foundation

- Initialize Hono Cloudflare Worker.
- Configure TypeScript.
- Configure Wrangler environments.
- Implement deployment mode configuration.
- Implement D1 bindings.
- Implement request IDs.
- Implement error handling.
- Implement health and readiness endpoints.
- Configure OpenAPI and Swagger UI.

## Phase 2 — Authentication and security

- Implement bearer JWT validation.
- Configure issuer, audience and JWKS.
- Implement authentication middleware.
- Implement BMAS node bearer authentication.
- Implement mode-aware authorization.
- Implement rate limiting.
- Configure CORS.
- Implement request-size limits.
- Implement audit logging.

## Phase 3 — Data access

- Implement primary D1 access.
- Implement Operations direct-access adapter.
- Implement Operations API adapter.
- Implement deployment-mode data resolution.
- Implement common repositories.

## Phase 4 — Nodes and streams

- Node CRUD.
- Node token provisioning and rotation.
- Node status.
- Stream CRUD.
- Stream start/stop proxy.
- Stream reassignment orchestration.
- Reassignment failure handling.

## Phase 5 — Catalog and archives

- Catalog CRUD.
- Catalog import.
- Private/global catalog scope.
- Archive registration.
- Archive listing.
- Archive coverage.
- BMAS heartbeat and event callbacks.

## Phase 6 — Query Engine

- Saved query CRUD.
- Ad-hoc query validation.
- Metadata filters.
- Catalog resolution.
- Archive authorization.
- R2 reference resolution.
- Query Service adapter.
- Runtime query execution.
- Query export.

Do not implement the OLAF Query Service itself.

## Phase 7 — Reports

- Report definition CRUD.
- Report configuration validation.
- Runtime report generation.
- Report preview.
- Report export.
- Temporary export lifecycle.

Do not implement report result persistence.

## Phase 8 — Hardening

- Integration tests.
- Authorization tests.
- Rate-limit tests.
- Reassignment failure tests.
- Query Service failure tests.
- Export cleanup tests.
- Load testing.
- Security review.
- Deployment documentation.

## Definition of done

A feature is complete when:

- Request validation exists.
- Authorization is enforced.
- Errors are handled consistently.
- Tests cover success and failure.
- OpenAPI documentation is updated.
- No secrets are logged.
- No query/report results are persisted.
- Operations and Customer modes are verified where applicable.
A few important implementation notes
There are four details I would preserve as the project moves into implementation:

Reassignment should have an in-progress lock. Even though a source can only belong to one node, two simultaneous reassignment requests could otherwise create conflicting operations. Use an atomic conditional update or a Durable Object/coordinator for the operation. Do not introduce an assignments table.

Bearer authentication needs a defined token issuer. Cloudflare Access is the identity boundary, but the API must validate an actual signed JWT. Configure the issuer, audience and JWKS explicitly rather than assuming the Access browser cookie is itself a bearer token.

Temporary exports need cleanup. Use a dedicated R2 prefix and lifecycle expiry. Do not create a permanent export-history or report-run table.

The Query Service contract should be versioned independently. The API should depend on a typed interface, not on the OLAF container's internal implementation. This allows the separate Query Service to evolve without forcing changes throughout the API.

These documents capture the agreed architecture, including the single-node-per-source model and ephemeral query/report results.

