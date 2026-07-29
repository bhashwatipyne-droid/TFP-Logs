# TheFinpedia Logs360

Logs360 is an observability and analytics warehouse built for TheFinpedia.

Instead of reading raw JSON log files manually, Logs360 converts application
logs and Cobrand traces into structured analytical tables that can be
queried using SQL and visualized in Streamlit — and permanently archives
every log file it processes as compressed Parquet, with a full audit trail
of what happened to each one.

The warehouse currently supports:

- API Logs
- Cobrand Logs

Future sources can be added without changing the warehouse architecture.

## Project Structure

```
finpedia-log-system/
│
├── logs/
│   ├── api/                     # Drop new API .log files here
│   ├── cobrand/                 # Drop new Cobrand .log files here
│   └── archive/                 # Permanent Parquet archive (auto-created)
│       ├── api/
│       │   └── <year>/<month>/
│       └── cobrand/
│           └── <year>/<month>/
│
├── etl/
│   ├── api_loader.py            # Loads API logs into the warehouse
│   ├── cobrand_loader.py        # Loads Cobrand logs into the warehouse
│   ├── archive.py               # Converts JSON logs into Parquet
│   ├── validator.py             # Verifies JSON / warehouse / Parquet row counts match
│   ├── manifest.py              # Records every file's lifecycle in archive_manifest
│   ├── file_ops.py              # The one place a log file is ever deleted
│   └── config.py                # AUTO_DELETE_JSON and other pipeline settings
│
├── parsers/
│   └── parser.py                # Parses raw Cobrand trace data
│
├── refresh_logs.py              # Orchestrator — the only script run daily
│
├── finpedia_logs.db             # DuckDB database (warehouse + manifest)
│
└── README.md                    # Documentation
```

## Folder Explanation

### `logs/`

Contains raw log files, plus the permanent Parquet archive.

```
logs/
    api/
        application-2026-07-08.log

    cobrand/
        cobrand-application-2026-07-08.log

    archive/
        api/2026/07/application-2026-07-08.parquet
        cobrand/2026/07/cobrand-application-2026-07-08.parquet
```

Simply drop new log files into `logs/api/` or `logs/cobrand/`. Running
`refresh_logs.py` automatically imports, archives, and validates them —
already-imported files are skipped on the import step, and already-archived
files are skipped on the archive step (unless the archive is found to be
corrupted, in which case it's rebuilt automatically — see **Self-Healing
Archive** below).

A JSON log file is only deleted after its row counts have been verified to
match across the JSON, the warehouse, and the Parquet archive. If validation
fails, the JSON is left in place and nothing further happens to it until the
underlying issue is fixed and the file is reprocessed.

### `etl/`

Contains the ETL pipeline. Each module has exactly one responsibility, and
`refresh_logs.py` is the only file that makes decisions — everything in
`etl/` is a pure utility that reports back what happened.

**`api_loader.py`**
- imports new API logs (parsed line-by-line as JSON, not via DuckDB's CSV/JSON auto-detection, to avoid crashing on malformed lines or deeply nested payloads)
- creates/updates `raw_logs_api`
- builds `event_fact_api`
- updates `event_catalog_api`
- synchronizes `capability_catalog`
- returns the list of files successfully processed, for the archiver

**`cobrand_loader.py`**
- imports new Cobrand logs
- parses traces
- flattens spans
- creates `span_fact`
- creates `span_context`
- synchronizes `dim_span`
- builds `cobrand_event_fact` (rebuilt from whatever `.log` files currently exist on disk — see note below)
- returns the list of files successfully processed, for the archiver

**`archive.py`**
- converts each processed JSON log into a compressed (ZSTD) Parquet file under `logs/archive/`
- parses lines in Python (matching the loaders), never handing raw log files to DuckDB's schema inference
- verifies an existing archive is actually readable before trusting it, self-healing if it isn't
- never deletes the source JSON — only reports back what was archived

**`validator.py`**
- confirms JSON row count == warehouse row count == Parquet row count for a given file
- for Cobrand files, "warehouse row count" is the sum of `cobrand_raw_logs` (trace-bearing lines) and `cobrand_event_fact` (standalone event lines), since a raw Cobrand line lands in exactly one of those two tables, never both

**`manifest.py`**
- maintains `archive_manifest`, a permanent audit trail: one row per source file, tracking it through `ARCHIVED → VALIDATED/FAILED → DELETED` (or `VALIDATED_KEPT` if auto-delete is off)

**`file_ops.py`**
- `delete_json_file()` is the only place in the codebase that ever deletes a source log file — logs the outcome and returns `True`/`False` rather than raising
- `compute_checksum()` fingerprints a file right before it's deleted, stored in the manifest

**`config.py`**
- `AUTO_DELETE_JSON` (default `True`) — controls whether a validated JSON log actually gets deleted. Turn it off during development:
  ```
  AUTO_DELETE_JSON=false python3 refresh_logs.py
  ```

### `parsers/`

Contains the OpenTelemetry-style trace parser (unchanged).

```
Trace
  ↓
Span
  ↓
Children
  ↓
Grandchildren
```

Converts nested traces into relational rows while preserving parent-child
relationships.

### `refresh_logs.py`

Master orchestrator. Runs the complete warehouse refresh — this is the only
script that needs to be executed daily.

```
Load API
  ↓
Load Cobrand
  ↓
Archive API
  ↓
Validate API
  ↓
Record in archive_manifest
  ↓
Delete API JSON (if AUTO_DELETE_JSON)
  ↓
Archive Cobrand
  ↓
Validate Cobrand
  ↓
Record in archive_manifest
  ↓
Delete Cobrand JSON (if AUTO_DELETE_JSON)
  ↓
Warehouse Ready
```

## Warehouse Architecture

```
Raw Logs
  ↓
Structured Facts
  ↓
Dimensions
  ↓
Permanent Parquet Archive + Audit Trail
  ↓
Streamlit Dashboard
```

### API Warehouse

**`raw_logs_api`** — stores imported API log lines.

**`event_fact_api`** — one row per API event. Main analytical table.

**`event_catalog_api`** — unique event actions, used for capability
classification.

**`capability_catalog`** — business capability mapping.

Example: `posting_job.*` → `Posting`

### Cobrand Warehouse

**`cobrand_raw_logs`** — stores raw Cobrand log lines that carry a trace
(`trace.method IS NOT NULL`).

**`span_fact`** — one row per execution span. Contains flattened analytical
fields:

- `user_id`
- `request_id`
- `content_id`
- `duration_ms`
- `result_status`
- `content_type`
- queue metrics
- callback metrics

This is the primary table used by dashboards.

**`span_context`** — stores the original `details` and `attributes` JSON
payloads. No information is lost. Used only when additional debugging
context is required.

**`dim_span`** — dimension table containing every unique Cobrand span.
Examples: `cobrand.generate`, `cobrand.image.generate`, `cobrand.s3.upload`.

**`cobrand_event_fact`** — standalone (non-trace) event lines. **Note:**
unlike the other tables, this one is rebuilt each run from whichever
`.log` files currently exist in `logs/cobrand/` — not from import history.
If a file's `.log` has already been archived and deleted, its rows stay in
`cobrand_event_fact` (the loader deliberately skips the rebuild rather than
recreating the table from an empty result when no files are present), but
this table should not be treated as an independent source of truth the way
`raw_logs_api` or `cobrand_raw_logs` can be.

### Archive & Audit Layer

**`logs/archive/api/<year>/<month>/*.parquet`** and
**`logs/archive/cobrand/<year>/<month>/*.parquet`** — every successfully
validated log file, permanently archived in compressed columnar form.

**`archive_manifest`** — one row per source file the pipeline has ever
touched:

| column | meaning |
|---|---|
| `source_file` | primary key |
| `log_type` | `api` or `cobrand` |
| `archive_path` | path to the Parquet file |
| `json_rows` / `warehouse_rows` / `parquet_rows` | row counts at validation time |
| `archived_at` / `validated_at` / `deleted_at` | timestamps for each stage |
| `status` | `ARCHIVED`, `VALIDATED`, `FAILED`, `DELETED`, or `VALIDATED_KEPT` |
| `checksum` | SHA-256 of the JSON file, captured right before deletion |

Query it directly for a full audit trail:

```sql
SELECT source_file, log_type, status, json_rows, warehouse_rows, parquet_rows,
       archived_at, validated_at, deleted_at
FROM archive_manifest
ORDER BY archived_at DESC;
```

## Self-Healing Archive

If a Parquet file already exists for a log but turns out to be unreadable
(e.g. a prior run was killed mid-write), `archive.py` detects this
automatically, deletes the corrupted file, and rebuilds it from the source
JSON on the same run — no manual intervention needed.

## Daily Workflow

1. Drop new files into `logs/api/` and/or `logs/cobrand/`.
2. Run:
   ```
   python3 refresh_logs.py
   ```
3. The warehouse refreshes, every processed file is archived to Parquet,
   validated, and recorded in `archive_manifest`. No duplicate imports
   occur, and JSON logs are only deleted once their data is confirmed safe
   in both the warehouse and the archive.

## Current Pipeline

**API Logs**

```
JSON
  ↓
raw_logs_api
  ↓
event_fact_api
  ↓
event_catalog_api
  ↓
capability_catalog
  ↓
Parquet Archive + archive_manifest
```

**Cobrand Logs**

```
JSON
  ↓
cobrand_raw_logs
  ↓
Trace Parser
  ↓
span_fact
  ↓
span_context
  ↓
dim_span
  ↓
Parquet Archive + archive_manifest
```
