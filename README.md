# TheFinpedia Logs360

Logs360 is an observability and analytics warehouse built for TheFinpedia.

Instead of reading raw JSON log files manually, Logs360 converts application logs and Cobrand traces into structured analytical tables that can be queried using SQL and visualized in a live Streamlit dashboard — and permanently archives every log file it processes as compressed Parquet, with a full audit trail of what happened to each one.

The warehouse currently supports:

- API Logs
- Cobrand Logs

Future sources can be added without changing the warehouse architecture.

Two things live in this repo: the ETL pipeline that builds the warehouse (runs locally, on your machine), and a Streamlit dashboard on top of it (runs locally for development, and is deployed publicly on Render for the team). They're connected by a small, deliberately narrow export step — see [Dashboard Deployment](#dashboard-deployment-render) below for why the deployed dashboard never touches the full 4GB+ warehouse directly.

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
│   ├── config.py                # AUTO_DELETE_JSON and other pipeline settings
│   ├── instrumentation_gap.py   # Discovers recurring unstructured log messages
│   └── signature_rules.py       # Hand-maintained failure-signature label mapping
│
├── parsers/
│   └── parser.py                # Parses raw Cobrand trace data
│
├── dashboard/                    # Streamlit dashboard (Logs360 UI)
│   ├── app.py                   # Entry point / Home page
│   ├── database.py              # Connection layer (local-safe + Render-fast dual mode)
│   ├── queries.py               # All SQL query functions, one place per metric
│   ├── charts.py                # Reusable Plotly chart helpers
│   ├── components.py            # Reusable UI: metric cards, event labeling, status badge
│   ├── styles.py                # Dark theme CSS
│   └── pages/                   # One file per dashboard page (see Dashboard Pages below)
│       ├── 1_Executive_Dashboard.py
│       ├── 2_Social_Media_Platform_Health.py
│       ├── 3_Failure_Explorer.py
│       ├── 4_User_Investigation.py
│       ├── 5_Workflow_Explorer.py
│       ├── 6_Capability_Coverage.py
│       ├── 7_Instrumentation_Gaps.py
│       ├── 8_Raw_Log_Search.py
│       └── 9_Settings.py
│
├── .streamlit/
│   └── config.toml              # Dark theme config (Streamlit's native theming)
│
├── refresh_logs.py              # ETL orchestrator — the only ETL script run daily
├── export_dashboard_tables.py   # Exports the dashboard's 6 needed tables to Parquet
├── sync_to_render.sh            # Full automation: refresh → export → upload → redeploy
├── startup.py                   # Runs on Render before Streamlit starts (see below)
├── requirements.txt             # Dashboard's Python dependencies (pinned — see note below)
│
├── finpedia_logs.db             # DuckDB database (warehouse + manifest) — LOCAL ONLY,
│                                 # never committed to git (it's multiple GB and growing)
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

Simply drop new log files into `logs/api/` or `logs/cobrand/`. Running `refresh_logs.py` automatically imports, archives, and validates them — already-imported files are skipped on the import step, and already-archived files are skipped on the archive step (unless the archive is found to be corrupted, in which case it's rebuilt automatically — see [Self-Healing Archive](#self-healing-archive) below).

A JSON log file is only deleted after its row counts have been verified to match across the JSON, the warehouse, and the Parquet archive. If validation fails, the JSON is left in place and nothing further happens to it until the underlying issue is fixed and the file is reprocessed.

### `etl/`

Contains the ETL pipeline. Each module has exactly one responsibility, and `refresh_logs.py` is the only file that makes decisions — everything in `etl/` is a pure utility that reports back what happened.

**`api_loader.py`**
- imports new API logs (parsed line-by-line as JSON, not via DuckDB's CSV/JSON auto-detection, to avoid crashing on malformed lines or deeply nested payloads)
- creates/updates `raw_logs_api`
- builds `event_fact_api`
- updates `event_catalog_api`
- synchronizes `capability_catalog` (the Event Registry — see below)
- synchronizes `instrumentation_gap_catalog` for API-sourced messages
- returns the list of files successfully processed, for the archiver

> **Note:** unlike the Cobrand side (see `cobrand_loader.py` below), `event_fact_api` is currently rebuilt in full from `raw_logs_api` on every run with new files, not incrementally. This is a known, deliberate deferral — `raw_logs_api` stores the complete raw JSON text of every API log line ever imported, which is why `finpedia_logs.db` is multiple gigabytes and growing daily, and why it can't be safely pruned without first giving `api_loader.py` the same incremental (`raw_id`-tracked, append-only) treatment already built for `span_fact`. Worth doing eventually; not urgent while local disk space isn't actually tight.

**`cobrand_loader.py`**
- imports new Cobrand logs
- parses traces incrementally — each raw trace row gets a stable, sequence-backed `raw_id` at insert time, and only rows not yet represented in `span_fact` get re-parsed on a given run. This means a refresh with no new Cobrand data costs almost nothing, regardless of how much historical data already exists — a deliberate fix once Cobrand volume was expected to grow into the tens of thousands of lines/day (a one-time full rebuild runs automatically and transparently if it ever detects an older, pre-`raw_id` `span_fact` table)
- flattens spans
- creates `span_fact` and `span_context` incrementally (append-only, never a full rebuild)
- synchronizes `dim_span`
- builds `cobrand_event_fact` (rebuilt from whatever `.log` files currently exist on disk — see note below)
- synchronizes `instrumentation_gap_catalog` for Cobrand-sourced messages
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
- maintains `archive_manifest`, a permanent audit trail: one row per source file, tracking it through `ARCHIVED` → `VALIDATED`/`FAILED` → `DELETED` (or `VALIDATED_KEPT` if auto-delete is off)

**`file_ops.py`**
- `delete_json_file()` is the only place in the codebase that ever deletes a source log file — logs the outcome and returns `True`/`False` rather than raising
- `compute_checksum()` fingerprints a file right before it's deleted, stored in the manifest

**`config.py`**
- `AUTO_DELETE_JSON` (default `True`) — controls whether a validated JSON log actually gets deleted. Turn it off during development:
  ```bash
  AUTO_DELETE_JSON=false python3 refresh_logs.py
  ```

**`instrumentation_gap.py`**
- shared by both loaders — discovers recurring log messages that have no structured `event_action`, normalizes them into a grouping key (`raw_pattern`), and maintains `instrumentation_gap_catalog`
- resolves each `raw_pattern` to a short canonical `signature` via `signature_rules.py`
- also powers a matching feature in the dashboard's Raw Log Search — searching a known signature (e.g. `VIDEO_MAX_FPS_TOO_LOW`) finds every raw log line that normalizes to that same pattern, even though the signature text itself never appears verbatim in the raw message
- see [Instrumentation Gap Catalog](#instrumentation-gap-catalog) below for the full picture

**`signature_rules.py`**
- a small, hand-maintained list mapping known recurring message patterns to short signature labels (e.g. `FFPROBE_INVALID_MEDIA`, `TWITTER_TOKEN_INVALID`) — deliberately just a flat list, not a rule engine. Add one entry whenever a new recurring, understood failure shows up in the pending backlog; nothing else needs to change

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

Converts nested traces into relational rows while preserving parent-child relationships.

### `refresh_logs.py`

Master orchestrator. Runs the complete warehouse refresh — this is the only ETL script that needs to be executed daily (though in practice, it now runs automatically every morning via `cron` as part of `sync_to_render.sh` — see [Keeping the Deployed Dashboard in Sync](#keeping-the-deployed-dashboard-in-sync) below).

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
Streamlit Dashboard (local dev, and deployed on Render)
```

### API Warehouse

- `raw_logs_api` — stores imported API log lines.
- `event_fact_api` — one row per API event. Main analytical table.
- `event_catalog_api` — unique event actions, used for capability classification.
- `capability_catalog` — the Event Registry: one row per event prefix (the token before the first dot in `event_action`, e.g. `posting_job.*` → `posting_job`), automatically discovered and enriched every run.

The philosophy: the ETL discovers events and maintains operational metadata; **humans classify**. The ETL never overwrites a human's classification.

| column | owner | notes |
|---|---|---|
| `event_prefix` | ETL | primary key |
| `capability` / `subsystem` / `description` | human | never touched by the ETL |
| `classification_status` | human | `Pending` → `Classified` / `Deprecated` / `Ignored`. Self-heals to `Pending` if ever `NULL` (e.g. after a schema migration), so a row can never silently vanish from the "needs review" list |
| `first_seen` | ETL | self-corrects via `LEAST(existing, new)` — a backfilled older log file will move this earlier automatically |
| `last_seen` / `event_count` | ETL | recomputed fresh every run from `event_fact_api`, not incremented |
| `sample_event` / `sample_service` | ETL | tied to the most recent occurrence of that prefix (via `ARG_MAX` on event time), not an arbitrary row |

Query the classification backlog, sorted by volume so the highest-impact prefixes surface first:

```sql
SELECT event_prefix, event_count, last_seen, sample_event
FROM capability_catalog
WHERE classification_status = 'Pending'
ORDER BY event_count DESC;
```

> **Note:** `event_time` throughout the API warehouse is stored as text in a non-ISO format (`"Sun, 12 Jul 2026 00:00:04 AM"`), and real data mixes in a second, self-contradictory variant (24-hour values with a spurious AM/PM suffix, e.g. `"18:13:04 PM"` — a bug in whatever service emits these logs). Every timestamp comparison in this warehouse — including in the dashboard — parses through a `COALESCE` of both formats rather than trusting one. The dashboard reuses this exact parsing logic directly from `etl/api_loader.py` rather than redefining it, so there's exactly one source of truth for it.

### Cobrand Warehouse

- `cobrand_raw_logs` — stores raw Cobrand log lines that carry a trace (`trace.method IS NOT NULL`). Each row carries a stable `raw_id`, assigned once at insert time, which is what makes `span_fact`'s parsing incremental (see `cobrand_loader.py` above).

- `span_fact` — one row per execution span. Contains flattened analytical fields:
  - `user_id`
  - `request_id`
  - `content_id`
  - `duration_ms`
  - `result_status`
  - `content_type`
  - queue metrics
  - callback metrics

  This is the primary table used by dashboards.

- `span_context` — stores the original `details` and `attributes` JSON payloads. No information is lost. Used only when additional debugging context is required.

- `dim_span` — dimension table containing every unique Cobrand span. Examples: `cobrand.generate`, `cobrand.image.generate`, `cobrand.s3.upload`.

- `cobrand_event_fact` — standalone (non-trace) event lines. Note: unlike the other tables, this one is rebuilt each run from whichever `.log` files currently exist in `logs/cobrand/` — not from import history. If a file's `.log` has already been archived and deleted, its rows stay in `cobrand_event_fact` (the loader deliberately skips the rebuild rather than recreating the table from an empty result when no files are present), but this table should not be treated as an independent source of truth the way `raw_logs_api` or `cobrand_raw_logs` can be.

  This is also where several real instrumentation gaps live (e.g. `VIDEO_MAX_FPS_TOO_LOW`) — they're standalone events, never wrapped in a trace, so they only ever appear here, not in `span_fact`. The dashboard's search and investigation pages query both tables for exactly this reason.

## Instrumentation Gap Catalog

While the Event Registry tracks structured events (ones with a real `event_action`), most log lines aren't structured at all — free-text messages like `"Twitter token refresh failed"` or a raw Node.js stack trace. `instrumentation_gap_catalog` is the companion catalog for exactly those: recurring unstructured messages that don't yet have a proper `event_action`, but probably should.

```
Raw message
  ↓
Noise filter        (drops framework/transport noise — "HTTP REQUEST"
                      alone was 98% of one real sample; without this
                      filter the catalog is unusable)
  ↓
First line only      (a multi-line stack trace is never used as the
                      grouping key — only its first line is; the full
                      trace is still preserved, in example_message)
  ↓
Normalize            (strip ISO timestamps → {timestamp}, UUIDs → {uuid},
                      hex addresses → {hex}, remaining digits → {n})
  ↓
raw_pattern           (the grouping key)
  ↓
signature             (a short canonical label, via signature_rules.py —
                       defaults to raw_pattern itself if no rule matches)
  ↓
instrumentation_gap_catalog
```

Populated from both warehouses — `event_fact_api` (API) and `cobrand_event_fact` (Cobrand) — via the same shared logic in `instrumentation_gap.py`, so the noise filter and normalization rules exist in exactly one place rather than being duplicated per loader.

| column | owner | notes |
|---|---|---|
| `raw_pattern` / `source_system` | ETL | composite primary key. `source_system` (`API`/`COBRAND`) is part of the key, not just a label — otherwise syncing both sources into one table risks one silently overwriting the other's row for an identical pattern |
| `signature` | ETL | resolved via `signature_rules.py`, re-resolved and overwritten every run — adding a new rule retroactively relabels an already-catalogued pattern next time it syncs, no manual migration needed. **Important:** a signature that hasn't matched any rule yet equals `raw_pattern` verbatim — this is intentional (see `signature_rules.py`'s docstring), but code consuming this table should not assume "signature is present" means "genuinely classified"; compare `signature != raw_pattern` to tell the two apart (the dashboard does this — see `components.format_event_label`) |
| `classification_status` / `probable_component` / `recommended_event_action` / `notes` | human | never touched by the ETL (same self-healing-to-`Pending` behavior as the Event Registry) |
| `occurrence_count` / `first_seen` / `last_seen` / `example_message` / `service_name` / `log_level` | ETL | recomputed fresh every run; `example_message` always holds the full original text (including any stack trace) even though `raw_pattern` is based on just the first line |

Example workflow: once a recurring pattern is understood, add a rule to `signature_rules.py`:

```python
{"pattern": r"ffprobe exited", "signature": "FFPROBE_INVALID_MEDIA"},
```

No other code changes — the next sync relabels every matching row automatically. To go further and record what the failure actually means for a specific row, use the human-owned columns directly:

```sql
UPDATE instrumentation_gap_catalog
SET recommended_event_action = 'video.validation.max_fps_failed',
    probable_component = 'Cobrand Video Generator'
WHERE raw_pattern LIKE '%Max FPS found from the video files%';
```

## Archive & Audit Layer

`logs/archive/api/<year>/<month>/*.parquet` and `logs/archive/cobrand/<year>/<month>/*.parquet` — every successfully validated log file, permanently archived in compressed columnar form.

`archive_manifest` — one row per source file the pipeline has ever touched:

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

### Self-Healing Archive

If a Parquet file already exists for a log but turns out to be unreadable (e.g. a prior run was killed mid-write), `archive.py` detects this automatically, deletes the corrupted file, and rebuilds it from the source JSON on the same run — no manual intervention needed.

## Daily Workflow (ETL only)

1. Drop new files into `logs/api/` and/or `logs/cobrand/`.
2. Run:
   ```bash
   python3 refresh_logs.py
   ```
3. The warehouse refreshes, every processed file is archived to Parquet, validated, and recorded in `archive_manifest`. No duplicate imports occur, and JSON logs are only deleted once their data is confirmed safe in both the warehouse and the archive.

In practice this step now runs automatically every morning as the first stage of `sync_to_render.sh` (see below) — running it manually is still useful for testing, or for an immediate refresh outside the schedule.

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
capability_catalog (Event Registry)
  ↓
instrumentation_gap_catalog
  ↓
Parquet Archive + archive_manifest
```

**Cobrand Logs**

```
JSON
  ↓
cobrand_raw_logs (raw_id-tracked)
  ↓
Trace Parser (incremental — only unprocessed raw_id rows)
  ↓
span_fact + span_context (append-only)
  ↓
instrumentation_gap_catalog
  ↓
Parquet Archive + archive_manifest
```

---

## Logs360 Dashboard

A Streamlit app on top of the warehouse — `dashboard/`. Every query it runs is defined once in `dashboard/queries.py`; pages call those functions and render the result, they never contain raw SQL themselves.

### Dashboard Pages

| page | what it shows |
|---|---|
| Executive Dashboard | Platform-wide KPIs (Errors, Warnings, Cobrand Failures, Posting Failures — each with a day-over-day delta arrow), events/errors over time, top services/platforms/event prefixes/errors/users |
| Social Media Platform Health | Health cards from `service_name` and `platform_name` (real columns) rather than a named subsystem list (Posting/Scheduler/Auth/WhatsApp/etc.) — the schema doesn't have a clean mapping to that taxonomy |
| Failure Explorer | Failures across both warehouses: timeline, distribution by source, top failed events/planners/users, and a paginated, expandable failure table with a date + time-of-day filter |
| User Investigation | Search by User ID — full chronological activity feed across both warehouses, platforms, planner IDs, error rate |
| Workflow Explorer | Chronological event sequence for a given Request/Planner/User ID. Deliberately **not** a reconstructed workflow model (no Gantt chart, no computed end-to-end duration, no cross-request correlation) — that would need a purpose-built table correlating events across ID boundaries, and whether `request_id` maps cleanly to "one workflow" hasn't been validated against real data. What's here is a correctly-ordered timeline, which is genuinely useful on its own |
| Capability Coverage | The Event Registry (`capability_catalog`): coverage KPIs, recent discoveries, filterable/sortable full catalog |
| Instrumentation Gaps | `instrumentation_gap_catalog`: recurring unstructured patterns awaiting classification |
| Raw Log Search | Full-text search across `message`, `error_message`, `error_stack`, and `event_action`, across all three source tables (`event_fact_api`, `span_fact`, `cobrand_event_fact`) — plus signature-aware search (see `instrumentation_gap.py` note above). CSV export |
| Settings | Read-only warehouse status, table row counts, ETL config visibility |

### Connection Handling — Local vs Render

`dashboard/database.py` runs in one of two modes, detected automatically via Render's own `RENDER` environment variable (always `"true"` there, unset locally):

- **Locally:** opens a fresh, short-lived, read-only connection per query, with retry/backoff on lock conflicts. This exists specifically so the dashboard never blocks `refresh_logs.py` if you happen to run both at once — verified directly that a long-lived connection causes a concurrent local writer to fail immediately.
- **On Render:** that concurrent-writer risk doesn't exist (`startup.py` is the only writer, and it always finishes before Streamlit starts serving). So on Render, one connection is opened once and reused for the process's lifetime, with each query getting its own thread-safe cursor via `.cursor()` — sharing the raw connection object directly across concurrent Streamlit sessions was tested and found to silently corrupt results, not just risk a crash, so this distinction matters. Confirmed ~7x faster per page load than the local-safe path under equivalent conditions.

### Running the Dashboard Locally

From the project root (not from inside `dashboard/`):

```bash
pip3 install -r requirements.txt
python3 -m streamlit run dashboard/app.py
```

`requirements.txt` pins `streamlit` and `starlette` to exact versions (not just a floor) — an unpinned `streamlit>=X` let Render resolve a newer `starlette` than Streamlit's bundled code expected, causing every request to fail with a 500 until pinned. `duckdb` is also exact-pinned, since several ETL behaviors this project depends on (sequence-backed column defaults, `ON CONFLICT DO UPDATE`, specific `TRY_STRPTIME` edge cases) were verified against one specific version.

## Dashboard Deployment (Render)

The deployed dashboard never touches `finpedia_logs.db` directly — that file is 4GB+ and growing (see `api_loader.py`'s note above), lives only on your local machine, and is never committed to git. Instead:

```
finpedia_logs.db  (local, 4GB+, never leaves your machine)
        ↓  export_dashboard_tables.py
dashboard_export/  (6 tables the dashboard actually queries, ~10MB compressed Parquet)
        ↓  zip + upload
Google Drive  (dashboard_export.zip)
        ↓  startup.py, on Render, at container start
finpedia_logs.db  (rebuilt as VIEWs directly over the Parquet — no data copy, near-instant)
        ↓
Streamlit
```

`export_dashboard_tables.py` exports only `event_fact_api`, `span_fact`, `cobrand_event_fact`, `capability_catalog`, `instrumentation_gap_catalog`, and `archive_manifest` — the exhaustive list of tables the dashboard's `queries.py` actually references, confirmed by grepping every `FROM` clause rather than guessed from memory. Everything else (`raw_logs_api`, `cobrand_raw_logs`, `span_context`, `dim_span`, `event_catalog_api`) is ETL-internal and never needed by the dashboard.

`startup.py` runs once, before Streamlit starts (`python startup.py && streamlit run dashboard/app.py ...`). If `finpedia_logs.db` doesn't already exist in the current container, it downloads `dashboard_export.zip` from Google Drive (via the file's Google Drive ID, `gdown.download(id=...)` — not the folder-download API, which was found to be considerably less reliable), extracts it, and creates the 6 tables as `VIEW`s directly over the Parquet files rather than copying the data into DuckDB's own storage.

**Render setup:**

- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python startup.py && streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0`
- `.streamlit/config.toml` (repo root, not inside `dashboard/`) sets Streamlit's native dark theme — this is the correct way to theme Streamlit; CSS injection alone was found to miss elements like the sidebar navigation links.
- Render's free tier spins its instance down after 15 minutes of inactivity (cold start: 30–60s on the next visit) and grants 750 free instance-hours/month — enough for normal usage patterns, but keeping the service artificially always-warm via constant pinging would consume nearly the entire monthly allowance on its own.

## Keeping the Deployed Dashboard in Sync

Since the deployed dashboard only ever sees what's in `dashboard_export.zip` on Google Drive, getting fresh data live requires re-running the export and re-uploading it — `sync_to_render.sh` automates the whole thing:

```
refresh_logs.py           (refresh the local warehouse)
        ↓
export_dashboard_tables.py (export the 6 dashboard tables to Parquet)
        ↓
zip + rclone               (overwrite dashboard_export.zip on Drive IN PLACE —
                             same file ID, so startup.py's hardcoded ID keeps working)
        ↓
curl (Render Deploy Hook)  (force a redeploy so startup.py re-runs with fresh data)
```

**One-time setup:**

1. Install `rclone` (`brew install rclone`) and run `rclone config` to authorize a `gdrive` remote (interactive — opens a browser to log into Google).
2. Get your Render Deploy Hook URL from the service's Settings page.
3. Run manually once to confirm each step works:
   ```bash
   RENDER_DEPLOY_HOOK_URL="<your-hook-url>" ./sync_to_render.sh
   ```
4. Schedule it daily via `cron` (`EDITOR=nano crontab -e`):
   ```
   0 7 * * * cd /path/to/finpedia_log_analysis && RENDER_DEPLOY_HOOK_URL="<your-hook-url>" ./sync_to_render.sh >> sync_log.txt 2>&1
   ```

Both the Deploy Hook URL and `rclone`'s Drive credentials are real secrets — they live only in your `crontab` entry and `rclone`'s local config, never committed to git.

> **Known caveat:** `startup.py` downloads by Google Drive file ID, not filename. `rclone copyto` is expected to update the existing file's content in place (preserving its ID) rather than creating a new file, but this should be spot-checked after the first automated run — compare the file's share-link ID against `DEFAULT_GDRIVE_FILE_ID` in `startup.py`. If it ever changes, no code change is needed: `startup.py` reads `GDRIVE_FILE_ID` from an environment variable first, so updating it in Render's dashboard is enough.

> **Not yet automated:** fetching new raw `.log` files onto the local machine in the first place (currently a manual download from S3 before running `sync_to_render.sh`). A real next step if this becomes a bigger source of friction.
