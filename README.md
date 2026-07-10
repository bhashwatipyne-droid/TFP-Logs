# TheFinpedia Logs360

Logs360 is an observability and analytics warehouse built for TheFinpedia.

Instead of reading raw JSON log files manually, Logs360 converts application logs and Cobrand traces into structured analytical tables that can be queried using SQL and visualized in Streamlit.

The warehouse currently supports:

- API Logs
- Cobrand Logs

Future sources can be added without changing the warehouse architecture.

---

# Project Structure

finpedia_log_analysis/

│
├── logs/
│   ├── api/
│   └── cobrand/
│
├── etl/
│   ├── api_loader.py
│   └── cobrand_loader.py
│
├── parsers/
│   └── parser.py
│
├── refresh_logs.py
│
├── finpedia_logs.db
│
└── README.md
```

# Folder Explanation

## logs/

Contains raw log files.

```
logs/
    api/
        application-2026-07-08.log

    cobrand/
        cobrand-application-2026-07-08.log
```

Simply drop new log files into these folders.

Running

```bash
python3 refresh_logs.py
```

automatically imports any new files.

Already imported files are skipped.

---

## etl/

Contains the ETL pipelines.

### api_loader.py

Responsible for

- importing API logs
- creating raw_logs
- building event_fact_api
- updating event_catalog_api
- synchronizing capability_catalog

---

### cobrand_loader.py

Responsible for

- importing Cobrand logs
- parsing traces
- flattening spans
- creating span_fact
- creating span_context
- synchronizing dim_span

---

## parsers/

Contains the OpenTelemetry-style parser.

Current parser:

```
Trace

↓
Span
↓
Children
↓
Grandchildren
```

The parser converts nested traces into relational rows while preserving parent-child relationships.

---

## refresh_logs.py

Master refresh script.

Runs the complete warehouse refresh.

```
API Logs
↓
Cobrand Logs
↓
Warehouse Ready
```

This is the only script that needs to be executed daily.

---

# Warehouse Architecture

```
Raw Logs
↓
Structured Facts
↓
Dimensions
↓
Streamlit Dashboard
```

---

## API Warehouse

### raw_logs

Stores imported API log files.

---

### event_fact_api

One row per API event. Main analytical table.

---

### event_catalog_api

Unique event actions. Used for capability classification.

---

### capability_catalog

Business capability mapping.

Example

```
posting_job.*

↓

Posting
```

---

## Cobrand Warehouse

### cobrand_raw_logs

Stores raw Cobrand traces.

---

### span_fact

One row per execution span. Contains flattened analytical fields.

Examples

- user_id
- request_id
- content_id
- duration_ms
- result_status
- content_type
- queue metrics
- callback metrics

This is the primary table used by dashboards.

---

### span_context

Stores the original

- details
- attributes

JSON payloads.

No information is lost. Used only when additional debugging context is required.

---

### dim_span

Dimension table containing every unique Cobrand span.

Examples

```
cobrand.generate

cobrand.image.generate

cobrand.s3.upload
```

---

# Daily Workflow

Drop new files into

```
logs/api/

logs/cobrand/
```

Run

```bash
python3 refresh_logs.py
```

Warehouse refreshes automatically. No duplicate imports occur.

---

# Current Pipeline

API Logs

```
JSON
↓
raw_logs
↓
event_fact_api
↓
event_catalog_api
↓
capability_catalog
```

Cobrand Logs

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
```
---
