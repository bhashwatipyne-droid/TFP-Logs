"""
archive.py

Converts processed JSON log files into compressed Parquet archives.

Workflow

JSON Log
    ↓
Parse line-by-line in Python (json.loads), skip malformed lines
    ↓
Write (source_file, raw_json) rows to Parquet (ZSTD)
    ↓
Verify row count
    ↓
Return archive info  (caller decides whether to delete the JSON)

Note: this module never deletes the source JSON. That decision belongs
to the orchestrator (refresh_logs.py), which only deletes a file after
the warehouse + parquet counts have both been validated.

Important: this deliberately does NOT use DuckDB's read_json_auto() or
read_csv() to read the source log file. Those both walked the file's
JSON structure to infer a schema/dialect, and in production that
caused two separate failures on the same class of file (deeply nested
trace.method/children payloads introduced 2026-07-12):
  - read_csv(): the CSV sniffer misread a comma-heavy line as 5
    columns instead of 1, raising InvalidInputException.
  - read_json_auto(): schema inference on one file (application-
    2026-06-19.log) tried to allocate ~32GB and hit an Out of Memory
    error; on another file (application-2026-07-13.log) it aborted
    entirely on the first malformed line it hit.
Parsing each line ourselves in Python — exactly like api_loader's
_import_log_file() — sidesteps all of that: malformed lines are
skipped individually and logged, and nothing ever asks DuckDB to
infer a schema over the whole file.
"""

import json
import logging
from pathlib import Path
import duckdb

logger = logging.getLogger(__name__)


def _parse_log_file(log_file: Path):
    """
    Read a JSON-lines log file and return (rows, skipped) where rows is
    a list of (source_file, raw_json_line) tuples for every line that
    parses as valid JSON, and skipped is a count of malformed lines.

    This mirrors api_loader._import_log_file() exactly, so archiving
    and importing always agree on which lines count as valid.
    """
    filename = log_file.name
    rows = []
    skipped = 0

    with open(log_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)  # validate only; store the raw line
            except json.JSONDecodeError as ex:
                logger.warning(
                    "%s line %d: skipping invalid JSON (%s)",
                    filename, line_no, ex,
                )
                skipped += 1
                continue
            rows.append((filename, line))

    return rows, skipped


def _write_parquet(con, rows, parquet_file: Path):
    """
    Write (source_file, raw_json) rows to a Parquet file via a scratch
    DuckDB table. Storing the raw JSON as a VARCHAR column (rather than
    letting DuckDB flatten it into inferred columns) is what avoids the
    schema-inference cost/crashes described above — validation only
    needs a row count, not the flattened shape.
    """
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _archive_staging "
        "(source_file VARCHAR, raw_json VARCHAR)"
    )

    con.executemany(
        "INSERT INTO _archive_staging VALUES (?, ?)",
        rows,
    )

    con.execute(
        f"""
        COPY (SELECT source_file, raw_json FROM _archive_staging)
        TO '{parquet_file}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        );
        """
    )

    con.execute("DROP TABLE _archive_staging")


def _count_parquet_rows(con, parquet_file: Path) -> int:
    """Return number of rows in a Parquet file. Safe to use read_parquet
    here — the schema is always our own flat (source_file, raw_json),
    never inferred from arbitrary nested JSON."""
    return con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{parquet_file}')
        """
    ).fetchone()[0]


def archive_logs(log_files, archive_dir: str):
    """
    Convert a specific list of JSON log files into compressed Parquet.

    Parameters
    ----------
    log_files
        Iterable of paths to .log files that should be archived.
        These should already be files that were successfully loaded
        into the warehouse (e.g. the return value of load_api_logs()
        or load_cobrand_logs()) — archive_logs() does not check that.

    archive_dir
        Destination root for parquet archive

    Returns
    -------
    list[dict]
        One entry per file that is safely archived (newly converted
        or already archived from a prior run), each shaped as:
            {"json_file": Path, "parquet_file": Path}
        Files that fail to archive are logged and simply omitted —
        they are not included in the returned list, so they won't be
        validated or deleted downstream.
    """

    con = duckdb.connect()

    archive = Path(archive_dir)
    archive.mkdir(parents=True, exist_ok=True)

    results = []

    if log_files is None:
        logger.error(
            "archive_logs(%s) received None instead of a file list — "
            "the upstream loader's refresh() must return a list "
            "(use [] for 'nothing to process', never fall off the end "
            "of the function). Skipping this archive step.",
            archive_dir,
        )
        return results

    log_files = [Path(f) for f in log_files]

    if not log_files:
        logger.info("No logs to archive in %s", archive_dir)
        return results

    logger.info("Archiving %d log file(s)", len(log_files))

    for log_file in log_files:

        try:

            # ---------------------------------------------------------
            # Build archive path
            # ---------------------------------------------------------

            filename = log_file.stem
            date = filename.split("-")[-3:]

            year = date[0]
            month = date[1]

            destination = archive / year / month
            destination.mkdir(parents=True, exist_ok=True)

            parquet_file = destination / f"{filename}.parquet"

            # Already archived (e.g. a previous run archived it but
            # crashed before the JSON could be validated/deleted).
            # Don't just trust that the file exists — a prior run could
            # have been killed mid-write and left a truncated/corrupt
            # parquet file behind. Verify it's actually readable before
            # skipping; if it's not, delete it and rebuild below.

            if parquet_file.exists():
                try:
                    _count_parquet_rows(con, parquet_file)
                    logger.info("%s already archived", log_file.name)
                    results.append({
                        "json_file": log_file,
                        "parquet_file": parquet_file,
                    })
                    continue
                except Exception as ex:
                    logger.warning(
                        "%s exists but is unreadable (%s) — rebuilding",
                        parquet_file.name, ex,
                    )
                    parquet_file.unlink(missing_ok=True)
                    # falls through to rebuild it below

            # ---------------------------------------------------------
            # Parse JSON lines in Python (same logic as the importer)
            # ---------------------------------------------------------

            rows, skipped = _parse_log_file(log_file)

            if skipped:
                logger.warning(
                    "%s: skipped %d malformed line(s) while archiving",
                    log_file.name, skipped,
                )

            if not rows:
                raise Exception("No valid JSON rows found to archive")

            # ---------------------------------------------------------
            # Write Parquet
            # ---------------------------------------------------------

            _write_parquet(con, rows, parquet_file)

            # ---------------------------------------------------------
            # Verify
            # ---------------------------------------------------------

            json_rows = len(rows)
            parquet_rows = _count_parquet_rows(con, parquet_file)

            if json_rows != parquet_rows:

                parquet_file.unlink(missing_ok=True)

                raise Exception(
                    f"Row count mismatch "
                    f"(JSON={json_rows}, Parquet={parquet_rows})"
                )

            logger.info(
                "Archived %s (%s rows)",
                log_file.name,
                f"{json_rows:,}",
            )

            results.append({
                "json_file": log_file,
                "parquet_file": parquet_file,
            })

        except Exception as ex:

            logger.error("Failed to archive %s: %s", log_file.name, ex)

    logger.info(
        "Archive summary — archived: %d, failed: %d",
        len(results),
        len(log_files) - len(results),
    )

    return results