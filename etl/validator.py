"""
validator.py

Validation helpers for Logs360 ETL.

Checks that:

JSON
    ↓
DuckDB Warehouse
    ↓
Parquet

all contain the same number of rows.
"""

import json
import logging
from pathlib import Path
import duckdb

logger = logging.getLogger(__name__)


class Validator:

    def __init__(self, db_path="finpedia_logs.db"):
        self.con = duckdb.connect(db_path)

    def close(self):
        """
        Release the DuckDB connection (and its file lock). Call this
        explicitly when done, or use Validator as a context manager —
        otherwise the lock is only released whenever Python happens to
        garbage-collect self.con, which isn't guaranteed to be prompt
        and can leave the .db file locked for the next run.
        """
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ---------------------------------------------------------
    # Generic helpers
    # ---------------------------------------------------------

    def json_row_count(self, log_file: Path) -> int:
        """
        Return number of *valid* JSON rows inside a JSON log file.

        Deliberately does not use read_json_auto() here. That both
        aborts entirely on the first malformed line in a file (e.g.
        application-2026-07-13.log) and can trigger runaway schema-
        inference memory use on deeply nested lines (the 32GB OOM seen
        while archiving application-2026-06-19.log). Counting valid
        lines in Python instead matches exactly what api_loader and
        archive.py each treat as a row, so all three stages of the
        pipeline agree on the same number.
        """
        count = 0

        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    continue
                count += 1

        return count

    def parquet_row_count(self, parquet_file: Path) -> int:
        """Return number of rows inside a parquet file."""

        return self.con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{parquet_file}')
        """).fetchone()[0]

    # ---------------------------------------------------------
    # Warehouse validation
    # ---------------------------------------------------------

    def api_row_count(self, source_file: str) -> int:
        """Rows loaded into raw_logs_api."""

        return self.con.execute("""
            SELECT COUNT(*)
            FROM raw_logs_api
            WHERE source_file = ?
        """, [source_file]).fetchone()[0]

    def cobrand_row_count(self, source_file: str) -> int:
        """
        Rows loaded into the Cobrand warehouse for one source file.

        cobrand_loader.py's importer only inserts lines into
        cobrand_raw_logs where "trace.method" IS NOT NULL — lines
        without a trace are standalone events that get built into
        cobrand_event_fact by a separate query instead. So a raw JSON
        line only lands in exactly one of these two tables, never
        both, and never neither (assuming clean ingestion). Counting
        just cobrand_raw_logs undercounts by however many standalone
        events the file contains — this must be the sum of both.
        """

        trace_rows = self.con.execute("""
            SELECT COUNT(*)
            FROM cobrand_raw_logs
            WHERE source_file = ?
        """, [source_file]).fetchone()[0]

        event_rows = self.con.execute("""
            SELECT COUNT(*)
            FROM cobrand_event_fact
            WHERE source_file = ?
        """, [source_file]).fetchone()[0]

        return trace_rows + event_rows

    # ---------------------------------------------------------
    # Validation
    # ---------------------------------------------------------

    def validate_api(self, json_file, parquet_file):

        source = Path(json_file).name

        json_rows = self.json_row_count(json_file)
        warehouse_rows = self.api_row_count(source)
        parquet_rows = self.parquet_row_count(parquet_file)

        passed = (
            json_rows ==
            warehouse_rows ==
            parquet_rows
        )

        if passed:
            logger.info("%s validated (rows=%d)", source, json_rows)
        else:
            logger.error(
                "%s FAILED validation (json=%d, warehouse=%d, parquet=%d)",
                source, json_rows, warehouse_rows, parquet_rows,
            )

        return {
            "passed": passed,
            "json_rows": json_rows,
            "warehouse_rows": warehouse_rows,
            "parquet_rows": parquet_rows
        }

    def validate_cobrand(self, json_file, parquet_file):

        source = Path(json_file).name

        json_rows = self.json_row_count(json_file)
        warehouse_rows = self.cobrand_row_count(source)
        parquet_rows = self.parquet_row_count(parquet_file)

        passed = (
            json_rows ==
            warehouse_rows ==
            parquet_rows
        )

        if passed:
            logger.info("%s validated (rows=%d)", source, json_rows)
        else:
            logger.error(
                "%s FAILED validation (json=%d, warehouse=%d, parquet=%d)",
                source, json_rows, warehouse_rows, parquet_rows,
            )

        return {
            "passed": passed,
            "json_rows": json_rows,
            "warehouse_rows": warehouse_rows,
            "parquet_rows": parquet_rows
        }