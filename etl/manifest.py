r"""
manifest.py

Maintains archive_manifest: a permanent audit trail of what happened
to every log file the pipeline has ever touched. One row per
source_file, upserted in place as the file moves through its
lifecycle:

    ARCHIVED  ->  VALIDATED  ->  DELETED
              \-> FAILED  (JSON kept on disk, nothing further happens
                            to that row until the file is reprocessed)

This is intentionally a thin wrapper around a single DuckDB
connection — pass in the same connection Validator already holds
(e.g. Manifest(validator.con)) rather than opening a third connection
to finpedia_logs.db. DuckDB only restricts *cross-process* concurrent
access to a database file; multiple connections/objects within the
same process sharing one connection are fine.
"""

import logging

logger = logging.getLogger(__name__)


class Manifest:

    def __init__(self, con):
        self.con = con
        self._ensure_table()

    def _ensure_table(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS archive_manifest (
                source_file     VARCHAR PRIMARY KEY,
                log_type        VARCHAR,

                archive_path    VARCHAR,

                json_rows       INTEGER,
                warehouse_rows  INTEGER,
                parquet_rows    INTEGER,

                imported_at     TIMESTAMP,
                archived_at     TIMESTAMP,
                validated_at    TIMESTAMP,
                deleted_at      TIMESTAMP,

                status          VARCHAR,

                checksum        VARCHAR,
                notes           VARCHAR
            )
        """)

    def record_archived(self, source_file: str, log_type: str, archive_path: str):
        """
        Upsert a row marking this file ARCHIVED. Safe to call again for
        the same source_file (e.g. a rerun after a corrupted-parquet
        rebuild) — source_file is the primary key, so this overwrites
        rather than duplicating.
        """
        self.con.execute("""
            INSERT INTO archive_manifest (
                source_file, log_type, archive_path,
                archived_at, status
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ARCHIVED')
            ON CONFLICT (source_file) DO UPDATE SET
                log_type     = excluded.log_type,
                archive_path = excluded.archive_path,
                archived_at  = excluded.archived_at,
                status       = excluded.status
        """, [source_file, log_type, str(archive_path)])

    def record_validation(
        self,
        source_file: str,
        json_rows: int,
        warehouse_rows: int,
        parquet_rows: int,
        passed: bool,
        notes: str = None,
    ):
        """
        Record the outcome of validation for a file that has already
        been archived (i.e. already has a row from record_archived).
        Sets status to VALIDATED or FAILED accordingly.
        """
        status = "VALIDATED" if passed else "FAILED"

        self.con.execute("""
            UPDATE archive_manifest
            SET
                json_rows      = ?,
                warehouse_rows = ?,
                parquet_rows   = ?,
                validated_at   = CURRENT_TIMESTAMP,
                status         = ?,
                notes          = ?
            WHERE source_file = ?
        """, [json_rows, warehouse_rows, parquet_rows, status, notes, source_file])

        logger.info("%s manifest status -> %s", source_file, status)

    def record_deleted(self, source_file: str, checksum: str = None):
        """
        Record that the source JSON has been deleted. Only call this
        after delete_json_file() actually succeeds.
        """
        self.con.execute("""
            UPDATE archive_manifest
            SET
                deleted_at = CURRENT_TIMESTAMP,
                status     = 'DELETED',
                checksum   = ?
            WHERE source_file = ?
        """, [checksum, source_file])

        logger.info("%s manifest status -> DELETED", source_file)

    def record_kept(self, source_file: str, notes: str = None):
        """
        Record that validation passed but the file was intentionally
        NOT deleted, because AUTO_DELETE_JSON is off. Distinguishes
        "kept on purpose" from "kept because validation failed."
        """
        self.con.execute("""
            UPDATE archive_manifest
            SET
                status = 'VALIDATED_KEPT',
                notes  = ?
            WHERE source_file = ?
        """, [notes or "AUTO_DELETE_JSON is disabled", source_file])

        logger.info("%s manifest status -> VALIDATED_KEPT (auto-delete off)", source_file)