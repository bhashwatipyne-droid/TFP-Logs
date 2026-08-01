"""
instrumentation_gap.py

Discovers recurring operational log messages that have no structured
event.action — the "Instrumentation Gap Catalog". Companion to
capability_catalog (the Event Registry) in api_loader.py, but for
unstructured messages rather than structured events:

    capability_catalog          -> "what structured capabilities exist?"
    instrumentation_gap_catalog -> "what operational behavior is still
                                     hidden in free-text log messages?"

Shared between api_loader.py and cobrand_loader.py so the noise-filter
list and normalization logic exist in exactly ONE place — the whole
point of this module. Each loader supplies its own source-specific SQL
(different table name, different columns, different raw timestamp
format) via sync_instrumentation_gap_catalog()'s source_sql parameter;
everything else here (normalization, noise filtering, upserting,
reporting) is common to both.

Each row separates two concepts that used to be conflated into one
"message_pattern" column:

    raw_pattern  -> the normalized message, used purely for grouping.
    signature    -> a short, human-readable canonical label, resolved
                     via signature_rules.py. Defaults to raw_pattern
                     itself when no rule matches yet.

This is a completely separate table from capability_catalog. Nothing
in here touches the Event Registry.
"""

import logging

from etl.signature_rules import resolve_signature

logger = logging.getLogger(__name__)


# Exact-match noise messages, excluded entirely before normalization.
# These are transport/framework-level logging, not operational signal,
# and should never appear in the catalog. HTTP REQUEST alone was
# 323,054 of 328,634 sampled API messages (98.3%) — without this
# filter the catalog is unusable, not just noisy. Extend this set as
# more noise patterns turn up in real output; treat any new one-line
# generic recurring "framework noise" entry surfacing in the pending
# backlog as a candidate to add here, the same way this entry was
# discovered.
NOISE_EXCLUDE_MESSAGES = {
    "HTTP REQUEST",
}


# Applied in order: ISO 8601 timestamps first (most specific), then
# UUIDs, then hex addresses (e.g. "0x396bc380" — a random memory
# pointer that appears in some Node.js/ffprobe crash output and
# differs on literally every invocation; concrete proof this mattered:
# a real corrupted-video ffprobe failure was landing in the catalog as
# a separate row per occurrence, with occurrence_count stuck at 1,
# purely because this unstripped hex value made every occurrence's
# "pattern" unique — digit-only \d+ doesn't touch it, since it
# contains letters a-f), then any remaining standalone digit runs.
# The digit-run rule is deliberately generic rather than field-
# specific — no separate regex for "user id" vs "planner id" vs
# "platformRow" vs "reconciled=N count". One rule, verified directly
# against real samples, correctly collapses all of those into the
# same {n} token without needing to enumerate every field name that
# might contain a number (see EVENT_PREFIX_SQL's "sso.partner" lesson
# in api_loader.py for why ad-hoc per-field exceptions don't scale).
_NORMALIZED_MESSAGE_TEMPLATE = r"""
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    __MESSAGE_EXPR__,
                    '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2}|Z)?',
                    '{timestamp}', 'g'
                ),
                '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
                '{uuid}', 'g'
            ),
            '0x[0-9a-fA-F]+',
            '{hex}', 'g'
        ),
        '\d+', '{n}', 'g'
    )
"""


def normalized_message_sql(message_expr: str = "message") -> str:
    """
    Return the SQL fragment that normalizes message_expr into a
    reusable pattern. Uses string replacement rather than an f-string
    for the template itself, since the regex patterns already use
    literal { } for quantifiers ({4}, {8}, etc.) as well as for the
    {timestamp}/{uuid}/{hex}/{n} placeholder tokens — mixing that with
    Python f-string brace-escaping would be unreadable and error-prone.
    """
    return _NORMALIZED_MESSAGE_TEMPLATE.replace("__MESSAGE_EXPR__", message_expr)


def _first_line_sql(message_expr: str = "message") -> str:
    """
    Extract just the first line of a message, trimmed. Used as the
    basis for the raw_pattern/grouping key only — example_message
    still stores and displays the full original text (including any
    stack trace) for real debugging, this is purely about what gets
    grouped on.

    IMPORTANT: this wraps the split in an explicit trim of
    space/CR/LF/tab — NOT DuckDB's plain trim(), which only strips
    spaces (verified directly). Without this, real log data with mixed
    line-ending conventions (\r\n on some lines, \n on others — common
    when multiple library versions/code paths format multi-line output
    differently) produces two BYTE-DIFFERENT raw_patterns that render
    completely identically on screen: split_part(msg, chr(10), 1) on a
    "\r\n"-terminated line leaves a trailing, invisible \r stuck on the
    end. This was caught directly in production output: the same
    visible pattern appeared as two separate catalog rows, each with a
    fraction of the true occurrence_count. Confirmed via direct test:
    'foo\r\nbar'.split on chr(10) alone leaves 'foo\r' (len 4, not 3).

    Single-line messages (the vast majority — HTTP-style messages,
    Twitter warnings, planner recovery messages, etc.) are completely
    unaffected: split_part on a string with no newlines just returns
    the whole string, and trimming adds nothing.
    """
    first_line = f"split_part({message_expr}, chr(10), 1)"
    return f"trim({first_line}, ' \r\n\t')"


def _noise_filter_sql(message_expr: str = "message") -> str:
    """SQL fragment excluding exact-match noise messages (pre-normalization)."""
    literals = ", ".join("'" + m.replace("'", "''") + "'" for m in NOISE_EXCLUDE_MESSAGES)
    return f"{message_expr} NOT IN ({literals})"


def _ensure_instrumentation_gap_schema(con):
    """
    Create/migrate instrumentation_gap_catalog.

    PRIMARY KEY is (source_system, raw_pattern) — syncing API and
    Cobrand as two separate calls into the same table means a single-
    column PK on raw_pattern alone would let the second sync silently
    overwrite the first source's data if the same normalized pattern
    ever appeared in both (verified directly against this exact
    scenario before shipping).

    Migration: an earlier version of this table used "message_pattern"
    as the column name (before raw_pattern/signature were split out).
    If that old column exists and the new one doesn't, it's renamed in
    place — DuckDB allows renaming a column that's part of a composite
    PRIMARY KEY, verified directly, so this preserves the constraint
    and any existing data (including whatever a human may have already
    entered in classification_status / probable_component / etc. by
    the time this migration runs) rather than dropping the table.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS instrumentation_gap_catalog (
            raw_pattern               VARCHAR,
            signature                 VARCHAR,
            source_system             VARCHAR,
            example_message           VARCHAR,
            service_name              VARCHAR,
            first_seen                TIMESTAMP,
            last_seen                 TIMESTAMP,
            occurrence_count          BIGINT,
            log_level                 VARCHAR,
            classification_status     VARCHAR,
            probable_component        VARCHAR,
            recommended_event_action  VARCHAR,
            notes                     VARCHAR,
            PRIMARY KEY (source_system, raw_pattern)
        )
    """)

    existing_columns = {
        row[0]
        for row in con.execute("DESCRIBE instrumentation_gap_catalog").fetchall()
    }

    if "message_pattern" in existing_columns and "raw_pattern" not in existing_columns:
        con.execute(
            "ALTER TABLE instrumentation_gap_catalog "
            "RENAME COLUMN message_pattern TO raw_pattern"
        )
        logger.info(
            "Migrated instrumentation_gap_catalog: message_pattern -> raw_pattern"
        )

    for column, coltype in [
        ("signature", "VARCHAR"),
        ("example_message", "VARCHAR"),
        ("service_name", "VARCHAR"),
        ("first_seen", "TIMESTAMP"),
        ("last_seen", "TIMESTAMP"),
        ("occurrence_count", "BIGINT"),
        ("log_level", "VARCHAR"),
        ("classification_status", "VARCHAR"),
        ("probable_component", "VARCHAR"),
        ("recommended_event_action", "VARCHAR"),
        ("notes", "VARCHAR"),
    ]:
        con.execute(
            f"ALTER TABLE instrumentation_gap_catalog ADD COLUMN IF NOT EXISTS {column} {coltype}"
        )


def _compute_instrumentation_gap_metadata(con, source_sql: str):
    """
    Aggregate a source-specific query into one row per normalized
    raw_pattern. Pure aggregation — no inserts, no updates.

    source_sql must be a SELECT yielding exactly these columns:
        message            VARCHAR (raw, NOT normalized)
        service_name       VARCHAR
        log_level          VARCHAR
        parsed_event_time  TIMESTAMP (already parsed by the caller —
                            API and Cobrand use different raw
                            timestamp formats, so that parsing is the
                            caller's responsibility, not this
                            function's)

    The caller is also responsible for filtering to "no structured
    event" rows (event_action IS NULL) and message IS NOT NULL —
    that's source-schema-specific and belongs in source_sql, not here.
    Noise filtering (NOISE_EXCLUDE_MESSAGES) IS handled here, uniformly
    for every caller.

    raw_pattern is computed from the message's FIRST LINE only (see
    _first_line_sql's docstring for why, and for the CRLF bug it
    fixes), then normalized. This has no effect on example_message
    below, which still uses ARG_MAX on the full, un-truncated raw
    message — the full stack trace (if any) is always preserved in the
    table, only the grouping key is shortened.

    signature is NOT computed here — it's resolved in Python via
    signature_rules.resolve_signature() after this returns, since it's
    a plain per-row string mapping rather than something that benefits
    from being expressed in SQL.
    """
    pattern_expr = normalized_message_sql(_first_line_sql("message"))

    return con.execute(f"""
        WITH source AS (
            {source_sql}
        ),
        tagged AS (
            SELECT
                *,
                {pattern_expr} AS raw_pattern
            FROM source
            WHERE {_noise_filter_sql("message")}
        )
        SELECT
            raw_pattern,
            MIN(parsed_event_time) AS first_seen,
            MAX(parsed_event_time) AS last_seen,
            COUNT(*) AS occurrence_count,
            ARG_MAX(message, parsed_event_time) AS example_message,
            ARG_MAX(service_name, parsed_event_time) AS service_name,
            ARG_MAX(log_level, parsed_event_time) AS log_level
        FROM tagged
        WHERE raw_pattern IS NOT NULL
        GROUP BY raw_pattern
    """).fetchdf()


def sync_instrumentation_gap_catalog(con, source_system: str, source_sql: str):
    """
    Sync instrumentation_gap_catalog from a source-specific aggregation.

    Mirrors sync_capability_catalog's design (api_loader.py) exactly,
    including the two lessons already learned there the hard way:

    - classification_status self-heals via
        COALESCE(instrumentation_gap_catalog.classification_status, 'Pending')
      so a NULL status (e.g. from a schema migration) can never
      silently and permanently fall out of the "needs review" query.

    - first_seen self-corrects via
        LEAST(instrumentation_gap_catalog.first_seen, excluded.first_seen)
      which is NULL-safe (DuckDB's LEAST skips NULL like MIN/MAX,
      verified directly, rather than propagating it) — matters here
      too, since some patterns' timestamps may fail to parse entirely.

    signature is recomputed and OVERWRITTEN on every sync (unlike
    classification_status/probable_component/recommended_event_action/
    notes, which are human-owned and never touched here). This is
    deliberate: signature is entirely derived from signature_rules.py,
    not human-edited in the table directly, so adding a new rule later
    should retroactively relabel an already-catalogued raw_pattern the
    next time it syncs, with zero manual data migration.

    Only occurrence_count, example_message, service_name, log_level,
    last_seen, and signature are ever updated for an existing row.

    Parameters
    ----------
    source_system : str
        A label like "API" or "COBRAND", stored per-row and used as
        part of the composite primary key (see schema notes above).
    source_sql : str
        See _compute_instrumentation_gap_metadata's docstring for the
        exact column contract this must satisfy.

    Returns
    -------
    (int, list[tuple])
        Total pattern count for this source_system, and the list of
        (raw_pattern, signature, occurrence_count, last_seen,
        example_message) rows still pending review, ordered by
        occurrence_count DESC.
    """
    _ensure_instrumentation_gap_schema(con)

    metadata = _compute_instrumentation_gap_metadata(con, source_sql)

    if metadata.empty:
        logger.info("No instrumentation gaps found for %s", source_system)
    else:
        metadata = metadata.copy()
        metadata["source_system"] = source_system
        metadata["signature"] = metadata["raw_pattern"].apply(resolve_signature)

        con.register("instrumentation_gap_metadata", metadata)

        con.execute("""
            INSERT INTO instrumentation_gap_catalog (
                raw_pattern, signature, source_system, classification_status,
                first_seen, last_seen, occurrence_count,
                example_message, service_name, log_level
            )
            SELECT
                raw_pattern, signature, source_system, 'Pending',
                first_seen, last_seen, occurrence_count,
                example_message, service_name, log_level
            FROM instrumentation_gap_metadata
            ON CONFLICT (source_system, raw_pattern) DO UPDATE SET
                signature         = excluded.signature,
                last_seen         = excluded.last_seen,
                occurrence_count  = excluded.occurrence_count,
                example_message   = excluded.example_message,
                service_name      = excluded.service_name,
                log_level         = excluded.log_level,
                classification_status = COALESCE(instrumentation_gap_catalog.classification_status, 'Pending'),
                first_seen        = LEAST(instrumentation_gap_catalog.first_seen, excluded.first_seen)
        """)

        con.unregister("instrumentation_gap_metadata")

    total = con.execute(
        "SELECT COUNT(*) FROM instrumentation_gap_catalog WHERE source_system = ?",
        [source_system],
    ).fetchone()[0]

    pending = con.execute("""
        SELECT raw_pattern, signature, occurrence_count, last_seen, example_message
        FROM instrumentation_gap_catalog
        WHERE source_system = ?
          AND (classification_status = 'Pending' OR classification_status IS NULL)
        ORDER BY occurrence_count DESC
    """, [source_system]).fetchall()

    return total, pending


def _truncate_for_console(text, max_chars: int = 200) -> str:
    """
    Shorten a message for console display only. The full text is
    always preserved in instrumentation_gap_catalog.example_message —
    this function only affects what gets printed to the log/console,
    which previously dumped entire multi-hundred-line stack traces
    into the run output, making the backlog printout unreadable.
    """
    if text is None:
        return "—"

    lines = text.splitlines()
    first_line = lines[0] if lines else text
    extra_lines = len(lines) - 1

    truncated = first_line[:max_chars]
    was_char_truncated = len(first_line) > max_chars

    suffix = ""
    if was_char_truncated:
        suffix += "…"
    if extra_lines > 0:
        suffix += (
            f"  (+{extra_lines} more line{'s' if extra_lines != 1 else ''} — "
            f"see instrumentation_gap_catalog.example_message for the full text)"
        )

    return truncated + suffix


def print_instrumentation_backlog(pending, source_system: str):
    """
    Print recurring unstructured messages awaiting review, ordered by
    occurrence_count descending — same reporting shape/None-safety as
    _print_classification_backlog in api_loader.py.

    signature is now the printed headline (falls back to raw_pattern
    automatically, since signature defaults to raw_pattern when no
    rule matches) — raw_pattern is shown as a secondary line so the
    underlying grouping key is still visible, not hidden.

    The Example field is truncated for console display (see
    _truncate_for_console) — full multi-line stack traces are stored
    in the database, but were previously dumped in full to the console
    log, making a backlog with more than one or two entries unreadable.
    """
    if not pending:
        logger.info("No pending instrumentation gaps for %s.", source_system)
        return

    logger.info("Instrumentation gaps awaiting review (%s):", source_system)

    for raw_pattern, signature, occurrence_count, last_seen, example_message in pending:
        logger.info("-" * 55)
        logger.info(signature if signature is not None else raw_pattern)
        if signature != raw_pattern:
            logger.info("  Raw Pattern : %s", raw_pattern)
        occ_display = f"{occurrence_count:,}" if occurrence_count is not None else "0"
        logger.info("  Occurrences : %s", occ_display)
        logger.info("  Last Seen   : %s", last_seen if last_seen is not None else "—")
        logger.info("  Example     : %s", _truncate_for_console(example_message))

    logger.info("-" * 55)
