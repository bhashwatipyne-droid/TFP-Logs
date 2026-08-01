"""
signature_rules.py

Small, hand-maintained mapping from recurring raw message patterns to
short, human-readable canonical failure signatures. Deliberately just
a plain Python list — no rule engine, no priority weighting, no
wildcard syntax beyond regex itself. Add one entry here whenever a new
recurring, understood failure shows up in the Instrumentation Gap
Catalog; nothing else in the ETL needs to change.

Rules are tried in order against raw_pattern (the normalized, first-
line-only message — see instrumentation_gap.py's _first_line_sql).
The first matching rule wins. If nothing matches, the signature
defaults to raw_pattern itself (see resolve_signature) — an unmapped
pattern is still fully usable in the catalog, it simply doesn't have a
short label yet.

This file intentionally does NOT attempt to classify by parsing
mid-stack-trace details (e.g. "moov atom not found" vs "Invalid data
found when processing input" as different failure modes of the same
underlying ffprobe error) — that's the harder "semantic classification
engine" the review flagged for a later iteration, not this one.
"""

import re

RULES = [
    {
        "pattern": r"ffprobe exited",
        "signature": "FFPROBE_INVALID_MEDIA",
    },
    {
        "pattern": r"moov atom not found",
        "signature": "INVALID_MP4_CONTAINER",
    },
    {
        "pattern": r"Max FPS",
        "signature": "VIDEO_MAX_FPS_TOO_LOW",
    },
    {
        "pattern": r"Couldn't find a single max width or height",
        "signature": "IMAGE_DIMENSIONS_NOT_FOUND",
    },
    {
        "pattern": r"Twitter token (validation|refresh) failed",
        "signature": "TWITTER_TOKEN_INVALID",
    },
    {
        "pattern": r"Cobrand is still in progress",
        "signature": "COBRAND_PENDING",
    },
    {
        "pattern": r"Scheduled post media is not ready yet",
        "signature": "MEDIA_NOT_READY",
    },
]

_COMPILED_RULES = [
    (re.compile(rule["pattern"], re.IGNORECASE), rule["signature"])
    for rule in RULES
]


def resolve_signature(raw_pattern):
    """
    Return the canonical signature for a raw_pattern, or raw_pattern
    itself if no rule matches.

    Pure Python, not SQL — called once per distinct raw_pattern during
    sync (a handful of rows), not once per raw log line (hundreds of
    thousands), so performance is a non-issue even as this list grows.

    Deliberately re-evaluated on every sync run for every row (not
    just new ones) — adding a new rule to RULES should retroactively
    give an existing, already-catalogued raw_pattern its new signature
    on the next run, without needing to touch the ETL or delete data.
    """
    if raw_pattern is None:
        return raw_pattern

    for compiled, signature in _COMPILED_RULES:
        if compiled.search(raw_pattern):
            return signature

    return raw_pattern
