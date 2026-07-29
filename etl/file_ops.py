"""
file_ops.py

Centralizes JSON log file lifecycle operations (currently just
deletion + checksumming) so there is exactly one place in the codebase
that ever removes a source log file. Nothing else in the pipeline
should call path.unlink() or os.remove() on a log file directly —
route it through delete_json_file() so every deletion is logged and
every caller gets a clear True/False instead of having to guard
against exceptions itself.
"""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def compute_checksum(path: Path, algorithm: str = "sha256") -> str:
    """
    Return the hex digest of a file's contents. Intended to be called
    right before deletion, while the file still exists, so
    archive_manifest keeps a permanent fingerprint of exactly what was
    deleted — useful for later proving a given parquet archive really
    does match the original JSON byte-for-byte.
    """
    h = hashlib.new(algorithm)

    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)

    return h.hexdigest()


def delete_json_file(path: Path) -> bool:
    """
    Delete a source JSON log file.

    Returns
    -------
    bool
        True if the file was deleted (or already gone), False if
        deletion was attempted and failed. Never raises — callers get
        a clean True/False instead of having to catch OSError
        themselves, and every outcome is logged either way.
    """
    path = Path(path)

    if not path.exists():
        logger.warning("%s already deleted (or never existed)", path.name)
        return False

    try:
        path.unlink()
    except OSError as ex:
        logger.error("Failed to delete %s: %s", path.name, ex)
        return False

    logger.info("%s deleted", path.name)
    return True