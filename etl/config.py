"""
config.py

Pipeline-wide configuration. Kept deliberately tiny and dependency-free
so it can be imported from anywhere without risk of circular imports.
"""

import os


def _env_flag(name: str, default: bool) -> bool:
    """
    Read a boolean flag from the environment, falling back to `default`
    if unset. Accepts common truthy/falsy spellings so this can be set
    from a shell, a cron environment, or a .env file without surprises.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


# Whether refresh_logs.py deletes a source .log file after it passes
# validation. Defaults to True (safe for the scheduled/production run).
#
# Turn it off during development so you can inspect files that would
# otherwise be deleted, without editing code:
#
#     AUTO_DELETE_JSON=false python3 refresh_logs.py
#
# Or change the default below if you want it off unless explicitly
# enabled.
AUTO_DELETE_JSON = _env_flag("AUTO_DELETE_JSON", default=True)