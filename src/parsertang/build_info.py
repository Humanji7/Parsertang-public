"""Build metadata utilities.

Production deployments do not include `.git`, so we stamp build info into
`data/build.json` during deploy and log it at startup.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any


def read_build_info(path: str | Path = "data/build.json") -> dict[str, Any] | None:
    build_path = Path(path)
    if not build_path.exists():
        return None

    try:
        data = json.loads(build_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(data, dict):
        return None

    return data


def log_build_info(logger: logging.Logger, path: str | Path = "data/build.json") -> None:
    info = read_build_info(path)
    if not info:
        logger.info("BUILD | missing")
        return

    git_sha = info.get("git_sha", "unknown")
    git_branch = info.get("git_branch", "unknown")
    built_at_utc = info.get("built_at_utc", "unknown")
    git_dirty = info.get("git_dirty", False)

    logger.info(
        "BUILD | sha=%s branch=%s dirty=%s built_at=%s",
        git_sha,
        git_branch,
        git_dirty,
        built_at_utc,
    )

