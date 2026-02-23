from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def append_jsonl(path: str, record: dict[str, Any]) -> None:
    """Append a single JSON record to a JSONL file.

    Best-effort: on failure, logs warning and continues.
    """
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True))
            f.write("\n")
    except Exception as exc:  # noqa: BLE001
        logger.warning("ALERT EVIDENCE | write failed path=%s err=%s", path, exc)

