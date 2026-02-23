from __future__ import annotations

import math
from typing import Iterable, List


def select_batch(
    symbols: Iterable[str],
    *,
    batch_size: int,
    batch_index: int,
) -> List[str]:
    items = list(symbols)
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if not items:
        return []
    batches = max(1, math.ceil(len(items) / batch_size))
    index = batch_index % batches
    start = index * batch_size
    return items[start : start + batch_size]


def build_allowlist_line(symbols: Iterable[str]) -> str:
    joined = ",".join(symbols)
    return f'SYMBOL_ALLOWLIST="{joined}"'
