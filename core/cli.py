\
"""Waste management cli — utility helpers."""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

_SLUG_RE = re.compile(r"[^\w-]+")


def report_collection(data: Dict[str, Any]) -> Dict[str, Any]:
    """Collection report helper — validates and normalises *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "truck_id" not in result:
        raise ValueError(f"Collection must have a \'truck_id\'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["truck_id"]).encode()).hexdigest()[:12]
    return result


def reroute_collections(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page through a list of Collection records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("reroute_collections: %d items after filter", len(out))
    return out[:limit]


def close_collection(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* applied."""
    updated = dict(record)
    updated.update(overrides)
    if "frequency" in updated and not isinstance(updated["frequency"], (int, float)):
        try:
            updated["frequency"] = float(updated["frequency"])
        except (TypeError, ValueError):
            pass
    return updated


def slugify_collection(text: str) -> str:
    """Convert *text* to a URL-safe Collection slug."""
    slug = _SLUG_RE.sub("-", text.lower().strip())
    return slug.strip("-")[:64]


def validate_collection(record: Dict[str, Any]) -> bool:
    """Return True if *record* satisfies all Collection invariants."""
    required = ["truck_id", "frequency", "date"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_collection: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def schedule_run_collection_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Split *records* into chunks of *batch_size* for bulk schedule_run."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
