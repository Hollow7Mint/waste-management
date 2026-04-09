"""Waste management service — Collection management."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class WasteService:
    """Collection service for the waste-management application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._truck_id = self._cfg.get("truck_id", None)
        logger.debug("WasteService ready (store=%s)", type(store).__name__)

    def report_collection(
        self, truck_id: Any, frequency: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Collection record."""
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "truck_id":   truck_id,
            "frequency":   frequency,
            "status":     "active",
            "created_at": datetime.utcnow().isoformat(),
            **extra,
        }
        saved = self._store.put(record)
        logger.info("report_collection: created %s", saved["id"])
        return saved

    def get_collection(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Collection by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_collection: %s not found", record_id)
        return record

    def reroute_collection(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Collection."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Collection not found: {record_id}")
        record.update(changes)
        record["updated_at"] = datetime.utcnow().isoformat()
        return self._store.put(record)

    def close_collection(self, record_id: str) -> bool:
        """Remove a Collection record; returns True if deleted."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("close_collection: removed %s", record_id)
        return True

    def list_collections(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return a filtered, paginated list of Collection records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_collections: %d results", len(results))
        return results

    def iter_collections(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Collection records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_collections(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
