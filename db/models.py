"""Waste management models — Collection management."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class WasteModels:
    """Collection models for the waste-management application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._zone_id = self._cfg.get("zone_id", None)
        logger.debug("WasteModels ready (store=%s)", type(store).__name__)

    def schedule_run_collection(
        self, zone_id: Any, volume: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Collection record."""
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "zone_id":   zone_id,
            "volume":   volume,
            "status":     "active",
            "created_at": datetime.utcnow().isoformat(),
            **extra,
        }
        saved = self._store.put(record)
        logger.info("schedule_run_collection: created %s", saved["id"])
        return saved

    def get_collection(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Collection by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_collection: %s not found", record_id)
        return record

    def dispatch_collection(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Collection."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Collection not found: {record_id}")
        record.update(changes)
        record["updated_at"] = datetime.utcnow().isoformat()
        return self._store.put(record)

    def log_pickup_collection(self, record_id: str) -> bool:
        """Remove a Collection record; returns True if deleted."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("log_pickup_collection: removed %s", record_id)
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
