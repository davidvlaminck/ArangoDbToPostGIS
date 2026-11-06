"""Base class for ArangoDB -> PostGIS syncers.

Subclasses define:

* ``ARANGO_COLLECTION`` – the Arango collection to read from.
* ``POSTGIS_TABLE`` – the fully-qualified PostGIS table to write to.
* ``FIELD_MAP`` – mapping of Arango field name -> PostGIS column name.
* ``_transform(doc)`` – optional per-document transformation hook.

The base class handles connection management, batched inserts, and logging.
"""

from __future__ import annotations

from typing import Any

from arango.collection import Collection
from psycopg import Connection

from arangodbtopostgis.db import (
    execute_insert,
    get_arango_db,
    get_postgis_connection,
)


class BaseSyncer:
    """Generic syncer between a single Arango collection and a PostGIS table."""

    ARANGO_COLLECTION: str = ""
    POSTGIS_TABLE: str = ""
    # Resume/checkpoint step name; defaults to collection name.
    RESUME_STEP: str = ""
    FIELD_MAP: dict[str, str] = {}

    # Optional list of required NOT NULL columns; rows missing these are skipped.
    REQUIRED_COLUMNS: tuple[str, ...] = ("uuid",)

    # Default value applied when a mapped field is missing and the column is NOT NULL.
    DEFAULTS: dict[str, Any] = {"actief": True}

    # Rows are inserted in batches of this size.
    BATCH_SIZE: int = 1000

    def __init__(self, arango_env: str = "dev", postgis_env: str = "prd") -> None:
        self.arango_env = arango_env
        self.postgis_env = postgis_env
        if not self.RESUME_STEP:
            self.RESUME_STEP = self.ARANGO_COLLECTION

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Fetch from Arango and insert into PostGIS; return row count."""
        rows = self.fetch()
        count = self.insert(rows)
        print(
            f"{self.__class__.__name__}: fetched {len(rows)} from '{self.ARANGO_COLLECTION}', "
            f"inserted {count} into {self.POSTGIS_TABLE}"
        )
        return count

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch(self) -> list[dict[str, Any]]:
        """Read all documents from the Arango collection and transform them."""
        db = get_arango_db(self.arango_env)
        collection = db.collection(self.ARANGO_COLLECTION)
        rows: list[dict[str, Any]] = []
        for doc in collection.all():
            row = self._transform(doc)
            if row is None:
                continue
            if self._is_valid(row):
                rows.append(row)
        return rows

    def _transform(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        """Map an Arango document to a PostGIS row.

        Subclasses may override this to implement custom field extraction.
        The default implementation applies ``FIELD_MAP`` and ``DEFAULTS``.
        """
        row: dict[str, Any] = {}
        for arango_field, pg_column in self.FIELD_MAP.items():
            row[pg_column] = doc.get(arango_field)
        for column, default in self.DEFAULTS.items():
            if row.get(column) is None:
                row[column] = default
        return row

    def _is_valid(self, row: dict[str, Any]) -> bool:
        """Return ``True`` when the row contains all required columns.

        Both ``None`` and empty strings are treated as missing — PostGIS
        rejects empty strings for ``uuid`` columns.
        """
        return all(row.get(col) is not None and row.get(col) != "" for col in self.REQUIRED_COLUMNS)

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------

    def insert(self, rows: list[dict[str, Any]]) -> int:
        """Bulk insert ``rows`` into the PostGIS table."""
        if not rows:
            return 0

        conn = get_postgis_connection(self.postgis_env)
        try:
            count = execute_insert(
                conn,
                self.POSTGIS_TABLE,
                self.FIELD_MAP,
                rows,
                batch_size=self.BATCH_SIZE,
            )
        finally:
            conn.close()
        return count

    # Backwards-compatible name.
    upsert = insert

    # ------------------------------------------------------------------
    # Convenience accessors (useful for subclasses / tests)
    # ------------------------------------------------------------------

    @property
    def arango_collection(self) -> Collection:
        """Return the underlying Arango collection handle."""
        return get_arango_db(self.arango_env).collection(self.ARANGO_COLLECTION)

    @property
    def postgis_connection(self) -> Connection:
        """Open and return a PostGIS connection."""
        return get_postgis_connection(self.postgis_env)
