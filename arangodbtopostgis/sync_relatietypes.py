"""Sync ``relatietypes`` from ArangoDB to PostGIS.

Rows without a ``uri`` are skipped — every synced relatietype must be
addressable by a stable identifier.
"""

from __future__ import annotations

import logging

from typing import Any

from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


class RelatietypesSyncer(BaseSyncer):
    ARANGO_COLLECTION = "relatietypes"
    POSTGIS_TABLE = "public.relatietypes"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "label": "label",
        "uri": "uri",
        "short": "short_uri",
        "definitie": "definitie",
        "actief": "actief",
        "gericht": "gericht",
    }
    REQUIRED_COLUMNS = ("uuid", "uri")

    def _transform(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        # Skip relatietypes that have no URI — they cannot be reliably
        # referenced from other tables.
        if not doc.get("uri"):
            return None
        return super()._transform(doc)


def main() -> None:
    RelatietypesSyncer().run()


if __name__ == "__main__":
    main()
