"""Sync ``assettypes`` from ArangoDB to PostGIS."""

from __future__ import annotations

import logging

from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


class AssettypesSyncer(BaseSyncer):
    ARANGO_COLLECTION = "assettypes"
    POSTGIS_TABLE = "public.assettypes"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "label": "label",
        "uri": "uri",
        "short_uri": "short_uri",
        "definitie": "definitie",
        "actief": "actief",
    }


def main() -> None:
    AssettypesSyncer().run()


if __name__ == "__main__":
    main()
