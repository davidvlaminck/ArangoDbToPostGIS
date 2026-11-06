"""Sync ``toezichtgroepen`` from ArangoDB to PostGIS."""

from __future__ import annotations

import logging

from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


class ToezichtgroepenSyncer(BaseSyncer):
    ARANGO_COLLECTION = "toezichtgroepen"
    POSTGIS_TABLE = "public.toezichtgroepen"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "type": "type_groep",
        "omschrijving": "omschrijving",
        "actief": "actief",
    }
    # actiefInterval, contactFiche -> extra Arango data -> ignored


def main() -> None:
    ToezichtgroepenSyncer().run()


if __name__ == "__main__":
    main()
