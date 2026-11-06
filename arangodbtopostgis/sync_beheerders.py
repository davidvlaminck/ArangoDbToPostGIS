"""Sync ``beheerders`` from ArangoDB to PostGIS."""

from __future__ import annotations

import logging

from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


class BeheerdersSyncer(BaseSyncer):
    ARANGO_COLLECTION = "beheerders"
    POSTGIS_TABLE = "public.beheerders"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "type": "type_beheerder",
        "referentie": "referentie",
        "actief": "actief",
    }
    # actiefInterval, contactFiche -> extra Arango data -> ignored


def main() -> None:
    BeheerdersSyncer().run()


if __name__ == "__main__":
    main()
