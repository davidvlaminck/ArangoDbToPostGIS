"""Sync ``toezichtgroepen`` from ArangoDB to PostGIS."""

from __future__ import annotations

from arangodbtopostgis.syncer import BaseSyncer


class ToezichtgroepenSyncer(BaseSyncer):
    ARANGO_COLLECTION = "toezichtgroepen"
    POSTGIS_TABLE = "public.toezichtgroepen"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "type": "typeGroep",
        "omschrijving": "referentie",
        "actief": "actief",
    }
    # actiefInterval, contactFiche -> extra Arango data -> ignored


def main() -> None:
    ToezichtgroepenSyncer().run()


if __name__ == "__main__":
    main()
