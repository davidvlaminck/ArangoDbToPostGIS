"""Sync ``beheerders`` from ArangoDB to PostGIS."""

from __future__ import annotations

from arangodbtopostgis.syncer import BaseSyncer


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
