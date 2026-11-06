"""Sync ``relatietypes`` from ArangoDB to PostGIS."""

from __future__ import annotations

from arangodbtopostgis.syncer import BaseSyncer


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


def main() -> None:
    RelatietypesSyncer().run()


if __name__ == "__main__":
    main()
