"""Sync ``bestekken`` from ArangoDB to PostGIS."""

from __future__ import annotations

from arangodbtopostgis.syncer import BaseSyncer


class BestekkenSyncer(BaseSyncer):
    ARANGO_COLLECTION = "bestekken"
    POSTGIS_TABLE = "public.bestekken"
    FIELD_MAP = {
        "uuid": "uuid",
        "eDeltaDossiernummer": "eDeltaDossiernummer",
        "eDeltaBesteknummer": "eDeltaBesteknummer",
        "aannemerNaam": "aannemerNaam",
        "awvId": "awvId",
        "aannemerReferentie": "aannemerReferentie",
        "actief": "actief",
    }
    # Skip docs missing required NOT NULL fields.
    REQUIRED_COLUMNS = ("uuid", "eDeltaDossiernummer", "eDeltaBesteknummer")


def main() -> None:
    BestekkenSyncer().run()


if __name__ == "__main__":
    main()
