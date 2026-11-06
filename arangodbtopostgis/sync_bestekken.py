"""Sync ``bestekken`` from ArangoDB to PostGIS."""

from __future__ import annotations

from arangodbtopostgis.syncer import BaseSyncer


class BestekkenSyncer(BaseSyncer):
    ARANGO_COLLECTION = "bestekken"
    POSTGIS_TABLE = "public.bestekken"
    FIELD_MAP = {
        "uuid": "uuid",
        "eDeltaDossiernummer": "e_delta_dossiernummer",
        "eDeltaBesteknummer": "e_delta_besteknummer",
        "aannemerNaam": "aannemer_naam",
        "awvId": "awv_id",
        "aannemerReferentie": "aannemer_referentie",
        "actief": "actief",
    }
    # Skip docs missing required NOT NULL fields.
    REQUIRED_COLUMNS = ("uuid", "e_delta_dossiernummer", "e_delta_besteknummer")


def main() -> None:
    BestekkenSyncer().run()


if __name__ == "__main__":
    main()
