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
    # eDelta dossier/bestek numbers can be missing in source data; keep row if uuid present.
    REQUIRED_COLUMNS = ("uuid",)


def main() -> None:
    BestekkenSyncer().run()


if __name__ == "__main__":
    main()
