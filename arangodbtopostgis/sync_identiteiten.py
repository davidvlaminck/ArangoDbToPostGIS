"""Sync ``identiteiten`` from ArangoDB to PostGIS."""

from __future__ import annotations

from arangodbtopostgis.syncer import BaseSyncer


class IdentiteitenSyncer(BaseSyncer):
    ARANGO_COLLECTION = "identiteiten"
    POSTGIS_TABLE = "public.identiteiten"
    FIELD_MAP = {
        "uuid": "uuid",
        "type": "typeIdentiteit",
        "naam": "naam",
        "voornaam": "voornaam",
        "gebruikersnaam": "gebruikersnaam",
        "systeem": "systeem",
        "voId": "voId",
        "bron": "bron",
        "actief": "actief",
    }
    # contactFiche, gebruikersrechtOrganisaties -> extra Arango data -> ignored


def main() -> None:
    IdentiteitenSyncer().run()


if __name__ == "__main__":
    main()
