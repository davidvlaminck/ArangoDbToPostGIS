"""Sync ``identiteiten`` from ArangoDB to PostGIS."""

from __future__ import annotations

import logging

from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


class IdentiteitenSyncer(BaseSyncer):
    ARANGO_COLLECTION = "identiteiten"
    POSTGIS_TABLE = "public.identiteiten"
    FIELD_MAP = {
        "uuid": "uuid",
        "type": "type_identiteit",
        "naam": "naam",
        "voornaam": "voornaam",
        "gebruikersnaam": "gebruikersnaam",
        "systeem": "systeem",
        "voId": "vo_id",
        "bron": "bron",
        "actief": "actief",
    }
    # contactFiche, gebruikersrechtOrganisaties -> extra Arango data -> ignored


def main() -> None:
    IdentiteitenSyncer().run()


if __name__ == "__main__":
    main()
