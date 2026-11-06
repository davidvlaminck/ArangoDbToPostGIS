"""Sync ``bestekken`` from ArangoDB to PostGIS.

After syncing the ``bestekken`` collection, inserts placeholder rows for any
bestek UUIDs referenced by ``bestekkoppelingen`` edges but missing from the
source/target data, so FK constraints remain satisfied while making missing
source records explicit.
"""

from __future__ import annotations

import logging

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


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

    def run(self) -> int:
        count = super().run()
        placeholders = self._insert_missing_placeholders()
        if placeholders:
            logger.info(f"{self.__class__.__name__}: inserted {placeholders} missing-bestek placeholder rows")
        return count + placeholders

    def _insert_missing_placeholders(self) -> int:
        db = get_arango_db(self.arango_env)
        # Collect referenced bestek UUIDs (full 36-char) and any available AWV id.
        query = """
            FOR e IN bestekkoppelingen
              LET id = e.DtcBestekkoppeling_bestekId.DtcIdentificator_identificator
              FILTER id != null
              LET bu = LEFT(id, 36)
              FILTER bu != ""
              COLLECT uuid = bu
                AGGREGATE awv_id = MAX(e.DtcBestekkoppeling_bestekAwvId.DtcIdentificator_identificator)
              RETURN {uuid: uuid, awv_id: awv_id}
        """
        refs = list(db.aql.execute(query))
        if not refs:
            return 0

        conn = get_postgis_connection(self.postgis_env)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT uuid::text FROM {self.POSTGIS_TABLE}")
                existing = {r[0] for r in cur.fetchall()}
            missing = [r for r in refs if r.get("uuid") and r["uuid"] not in existing]
            if not missing:
                return 0

            sql = f"""
                INSERT INTO {self.POSTGIS_TABLE}
                    (uuid, e_delta_dossiernummer, e_delta_besteknummer, awv_id,
                     aannemer_naam, aannemer_referentie, actief)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (uuid) DO NOTHING
            """
            params = [
                (
                    m["uuid"],
                    None,
                    None,
                    m.get("awv_id") or None,
                    "<missing bestek>",
                    "placeholder: referenced by bestekkoppelingen but missing from Arango bestekken",
                    False,
                )
                for m in missing
            ]
            with conn.cursor() as cur:
                cur.executemany(sql, params)
            conn.commit()
            for m in missing[:20]:
                logger.info(
                    f"{self.__class__.__name__}: placeholder bestek {m['uuid']}"
                    + (f" (awv_id={m.get('awv_id')})" if m.get("awv_id") else "")
                )
            if len(missing) > 20:
                logger.info(f"{self.__class__.__name__}: ... {len(missing) - 20} more placeholders")
            return len(missing)
        finally:
            conn.close()


def main() -> None:
    BestekkenSyncer().run()


if __name__ == "__main__":
    main()
