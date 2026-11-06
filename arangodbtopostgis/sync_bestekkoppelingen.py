"""Sync ``bestekkoppelingen`` edge collection from ArangoDB to PostGIS.

Resumable by Arango edge ``_key`` cursor. For each processed asset batch, rows
are deleted+inserted and the cursor is committed in the same transaction.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime
from typing import Any

import psycopg

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.resume import mark_step_done, save_cursor_and_commit

logger = logging.getLogger(__name__)


class BestekkoppelingenSyncer:
    ARANGO_COLLECTION = "bestekkoppelingen"
    POSTGIS_TABLE = "public.bestekkoppelingen"
    RESUME_STEP = "bestekkoppelingen"
    BATCH_SIZE = 2000

    def __init__(self, arango_env: str = "dev", postgis_env: str = "prd", *, limit: int | None = None) -> None:
        self.arango_env = arango_env
        self.postgis_env = postgis_env
        self.limit = limit

    @staticmethod
    def _parse_bestek_uuid(item: dict[str, Any]) -> str | None:
        ident = (item.get("DtcBestekkoppeling_bestekId") or {}).get("DtcIdentificator_identificator")
        if isinstance(ident, str) and ident:
            return ident[:36] if len(ident) >= 36 else ident
        return None

    @staticmethod
    def _asset_uuid_from_edge(edge: dict[str, Any]) -> str | None:
        from_ref = edge.get("_from")
        if not isinstance(from_ref, str) or "/" not in from_ref:
            return None
        return from_ref.split("/", 1)[1] or None

    @staticmethod
    def _parse_dt(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, str):
            text = value.strip()
            return text or None
        return None

    @staticmethod
    def _status_tail(value: Any) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None
        return value.rsplit("/", 1)[-1] if "/" in value else value.strip()

    def _row_from_edge(self, edge: dict[str, Any]) -> dict[str, Any] | None:
        asset_uuid = self._asset_uuid_from_edge(edge)
        bestek_uuid = self._parse_bestek_uuid(edge)
        volgnummer_raw = edge.get("volgnummer")
        if volgnummer_raw is None:
            volgnummer_raw = edge.get("volgorde")
        if not asset_uuid or not bestek_uuid or volgnummer_raw is None:
            return None
        try:
            volgnummer = int(volgnummer_raw)
        except (TypeError, ValueError):
            return None
        if volgnummer < -32768 or volgnummer > 32767:
            return None
        return {
            "_key": edge.get("_key"),
            "asset_uuid": asset_uuid,
            "bestek_uuid": bestek_uuid,
            "volgnummer": volgnummer,
            "start_datum": self._parse_dt(edge.get("DtcBestekkoppeling_actiefVan")),
            "eind_datum": self._parse_dt(edge.get("DtcBestekkoppeling_actiefTot")),
            "koppeling_status": self._status_tail(edge.get("DtcBestekkoppeling_status")),
        }

    def _replace_and_checkpoint(
        self, conn: psycopg.Connection, rows: list[dict[str, Any]], cursor: str | None
    ) -> int:
        if not rows and cursor is None:
            return 0
        by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_asset.setdefault(row["asset_uuid"], []).append(row)
        inserted = 0
        try:
            with conn.cursor() as cur:
                for asset_uuid, asset_rows in by_asset.items():
                    try:
                        cur.execute("SAVEPOINT bk_asset")
                        dedup: dict[int, dict[str, Any]] = {}
                        for row in sorted(asset_rows, key=lambda r: r["volgnummer"]):
                            vn = row["volgnummer"]
                            if vn in dedup:
                                logger.info(
                                    f"{self.__class__.__name__}: duplicate volgnummer {vn} "
                                    f"for asset {asset_uuid}; keeping last"
                                )
                            dedup[vn] = row
                        cur.execute(
                            f"DELETE FROM {self.POSTGIS_TABLE} WHERE asset_uuid = %s::uuid",
                            (asset_uuid,),
                        )
                        for row in dedup.values():
                            cur.execute(
                                f"""
                                INSERT INTO {self.POSTGIS_TABLE}
                                    (asset_uuid, bestek_uuid, volgnummer, start_datum, eind_datum, koppeling_status)
                                VALUES (%s::uuid, %s::uuid, %s::smallint, %s::timestamptz, %s::timestamptz, %s)
                                """,
                                (
                                    row["asset_uuid"],
                                    row["bestek_uuid"],
                                    row["volgnummer"],
                                    row["start_datum"],
                                    row["eind_datum"],
                                    row["koppeling_status"],
                                ),
                            )
                            inserted += 1
                        cur.execute("RELEASE SAVEPOINT bk_asset")
                    except Exception as exc:
                        cur.execute("ROLLBACK TO SAVEPOINT bk_asset")
                        logger.warning(f"{self.__class__.__name__}: skipped bestekkoppelingen for {asset_uuid}: {exc}")
            save_cursor_and_commit(conn, self.RESUME_STEP, cursor)
        except Exception:
            conn.rollback()
            raise
        return inserted

    def run_resumable(self, resume_cursor: str | None = None) -> int:
        db = get_arango_db(self.arango_env)
        conn = get_postgis_connection(self.postgis_env)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.bestekkoppelingen')")
                if cur.fetchone()[0] is None:
                    raise RuntimeError("public.bestekkoppelingen missing; run setup_postgis_tables first")

            after = resume_cursor
            if after == "__DONE__":
                return 0
            bind: dict[str, Any] = {"after": after or ""}
            limit_clause = "LIMIT @limit" if self.limit else ""
            if self.limit:
                bind["limit"] = self.limit
            query = f"""
                FOR e IN bestekkoppelingen
                FILTER e._key > @after
                SORT e._key ASC
                {limit_clause}
                RETURN {{
                    _key: e._key, _from: e._from,
                    volgnummer: e.volgnummer != null ? e.volgnummer : e.volgorde,
                    DtcBestekkoppeling_bestekId: e.DtcBestekkoppeling_bestekId,
                    DtcBestekkoppeling_actiefVan: e.DtcBestekkoppeling_actiefVan,
                    DtcBestekkoppeling_actiefTot: e.DtcBestekkoppeling_actiefTot,
                    DtcBestekkoppeling_status: e.DtcBestekkoppeling_status
                }}
            """

            inserted = skipped_invalid = fetched = batch_n = 0
            batch: list[dict[str, Any]] = []
            last_key: str | None = None
            for edge in db.aql.execute(query, bind_vars=bind, batch_size=self.BATCH_SIZE):
                last_key = edge.get("_key") or last_key
                row = self._row_from_edge(edge)
                fetched += 1
                if row is None:
                    skipped_invalid += 1
                    continue
                batch.append(row)
                if len(batch) >= self.BATCH_SIZE:
                    inserted += self._replace_and_checkpoint(conn, batch, last_key)
                    batch = []
                    batch_n += 1
                    if batch_n % 50 == 0:
                        logger.info(
                            f"{self.__class__.__name__}: progress fetched={fetched} "
                            f"inserted={inserted} cursor={last_key}"
                        )
            if batch or last_key:
                inserted += self._replace_and_checkpoint(conn, batch, last_key)
            mark_step_done(conn, self.RESUME_STEP)
        finally:
            conn.close()
        logger.info(
            f"{self.__class__.__name__}: fetched {fetched} from '{self.ARANGO_COLLECTION}'"
            + (f" (limit={self.limit})" if self.limit else "")
            + (f", {skipped_invalid} skipped (invalid)" if skipped_invalid else "")
            + f"; inserted {inserted} into {self.POSTGIS_TABLE}"
        )
        return inserted

    def run(self) -> int:
        return self.run_resumable(None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync bestekkoppelingen ArangoDB -> PostGIS")
    parser.add_argument("--arango-env", default="dev")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--limit", type=int, default=int(os.environ.get("BESTEKKOPPELINGEN_LIMIT", "0")))
    args = parser.parse_args()
    limit = args.limit if args.limit and args.limit > 0 else None
    BestekkoppelingenSyncer(arango_env=args.arango_env, postgis_env=args.postgis_env, limit=limit).run()


if __name__ == "__main__":
    main()
