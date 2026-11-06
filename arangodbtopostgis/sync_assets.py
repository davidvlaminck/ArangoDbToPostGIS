"""Sync ``assets`` from ArangoDB to PostGIS.

Column mapping follows ``attributes.md``. Geometry is derived from ``wkt``.
Bestekkoppelingen are synced separately by ``BestekkoppelingenSyncer`` (resumable).
``bs`` is excluded from ``assets.attributes``. By default this script syncs a
limited test batch (10,000 rows); pass ``--limit 0`` or set ``ASSETS_LIMIT=0``
for a full sync.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.resume import mark_step_done, save_cursor_and_commit
from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)

NS_COLUMNS = ("geo", "wkt", "loc", "grp", "wl", "bz", "tz", "lgc", "ins", "ond", "vtc")

EXCLUDED_ATTRIBUTE_KEYS = {
    "@id", "@type", "_id", "_key", "_rev", "uuid", "AIMDBStatus_isActief", "isActief", "isDeleted",
    "assettype_key", "AIMObject_assetId", "AIMObject_typeURI", "AIMToestand_toestand", "toestand",
    "AIMObject_notitie", "AIMNaamObject_naam", "NaampadObject_naampad", "naampad_parts", "naampad_parent",
    "AIMVersie_assetVersie", "datumOprichtingObject", "AIMObject_datumOprichtingObject", "geometry", "bs",
    *NS_COLUMNS,
}


class AssetsSyncer(BaseSyncer):
    ARANGO_COLLECTION = "assets"
    POSTGIS_TABLE = "public.assets"
    RESUME_STEP = "assets"
    FIELD_MAP = {
        "uuid": "uuid", "assettype": "assettype", "actief": "actief",
        "asset_id_identificator": "asset_id_identificator", "asset_id_toegekend_door": "asset_id_toegekend_door",
        "type_uri": "type_uri", "toestand": "toestand", "notitie": "notitie", "naam": "naam",
        "naampad": "naampad", "naampad_parts": "naampad_parts", "naampad_parent": "naampad_parent",
        "asset_versie_context": "asset_versie_context", "asset_versie_timestamp": "asset_versie_timestamp",
        "asset_versie_versienummer": "asset_versie_versienummer", "datum_oprichting_object": "datum_oprichting_object",
        **{ns: ns for ns in NS_COLUMNS}, "attributes": "attributes",
    }
    REQUIRED_COLUMNS = ("uuid",)
    DEFAULTS: dict[str, Any] = {}

    def __init__(self, arango_env: str = "dev", postgis_env: str = "prd", *, limit: int | None = 10000) -> None:
        super().__init__(arango_env=arango_env, postgis_env=postgis_env)
        self.limit = limit
        self._assettype_key_to_uuid: dict[str, str] = {}

    def _load_assettype_lookup(self) -> None:
        db = get_arango_db(self.arango_env)
        cursor = db.aql.execute("FOR at IN assettypes RETURN {_key: at._key, uuid: at.uuid}")
        self._assettype_key_to_uuid = {d["_key"]: d["uuid"] for d in cursor if d.get("_key") and d.get("uuid")}
        logger.info(f"{self.__class__.__name__}: loaded {len(self._assettype_key_to_uuid)} assettype mappings")

    def fetch(self) -> list[dict[str, Any]]:
        return []

    def _pg_columns(self, conn, table: str = "assets") -> set[str]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",
                (table,),
            )
            return {r[0] for r in cur.fetchall()}

    def _insert_geometrie(self, conn: psycopg.Connection, rows: list[dict[str, Any]], *, commit: bool = True) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO public.geometrie (asset_uuid, wkt, geom)
            VALUES (%s::uuid, %s, ST_Force3DZ(ST_SetSRID(ST_GeomFromEWKT(%s), 3812)))
        """
        inserted = 0
        with conn.cursor() as cur:
            for row in rows:
                try:
                    cur.execute("SAVEPOINT geom_row")
                    cur.execute(sql, (row["asset_uuid"], row["wkt"], row["wkt"]))
                    cur.execute("RELEASE SAVEPOINT geom_row")
                    inserted += 1
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT geom_row")
                    logger.warning(f"{self.__class__.__name__}: skipped geometry for {row['asset_uuid']}: {exc}")
        if commit:
            conn.commit()
        return inserted

    def run_resumable(self, resume_cursor: str | None = None) -> int:
        self._load_assettype_lookup()
        db = get_arango_db(self.arango_env)
        after = None if resume_cursor in (None, "", "__DONE__") else resume_cursor
        bind: dict[str, Any] = {"after": after or ""}
        limit_clause = "LIMIT @limit" if self.limit else ""
        if self.limit:
            bind["limit"] = self.limit
        query = f"FOR d IN assets FILTER d._key > @after SORT d._key ASC {limit_clause} RETURN d"
        conn = get_postgis_connection(self.postgis_env)
        try:
            pg_cols = self._pg_columns(conn)
            field_map = {src: dst for src, dst in self.FIELD_MAP.items() if dst in pg_cols}
            if not field_map:
                raise RuntimeError("no matching columns found for assets sync")
            extra = sorted(set(self.FIELD_MAP) - set(field_map))
            if extra:
                logger.info(f"{self.__class__.__name__}: skipping unmapped/missing columns: {extra}")
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.geometrie')")
                has_geometrie = cur.fetchone()[0] is not None
            if not has_geometrie:
                logger.info(f"{self.__class__.__name__}: public.geometrie missing; geometry sync skipped")

            fetched = inserted = geom_inserted = unknown_types = batch_n = 0
            batch: list[dict[str, Any]] = []
            geom_batch: list[dict[str, Any]] = []
            last_key: str | None = after
            for doc in db.aql.execute(query, bind_vars=bind, batch_size=self.BATCH_SIZE):
                last_key = doc.get("_key") or last_key
                row = self._transform(doc)
                if row is None:
                    continue
                if not row.get("assettype"):
                    unknown_types += 1
                if not self._is_valid(row):
                    continue
                batch.append(row)
                if has_geometrie and row.get("wkt"):
                    geom_batch.append({"asset_uuid": row["uuid"], "wkt": row["wkt"]})
                fetched += 1
                if len(batch) >= self.BATCH_SIZE:
                    self._commit_asset_batch(conn, field_map, batch, geom_batch if has_geometrie else [], last_key)
                    inserted += len(batch)
                    geom_inserted += len(geom_batch) if has_geometrie else 0
                    batch = []
                    geom_batch = []
                    batch_n += 1
                    if batch_n % 50 == 0:
                        logger.info(f"{self.__class__.__name__}: progress fetched={fetched} inserted={inserted} geometrie={geom_inserted} cursor={last_key}")
            if batch or last_key != after:
                self._commit_asset_batch(conn, field_map, batch, geom_batch if has_geometrie else [], last_key)
                inserted += len(batch)
                geom_inserted += len(geom_batch) if has_geometrie else 0
            mark_step_done(conn, self.RESUME_STEP)
        finally:
            conn.close()
        logger.info(
            f"{self.__class__.__name__}: fetched {fetched} from '{self.ARANGO_COLLECTION}'"
            + (f" (limit={self.limit})" if self.limit else "")
            + (f", {unknown_types} with unresolved assettype" if unknown_types else "")
            + f"; inserted {inserted} into {self.POSTGIS_TABLE}, {geom_inserted} into public.geometrie"
        )
        return inserted

    def _commit_asset_batch(self, conn: psycopg.Connection, field_map: dict[str, str], batch: list[dict[str, Any]], geom_batch: list[dict[str, Any]], cursor: str | None) -> None:
        if not batch and cursor is None:
            return
        try:
            if batch:
                self._insert_rows(conn, field_map, batch)
            if geom_batch:
                self._insert_geometrie(conn, geom_batch, commit=False)
            save_cursor_and_commit(conn, self.RESUME_STEP, cursor)
        except Exception:
            conn.rollback()
            raise

    def _insert_rows(self, conn: psycopg.Connection, field_map: dict[str, str], rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        columns = list(field_map.values())
        col_list = ", ".join('"' + c.replace('"', '""') + '"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        table = ".".join('"' + part.replace('"', '""') + '"' for part in self.POSTGIS_TABLE.split("."))
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        params = [[None if row.get(col) == "" else row.get(col) for col in columns] for row in rows]
        with conn.cursor() as cur:
            cur.executemany(sql, params)

    def run(self) -> int:
        return self.run_resumable(None)

    @staticmethod
    def _jsonb(value: Any) -> Jsonb | None:
        if value is None or value == "" or value == [] or value == {}:
            return None
        return Jsonb(value)

    def _resolve_assettype(self, doc: dict[str, Any]) -> str | None:
        key = doc.get("assettype_key")
        return self._assettype_key_to_uuid.get(str(key)) if key else None

    @staticmethod
    def _toestand(doc: dict[str, Any]) -> str | None:
        val = doc.get("AIMToestand_toestand") or doc.get("toestand")
        if not val or not isinstance(val, str):
            return None
        return val.rsplit("/", 1)[-1] if "/" in val else val

    @staticmethod
    def _naampad_parts(doc: dict[str, Any]) -> Jsonb | None:
        parts = doc.get("naampad_parts")
        if isinstance(parts, list) and parts:
            return Jsonb(parts)
        naampad = doc.get("NaampadObject_naampad")
        if isinstance(naampad, str) and naampad:
            derived = [p for p in naampad.split("/") if p != ""]
            return Jsonb(derived) if derived else None
        return None

    @staticmethod
    def _naampad_parent(doc: dict[str, Any]) -> str | None:
        parent = doc.get("naampad_parent")
        if isinstance(parent, str) and parent:
            return parent
        parts = doc.get("naampad_parts")
        if isinstance(parts, list) and len(parts) > 1:
            return "/".join(str(p) for p in parts[:-1])
        naampad = doc.get("NaampadObject_naampad")
        if isinstance(naampad, str) and "/" in naampad:
            return "/".join(naampad.split("/")[:-1]) or None
        return None

    @staticmethod
    def _to_date(value: Any) -> date | None:
        if value is None or value == "":
            return None
        if isinstance(value, (datetime, date)):
            return value.date() if isinstance(value, datetime) else value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
        return None

    @staticmethod
    def _remaining_attributes(doc: dict[str, Any]) -> Jsonb | None:
        remaining = {k: v for k, v in doc.items() if k not in EXCLUDED_ATTRIBUTE_KEYS}
        return Jsonb(remaining) if remaining else None

    def _transform(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        asset_id = doc.get("AIMObject_assetId") or {}
        versie = doc.get("AIMVersie_assetVersie") or {}
        naampad = doc.get("NaampadObject_naampad") or None
        naam = doc.get("AIMNaamObject_naam") or None
        notitie = doc.get("AIMObject_notitie") or None
        is_actief = doc.get("AIMDBStatus_isActief")
        if is_actief is None:
            is_actief = not bool(doc.get("isDeleted", False))
        row: dict[str, Any] = {
            "uuid": doc.get("_key") or doc.get("uuid"),
            "assettype": self._resolve_assettype(doc),
            "actief": is_actief,
            "asset_id_identificator": asset_id.get("DtcIdentificator_identificator"),
            "asset_id_toegekend_door": asset_id.get("DtcIdentificator_toegekendDoor"),
            "type_uri": doc.get("AIMObject_typeURI") or doc.get("@type"),
            "toestand": self._toestand(doc),
            "notitie": notitie, "naam": naam, "naampad": naampad,
            "naampad_parts": self._naampad_parts(doc), "naampad_parent": self._naampad_parent(doc),
            "asset_versie_context": versie.get("DtcAssetVersie_context"),
            "asset_versie_timestamp": versie.get("DtcAssetVersie_timestamp"),
            "asset_versie_versienummer": str(versie["DtcAssetVersie_versienummer"]) if versie.get("DtcAssetVersie_versienummer") is not None else None,
            "datum_oprichting_object": self._to_date(doc.get("datumOprichtingObject") or doc.get("AIMObject_datumOprichtingObject")),
        }
        for ns in NS_COLUMNS:
            if ns == "wkt":
                val = doc.get("wkt")
                row["wkt"] = val if isinstance(val, str) and val else None
            else:
                row[ns] = self._jsonb(doc.get(ns))
        row["attributes"] = self._remaining_attributes(doc)
        return row


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync assets ArangoDB -> PostGIS")
    parser.add_argument("--arango-env", default="dev")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--limit", type=int, default=int(os.environ.get("ASSETS_LIMIT", "10000")),
                        help="Max assets to sync; 0 = no limit (default: 10000)")
    args = parser.parse_args()
    limit = args.limit if args.limit and args.limit > 0 else None
    AssetsSyncer(arango_env=args.arango_env, postgis_env=args.postgis_env, limit=limit).run()


if __name__ == "__main__":
    main()
