"""Sync ``assets`` from ArangoDB to PostGIS.

Column mapping follows ``attributes.md``. Geometry is derived from ``wkt``.
Bestekkoppelingen are synced from the Arango ``bestekkoppelingen`` edge collection
(using edge ``volgnummer`` for ordering). ``bs`` is excluded from ``assets.attributes``.
By default this script syncs a limited test batch (10,000 rows); pass
``--limit 0`` or set ``ASSETS_LIMIT=0`` for a full sync.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from arangodbtopostgis.db import get_arango_db
from arangodbtopostgis.syncer import BaseSyncer

NS_COLUMNS = ("geo", "wkt", "loc", "grp", "wl", "bz", "tz", "lgc", "ins", "ond", "vtc")

# Source keys consumed by scalar/NS columns or intentionally excluded from `attributes`.
# geometry has dedicated handling; bs/bestekkoppelingen have dedicated handling.
EXCLUDED_ATTRIBUTE_KEYS = {
    "@id",
    "@type",
    "_id",
    "_key",
    "_rev",
    "uuid",
    "AIMDBStatus_isActief",
    "isActief",
    "isDeleted",
    "assettype_key",
    "AIMObject_assetId",
    "AIMObject_typeURI",
    "AIMToestand_toestand",
    "toestand",
    "AIMObject_notitie",
    "AIMNaamObject_naam",
    "NaampadObject_naampad",
    "naampad_parts",
    "naampad_parent",
    "AIMVersie_assetVersie",
    "datumOprichtingObject",
    "AIMObject_datumOprichtingObject",
    "geometry",  # derived GeoJSON, separate handling
    "bs",        # synced via bestekkoppelingen edge collection
    *NS_COLUMNS,
}


class AssetsSyncer(BaseSyncer):
    ARANGO_COLLECTION = "assets"
    POSTGIS_TABLE = "public.assets"
    BESTEKKOPPELINGEN_TABLE = "public.bestekkoppelingen"
    # FIELD_MAP keys are output row keys; values are PostGIS columns.
    FIELD_MAP = {
        "uuid": "uuid",
        "assettype": "assettype",
        "actief": "actief",  # legacy NOT NULL column in existing DBs; mirrors isActief
        "asset_id_identificator": "asset_id_identificator",
        "asset_id_toegekend_door": "asset_id_toegekend_door",
        "type_uri": "type_uri",
        "toestand": "toestand",
        "notitie": "notitie",
        "naam": "naam",
        "naampad": "naampad",
        "naampad_parts": "naampad_parts",
        "naampad_parent": "naampad_parent",
        "asset_versie_context": "asset_versie_context",
        "asset_versie_timestamp": "asset_versie_timestamp",
        "asset_versie_versienummer": "asset_versie_versienummer",
        "datum_oprichting_object": "datum_oprichting_object",
        **{ns: ns for ns in NS_COLUMNS},
        "attributes": "attributes",
    }
    REQUIRED_COLUMNS = ("uuid",)
    DEFAULTS: dict[str, Any] = {}

    def __init__(
        self,
        arango_env: str = "dev",
        postgis_env: str = "prd",
        *,
        limit: int | None = 10000,
    ) -> None:
        super().__init__(arango_env=arango_env, postgis_env=postgis_env)
        self.limit = limit
        self._assettype_key_to_uuid: dict[str, str] = {}

    def _load_assettype_lookup(self) -> None:
        db = get_arango_db(self.arango_env)
        cursor = db.aql.execute("FOR at IN assettypes RETURN {_key: at._key, uuid: at.uuid}")
        self._assettype_key_to_uuid = {
            d["_key"]: d["uuid"] for d in cursor if d.get("_key") and d.get("uuid")
        }
        print(f"{self.__class__.__name__}: loaded {len(self._assettype_key_to_uuid)} assettype mappings")

    def fetch(self) -> list[dict[str, Any]]:
        # Assets can be very large (millions); upsert in streaming batches.
        return []

    def _pg_columns(self, conn, table: str = "assets") -> set[str]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s",
                (table,),
            )
            return {r[0] for r in cur.fetchall()}

    def _upsert_geometrie(self, conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
        """Insert geometry rows parsed from top-level `wkt` (SRID=3812). WKT leads; no fallback."""
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
                    print(
                        f"{self.__class__.__name__}: skipped geometry for {row['asset_uuid']}: {exc}"
                    )
        conn.commit()
        return inserted

    @staticmethod
    def _parse_bestek_uuid(item: dict[str, Any]) -> str | None:
        """Extract full bestek UUID for ``public.bestekken.uuid``.

        Primary: ``DtcBestekkoppeling_bestekId.DtcIdentificator_identificator``.
        It is often stored as ``<full-uuid>-<suffix>``; we take the first 36 chars.
        ``_to`` is a short bestek key in this Arango model and is NOT used as the UUID.
        """
        ident = (item.get("DtcBestekkoppeling_bestekId") or {}).get("DtcIdentificator_identificator")
        if isinstance(ident, str) and ident:
            candidate = ident[:36] if len(ident) >= 36 else ident
            return candidate or None
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

    def _bestekkoppeling_row_from_edge(self, edge: dict[str, Any]) -> dict[str, Any] | None:
        asset_uuid = self._asset_uuid_from_edge(edge)
        bestek_uuid = self._parse_bestek_uuid(edge)
        volgnummer_raw = edge.get("volgnummer")
        if not asset_uuid or not bestek_uuid or volgnummer_raw is None:
            return None
        try:
            # Arango attribute -> PostGIS `volgnummer` (order key). May be 0-based.
            volgnummer = int(volgnummer_raw)
        except (TypeError, ValueError):
            return None
        if volgnummer < -32768 or volgnummer > 32767:
            return None
        return {
            "asset_uuid": asset_uuid,
            "bestek_uuid": bestek_uuid,
            "volgnummer": volgnummer,
            "start_datum": self._parse_dt(edge.get("DtcBestekkoppeling_actiefVan")),
            "eind_datum": self._parse_dt(edge.get("DtcBestekkoppeling_actiefTot")),
            "koppeling_status": self._status_tail(edge.get("DtcBestekkoppeling_status")),
        }

    def _replace_bestekkoppelingen(self, conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
        """Replace bestekkoppelingen for affected assets.

        Delete+insert is used because an ordered list can shrink and stale
        ``volgnummer`` rows must not survive. Duplicate (asset, volgnummer)
        rows within the same replacement keep the last row and log a warning.
        """
        if not rows:
            return 0
        by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_asset.setdefault(row["asset_uuid"], []).append(row)
        inserted = 0
        with conn.cursor() as cur:
            for asset_uuid, asset_rows in by_asset.items():
                try:
                    cur.execute("SAVEPOINT bk_asset")
                    dedup: dict[int, dict[str, Any]] = {}
                    for row in sorted(asset_rows, key=lambda r: r["volgnummer"]):
                        vn = row["volgnummer"]
                        if vn in dedup:
                            print(
                                f"{self.__class__.__name__}: duplicate volgnummer {vn} "
                                f"for asset {asset_uuid}; keeping last"
                            )
                        dedup[vn] = row
                    cur.execute(
                        f"DELETE FROM {self.BESTEKKOPPELINGEN_TABLE} WHERE asset_uuid = %s::uuid",
                        (asset_uuid,),
                    )
                    for row in dedup.values():
                        cur.execute(
                            f"""
                            INSERT INTO {self.BESTEKKOPPELINGEN_TABLE}
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
                    print(
                        f"{self.__class__.__name__}: skipped bestekkoppelingen for {asset_uuid}: {exc}"
                    )
        conn.commit()
        return inserted

    def _sync_bestekkoppelingen(self, conn: psycopg.Connection, db) -> dict[str, int]:
        """Sync bestekkoppelingen from the Arango ``bestekkoppelingen`` edge collection.

        Uses edge ``volgnummer`` as the order value stored in PostGIS ``volgnummer``.
        Edges missing ``volgnummer`` are skipped (counted) because order cannot be
        represented reliably without it.
        """
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.bestekkoppelingen')")
            if cur.fetchone()[0] is None:
                print(f"{self.__class__.__name__}: public.bestekkoppelingen missing; bk sync skipped")
                return {"inserted": 0, "skipped_no_volgnummer": 0, "skipped_invalid": 0}

        query = """
            FOR e IN bestekkoppelingen
            LET vn = e.volgnummer != null ? e.volgnummer : e.volgorde
            FILTER vn != null
            SORT e._from ASC, vn ASC
            RETURN {
                _from: e._from,
                volgnummer: vn,
                DtcBestekkoppeling_bestekId: e.DtcBestekkoppeling_bestekId,
                DtcBestekkoppeling_actiefVan: e.DtcBestekkoppeling_actiefVan,
                DtcBestekkoppeling_actiefTot: e.DtcBestekkoppeling_actiefTot,
                DtcBestekkoppeling_status: e.DtcBestekkoppeling_status
            }
        """
        missing_query = "RETURN LENGTH(FOR e IN bestekkoppelingen FILTER e.volgnummer == null AND e.volgorde == null RETURN 1)"
        skipped_no_volgnummer = int(list(db.aql.execute(missing_query))[0])

        inserted = skipped_invalid = 0
        by_asset: dict[str, list[dict[str, Any]]] = {}
        batch_n = 0
        for edge in db.aql.execute(query, batch_size=self.BATCH_SIZE):
            row = self._bestekkoppeling_row_from_edge(edge)
            if row is None:
                skipped_invalid += 1
                continue
            asset_uuid = row["asset_uuid"]
            by_asset.setdefault(asset_uuid, []).append(row)
            if len(by_asset) >= self.BATCH_SIZE:
                inserted += self._replace_bestekkoppelingen(
                    conn, [r for rows in by_asset.values() for r in rows]
                )
                by_asset = {}
                batch_n += 1
                if batch_n % 50 == 0:
                    print(
                        f"{self.__class__.__name__}: bestekkoppelingen progress "
                        f"inserted={inserted} skipped_no_volgnummer={skipped_no_volgnummer}"
                    )
        if by_asset:
            inserted += self._replace_bestekkoppelingen(
                conn, [r for rows in by_asset.values() for r in rows]
            )
        return {
            "inserted": inserted,
            "skipped_no_volgnummer": skipped_no_volgnummer,
            "skipped_invalid": skipped_invalid,
        }

    def run(self) -> int:
        self._load_assettype_lookup()
        db = get_arango_db(self.arango_env)
        query = "FOR d IN assets LIMIT @limit RETURN d" if self.limit else "FOR d IN assets RETURN d"
        bind: dict[str, Any] = {"limit": self.limit} if self.limit else {}
        conn = self.postgis_connection
        try:
            pg_cols = self._pg_columns(conn)
            field_map = {src: dst for src, dst in self.FIELD_MAP.items() if dst in pg_cols}
            if not field_map:
                raise RuntimeError("no matching columns found for assets sync")
            extra = sorted(set(self.FIELD_MAP) - set(field_map))
            if extra:
                print(f"{self.__class__.__name__}: skipping unmapped/missing columns: {extra}")
            has_geometrie = "geometrie" in self._pg_columns(conn, "public.geometrie".split(".")[-1])
            # _pg_columns takes table name
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.geometrie')")
                has_geometrie = cur.fetchone()[0] is not None
            if not has_geometrie:
                print(f"{self.__class__.__name__}: public.geometrie missing; geometry sync skipped")

            fetched = upserted = geom_upserted = unknown_types = batch_n = 0
            batch: list[dict[str, Any]] = []
            geom_batch: list[dict[str, Any]] = []
            for doc in db.aql.execute(query, bind_vars=bind, batch_size=self.BATCH_SIZE):
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
                    from arangodbtopostgis.db import execute_upsert

                    upserted += execute_upsert(
                        conn, self.POSTGIS_TABLE, field_map, batch, batch_size=self.BATCH_SIZE
                    )
                    if has_geometrie:
                        geom_upserted += self._upsert_geometrie(conn, geom_batch)
                        geom_batch = []
                    batch_n += 1
                    if batch_n % 50 == 0:
                        print(
                            f"{self.__class__.__name__}: progress fetched={fetched} "
                            f"upserted={upserted} geometrie={geom_upserted}"
                        )
                    batch = []
            if batch:
                from arangodbtopostgis.db import execute_upsert

                upserted += execute_upsert(
                    conn, self.POSTGIS_TABLE, field_map, batch, batch_size=self.BATCH_SIZE
                )
                if has_geometrie:
                    geom_upserted += self._upsert_geometrie(conn, geom_batch)

            bk_stats = self._sync_bestekkoppelingen(conn, db)
        finally:
            conn.close()
        print(
            f"{self.__class__.__name__}: fetched {fetched} rows "
            f"from Arango collection '{self.ARANGO_COLLECTION}'"
            + (f" (limit={self.limit})" if self.limit else "")
            + (f", {unknown_types} with unresolved assettype" if unknown_types else "")
        )
        print(
            f"{self.__class__.__name__}: upserted {upserted} into {self.POSTGIS_TABLE}, "
            f"{geom_upserted} into public.geometrie, "
            f"{bk_stats['inserted']} into {self.BESTEKKOPPELINGEN_TABLE}"
            + (f", {bk_stats['skipped_no_volgnummer']} skipped (no volgnummer)" if bk_stats["skipped_no_volgnummer"] else "")
            + (f", {bk_stats['skipped_invalid']} skipped (invalid)" if bk_stats["skipped_invalid"] else "")
        )
        return upserted

    @staticmethod
    def _jsonb(value: Any) -> Jsonb | None:
        if value is None:
            return None
        if value == "" or value == [] or value == {}:
            return None
        return Jsonb(value)

    def _resolve_assettype(self, doc: dict[str, Any]) -> str | None:
        key = doc.get("assettype_key")
        if not key:
            return None
        return self._assettype_key_to_uuid.get(str(key))

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
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            # Arango stores ISO dates like "2010-01-01" (sometimes with time).
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
        return None

    @staticmethod
    def _remaining_attributes(doc: dict[str, Any]) -> Jsonb | None:
        remaining = {k: v for k, v in doc.items() if k not in EXCLUDED_ATTRIBUTE_KEYS}
        if not remaining:
            return None
        return Jsonb(remaining)

    def _transform(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        asset_id = doc.get("AIMObject_assetId") or {}
        versie = doc.get("AIMVersie_assetVersie") or {}
        naampad = doc.get("NaampadObject_naampad")
        if naampad == "":
            naampad = None
        naam = doc.get("AIMNaamObject_naam")
        if naam == "":
            naam = None
        notitie = doc.get("AIMObject_notitie")
        if notitie == "":
            notitie = None

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
            "notitie": notitie,
            "naam": naam,
            "naampad": naampad,
            "naampad_parts": self._naampad_parts(doc),
            "naampad_parent": self._naampad_parent(doc),
            "asset_versie_context": versie.get("DtcAssetVersie_context"),
            "asset_versie_timestamp": versie.get("DtcAssetVersie_timestamp"),
            "asset_versie_versienummer": (
                str(versie["DtcAssetVersie_versienummer"])
                if versie.get("DtcAssetVersie_versienummer") is not None
                else None
            ),
            "datum_oprichting_object": self._to_date(
                doc.get("datumOprichtingObject") or doc.get("AIMObject_datumOprichtingObject")
            ),
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
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.environ.get("ASSETS_LIMIT", "10000")),
        help="Max assets to sync; 0 = no limit (default: 10000)",
    )
    args = parser.parse_args()
    limit = args.limit if args.limit and args.limit > 0 else None
    AssetsSyncer(arango_env=args.arango_env, postgis_env=args.postgis_env, limit=limit).run()


if __name__ == "__main__":
    main()
