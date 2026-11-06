"""Sync ``assetrelaties`` (Arango edge collection) to PostGIS ``public.assetrelaties``.

Mapping choices
--------------
- ``uuid``: edge ``_key`` (stable edge UUID in this dataset).
- ``bron_asset_uuid`` / ``doel_asset_uuid``: parsed from ``_from`` / ``_to`` (``assets/<uuid>``).
- ``relatietype_uuid`` (FK to ``relatietypes``): resolved via lookup built from the
  Arango ``relatietypes`` collection.
  Preferred key: ``relatietype_key`` -> ``relatietypes._key`` (explicit 4-char key present
  on edges). Fallbacks: ``RelatieObject_typeURI`` / ``@type`` -> ``relatietypes.uri``.
  The limited size of ``relatietypes`` makes in-memory lookup cheap and robust.
- ``bron_type`` / ``doel_type``: ``RelatieObject_bron.@type`` / ``RelatieObject_doel.@type``.
- ``type_uri``: ``RelatieObject_typeURI`` or ``@type``.
- ``asset_id_identificator`` / ``asset_id_toegekend_door``: flattened from
  ``RelatieObject_assetId`` (DtcIdentificator).
- ``actief``: ``AIMDBStatus_isActief``.
- Excluded (redundant): ``RelatieObject_bronAssetId``, ``RelatieObject_doelAssetId``.
- Remaining unmapped fields: ``attributes`` (jsonb).
"""

from __future__ import annotations

import argparse
import os
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from arangodbtopostgis.db import get_arango_db, get_postgis_connection

EXCLUDED_ATTRIBUTE_KEYS = {
    "_id",
    "_key",
    "_rev",
    "_from",
    "_to",
    "@id",
    "@type",
    "uuid",
    "relatietype_key",
    "RelatieObject_bron",
    "RelatieObject_doel",
    "RelatieObject_bronAssetId",
    "RelatieObject_doelAssetId",
    "RelatieObject_assetId",
    "RelatieObject_typeURI",
    "AIMDBStatus_isActief",
    "isActief",
    "isDeleted",
}


def _sanitize(value: Any) -> Any:
    if value == "":
        return None
    return value


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qtable(name: str) -> str:
    return ".".join(_qident(p) for p in name.split("."))


def _asset_uuid_from_ref(ref: Any) -> str | None:
    if not isinstance(ref, str) or "/" not in ref:
        return None
    return ref.split("/", 1)[1] or None


class AssetrelatiesSyncer:
    ARANGO_COLLECTION = "assetrelaties"
    POSTGIS_TABLE = "public.assetrelaties"
    BATCH_SIZE = 2000
    FIELD_MAP = {
        "uuid": "uuid",
        "bron_asset_uuid": "bron_asset_uuid",
        "doel_asset_uuid": "doel_asset_uuid",
        "relatietype_uuid": "relatietype_uuid",
        "bron_type": "bron_type",
        "doel_type": "doel_type",
        "type_uri": "type_uri",
        "asset_id_identificator": "asset_id_identificator",
        "asset_id_toegekend_door": "asset_id_toegekend_door",
        "actief": "actief",
        "attributes": "attributes",
    }
    REQUIRED_COLUMNS = ("uuid", "bron_asset_uuid", "doel_asset_uuid", "relatietype_uuid")

    def __init__(
        self,
        arango_env: str = "dev",
        postgis_env: str = "prd",
        *,
        limit: int | None = None,
    ) -> None:
        self.arango_env = arango_env
        self.postgis_env = postgis_env
        self.limit = limit
        self._rt_by_key: dict[str, str] = {}
        self._rt_by_uri: dict[str, str] = {}

    def _load_relatietype_lookups(self) -> None:
        db = get_arango_db(self.arango_env)
        cursor = db.aql.execute("FOR rt IN relatietypes RETURN {_key: rt._key, uuid: rt.uuid, uri: rt.uri}")
        for d in cursor:
            uuid = d.get("uuid")
            key = d.get("_key")
            uri = d.get("uri")
            if not uuid:
                continue
            if key:
                self._rt_by_key[str(key)] = uuid
            if uri:
                self._rt_by_uri[str(uri)] = uuid
        print(
            f"{self.__class__.__name__}: loaded {len(self._rt_by_key)} relatietype key mappings, "
            f"{len(self._rt_by_uri)} uri mappings"
        )

    def _resolve_relatietype(self, doc: dict[str, Any]) -> str | None:
        rt_key = doc.get("relatietype_key")
        if rt_key is not None:
            resolved = self._rt_by_key.get(str(rt_key))
            if resolved:
                return resolved
        for uri in (doc.get("RelatieObject_typeURI"), doc.get("@type")):
            if isinstance(uri, str) and uri:
                resolved = self._rt_by_uri.get(uri)
                if resolved:
                    return resolved
        return None

    @staticmethod
    def _remaining_attributes(doc: dict[str, Any]) -> Jsonb | None:
        remaining = {k: v for k, v in doc.items() if k not in EXCLUDED_ATTRIBUTE_KEYS}
        return Jsonb(remaining) if remaining else None

    def _transform(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        uuid = doc.get("_key") or doc.get("uuid")
        bron = _asset_uuid_from_ref(doc.get("_from"))
        doel = _asset_uuid_from_ref(doc.get("_to"))
        if not uuid or not bron or not doel:
            return None
        bron_obj = doc.get("RelatieObject_bron") or {}
        doel_obj = doc.get("RelatieObject_doel") or {}
        asset_id = doc.get("RelatieObject_assetId") or {}
        type_uri = doc.get("RelatieObject_typeURI") or doc.get("@type")
        actief = doc.get("AIMDBStatus_isActief")
        if actief is None:
            is_deleted = doc.get("isDeleted")
            if is_deleted is not None:
                actief = not bool(is_deleted)
        return {
            "uuid": uuid,
            "bron_asset_uuid": bron,
            "doel_asset_uuid": doel,
            "relatietype_uuid": self._resolve_relatietype(doc),
            "bron_type": bron_obj.get("@type") if isinstance(bron_obj, dict) else None,
            "doel_type": doel_obj.get("@type") if isinstance(doel_obj, dict) else None,
            "type_uri": type_uri if isinstance(type_uri, str) and type_uri else None,
            "asset_id_identificator": (asset_id.get("DtcIdentificator_identificator") if isinstance(asset_id, dict) else None),
            "asset_id_toegekend_door": (asset_id.get("DtcIdentificator_toegekendDoor") if isinstance(asset_id, dict) else None),
            "actief": actief,
            "attributes": self._remaining_attributes(doc),
        }

    def _upsert_batch(self, conn: psycopg.Connection, rows: list[dict[str, Any]]) -> tuple[int, int]:
        """Insert rows with per-row savepoints so one bad FK/missing ref doesn't abort the batch."""
        if not rows:
            return 0, 0
        columns = list(self.FIELD_MAP.values())
        col_list = ", ".join(_qident(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"""
            INSERT INTO {_qtable(self.POSTGIS_TABLE)} ({col_list})
            VALUES ({placeholders})
        """
        inserted = skipped = 0
        with conn.cursor() as cur:
            for row in rows:
                params = [_sanitize(row.get(col)) for col in columns]
                try:
                    cur.execute("SAVEPOINT ar_row")
                    cur.execute(sql, params)
                    cur.execute("RELEASE SAVEPOINT ar_row")
                    inserted += 1
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT ar_row")
                    skipped += 1
                    if skipped <= 10:
                        print(
                            f"{self.__class__.__name__}: skipped {row.get('uuid')} "
                            f"(bron={row.get('bron_asset_uuid')} doel={row.get('doel_asset_uuid')}): {exc}"
                        )
        conn.commit()
        return inserted, skipped

    def run(self) -> int:
        self._load_relatietype_lookups()
        db = get_arango_db(self.arango_env)
        query = (
            "FOR d IN assetrelaties LIMIT @limit RETURN d"
            if self.limit
            else "FOR d IN assetrelaties RETURN d"
        )
        bind: dict[str, Any] = {"limit": self.limit} if self.limit else {}
        conn = get_postgis_connection(self.postgis_env)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.assetrelaties')")
                if cur.fetchone()[0] is None:
                    raise RuntimeError("public.assetrelaties table missing; run setup_postgis_tables first")

            fetched = upserted = skipped_invalid = skipped_db = batch_n = 0
            batch: list[dict[str, Any]] = []
            for doc in db.aql.execute(query, bind_vars=bind, batch_size=self.BATCH_SIZE):
                row = self._transform(doc)
                if row is None or not all(row.get(c) is not None and row.get(c) != "" for c in self.REQUIRED_COLUMNS):
                    skipped_invalid += 1
                    if skipped_invalid <= 10 and row is not None and not row.get("relatietype_uuid"):
                        print(
                            f"{self.__class__.__name__}: skipped {row.get('uuid')} "
                            f"(unresolved relatietype key={doc.get('relatietype_key')} "
                            f"type={doc.get('RelatieObject_typeURI') or doc.get('@type')})"
                        )
                    continue
                batch.append(row)
                fetched += 1
                if len(batch) >= self.BATCH_SIZE:
                    ins, skip = self._upsert_batch(conn, batch)
                    upserted += ins
                    skipped_db += skip
                    batch = []
                    batch_n += 1
                    if batch_n % 100 == 0:
                        print(
                            f"{self.__class__.__name__}: progress fetched={fetched} "
                            f"upserted={upserted} skipped_db={skipped_db}"
                        )
            if batch:
                ins, skip = self._upsert_batch(conn, batch)
                upserted += ins
                skipped_db += skip
        finally:
            conn.close()
        print(
            f"{self.__class__.__name__}: fetched {fetched} from '{self.ARANGO_COLLECTION}'"
            + (f" (limit={self.limit})" if self.limit else "")
            + (f", {skipped_invalid} skipped (invalid/missing relatietype)" if skipped_invalid else "")
            + (f", {skipped_db} skipped (db/FK)" if skipped_db else "")
        )
        print(f"{self.__class__.__name__}: upserted {upserted} into {self.POSTGIS_TABLE}")
        return upserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync assetrelaties ArangoDB -> PostGIS")
    parser.add_argument("--arango-env", default="dev")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.environ.get("ASSETRELATIES_LIMIT", "0")),
        help="Max assetrelaties to sync; 0 = no limit (default: 0)",
    )
    args = parser.parse_args()
    limit = args.limit if args.limit and args.limit > 0 else None
    AssetrelatiesSyncer(arango_env=args.arango_env, postgis_env=args.postgis_env, limit=limit).run()


if __name__ == "__main__":
    main()
