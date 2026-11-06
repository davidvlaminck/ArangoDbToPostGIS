"""Sync ``betrokkenerelaties`` (Arango edge collection) to PostGIS ``public.betrokkenerelaties``.

Mapping choices
--------------
- ``uuid``: edge ``_key``.
- Source can be an asset or an agent: stored in nullable ``bron_asset_uuid`` /
  ``bron_agent_uuid`` (only one populated).
- Target is always an agent: ``doel_agent_uuid`` (NOT NULL).
- ``rol``: separate text attribute, taken from edge ``rol`` (fallback
  ``HeeftBetrokkene_rol``).
- ``relatietype_uuid``: resolved like assetrelaties.
- Remaining unmapped fields: ``attributes`` (jsonb).
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from psycopg.types.json import Jsonb

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.resume import mark_step_done, save_cursor_and_commit

logger = logging.getLogger(__name__)

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
    "rol",
    "HeeftBetrokkene_rol",
}


def _sanitize(value: Any) -> Any:
    if value == "":
        return None
    return value


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qtable(name: str) -> str:
    return ".".join(_qident(p) for p in name.split("."))


def _uuid_from_ref(ref: Any, prefix: str) -> str | None:
    if not isinstance(ref, str) or "/" not in ref:
        return None
    head, _, tail = ref.partition("/")
    if head != prefix or not tail:
        return None
    return tail


class BetrokkenerelatiesSyncer:
    ARANGO_COLLECTION = "betrokkenerelaties"
    POSTGIS_TABLE = "public.betrokkenerelaties"
    RESUME_STEP = "betrokkenerelaties"
    BATCH_SIZE = 2000
    FIELD_MAP = {
        "uuid": "uuid",
        "bron_asset_uuid": "bron_asset_uuid",
        "bron_agent_uuid": "bron_agent_uuid",
        "doel_agent_uuid": "doel_agent_uuid",
        "relatietype_uuid": "relatietype_uuid",
        "rol": "rol",
        "bron_type": "bron_type",
        "doel_type": "doel_type",
        "type_uri": "type_uri",
        "asset_id_identificator": "asset_id_identificator",
        "asset_id_toegekend_door": "asset_id_toegekend_door",
        "actief": "actief",
        "attributes": "attributes",
    }
    REQUIRED_COLUMNS = ("uuid", "doel_agent_uuid", "relatietype_uuid")

    def __init__(self, arango_env: str = "dev", postgis_env: str = "prd", *, limit: int | None = None) -> None:
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
        bron_ref = doc.get("_from")
        doel_ref = doc.get("_to")
        bron_asset = _uuid_from_ref(bron_ref, "assets")
        bron_agent = _uuid_from_ref(bron_ref, "agents")
        doel_agent = _uuid_from_ref(doel_ref, "agents")
        if not uuid or not doel_agent:
            return None
        if bron_asset is None and bron_agent is None:
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
        rol = doc.get("rol")
        if not isinstance(rol, str):
            rol = doc.get("HeeftBetrokkene_rol")
        return {
            "uuid": uuid,
            "bron_asset_uuid": bron_asset,
            "bron_agent_uuid": bron_agent,
            "doel_agent_uuid": doel_agent,
            "relatietype_uuid": self._resolve_relatietype(doc),
            "rol": rol if isinstance(rol, str) and rol else None,
            "bron_type": bron_obj.get("@type") if isinstance(bron_obj, dict) else None,
            "doel_type": doel_obj.get("@type") if isinstance(doel_obj, dict) else None,
            "type_uri": type_uri if isinstance(type_uri, str) and type_uri else None,
            "asset_id_identificator": (asset_id.get("DtcIdentificator_identificator") if isinstance(asset_id, dict) else None),
            "asset_id_toegekend_door": (asset_id.get("DtcIdentificator_toegekendDoor") if isinstance(asset_id, dict) else None),
            "actief": actief,
            "attributes": self._remaining_attributes(doc),
        }

    def _insert_batch_and_checkpoint(self, conn: Any, rows: list[dict[str, Any]], cursor: str | None) -> tuple[int, int]:
        if not rows and cursor is None:
            return 0, 0
        columns = list(self.FIELD_MAP.values())
        col_list = ", ".join(_qident(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {_qtable(self.POSTGIS_TABLE)} ({col_list}) VALUES ({placeholders})"
        inserted = skipped = 0
        try:
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
                            logger.warning(f"{self.__class__.__name__}: skipped {row.get('uuid')}: {exc}")
            save_cursor_and_commit(conn, self.RESUME_STEP, cursor)
        except Exception:
            conn.rollback()
            raise
        return inserted, skipped

    _upsert_batch = _insert_batch_and_checkpoint

    def run_resumable(self, resume_cursor: str | None = None) -> int:
        self._load_relatietype_lookups()
        db = get_arango_db(self.arango_env)
        after = None if resume_cursor in (None, "", "__DONE__") else resume_cursor
        bind: dict[str, Any] = {"after": after or ""}
        limit_clause = "LIMIT @limit" if self.limit else ""
        if self.limit:
            bind["limit"] = self.limit
        query = f"""
            FOR d IN betrokkenerelaties
            FILTER d._key > @after
            SORT d._key ASC
            {limit_clause}
            RETURN d
        """
        conn = get_postgis_connection(self.postgis_env)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.betrokkenerelaties')")
                row = cur.fetchone()
                if row is None or row[0] is None:
                    raise RuntimeError("public.betrokkenerelaties table missing; run setup_postgis_tables first")
            fetched = inserted = skipped_invalid = skipped_db = batch_n = 0
            batch = []
            last_key: str | None = after
            for doc in db.aql.execute(query, bind_vars=bind, batch_size=self.BATCH_SIZE):
                last_key = doc.get("_key") or last_key
                transformed = self._transform(doc)
                if transformed is None or not all(transformed.get(c) is not None and transformed.get(c) != "" for c in self.REQUIRED_COLUMNS):
                    skipped_invalid += 1
                    continue
                batch.append(transformed)
                fetched += 1
                if len(batch) >= self.BATCH_SIZE:
                    ins, skip = self._insert_batch_and_checkpoint(conn, batch, last_key)
                    inserted += ins
                    skipped_db += skip
                    batch = []
                    batch_n += 1
            if batch or last_key != after:
                ins, skip = self._insert_batch_and_checkpoint(conn, batch, last_key)
                inserted += ins
                skipped_db += skip
            mark_step_done(conn, self.RESUME_STEP)
        finally:
            conn.close()
        logger.info(f"{self.__class__.__name__}: fetched {fetched}; inserted {inserted}")
        return inserted

    def run(self) -> int:
        return self.run_resumable(None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync betrokkenerelaties ArangoDB -> PostGIS")
    parser.add_argument("--arango-env", default="dev")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--limit", type=int, default=int(os.environ.get("BETROKKENERELATIES_LIMIT", "0")))
    args = parser.parse_args()
    limit = args.limit if args.limit and args.limit > 0 else None
    BetrokkenerelatiesSyncer(arango_env=args.arango_env, postgis_env=args.postgis_env, limit=limit).run()


if __name__ == "__main__":
    main()
