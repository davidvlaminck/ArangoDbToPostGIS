"""Parallel chunked export of Arango ``betrokkenerelaties`` edge collection → PostGIS.

Uses the same mechanism as ``export_assets``: key-range partitioning,
per-worker Arango + Postgres connections, COPY protocol.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from typing import Any

from arangodbtopostgis.db import get_arango_db
from arangodbtopostgis.export_table import export_table

logger = logging.getLogger(__name__)

COLLECTION = "betrokkenerelaties"
TABLE = "public.betrokkenerelaties"
RESUME_STEP = "betrokkenerelaties"

COLUMNS = (
    "uuid", "bron_asset_uuid", "bron_agent_uuid", "doel_agent_uuid", "rol",
    "bron_type", "doel_type", "type_uri",
    "asset_id_identificator", "asset_id_toegekend_door", "actief", "attributes",
)

AQL_QUERY = "FOR d IN betrokkenerelaties {where} SORT d._key ASC RETURN d"

EXCLUDED_ATTRIBUTE_KEYS = {
    "_id", "_key", "_rev", "_from", "_to", "@id", "@type", "uuid",
    "relatietype_key", "RelatieObject_bron", "RelatieObject_doel",
    "RelatieObject_bronAssetId", "RelatieObject_doelAssetId",
    "RelatieObject_assetId", "RelatieObject_typeURI",
    "AIMDBStatus_isActief", "isActief", "isDeleted",
    "rol", "HeeftBetrokkene_rol",
}


def _uuid_from_ref(ref: Any, prefix: str) -> str | None:
    if not isinstance(ref, str) or "/" not in ref:
        return None
    head, _, tail = ref.partition("/")
    if head != prefix or not tail:
        return None
    return tail


def _load_relatietype_lookups(db: Any) -> tuple[dict[str, str], dict[str, str]]:
    cursor = db.aql.execute("FOR rt IN relatietypes RETURN {_key: rt._key, uuid: rt.uuid, uri: rt.uri}")
    by_key: dict[str, str] = {}
    by_uri: dict[str, str] = {}
    for d in cursor:
        uuid = d.get("uuid")
        key = d.get("_key")
        uri = d.get("uri")
        if not uuid:
            continue
        if key:
            by_key[str(key)] = uuid
        if uri:
            by_uri[str(uri)] = uuid
    logger.info("betrokkenerelaties: loaded %d relatietype key mappings, %d uri mappings", len(by_key), len(by_uri))
    return by_key, by_uri


def _load_agent_lookups(db: Any) -> dict[str, str]:
    cursor = db.aql.execute("FOR a IN agents RETURN {_key: a._key, uuid: a.uuid}")
    by_key: dict[str, str] = {}
    for d in cursor:
        key = d.get("_key")
        uuid = d.get("uuid")
        if key and uuid:
            by_key[str(key)] = uuid
    logger.info("betrokkenerelaties: loaded %d agent mappings", len(by_key))
    return by_key


def _remaining_attributes(doc: dict[str, Any]) -> str | None:
    remaining = {k: v for k, v in doc.items() if k not in EXCLUDED_ATTRIBUTE_KEYS}
    if remaining == {"HeeftBetrokkene_specifiekeContactinfo": []}:
        return None
    return json.dumps(remaining, separators=(",", ":")) if remaining else None


def _make_transform(
    rt_by_key: dict[str, str], rt_by_uri: dict[str, str], agent_by_key: dict[str, str]
) -> Callable[[dict[str, Any]], dict[str, Any] | None]:
    def transform(doc: dict[str, Any]) -> dict[str, Any] | None:
        uuid = doc.get("_key") or doc.get("uuid")
        bron_ref = doc.get("_from")
        doel_ref = doc.get("_to")
        bron_asset = _uuid_from_ref(bron_ref, "assets")
        bron_agent_key = _uuid_from_ref(bron_ref, "agents")
        doel_agent_key = _uuid_from_ref(doel_ref, "agents")
        bron_agent = agent_by_key.get(bron_agent_key) if bron_agent_key else None
        doel_agent = agent_by_key.get(doel_agent_key) if doel_agent_key else None
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
            "rol": rol if isinstance(rol, str) and rol else None,
            "bron_type": bron_obj.get("@type") if isinstance(bron_obj, dict) else None,
            "doel_type": doel_obj.get("@type") if isinstance(doel_obj, dict) else None,
            "type_uri": type_uri if isinstance(type_uri, str) and type_uri else None,
            "asset_id_identificator": (
                asset_id.get("DtcIdentificator_identificator") if isinstance(asset_id, dict) else None
            ),
            "asset_id_toegekend_door": (
                asset_id.get("DtcIdentificator_toegekendDoor") if isinstance(asset_id, dict) else None
            ),
            "actief": actief,
            "attributes": _remaining_attributes(doc),
        }
    return transform


def export_betrokkenerelaties(
    arango_env: str = "prd",
    postgis_env: str = "prd",
    *,
    workers: int = 4,
) -> int:
    """Full fresh export of Arango ``betrokkenerelaties`` into PostGIS via parallel COPY chunks.

    Returns the number of rows loaded.
    """
    db = get_arango_db(arango_env)
    rt_by_key, rt_by_uri = _load_relatietype_lookups(db)
    agent_by_key = _load_agent_lookups(db)
    transform = _make_transform(rt_by_key, rt_by_uri, agent_by_key)

    return export_table(
        collection=COLLECTION,
        table=TABLE,
        columns=COLUMNS,
        aql_query=AQL_QUERY,
        transform=transform,
        arango_env=arango_env,
        postgis_env=postgis_env,
        workers=workers,
        resume_step=RESUME_STEP,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Arango betrokkenerelaties to PostGIS via parallel COPY chunks")
    parser.add_argument("--arango-env", default="prd")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel chunk workers")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    export_betrokkenerelaties(arango_env=args.arango_env, postgis_env=args.postgis_env, workers=args.workers)


if __name__ == "__main__":
    main()
