"""Parallel chunked export of Arango ``assetrelaties`` edge collection → PostGIS.

Uses the same mechanism as ``sync_assets``: key-range partitioning,
per-worker Arango + Postgres connections, COPY protocol via ``sync_parallel``.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from arangodbtopostgis.db import get_arango_db
from arangodbtopostgis.sync_parallel import parallel_export

logger = logging.getLogger(__name__)

COLLECTION = "assetrelaties"
PG_TABLE = "assetrelaties"
RESUME_STEP = "assetrelaties"

COLUMNS = (
    "uuid", "bron_asset_uuid", "doel_asset_uuid", "relatietype_uuid",
    "bron_type", "doel_type", "type_uri",
    "asset_id_identificator", "asset_id_toegekend_door", "actief", "attributes",
)

EXCLUDED_ATTRIBUTE_KEYS = {
    "_id", "_key", "_rev", "_from", "_to", "@id", "@type", "uuid",
    "relatietype_key", "RelatieObject_bron", "RelatieObject_doel",
    "RelatieObject_bronAssetId", "RelatieObject_doelAssetId",
    "RelatieObject_assetId", "RelatieObject_typeURI",
    "AIMDBStatus_isActief", "isActief", "isDeleted",
}


def _asset_uuid_from_ref(ref: Any) -> str | None:
    if not isinstance(ref, str) or "/" not in ref:
        return None
    return ref.split("/", 1)[1] or None


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
    logger.info("assetrelaties: loaded %d relatietype key mappings, %d uri mappings", len(by_key), len(by_uri))
    return by_key, by_uri


def _remaining_attributes(doc: dict[str, Any]) -> str | None:
    remaining = {k: v for k, v in doc.items() if k not in EXCLUDED_ATTRIBUTE_KEYS}
    return json.dumps(remaining, separators=(",", ":")) if remaining else None


def _resolve_relatietype(doc: dict[str, Any], by_key: dict[str, str], by_uri: dict[str, str]) -> str | None:
    rt_key = doc.get("relatietype_key")
    if rt_key is not None:
        resolved = by_key.get(str(rt_key))
        if resolved:
            return resolved
    for uri in (doc.get("RelatieObject_typeURI"), doc.get("@type")):
        if isinstance(uri, str) and uri:
            resolved = by_uri.get(uri)
            if resolved:
                return resolved
    return None


def _transform(doc: dict[str, Any], lookup: tuple[dict[str, str], dict[str, str]]) -> dict[str, Any] | None:
    by_key, by_uri = lookup
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
        "relatietype_uuid": _resolve_relatietype(doc, by_key, by_uri),
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


def export_assetrelaties(
    arango_env: str = "prd",
    postgis_env: str = "prd",
    *,
    workers: int = 4,
) -> int:
    """Full fresh export of Arango ``assetrelaties`` into PostGIS via parallel COPY chunks.

    Returns the number of rows loaded.
    """
    db = get_arango_db(arango_env)
    by_key, by_uri = _load_relatietype_lookups(db)
    lookup = (by_key, by_uri)

    return parallel_export(
        arango_env=arango_env,
        postgis_env=postgis_env,
        step=RESUME_STEP,
        collection=COLLECTION,
        pg_table=PG_TABLE,
        columns=COLUMNS,
        geom_table=None,
        geom_columns=(),
        geom_map=None,
        transform=_transform,
        lookup=lookup,
        workers=workers,
        pbar_desc="assetrelaties",
        truncate_tables=(),
        post_load=None,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Arango assetrelaties to PostGIS via parallel COPY chunks")
    parser.add_argument("--arango-env", default="prd")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel chunk workers")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    export_assetrelaties(arango_env=args.arango_env, postgis_env=args.postgis_env, workers=args.workers)


if __name__ == "__main__":
    main()