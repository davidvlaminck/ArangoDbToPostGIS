"""Bulk-load Arango ``assets`` into PostGIS with COPY + parallel chunked loading.

Entry point for the ``assets`` export step. The generic parallel chunk machinery
lives in ``arangodbtopostgis.sync_parallel``; this module only supplies the
asset-specific transform, column set and geometry post-processing.

Run standalone::

    python -m arangodbtopostgis.export_assets [--workers 4]

Or opt into it from ``main.py`` by setting ``USE_ASSET_EXPORT=1`` (assets step only).
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime
from typing import Any

from arangodbtopostgis.db import get_arango_db
from arangodbtopostgis.sync_parallel import parallel_export

logger = logging.getLogger(__name__)

NS_COLUMNS = ("geo", "wkt", "loc", "grp", "wl", "bz", "tz", "lgc", "ins", "ond", "vtc")

EXCLUDED_ATTRIBUTE_KEYS = {
    "@id", "@type", "_id", "_key", "_rev", "uuid", "AIMDBStatus_isActief", "isActief", "isDeleted",
    "assettype_key", "AIMObject_assetId", "AIMObject_typeURI", "AIMToestand_toestand", "toestand",
    "AIMObject_notitie", "AIMNaamObject_naam", "NaampadObject_naampad", "naampad_parts", "naampad_parent",
    "AIMVersie_assetVersie", "datumOprichtingObject", "AIMObject_datumOprichtingObject", "geometry", "bs",
    *NS_COLUMNS,
}

ASSETS_COLUMNS = (
    "uuid", "assettype", "actief", "asset_id_identificator", "asset_id_toegekend_door",
    "type_uri", "toestand", "notitie", "naam", "naampad", "naampad_parts", "naampad_parent",
    "asset_versie_context", "asset_versie_timestamp", "asset_versie_versienummer", "datum_oprichting_object",
    "geo", "wkt", "loc", "grp", "wl", "bz", "tz", "lgc", "ins", "ond", "vtc", "attributes",
)


def _jsonb(value: Any) -> str | None:
    if value is None or value == "" or value == [] or value == {}:
        return None
    return json.dumps(value, separators=(",", ":"))


def _to_date(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10]).isoformat()
        except ValueError:
            return None
    return None


def _toestand(doc: dict[str, Any]) -> str | None:
    val = doc.get("AIMToestand_toestand") or doc.get("toestand")
    if not val or not isinstance(val, str):
        return None
    return val.rsplit("/", 1)[-1] if "/" in val else val


def _naampad_parts(doc: dict[str, Any]) -> str | None:
    parts = doc.get("naampad_parts")
    if isinstance(parts, list) and parts:
        return _jsonb(parts)
    naampad = doc.get("NaampadObject_naampad")
    if isinstance(naampad, str) and naampad:
        derived = [p for p in naampad.split("/") if p != ""]
        return _jsonb(derived) if derived else None
    return None


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


def _remaining_attributes(doc: dict[str, Any]) -> str | None:
    remaining = {k: v for k, v in doc.items() if k not in EXCLUDED_ATTRIBUTE_KEYS}
    return _jsonb(remaining) if remaining else None


def _load_assettype_lookup(db: Any) -> dict[str, str]:
    cursor = db.aql.execute("FOR at IN assettypes RETURN {_key: at._key, uuid: at.uuid}")
    return {d["_key"]: d["uuid"] for d in cursor if d.get("_key") and d.get("uuid")}


def _resolve_assettype(doc: dict[str, Any], lookup: dict[str, str]) -> str | None:
    key = doc.get("assettype_key")
    return lookup.get(str(key)) if key else None


def _transform(doc: dict[str, Any], lookup: dict[str, str]) -> dict[str, Any] | None:
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
        "assettype": _resolve_assettype(doc, lookup),
        "actief": is_actief,
        "asset_id_identificator": asset_id.get("DtcIdentificator_identificator") if isinstance(asset_id, dict) else None,
        "asset_id_toegekend_door": asset_id.get("DtcIdentificator_toegekendDoor") if isinstance(asset_id, dict) else None,
        "type_uri": doc.get("AIMObject_typeURI") or doc.get("@type"),
        "toestand": _toestand(doc),
        "notitie": notitie, "naam": naam, "naampad": naampad,
        "naampad_parts": _naampad_parts(doc), "naampad_parent": _naampad_parent(doc),
        "asset_versie_context": versie.get("DtcAssetVersie_context") if isinstance(versie, dict) else None,
        "asset_versie_timestamp": (
            versie.get("DtcAssetVersie_timestamp") if isinstance(versie, dict) else None
        ),
        "asset_versie_versienummer": (
            str(versie["DtcAssetVersie_versienummer"])
            if isinstance(versie, dict) and versie.get("DtcAssetVersie_versienummer") is not None
            else None
        ),
        "datum_oprichting_object": _to_date(doc.get("datumOprichtingObject") or doc.get("AIMObject_datumOprichtingObject")),
    }
    for ns in NS_COLUMNS:
        if ns == "wkt":
            val = doc.get("wkt")
            row["wkt"] = val if isinstance(val, str) and val else None
        else:
            row[ns] = _jsonb(doc.get(ns))
    row["attributes"] = _remaining_attributes(doc)
    return row


def _compute_geometry(conn: Any) -> None:
    """Compute geometry in bulk for all ``geometrie`` rows and restore NOT NULL."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.geometrie "
            "SET geom = ST_Force3DZ(ST_SetSRID(ST_GeomFromEWKT(wkt), 3812))"
        )
        cur.execute("ALTER TABLE public.geometrie ALTER COLUMN geom SET NOT NULL")


def export_assets(
    arango_env: str = "dev",
    postgis_env: str = "prd",
    *,
    workers: int = 4,
) -> int:
    """Full fresh export of Arango ``assets`` into PostGIS via parallel COPY chunks.

    Returns the number of asset rows loaded in this run.
    """
    db = get_arango_db(arango_env)
    lookup = _load_assettype_lookup(db)
    logger.info("assets: loaded %d assettype mappings", len(lookup))

    return parallel_export(
        arango_env=arango_env,
        postgis_env=postgis_env,
        step="assets",
        collection="assets",
        pg_table="assets",
        columns=ASSETS_COLUMNS,
        geom_table="geometrie",
        geom_columns=("asset_uuid", "wkt"),
        geom_map=lambda row: (row["uuid"], row["wkt"]) if row.get("wkt") else None,
        transform=_transform,
        lookup=lookup,
        workers=workers,
        pbar_desc="assets",
        truncate_tables=("geometrie",),
        post_load=_compute_geometry,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Arango assets to PostGIS via parallel COPY chunks")
    parser.add_argument("--arango-env", default="dev")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel chunk workers")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    export_assets(arango_env=args.arango_env, postgis_env=args.postgis_env, workers=args.workers)


if __name__ == "__main__":
    main()
