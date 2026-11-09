"""Bulk-load Arango ``assets`` into PostGIS with COPY + parallel chunked loading (alternative #5).

Splits the assets collection into N chunks by ``_key`` range and loads each chunk
concurrently with its own Arango + Postgres connection. Each worker loads both
``assets`` and ``geometrie`` for its chunk (mirrors the per-batch behavior of
``sync_assets.py``).

Run standalone::

    python -m arangodbtopostgis.export_assets [--workers 4]

Or opt into it from ``main.py`` by setting ``USE_ASSET_EXPORT=1`` (assets step only).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any

from tqdm import tqdm

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.resume import (
    CURSOR_KEY,
    DONE_KEY,
    clear_param,
    get_param,
    save_cursor_and_commit,
    set_param,
    truncate_table,
)

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

GEOMETRIE_COLUMNS = ("asset_uuid", "wkt")

_CHUNK = 1 << 16  # 64 KiB chunks when streaming CSV into COPY


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


def _csv_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _row_csv(row: dict[str, Any], columns: tuple[str, ...]) -> str:
    out = io.StringIO()
    csv.writer(out).writerow([_csv_value(row.get(c)) for c in columns])
    return out.getvalue()


def _key_boundaries(db: Any, num_workers: int) -> tuple[int, list[tuple[str | None, str | None]]]:
    """Return ``(count, [(lo, hi), ...])`` partitioning ``assets`` by ``_key``."""
    count = int(list(db.aql.execute("RETURN LENGTH(assets)"))[0])
    if count == 0:
        return count, []
    if num_workers <= 1:
        return count, [(None, None)]

    boundaries: list[str] = []
    for i in range(1, num_workers):
        offset = (count * i) // num_workers
        key = list(
            db.aql.execute(
                f"FOR d IN assets SORT d._key ASC LIMIT {offset}, 1 RETURN d._key",
            )
        )[0]
        boundaries.append(key)

    ranges: list[tuple[str | None, str | None]] = []
    prev: str | None = None
    for b in boundaries:
        ranges.append((prev, b))
        prev = b
    ranges.append((prev, None))
    return count, ranges


def _load_chunk(
    worker_id: int,
    arango_env: str,
    postgis_env: str,
    lookup: dict[str, str],
    lo: str | None,
    hi: str | None,
    resume_cursor: str | None,
    cursor_key: str,
    pbar: tqdm,
    pbar_lock: threading.Lock,
) -> dict[str, int]:
    """Load one ``_key`` range chunk (assets + geometrie) via batched COPY.

    Commits every 10k assets together with the matching geometrie rows and a
    resume cursor, so a stopped run can restart from the last committed
    ``_key`` without duplicates. ``pbar`` is advanced by this worker under
    ``pbar_lock``.
    """
    db = get_arango_db(arango_env)

    conditions: list[str] = []
    bind_vars: dict[str, Any] = {}
    if lo is not None:
        conditions.append("d._key >= @lo")
        bind_vars["lo"] = lo
    if hi is not None:
        conditions.append("d._key < @hi")
        bind_vars["hi"] = hi
    if resume_cursor:
        conditions.append("d._key > @after")
        bind_vars["after"] = resume_cursor
    where = " AND ".join(conditions)
    if where:
        where = "FILTER " + where
    query = f"FOR d IN assets {where} SORT d._key ASC RETURN d"

    range_tag = f"[{lo!r}, {hi!r})"
    logger.info("worker %d: starting chunk %s (resume from %r)", worker_id, range_tag, resume_cursor)

    t0 = time.perf_counter()
    n_assets = 0
    n_geom = 0
    last_key: str | None = resume_cursor

    conn = get_postgis_connection(postgis_env)
    try:
        cols = ", ".join('"' + c + '"' for c in ASSETS_COLUMNS)
        sql_assets = f'COPY public.assets ({cols}) FROM STDIN WITH (FORMAT csv)'
        sql_geom = "COPY public.geometrie (asset_uuid, wkt) FROM STDIN WITH (FORMAT csv)"

        def flush(batch: list[str], geom_batch: list[tuple[str, str]], key: str | None) -> None:
            """COPY one batch of assets + geometrie and persist the cursor."""
            if not batch:
                return
            with conn.cursor() as cur:
                with cur.copy(sql_assets) as copy_assets:
                    for csv_row in batch:
                        copy_assets.write(csv_row)
                with cur.copy(sql_geom) as copy_geom:
                    for uuid, wkt in geom_batch:
                        buf = io.StringIO()
                        csv.writer(buf).writerow([uuid, wkt])
                        copy_geom.write(buf.getvalue())
            save_cursor_and_commit(conn, cursor_key, key)

        batch: list[str] = []
        geom_batch: list[tuple[str, str]] = []
        for doc in db.aql.execute(query, bind_vars=bind_vars, batch_size=10000):
            last_key = doc.get("_key") or last_key
            try:
                row = _transform(doc, lookup)
            except Exception:
                continue
            if row is None:
                continue
            batch.append(_row_csv(row, ASSETS_COLUMNS))
            n_assets += 1
            if row.get("wkt"):
                geom_batch.append((row["uuid"], row["wkt"]))
            if len(batch) >= 10000:
                flush(batch, geom_batch, last_key)
                with pbar_lock:
                    pbar.update(10000)
                batch = []
                geom_batch = []
        if batch:
            flush(batch, geom_batch, last_key)
            with pbar_lock:
                pbar.update(len(batch))
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "worker %d: done — %d assets, %d geometry in %.1fs",
        worker_id, n_assets, n_geom, elapsed,
    )
    return {"assets": n_assets, "geom": n_geom}


def export_assets(
    arango_env: str = "dev",
    postgis_env: str = "prd",
    *,
    workers: int = 4,
) -> int:
    """Full fresh export of Arango ``assets`` into PostGIS via parallel COPY chunks.

    Resumable: each worker's last committed ``_key`` is stored in
    ``sync_cursor:assets:worker:<i>`` and committed with the data batch, so a
    stopped run resumes without duplicates. Returns the number of asset rows
    loaded in this run.
    """
    t0 = time.perf_counter()
    db = get_arango_db(arango_env)
    lookup = _load_assettype_lookup(db)
    logger.info("assets: loaded %d assettype mappings", len(lookup))

    count, ranges = _key_boundaries(db, workers)
    logger.info("assets: %d chunk ranges for %d workers", len(ranges), workers)

    # --- truncate target tables for a fresh run only ---
    conn = get_postgis_connection(postgis_env)
    try:
        done = get_param(conn, DONE_KEY.format(step="assets"))
        if done and done.get("value_bool") is True:
            logger.info("assets: marked done; skipping")
            return 0
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.assets")
            existing = int(cur.fetchone()[0])
        if existing == 0:
            # fresh run: ensure a clean slate
            truncate_table(conn, "public.assets")
            truncate_table(conn, "public.geometrie")
            for i in range(workers):
                clear_param(conn, CURSOR_KEY.format(step=f"assets:worker:{i}"))
            conn.commit()
        # geometrie.geom is NOT NULL; we COPY only (asset_uuid, wkt) and compute
        # geom in bulk afterwards, so drop the constraint for the load window.
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE public.geometrie ALTER COLUMN geom DROP NOT NULL")
        conn.commit()
    finally:
        conn.close()

    # --- determine per-worker resume cursors from the data already loaded ---
    # Prefer the per-worker cursor param (last committed _key); fall back to
    # max(uuid) per chunk range so partial loads from older exporter runs
    # (which never wrote per-worker cursors) still resume correctly.
    cursors: dict[int, str | None] = {}
    conn = get_postgis_connection(postgis_env)
    try:
        for i, (lo, hi) in enumerate(ranges):
            param = get_param(conn, CURSOR_KEY.format(step=f"assets:worker:{i}"))
            cursor = param.get("value_text") if param else None
            if cursor is None:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT max(uuid::text) FROM public.assets "
                        "WHERE (CAST(%s AS uuid) IS NULL OR uuid >= CAST(%s AS uuid)) "
                        "  AND (CAST(%s AS uuid) IS NULL OR uuid < CAST(%s AS uuid))",
                        (lo, lo, hi, hi),
                    )
                    row = cur.fetchone()
                    cursor = str(row[0]) if row and row[0] else None
            if hi is not None and cursor is not None and cursor >= hi:
                cursor = None
            cursors[i] = cursor
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.assets")
            already_loaded = int(cur.fetchone()[0])
    finally:
        conn.close()

    # --- parallel load of assets + geometrie ---
    n_assets = 0
    n_geom = 0
    pbar_lock = threading.Lock()
    with tqdm(total=count, desc="assets", unit="asset", unit_scale=True, ncols=80, initial=already_loaded) as pbar:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for i, (lo, hi) in enumerate(ranges):
                cursor = cursors[i]
                if hi is not None and cursor is not None and cursor >= hi:
                    logger.info("assets: chunk %d [%s, %s) already complete, skipping", i, lo, hi)
                    continue
                futures[executor.submit(
                    _load_chunk, i, arango_env, postgis_env, lookup, lo, hi, cursor,
                    CURSOR_KEY.format(step=f"assets:worker:{i}"), pbar, pbar_lock,
                )] = (i, lo, hi)
            for fut in as_completed(futures):
                i, lo, hi = futures[fut]
                try:
                    result = fut.result()
                    n_assets += result["assets"]
                    n_geom += result["geom"]
                    logger.info(
                        "assets: chunk %d [%s, %s) loaded %d assets, %d geometry",
                        i, lo, hi, result["assets"], result["geom"],
                    )
                except Exception as exc:
                    logger.error("assets: chunk %d [%s, %s) failed: %s", i, lo, hi, exc)
                    raise

    # --- compute geometry in bulk and restore NOT NULL ---
    conn = get_postgis_connection(postgis_env)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.geometrie "
                "SET geom = ST_Force3DZ(ST_SetSRID(ST_GeomFromEWKT(wkt), 3812))"
            )
            cur.execute("ALTER TABLE public.geometrie ALTER COLUMN geom SET NOT NULL")
        conn.commit()
        logger.info("assets: computed geometry for %d rows", n_geom)
    finally:
        conn.close()

    # Mark the resume step done so a full re-run starts clean.
    conn = get_postgis_connection(postgis_env)
    try:
        set_param(conn, DONE_KEY.format(step="assets"), value_bool=True)
        for i in range(workers):
            clear_param(conn, CURSOR_KEY.format(step=f"assets:worker:{i}"))
        conn.commit()
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "assets: export complete — %d assets, %d geometry in %.1fs",
        n_assets, n_geom, elapsed,
    )
    return n_assets


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
