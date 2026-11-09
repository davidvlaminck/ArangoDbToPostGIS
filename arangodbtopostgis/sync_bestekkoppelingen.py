"""Parallel chunked export of Arango ``bestekkoppelingen`` edge collection → PostGIS.

Uses the same mechanism as ``sync_assets``: key-range partitioning,
per-worker Arango + Postgres connections, COPY protocol via ``sync_parallel``.

Because ``bestekkoppelingen`` dedups by (asset_uuid, volgnummer) keeping the
last row, rows are first COPY'd into a staging table, then deduplicated and
inserted into the real table in a single post-load step.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from typing import Any

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.sync_parallel import parallel_export

logger = logging.getLogger(__name__)

COLLECTION = "bestekkoppelingen"
STAGING_TABLE = "bestekkoppelingen_staging"
REAL_TABLE = "bestekkoppelingen"
RESUME_STEP = "bestekkoppelingen"

STAGING_COLUMNS = (
    "_key", "asset_uuid", "bestek_uuid", "volgnummer",
    "start_datum", "eind_datum", "koppeling_status",
)

REAL_COLUMNS = (
    "asset_uuid", "bestek_uuid", "volgnummer",
    "start_datum", "eind_datum", "koppeling_status",
)

AQL_QUERY = """
    FOR d IN bestekkoppelingen {where}
    SORT d._key ASC
    RETURN {
        _key: d._key,
        _from: d._from,
        volgnummer: d.volgnummer != null ? d.volgnummer : d.volgorde,
        DtcBestekkoppeling_bestekId: d.DtcBestekkoppeling_bestekId,
        DtcBestekkoppeling_actiefVan: d.DtcBestekkoppeling_actiefVan,
        DtcBestekkoppeling_actiefTot: d.DtcBestekkoppeling_actiefTot,
        DtcBestekkoppeling_status: d.DtcBestekkoppeling_status
    }
"""


def _parse_bestek_uuid(item: dict[str, Any]) -> str | None:
    ident = (item.get("DtcBestekkoppeling_bestekId") or {}).get("DtcIdentificator_identificator")
    if isinstance(ident, str) and ident:
        return ident[:36] if len(ident) >= 36 else ident
    return None


def _asset_uuid_from_edge(edge: dict[str, Any]) -> str | None:
    from_ref = edge.get("_from")
    if not isinstance(from_ref, str) or "/" not in from_ref:
        return None
    return from_ref.split("/", 1)[1] or None


def _parse_dt(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _status_tail(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.rsplit("/", 1)[-1] if "/" in value else value.strip()


def _ensure_staging_table(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.bestekkoppelingen_staging (
                _key text NOT NULL,
                asset_uuid uuid NOT NULL,
                bestek_uuid uuid NOT NULL,
                volgnummer smallint NOT NULL,
                start_datum timestamptz,
                eind_datum timestamptz,
                koppeling_status text
            )
            """
        )
    conn.commit()


def _transform(doc: dict[str, Any]) -> dict[str, Any] | None:
    asset_uuid = _asset_uuid_from_edge(doc)
    bestek_uuid = _parse_bestek_uuid(doc)
    volgnummer_raw = doc.get("volgnummer")
    if volgnummer_raw is None:
        volgnummer_raw = doc.get("volgorde")
    if not asset_uuid or not bestek_uuid or volgnummer_raw is None:
        return None
    try:
        volgnummer = int(volgnummer_raw)
    except (TypeError, ValueError):
        return None
    if volgnummer < -32768 or volgnummer > 32767:
        return None
    return {
        "_key": doc.get("_key"),
        "asset_uuid": asset_uuid,
        "bestek_uuid": bestek_uuid,
        "volgnummer": volgnummer,
        "start_datum": _parse_dt(doc.get("DtcBestekkoppeling_actiefVan")),
        "eind_datum": _parse_dt(doc.get("DtcBestekkoppeling_actiefTot")),
        "koppeling_status": _status_tail(doc.get("DtcBestekkoppeling_status")),
    }


def _dedup_insert(conn: Any) -> None:
    """Deduplicate staging rows and insert into the real table."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.bestekkoppelingen")
        cur.execute(
            """
            INSERT INTO public.bestekkoppelingen
                (asset_uuid, bestek_uuid, volgnummer, start_datum, eind_datum, koppeling_status)
            SELECT asset_uuid, bestek_uuid, volgnummer, start_datum, eind_datum, koppeling_status
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY asset_uuid, volgnummer
                        ORDER BY _key DESC
                    ) AS rn
                FROM public.bestekkoppelingen_staging
            ) s
            WHERE rn = 1
            """
        )
        cur.execute("TRUNCATE TABLE public.bestekkoppelingen_staging")
    conn.commit()
    logger.info("bestekkoppelingen: deduplicated and inserted into real table")


def export_bestekkoppelingen(
    arango_env: str = "prd",
    postgis_env: str = "prd",
    *,
    workers: int = 4,
) -> int:
    """Full fresh export of Arango ``bestekkoppelingen`` into PostGIS via parallel COPY chunks.

    Returns the number of rows loaded into the real table.
    """
    conn = get_postgis_connection(postgis_env)
    try:
        _ensure_staging_table(conn)
    finally:
        conn.close()

    return parallel_export(
        arango_env=arango_env,
        postgis_env=postgis_env,
        step=RESUME_STEP,
        collection=COLLECTION,
        pg_table=STAGING_TABLE,
        columns=STAGING_COLUMNS,
        geom_table=None,
        geom_columns=(),
        geom_map=None,
        transform=_transform,
        lookup={},  # not used for bestekkoppelingen
        workers=workers,
        pbar_desc="bestekkoppelingen",
        truncate_tables=(STAGING_TABLE,),
        post_load=_dedup_insert,
        cursor_column="_key",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Arango bestekkoppelingen to PostGIS via parallel COPY chunks")
    parser.add_argument("--arango-env", default="prd")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel chunk workers")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    export_bestekkoppelingen(arango_env=args.arango_env, postgis_env=args.postgis_env, workers=args.workers)


if __name__ == "__main__":
    main()