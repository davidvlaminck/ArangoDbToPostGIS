"""Generic parallel chunked exporter for ArangoDB collections → PostGIS tables.

Provides ``export_table`` which:

1. Computes ``_key`` range boundaries by sampling sorted ``_key``
2. Spawns N worker threads, each with its own Arango + Postgres connection
3. Each worker AQL-queries its chunk, transforms docs, COPYs to Postgres
4. Truncates target table before load; runs optional post_load callback after

Run a single table::

    python -m arangodbtopostgis.export_table --table assetrelaties --workers 4
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.resume import (
    CURSOR_KEY,
    DONE_KEY,
    clear_param,
    set_param,
    truncate_table,
)

logger = logging.getLogger(__name__)


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


def _key_boundaries(
    db: Any, collection: str, num_workers: int
) -> list[tuple[str | None, str | None]]:
    """Return ``[(lo, hi), ...]`` key ranges that partition ``collection`` by ``_key``."""
    count = int(list(db.aql.execute(f"RETURN LENGTH({collection})"))[0])
    if count == 0:
        return []
    if num_workers <= 1:
        return [(None, None)]

    boundaries: list[str] = []
    for i in range(1, num_workers):
        offset = (count * i) // num_workers
        key = list(
            db.aql.execute(
                f"FOR d IN {collection} SORT d._key ASC LIMIT {offset}, 1 RETURN d._key"
            )
        )[0]
        boundaries.append(key)

    ranges: list[tuple[str | None, str | None]] = []
    prev: str | None = None
    for b in boundaries:
        ranges.append((prev, b))
        prev = b
    ranges.append((prev, None))
    return ranges


def _load_chunk(
    worker_id: int,
    arango_env: str,
    postgis_env: str,
    collection: str,
    aql_query: str,
    transform: Callable[[dict[str, Any]], dict[str, Any] | None],
    columns: tuple[str, ...],
    table: str,
    lo: str | None,
    hi: str | None,
    geom_columns: tuple[str, ...] | None = None,
    geom_table: str | None = None,
) -> dict[str, int]:
    """Load one ``_key`` range chunk via COPY. Returns row counts."""
    db = get_arango_db(arango_env)

    conditions: list[str] = []
    bind_vars: dict[str, Any] = {}
    if lo is not None:
        conditions.append("d._key >= @lo")
        bind_vars["lo"] = lo
    if hi is not None:
        conditions.append("d._key < @hi")
        bind_vars["hi"] = hi
    where = " AND ".join(conditions)
    if where:
        where = "FILTER " + where
    query = aql_query.format(where=where)

    range_tag = f"[{lo!r}, {hi!r})"
    logger.info("worker %d: starting chunk %s", worker_id, range_tag)

    t0 = time.perf_counter()
    n_rows = 0
    n_geom = 0
    geom_rows: list[tuple[Any, ...]] = []
    last_key: str | None = None

    conn = get_postgis_connection(postgis_env)
    try:
        cols = ", ".join('"' + c + '"' for c in columns)
        sql = f'COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)'

        with conn.cursor() as cur:
            with cur.copy(sql) as copy:
                batch_n = 0
                for doc in db.aql.execute(query, bind_vars=bind_vars, batch_size=10000):
                    last_key = doc.get("_key") or last_key
                    try:
                        row = transform(doc)
                    except Exception:
                        continue
                    if row is None:
                        continue
                    copy.write(_row_csv(row, columns))
                    n_rows += 1
                    batch_n += 1
                    if batch_n % 50000 == 0:
                        logger.info(
                            "worker %d: cursor='%s' progress — %d rows processed",
                            worker_id, last_key, n_rows,
                        )
                    if geom_columns and geom_table:
                        geom_values = tuple(row.get(c) for c in geom_columns)
                        if any(v is not None for v in geom_values):
                            geom_rows.append(geom_values)
                            n_geom += 1

            if geom_columns and geom_table:
                geom_cols = ", ".join('"' + c + '"' for c in geom_columns)
                sql_geom = f"COPY {geom_table} ({geom_cols}) FROM STDIN WITH (FORMAT csv)"
                with cur.copy(sql_geom) as copy_geom:
                    for values in geom_rows:
                        buf = io.StringIO()
                        csv.writer(buf).writerow([_csv_value(v) for v in values])
                        copy_geom.write(buf.getvalue())
        conn.commit()
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "worker %d: done — %d rows, %d geometry in %.1fs",
        worker_id, n_rows, n_geom, elapsed,
    )
    return {"rows": n_rows, "geom": n_geom}


def export_table(
    collection: str,
    table: str,
    columns: tuple[str, ...],
    aql_query: str,
    transform: Callable[[dict[str, Any]], dict[str, Any] | None],
    arango_env: str = "prd",
    postgis_env: str = "prd",
    workers: int = 4,
    resume_step: str | None = None,
    geom_columns: tuple[str, ...] | None = None,
    geom_table: str | None = None,
    post_load: Callable[[Any], None] | None = None,
) -> int:
    """Full fresh export of an Arango collection into a PostGIS table via parallel COPY chunks.

    Returns the number of rows loaded.
    """
    t0 = time.perf_counter()
    db = get_arango_db(arango_env)

    ranges = _key_boundaries(db, collection, workers)
    logger.info("%s: %d chunk ranges for %d workers", collection, len(ranges), workers)

    conn = get_postgis_connection(postgis_env)
    try:
        truncate_table(conn, table)
        if geom_table:
            truncate_table(conn, geom_table)
        if geom_table:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {geom_table} ALTER COLUMN geom DROP NOT NULL")
        conn.commit()
    finally:
        conn.close()

    n_rows = 0
    n_geom = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _load_chunk, i, arango_env, postgis_env, collection, aql_query,
                transform, columns, table, lo, hi, geom_columns, geom_table,
            ): (lo, hi)
            for i, (lo, hi) in enumerate(ranges)
        }
        for fut in as_completed(futures):
            lo, hi = futures[fut]
            try:
                result = fut.result()
                n_rows += result["rows"]
                n_geom += result["geom"]
                logger.info(
                    "%s: chunk [%s, %s) loaded %d rows, %d geometry",
                    collection, lo, hi, result["rows"], result["geom"],
                )
            except Exception as exc:
                logger.error("%s: chunk [%s, %s) failed: %s", collection, lo, hi, exc)
                raise

    if post_load:
        conn = get_postgis_connection(postgis_env)
        try:
            post_load(conn)
        finally:
            conn.close()

    if resume_step:
        conn = get_postgis_connection(postgis_env)
        try:
            set_param(conn, DONE_KEY.format(step=resume_step), value_bool=True)
            clear_param(conn, CURSOR_KEY.format(step=resume_step))
            conn.commit()
        finally:
            conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "%s: export complete — %d rows, %d geometry in %.1fs",
        collection, n_rows, n_geom, elapsed,
    )
    return n_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Export an Arango collection to PostGIS via parallel COPY chunks")
    parser.add_argument("--table", required=True, help="Table/collection name to export")
    parser.add_argument("--arango-env", default="prd")
    parser.add_argument("--postgis-env", default="prd")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel chunk workers")
    parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    raise SystemExit(
        "Use a specific exporter module (export_assets, export_assetrelaties, etc.) instead"
    )


if __name__ == "__main__":
    main()
