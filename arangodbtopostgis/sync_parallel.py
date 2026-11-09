"""Parallel chunked export of one ArangoDB collection into PostGIS tables.

Generic machinery reused by the per-table export scripts (e.g. export_assets).
Work is split by ``_key`` range across N threads. Each thread COPYs batches of
rows (assets + any dependent ``geometrie``-style table) into PostGIS, committing
every sub-batch together with a resume cursor so an interrupted run can resume
from the last committed ``_key`` without duplicates.
"""

from __future__ import annotations

import csv
import io
import logging
import threading
import time
from collections.abc import Callable
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


def key_boundaries(
    db: Any, collection: str, num_workers: int
) -> tuple[int, list[tuple[str | None, str | None]]]:
    """Return ``(count, [(lo, hi), ...])`` partitioning ``collection`` by ``_key``."""
    count = int(list(db.aql.execute(f"RETURN LENGTH({collection})"))[0])
    if count == 0:
        return count, []
    if num_workers <= 1:
        return count, [(None, None)]

    boundaries: list[str] = []
    for i in range(1, num_workers):
        offset = (count * i) // num_workers
        key = list(
            db.aql.execute(
                f"FOR d IN {collection} SORT d._key ASC LIMIT {offset}, 1 RETURN d._key",
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


def worker_cursor_key(step: str, worker_id: int) -> str:
    """Resume cursor key for worker ``worker_id`` (one param per worker)."""
    return f"{CURSOR_KEY.format(step=step)}:w{worker_id}"


def load_chunk(
    worker_id: int,
    arango_env: str,
    postgis_env: str,
    step: str,
    collection: str,
    *,
    pg_table: str,
    columns: tuple[str, ...],
    geom_table: str | None,
    geom_columns: tuple[str, ...],
    geom_map: Callable[[dict[str, Any]], tuple[str, str] | None],
    transform: Callable[[dict[str, Any], dict[str, str]], dict[str, Any] | None],
    lookup: dict[str, str],
    cursor_key: str,
    lo: str | None,
    hi: str | None,
    resume_cursor: str | None,
    pbar: tqdm,
    pbar_lock: threading.Lock,
    batch_size: int = 10000,
) -> dict[str, int]:
    """Load one ``_key`` range chunk (main table + optional related table) via batched COPY.

    Commits every ``batch_size`` rows together with a resume cursor, so a stopped
    run resumes from the last committed ``_key`` without duplicates.
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
    query = f"FOR d IN {collection} {where} SORT d._key ASC RETURN d"

    range_tag = f"[{lo!r}, {hi!r})"
    logger.info("worker %d: starting chunk %s (resume from %r)", worker_id, range_tag, resume_cursor)

    t0 = time.perf_counter()
    n_rows = 0
    n_geom = 0
    last_key: str | None = resume_cursor

    conn = get_postgis_connection(postgis_env)
    try:
        cols = ", ".join('"' + c + '"' for c in columns)
        sql_main = f'COPY public.{pg_table} ({cols}) FROM STDIN WITH (FORMAT csv)'
        sql_geom = (
            f"COPY public.{geom_table} ({', '.join('\"' + c + '\"' for c in geom_columns)}) "
            "FROM STDIN WITH (FORMAT csv)"
        ) if geom_table else None

        def flush(
            batch: list[str],
            geom_batch: list[tuple[str, str]],
            key: str | None,
        ) -> None:
            if not batch:
                return
            with conn.cursor() as cur:
                with cur.copy(sql_main) as copy_main:
                    for csv_row in batch:
                        copy_main.write(csv_row)
                if sql_geom is not None:
                    with cur.copy(sql_geom) as copy_geom:
                        for uuid, wkt in geom_batch:
                            buf = io.StringIO()
                            csv.writer(buf).writerow([uuid, wkt])
                            copy_geom.write(buf.getvalue())
            save_cursor_and_commit(conn, step, key)

        batch: list[str] = []
        geom_batch: list[tuple[str, str]] = []
        for doc in db.aql.execute(query, bind_vars=bind_vars, batch_size=batch_size):
            last_key = doc.get("_key") or last_key
            try:
                row = transform(doc, lookup)
            except Exception:
                continue
            if row is None:
                continue
            batch.append(_row_csv(row, columns))
            if geom_map is not None:
                geom_pair = geom_map(row)
                if geom_pair:
                    geom_batch.append(geom_pair)
            if len(batch) >= batch_size:
                n_rows += len(batch)
                n_geom += len(geom_batch)
                flush(batch, geom_batch, last_key)
                with pbar_lock:
                    pbar.update(len(batch))
                batch = []
                geom_batch = []
        if batch:
            n_rows += len(batch)
            n_geom += len(geom_batch)
            flush(batch, geom_batch, last_key)
            with pbar_lock:
                pbar.update(len(batch))
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    logger.debug(
        "worker %d: done — %d rows, %d related in %.1fs",
        worker_id, n_rows, n_geom, elapsed,
    )
    return {"rows": n_rows, "geom": n_geom}


def parallel_export(
    *,
    arango_env: str = "prd",
    postgis_env: str = "prd",
    step: str,
    collection: str,
    pg_table: str,
    columns: tuple[str, ...],
    geom_table: str | None = None,
    geom_columns: tuple[str, ...] = (),
    geom_map: Callable[[dict[str, Any]], tuple[str, str] | None] | None = None,
    transform: Callable[[dict[str, Any], dict[str, str]], dict[str, Any] | None],
    lookup: dict[str, str],
    workers: int = 4,
    pbar_desc: str = "rows",
    truncate_tables: tuple[str, ...] = (),
    post_load: Callable[[Any], None] | None = None,
    cursor_column: str = "uuid",
) -> int:
    """Parallel, resumable bulk-export of one ArangoDB collection into PostGIS.

    Splits ``collection`` into ``workers`` key-range chunks and loads each
    concurrently with batched COPY + per-sub-batch resume cursors. Returns the
    number of rows loaded in this run.
    """
    workers = max(1, workers)
    t0 = time.perf_counter()
    db = get_arango_db(arango_env)
    logger.info("%s: loaded %s lookup mappings", step, len(lookup))

    count, ranges = key_boundaries(db, collection, workers)
    logger.info("%s: %d chunk ranges for %d workers", step, len(ranges), workers)

    if not ranges:
        logger.info("%s: nothing to sync", step)
        return 0

    # --- truncate target tables for a fresh run only ---
    conn = get_postgis_connection(postgis_env)
    try:
        done = get_param(conn, DONE_KEY.format(step=step))
        if done and done.get("value_bool") is True:
            logger.info("%s: marked done; skipping", step)
            return 0
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM public.{pg_table}")
            existing = int(cur.fetchone()[0])
        if existing == 0:
            tables = (f"public.{pg_table}",) + tuple(
                f"public.{t}" for t in truncate_tables
            )
            for table in tables:
                truncate_table(conn, table)
            for i in range(workers):
                clear_param(conn, worker_cursor_key(step, i))
            conn.commit()
        if geom_table is not None:
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE public.{geom_table} ALTER COLUMN geom DROP NOT NULL"
                )
        conn.commit()
    finally:
        conn.close()

    # --- determine per-worker resume cursors from the data already loaded ---
    cursors: dict[int, str | None] = {}
    conn = get_postgis_connection(postgis_env)
    try:
        for i, (lo, hi) in enumerate(ranges):
            param = get_param(conn, worker_cursor_key(step, i))
            cursor = param.get("value_text") if param else None
            if cursor is None:
                with conn.cursor() as cur:
                    if cursor_column == "uuid":
                        cur.execute(
                            f"SELECT max(uuid::text) FROM public.{pg_table} "
                            "WHERE (CAST(%s AS uuid) IS NULL OR uuid >= CAST(%s AS uuid)) "
                            "  AND (CAST(%s AS uuid) IS NULL OR uuid < CAST(%s AS uuid))",
                            (lo, lo, hi, hi),
                        )
                    else:
                        cur.execute(
                            f"SELECT max({cursor_column}) FROM public.{pg_table} "
                            "WHERE (%s::text IS NULL OR {cursor_column} >= %s::text) "
                            "  AND (%s::text IS NULL OR {cursor_column} < %s::text)".format(cursor_column=cursor_column),
                            (lo, lo, hi, hi),
                        )
                    row = cur.fetchone()
                    cursor = str(row[0]) if row and row[0] else None
                if hi is not None and cursor is not None and cursor >= hi:
                    cursor = None
            cursors[i] = cursor
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM public.{pg_table}")
            already_loaded = int(cur.fetchone()[0])
    finally:
        conn.close()

    # --- parallel load ---
    n_rows = 0
    n_geom = 0
    pbar_lock = threading.Lock()
    with tqdm(
        total=count,
        desc=pbar_desc,
        unit="row",
        unit_scale=True,
        ncols=80,
        initial=already_loaded,
    ) as pbar:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for i, (lo, hi) in enumerate(ranges):
                cursor = cursors[i]
                if hi is not None and cursor is not None and cursor >= hi:
                    logger.info(
                        "%s: chunk %d [%s, %s) already complete, skipping",
                        step, i, lo, hi,
                    )
                    continue
                futures[executor.submit(
                    load_chunk,
                    i,
                    arango_env,
                    postgis_env,
                    step,
                    collection,
                    pg_table=pg_table,
                    columns=columns,
                    geom_table=geom_table,
                    geom_columns=geom_columns,
                    geom_map=geom_map,  # type: ignore[arg-type]
                    transform=transform,
                    lookup=lookup,
                    cursor_key=worker_cursor_key(step, i),
                    lo=lo,
                    hi=hi,
                    resume_cursor=cursor,
                    pbar=pbar,
                    pbar_lock=pbar_lock,
                )] = (i, lo, hi)
            for fut in as_completed(futures):
                i, lo, hi = futures[fut]
                try:
                    result = fut.result()
                    n_rows += result["rows"]
                    n_geom += result["geom"]
                    logger.info(
                        "%s: chunk %d [%s, %s) loaded %d rows, %d related",
                        step, i, lo, hi, result["rows"], result["geom"],
                    )
                except Exception as exc:
                    logger.error(
                        "%s: chunk %d [%s, %s) failed: %s", i, lo, hi, exc,
                    )
                    raise

    # --- post-load (e.g. compute geometry + restore NOT NULL) ---
    conn = get_postgis_connection(postgis_env)
    try:
        if post_load is not None:
            post_load(conn)
        conn.commit()
    finally:
        conn.close()

    # Mark the resume step done so a full re-run starts clean.
    conn = get_postgis_connection(postgis_env)
    try:
        set_param(conn, DONE_KEY.format(step=step), value_bool=True)
        for i in range(workers):
            clear_param(conn, worker_cursor_key(step, i))
        conn.commit()
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "%s: export complete — %d rows, %d related in %.1fs",
        step, n_rows, n_geom, elapsed,
    )
    return n_rows
