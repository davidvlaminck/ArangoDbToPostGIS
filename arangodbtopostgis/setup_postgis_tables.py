"""Ensure PostGIS tables exist before syncing data.

``params`` acts as the schema sentinel: if it is missing, all managed tables
are dropped and recreated from the SQL definition so schema changes are applied
cleanly.

Bulk-optimized schema: foreign-key constraints and secondary indexes live in
separate SQL files (``sql/postgis_relations_drop.sql`` and
``sql/postgis_relations_create.sql``), not in ``postgis_tables.sql``. They can
be temporarily dropped before bulk relation loads and restored afterward as the
final sync step.
"""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg

logger = logging.getLogger(__name__)

SQL_FILE: Path = Path(__file__).resolve().parents[1] / "sql" / "postgis_tables.sql"
RELATIONS_DROP_FILE: Path = (
    Path(__file__).resolve().parents[1] / "sql" / "postgis_relations_drop.sql"
)
RELATIONS_CREATE_FILE: Path = (
    Path(__file__).resolve().parents[1] / "sql" / "postgis_relations_create.sql"
)

# Tables this module is responsible for creating. Order matters for drops only
# as documentation; drops use CASCADE. ``params`` is the sentinel table.
MANAGED_TABLES: tuple[str, ...] = (
    "params",
    "agents",
    "assettypes",
    "assets",
    "geometrie",
    "bestekken",
    "bestekkoppelingen",
    "beheerders",
    "toezichtgroepen",
    "identiteiten",
    "relatietypes",
    "assetrelaties",
    "betrokkenerelaties",
)


def table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    """Return ``True`` when ``table_name`` exists in the public schema."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables "
            "  WHERE table_schema = 'public' AND table_name = %s"
            ")",
            (table_name,),
        )
        row = cur.fetchone()
    return bool(row[0]) if row else False


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _split_dollar_aware(text: str) -> list[str]:
    """Split SQL text into statements on ``;`` outside of ``$$ ... $$`` blocks."""
    statements: list[str] = []
    buf: list[str] = []
    in_dollar = False
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if text.startswith("$$", i):
            in_dollar = not in_dollar
            buf.append("$$")
            i += 2
            continue
        if ch == ";" and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _load_relations_sql(path: Path) -> list[str]:
    """Load and split a relations SQL file into executable statements."""
    text = path.read_text()
    return _split_dollar_aware(text)


def drop_managed_tables(conn: psycopg.Connection) -> None:
    """Drop all managed tables so the schema can be recreated cleanly."""
    # Drop in reverse dependency-aware order; CASCADE handles any remaining refs.
    for table in reversed(MANAGED_TABLES):
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{_qident(table)} CASCADE")
    conn.commit()
    logger.info(f"Dropped managed tables: {', '.join(MANAGED_TABLES)}")


def create_tables(conn: psycopg.Connection) -> None:
    sql = SQL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"Created managed tables from {SQL_FILE.name}: {', '.join(MANAGED_TABLES)}")


def drop_relations(conn: psycopg.Connection) -> None:
    """Drop all FK constraints and secondary indexes on managed tables.

    Idempotent: every statement uses IF EXISTS so missing objects are no-ops.
    Called before bulk loads so relations can be loaded without
    constraint/index overhead — even when tables are (re)created, this ensures
    any pre-existing relations are removed.
    """
    for stmt in _load_relations_sql(RELATIONS_DROP_FILE):
        with conn.cursor() as cur:
            cur.execute(stmt)
    conn.commit()
    logger.info("Dropped FK constraints and secondary indexes on managed tables")


def create_relations(conn: psycopg.Connection) -> None:
    """Create all FK constraints and secondary indexes on managed tables.

    Idempotent: every statement uses IF NOT EXISTS / DO blocks so existing
    objects are no-ops. Called as the final step after all data has been synced.
    """
    for stmt in _load_relations_sql(RELATIONS_CREATE_FILE):
        with conn.cursor() as cur:
            cur.execute(stmt)
    conn.commit()
    logger.info("Created FK constraints and secondary indexes on managed tables")


def ensure_tables(conn: psycopg.Connection) -> bool:
    """Create managed tables, resetting all of them when the sentinel is missing.

    Returns ``True`` when a fresh start was performed (sentinel missing, tables
    dropped and recreated). Callers use this to decide whether to drop existing
    FK constraints and secondary indexes before bulk loading.
    """
    if not table_exists(conn, "params"):
        existing = [t for t in MANAGED_TABLES if t != "params" and table_exists(conn, t)]
        if existing:
            logger.info(
                "Sentinel table 'params' is missing; dropping and recreating all managed tables "
                f"(existing: {', '.join(existing)})."
            )
        else:
            logger.info("Sentinel table 'params' is missing; creating managed tables.")
        drop_managed_tables(conn)
        create_tables(conn)
        # Tables were just recreated (CASCADE drops any relations on them), but
        # drop any pre-existing relations explicitly to satisfy the fresh-start
        # requirement that indexes/FKs are dropped even when tables are recreated.
        drop_relations(conn)
        return True

    missing = [t for t in MANAGED_TABLES if not table_exists(conn, t)]
    if not missing:
        logger.info("All managed tables already exist — nothing to create.")
        return False

    # params exists but others are missing: create via idempotent SQL without dropping.
    sql = SQL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"Created missing tables: {', '.join(missing)}")
    return False


def main() -> None:
    from arangodbtopostgis.db import get_postgis_connection

    conn = get_postgis_connection("prd")
    try:
        ensure_tables(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
