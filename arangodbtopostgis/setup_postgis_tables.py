"""Ensure PostGIS tables exist before syncing data.

``params`` acts as the schema sentinel: if it is missing, all managed tables
are dropped and recreated from the SQL definition so schema changes are applied
cleanly.
"""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg

logger = logging.getLogger(__name__)

SQL_FILE: Path = Path(__file__).resolve().parents[1] / "sql" / "postgis_tables.sql"

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


def ensure_tables(conn: psycopg.Connection) -> None:
    """Create managed tables, resetting all of them when the sentinel is missing."""
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
        return

    missing = [t for t in MANAGED_TABLES if not table_exists(conn, t)]
    if not missing:
        logger.info("All managed tables already exist — nothing to create.")
        return

    # params exists but others are missing: create via idempotent SQL without dropping.
    sql = SQL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"Created missing tables: {', '.join(missing)}")


def main() -> None:
    from arangodbtopostgis.db import get_postgis_connection

    conn = get_postgis_connection("prd")
    try:
        ensure_tables(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
