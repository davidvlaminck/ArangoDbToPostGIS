"""Ensure PostGIS tables exist before syncing data."""

from __future__ import annotations

from pathlib import Path

import psycopg

SQL_FILE: Path = Path(__file__).resolve().parents[2] / "sql" / "postgis_tables.sql"

# Tables this module is responsible for creating.
MANAGED_TABLES: tuple[str, ...] = (
    "params",
    "agents",
    "assettypes",
    "bestekken",
    "beheerders",
    "toezichtgroepen",
    "identiteiten",
    "relatietypes",
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


def ensure_tables(conn: psycopg.Connection) -> None:
    """Create any managed tables that are missing."""
    missing = [t for t in MANAGED_TABLES if not table_exists(conn, t)]
    if not missing:
        print("All managed tables already exist — nothing to create.")
        return

    sql = SQL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"Created missing tables: {', '.join(missing)}")


def main() -> None:
    from arangodbtopostgis.db import get_postgis_connection

    conn = get_postgis_connection("prd")
    try:
        ensure_tables(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
