import sys
from pathlib import Path

# Allow running as: python3 scripts/setup_postgis_tables.py
sys.path.insert(0, str(Path(__file__).resolve().parent))

import psycopg
from read_creds import postgis_creds

SQL_FILE = Path(__file__).resolve().parents[1] / "sql" / "postgis_tables.sql"


def table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables "
            "  WHERE table_schema = 'public' AND table_name = %s"
            ")",
            (table_name,),
        )
        return cur.fetchone()[0]


MANAGED_TABLES = [
    "params",
    "agents",
    "assettypes",
    "bestekken",
    "beheerders",
    "toezichtgroepen",
    "identiteiten",
    "relatietypes",
]


def ensure_tables(conn) -> None:
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
    creds = postgis_creds("prd")
    conn = psycopg.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["user"],
        password=creds["password"],
    )
    try:
        ensure_tables(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
