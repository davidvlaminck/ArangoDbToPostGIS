import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import psycopg
from arango import ArangoClient
from read_creds import arango_creds, postgis_creds


def check_postgis_access() -> bool:
    creds = postgis_creds("prd")
    try:
        conn = psycopg.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["user"],
            password=creds["password"],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        print("PostGIS access OK")
        return True
    except Exception as exc:
        print(f"PostGIS access FAILED: {exc}")
        return False


def check_arango_access() -> bool:
    creds = arango_creds("dev")
    try:
        client = ArangoClient(hosts=creds["hosts"])
        db = client.db(
            creds["database"],
            username=creds["username"],
            password=creds["password"],
        )
        db.collections()
        print("ArangoDB access OK")
        return True
    except Exception as exc:
        print(f"ArangoDB access FAILED: {exc}")
        return False


def setup_postgis_tables() -> None:
    import psycopg
    from read_creds import postgis_creds
    from setup_postgis_tables import ensure_tables

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


def fill_database() -> None:
    from sync_agents import main as sync_agents_main
    from sync_assettypes import main as sync_assettypes_main
    from sync_beheerders import main as sync_beheerders_main
    from sync_bestekken import main as sync_bestekken_main
    from sync_identiteiten import main as sync_identiteiten_main
    from sync_relatietypes import main as sync_relatietypes_main
    from sync_toezichtgroepen import main as sync_toezichtgroepen_main

    sync_agents_main()
    sync_assettypes_main()
    sync_beheerders_main()
    sync_bestekken_main()
    sync_identiteiten_main()
    sync_relatietypes_main()
    sync_toezichtgroepen_main()


def main() -> None:
    postgis_ok = check_postgis_access()
    arango_ok = check_arango_access()

    if not postgis_ok or not arango_ok:
        print("Aborting: database access check failed.")
        sys.exit(1)

    setup_postgis_tables()
    fill_database()


if __name__ == "__main__":
    main()
