"""Entry point: check access, ensure tables, then run all syncers."""

from __future__ import annotations

import sys

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.setup_postgis_tables import ensure_tables
from arangodbtopostgis.sync_agents import AgentsSyncer
from arangodbtopostgis.sync_assetrelaties import AssetrelatiesSyncer
from arangodbtopostgis.sync_assets import AssetsSyncer
from arangodbtopostgis.sync_assettypes import AssettypesSyncer
from arangodbtopostgis.sync_beheerders import BeheerdersSyncer
from arangodbtopostgis.sync_bestekken import BestekkenSyncer
from arangodbtopostgis.sync_identiteiten import IdentiteitenSyncer
from arangodbtopostgis.sync_relatietypes import RelatietypesSyncer
from arangodbtopostgis.sync_toezichtgroepen import ToezichtgroepenSyncer


def _postgis_install_instructions(dbname: str, user: str, exc: Exception) -> str:
    return (
        f"PostGIS extension is not installed (or could not be enabled) in database '{dbname}'.\n"
        f"Error: {exc}\n\n"
        "To install/enable it, connect as a PostgreSQL superuser (or a role with CREATE privilege "
        f"on database '{dbname}') and run:\n"
        f"  psql -d {dbname} -U <superuser> -c \"CREATE EXTENSION IF NOT EXISTS postgis;\"\n\n"
        "If the PostGIS packages are not installed on the server, install them first (Debian/Ubuntu):\n"
        "  sudo apt install postgis postgresql-<version>-postgis-3\n"
        "then rerun the CREATE EXTENSION command. Verify with:\n"
        "  psql -d {dbname} -c \"SELECT extname, extversion FROM pg_extension WHERE extname='postgis';\"\n"
        f"(current app user: '{user}')"
    )


def check_postgis_access() -> bool:
    """Verify PostGIS credentials and ensure the PostGIS extension is available."""
    conn = None
    try:
        conn = get_postgis_connection("prd")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
            if cur.fetchone() is None:
                # Try to enable it; this requires sufficient privileges.
                conn.rollback()
                try:
                    conn.autocommit = True
                    with conn.cursor() as c2:
                        c2.execute("CREATE EXTENSION IF NOT EXISTS postgis")
                    conn.autocommit = False
                    with conn.cursor() as c2:
                        c2.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
                        if c2.fetchone() is None:
                            raise RuntimeError("CREATE EXTENSION reported success but postgis not found")
                    print("PostGIS access OK (enabled 'postgis' extension)")
                    return True
                except Exception as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    dbname = conn.info.dbname if conn.info else "awvinfra"
                    user = conn.info.user if conn.info else "unknown"
                    print("PostGIS access FAILED: PostGIS extension missing.")
                    print(_postgis_install_instructions(dbname, user, exc))
                    return False
    except Exception as exc:
        print(f"PostGIS access FAILED: {exc}")
        return False
    finally:
        if conn is not None:
            conn.close()
    print("PostGIS access OK (extension 'postgis' present)")
    return True


def check_arango_access() -> bool:
    """Verify the ArangoDB credentials are valid."""
    try:
        db = get_arango_db("dev")
        db.collections()
    except Exception as exc:
        print(f"ArangoDB access FAILED: {exc}")
        return False
    print("ArangoDB access OK")
    return True


def setup_postgis_tables() -> None:
    """Ensure all managed PostGIS tables exist."""
    conn = get_postgis_connection("prd")
    try:
        ensure_tables(conn)
    finally:
        conn.close()


def fill_database() -> None:
    """Run every configured syncer."""
    syncers = [
        AgentsSyncer(),
        AssettypesSyncer(),
        BeheerdersSyncer(),
        BestekkenSyncer(),
        IdentiteitenSyncer(),
        RelatietypesSyncer(),
        ToezichtgroepenSyncer(),
        AssetsSyncer(limit=None),
        AssetrelatiesSyncer(),
    ]
    for syncer in syncers:
        syncer.run()


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
