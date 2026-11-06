"""Entry point: check access, ensure tables, then run all syncers."""

from __future__ import annotations

import sys

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.setup_postgis_tables import ensure_tables
from arangodbtopostgis.sync_agents import AgentsSyncer
from arangodbtopostgis.sync_assettypes import AssettypesSyncer
from arangodbtopostgis.sync_beheerders import BeheerdersSyncer
from arangodbtopostgis.sync_bestekken import BestekkenSyncer
from arangodbtopostgis.sync_identiteiten import IdentiteitenSyncer
from arangodbtopostgis.sync_relatietypes import RelatietypesSyncer
from arangodbtopostgis.sync_toezichtgroepen import ToezichtgroepenSyncer


def check_postgis_access() -> bool:
    """Verify the PostGIS credentials are valid."""
    try:
        conn = get_postgis_connection("prd")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
    except Exception as exc:
        print(f"PostGIS access FAILED: {exc}")
        return False
    print("PostGIS access OK")
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
