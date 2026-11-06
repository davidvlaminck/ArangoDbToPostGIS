"""Entry point: check access, ensure tables, then run all syncers."""

from __future__ import annotations

import logging
import sys
from typing import Any

from arangodbtopostgis.db import get_arango_db, get_postgis_connection
from arangodbtopostgis.resume import RESUMABLE_STEPS, STEP_ORDER, get_param, prepare_step
from arangodbtopostgis.setup_postgis_tables import ensure_tables
from arangodbtopostgis.sync_agents import AgentsSyncer
from arangodbtopostgis.sync_assetrelaties import AssetrelatiesSyncer
from arangodbtopostgis.sync_assets import AssetsSyncer
from arangodbtopostgis.sync_assettypes import AssettypesSyncer
from arangodbtopostgis.sync_beheerders import BeheerdersSyncer
from arangodbtopostgis.sync_bestekken import BestekkenSyncer
from arangodbtopostgis.sync_bestekkoppelingen import BestekkoppelingenSyncer
from arangodbtopostgis.sync_betrokkenerelaties import BetrokkenerelatiesSyncer
from arangodbtopostgis.sync_identiteiten import IdentiteitenSyncer
from arangodbtopostgis.sync_relatietypes import RelatietypesSyncer
from arangodbtopostgis.sync_toezichtgroepen import ToezichtgroepenSyncer

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


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
                    logger.info("PostGIS access OK (enabled 'postgis' extension)")
                    return True
                except Exception as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    dbname = conn.info.dbname if conn.info else "awvinfra"
                    user = conn.info.user if conn.info else "unknown"
                    logger.warning("PostGIS access FAILED: PostGIS extension missing.")
                    logger.error(_postgis_install_instructions(dbname, user, exc))
                    return False
    except Exception as exc:
        logger.warning(f"PostGIS access FAILED: {exc}")
        return False
    finally:
        if conn is not None:
            conn.close()
    logger.info("PostGIS access OK (extension 'postgis' present)")
    return True


def check_arango_access() -> bool:
    """Verify the ArangoDB credentials are valid."""
    try:
        db = get_arango_db("dev")
        db.collections()
    except Exception as exc:
        logger.warning(f"ArangoDB access FAILED: {exc}")
        return False
    logger.info("ArangoDB access OK")
    return True


def setup_postgis_tables() -> None:
    """Ensure all managed PostGIS tables exist."""
    conn = get_postgis_connection("prd")
    try:
        ensure_tables(conn)
    finally:
        conn.close()


def fill_database() -> None:
    """Run every configured syncer with params-based resume/checkpoints."""
    syncers: list[Any] = [
        AgentsSyncer(),
        AssettypesSyncer(),
        BeheerdersSyncer(),
        BestekkenSyncer(),
        IdentiteitenSyncer(),
        RelatietypesSyncer(),
        ToezichtgroepenSyncer(),
        AssetsSyncer(limit=None),
        BestekkoppelingenSyncer(),
        AssetrelatiesSyncer(),
        BetrokkenerelatiesSyncer(),
    ]
    conn0 = get_postgis_connection("prd")
    try:
        param = get_param(conn0, "sync_current_step")
    finally:
        conn0.close()
    current = param.get("value_text") if param else None
    start = STEP_ORDER.index(current) if current in STEP_ORDER else 0
    syncers = syncers[start:]
    for syncer in syncers:
        step = getattr(syncer, "RESUME_STEP", syncer.ARANGO_COLLECTION)
        if step not in STEP_ORDER:
            raise ValueError(f"unknown resume step: {step!r}")
        conn = get_postgis_connection("prd")
        try:
            cursor = prepare_step(conn, step)
        finally:
            conn.close()
        if cursor == "__DONE__":
            continue
        if step in RESUMABLE_STEPS and hasattr(syncer, "run_resumable"):
            syncer.run_resumable(resume_cursor=cursor)
        else:
            syncer.run()


def main() -> None:
    _configure_logging()
    postgis_ok = check_postgis_access()
    arango_ok = check_arango_access()
    if not postgis_ok or not arango_ok:
        logger.warning("Aborting: database access check failed.")
        sys.exit(1)

    setup_postgis_tables()
    fill_database()


if __name__ == "__main__":
    main()
