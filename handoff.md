# Handoff: AWVInfraPostGISSyncer → ArangoDbToPostGIS

## Purpose

Both repos build the same PostGIS schema from different sources:
- **AWVInfraPostGISSyncer** (this repo) — syncs from AWVInfra (Flanders) REST API. Production-hardened.
- **ArangoDbToPostGIS** (`/home/davidlinux/PycharmProjects/ArangoDbToPostGIS`) — copies from ArangoDB. Newer, still building out.

The `attribuutwaarden` table is already gone in both, replaced by `jsonb` `attributes` columns — that change is aligned.

This document lists optimizations proven in AWVInfraPostGISSyncer that should be ported to ArangoDbToPostGIS, ordered by impact.

---

## 1. Connection pooling (highest impact)

**Where in AWVInfraPostGISSyncer:** `PostGISConnector.py` — `ThreadedConnectionPool(minconn=5, maxconn=20)` with dead-connection detection (`connection.closed` checks), 3-attempt health-check retry before returning a connection, rollback + `autocommit=False` enforcement on checkout/checkin, and a `main_connection` property with the same recovery logic.

**Where in ArangoDbToPostGIS:** `arangodbtopostgis/db.py` — `get_postgis_connection()` opens a fresh `psycopg.connect(...)` and every syncer closes it after each batch. One connection open/close per batch is the single largest inefficiency at scale.

**Action:** Add a pool-backed connector to `arangodbtopostgis/db.py` (mirroring `PostGISConnector`) and reuse it across syncers. Stop opening/closing connections per batch.

---

## 2. Bulk `execute_values` instead of per-row `execute` + savepoints

**Where in AWVInfraPostGISSyncer:** `AssetUpdater.py` — `perform_insert_update_from_values` uses `psycopg2.extras.execute_values` with a `WITH s AS (VALUES ...)` CTE that joins `assettypes` on URI server-side, splits insert vs update via `to_insert`/`to_update` CTEs, and handles `NotNullViolation` → `AssetTypeMissingError`. Thousands of rows in a single round-trip.

**Where in ArangoDbToPostGIS:** `sync_assets.py`, `sync_betrokkenerelaties.py`, `sync_assetrelaties.py` — per-row `cur.execute(sql, params)` wrapped in `SAVEPOINT`/`ROLLBACK TO SAVEPOINT`. One savepoint per row is orders of magnitude slower than `execute_values`.

**Action:** Replace per-row insert loops with `psycopg.extras.execute_values` bulk inserts. Keep savepoint-per-row only if true row-level error isolation is required (rarely worth the cost). Note: ArangoDbToPostGIS uses `psycopg` (v3), where `execute_values` lives in `psycopg.extras`.

---

## 3. Centralized constants module

**Where in AWVInfraPostGISSyncer:** `constants.py` — all magic strings in one module: environment base URLs (prd/tei/dev/aim), Kenmerk UUIDs, pause window defaults, dummy/special UUIDs, URL prefixes.

**Where in ArangoDbToPostGIS:** `EXCLUDED_ATTRIBUTE_KEYS` sets, `NS_COLUMNS`, and config values are scattered across `sync_assets.py`, `sync_betrokkenerelaties.py`, etc.

**Action:** Create `arangodbtopostgis/constants.py` and move:
- `EXCLUDED_ATTRIBUTE_KEYS` sets
- `NS_COLUMNS`
- Any hardcoded UUIDs/URIs
- Batch sizes / default limits

---

## 4. Strongly-typed params table with timezone normalization

**Where in AWVInfraPostGISSyncer:** `PostGISConnector.py` — `param_type_map` maps every param key to a type (`bool`/`int`/`text`/`timestamp`); `add_params_entry` normalizes timestamps to Brussels TZ on read. The `public.params` table is the single source of truth for sync state.

**Where in ArangoDbToPostGIS:** `arangodbtopostgis/resume.py` already uses `public.params` for cursors but without a type map or TZ handling.

**Action:** Add a `PARAM_TYPE_MAP` dict to `resume.py` (or a new `state.py`) mirroring AWVInfraPostGISSyncer's approach, and normalize timestamps on read.

---

## 5. Database views for query optimization

**Where in AWVInfraPostGISSyncer:** `setup_tables_querys.sql` creates:
- `aantallen` — row count view across all tables
- `assets_geometry` — coalesced geometry from `locatie` + `geometrie`
- `bevestiging_relaties`, `sturing_relaties`, `hoortbij_relaties`, `voedt_relaties`, `ligtop_relaties` — relation-type filtered views
- Schemas `asset_views` and `asset_daily_views` for live/materialized snapshots

**Where in ArangoDbToPostGIS:** `sql/postgis_tables.sql` has **no views**.

**Action:** Add equivalent views to `sql/postgis_tables.sql`, especially `aantallen` and relation-type filtered views.

---

## 6. Comprehensive indexing

**Where in AWVInfraPostGISSyncer:** `setup_tables_querys.sql` — indexes on FK columns and filtered columns:
- `assets(assettype)`, `assets(actief)`
- `attribuutKoppelingen(assettypeUuid)`, `attribuutKoppelingen(attribuutUuid)`
- `attribuutWaarden(assetUuid)`, `attribuutWaarden(attribuutUuid)`, composite `(assetUuid, attribuutUuid)`
- `betrokkeneRelaties(doelUuid)`, `betrokkeneRelaties(bronUuid)`
- `assetRelaties(bronUuid)`, `assetRelaties(doelUuid)`, `assetRelaties(relatietype)`
- GIST indexes on geometry columns

**Where in ArangoDbToPostGIS:** `sql/postgis_tables.sql` has some indexes but is missing several FK and filtered-column indexes.

**Action:** Audit `sql/postgis_tables.sql` against AWVInfraPostGISSyncer's indexes and add missing ones, especially on `relatietype_uuid`, `bron_*_uuid`, `doel_*_uuid`, and `actief`.

---

## 7. `NOT VALID` foreign keys (already aligned)

Both repos use `NOT VALID` FKs so data loads aren't blocked by constraint validation. No action needed; keep the pattern.

---

## 8. Structured error handling with retry + backoff

**Where in AWVInfraPostGISSyncer:** `SyncManager.py` and `FillManager.py` — `ConnectionError` → rollback, sleep 30–60s, retry; `psycopg2.OperationalError` → count errors, exit after max threshold; fill reset on missing dependencies (`FillResetError`).

**Where in ArangoDbToPostGIS:** `main.py`/`fill_database()` has basic try/except but no retry budget, no backoff, no max-error threshold.

**Action:** Add a retry-with-backoff wrapper and max-consecutive-error threshold in `main.py`/`fill_database()`.

---

## Summary table

| Area | AWVInfraPostGISSyncer | ArangoDbToPostGIS | Priority |
|------|----------------------|-------------------|----------|
| Connection pool | `ThreadedConnectionPool` + health checks | Open/close per op | High |
| Bulk insert | `execute_values` + CTE | Per-row `execute` + savepoints | High |
| Constants | `constants.py` | Scattered hardcoded | Medium |
| Params typing | `param_type_map` + Brussels TZ | Simple key/value | Medium |
| Views | `aantallen`, geometry, rel-type views | None | Medium |
| Indexes | Comprehensive FK + filtered | Partial | Medium |
| Error handling | Retry + backoff + max errors | Basic try/except | Medium |
| Fill vs Sync | Separate fill + incremental event sync | Full reload only | Low (architectural) |

---

## Suggested order of work

1. **Connection pooling** in `arangodbtopostgis/db.py` — affects every syncer, biggest perf gain
2. **Bulk `execute_values`** in `sync_assets.py`, `sync_betrokkenerelaties.py`, `sync_assetrelaties.py`, `sync_bestekkoppelingen.py`
3. **`constants.py`** — move `EXCLUDED_ATTRIBUTE_KEYS`, `NS_COLUMNS`, etc.
4. **Views + indexes** in `sql/postgis_tables.sql`
5. **Params type map** in `resume.py`
6. **Retry/backoff** in `main.py`

---

## Key file references

**AWVInfraPostGISSyncer (this repo):**
- `PostGISConnector.py` — connection pool, param_type_map, timezone normalization
- `AssetUpdater.py` — execute_values bulk upsert pattern
- `constants.py` — centralized magic strings/UUIDs/URLs
- `setup_tables_querys.sql` — table DDL, indexes, views, NOT VALID FKs
- `SyncManager.py` — retry/backoff, staggered syncer start
- `FillManager.py` — fill/reset error handling

**ArangoDbToPostGIS (target repo):**
- `arangodbtopostgis/db.py` — connection helpers (target for pooling)
- `arangodbtopostgis/resume.py` — params-based checkpoints (target for type map)
- `arangodbtopostgis/sync_assets.py` — per-row inserts (target for execute_values)
- `arangodbtopostgis/sync_betrokkenerelaties.py` — per-row inserts (target for execute_values)
- `arangodbtopostgis/sync_assetrelaties.py` — per-row inserts (target for execute_values)
- `arangodbtopostgis/sync_bestekkoppelingen.py` — per-row deletes+inserts (target for execute_values)
- `arangodbtopostgis/main.py` — entry point (target for retry/backoff)
- `sql/postgis_tables.sql` — schema DDL (target for views + indexes)
- `pyproject.toml` — uses `psycopg` (v3), not `psycopg2`; `execute_values` lives in `psycopg.extras`
