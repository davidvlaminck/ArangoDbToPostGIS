# Working tree changes vs last committed master (`main` HEAD)

Generated: 2026-11-19

**Branch:** `main`
**State:** 14 modified tracked files + 11 untracked files (all uncommitted)
**Scope:** 1619 insertions, 897 deletions across tracked source files

## Summary

A coworker copied a newer version of the pipeline over the working tree, touching nearly every core module. The changes are **uncommitted**. The diff centers on four themes:

1. **New relation-table syncers** (`bestekkoppelingen`, `assetrelaties`, `betrokkenerelaties`) — bulk `COPY` inserts with checkpointing, replacing the old per-table `export_*` functions.
2. **Bulk-optimized schema** — FK constraints and secondary indexes moved out of `sql/postgis_tables.sql` into `setup_postgis_tables.py`, temporarily dropped before bulk relation loads and restored afterward.
3. **Resilient AQL cursor paging** — new `aql_cursor.py` module with page restart, heartbeat logging, and bounded retry on expired cursors.
4. **Default environment switched `prd` → `dev`** across `main.py`, `db.py`, `syncer.py`, `config.py` (plus `tqdm` removed from deps).
5. **New tests** covering cursor paging, relation-sync orchestration, schema deferral/drop/restore, and bulk inserts.

---

## File-by-file breakdown

### Modified tracked files

| File | Change | Notes |
|------|--------|-------|
| `arangodbtopostgis/main.py` | Rewired pipeline | Removed `--env` CLI arg; defaults to `dev` for Arango checks, `prd` for PostGIS. Replaced `EXPORTERS` dict + `mark_step_started` with a syncer list + `prepare_step`. Added `RELATION_STEPS` grouping (`bestekkoppelingen`, `assetrelaties`, `betrokkenerelaties`) that runs inside a single drop/restore window for relation FKs/indexes. |
| `arangodbtopostgis/sync_assets.py` | Bulk geometry insert + resumable paging | `_insert_geometrie` now does one bulk `INSERT` with fallback to per-row. `run_resumable` pages Arango results via `iter_aql_page` with checkpoint cursors. |
| `arangodbtopostgis/sync_assetrelaties.py` | COPY-based batch insert | `_insert_batch_and_checkpoint` uses `cursor.copy` with per-row `execute` fallback on failure. |
| `arangodbtopostgis/sync_bestekkoppelingen.py` | Relation sync converted | Uses `BaseSyncer`/`run_resumable` pattern with checkpointing. |
| `arangodbtopostgis/sync_betrokkenerelaties.py` | Relation sync converted | Same pattern as above. |
| `arangodbtopostgis/syncer.py` | Default `arango_env` → `dev` | `BaseSyncer.__init__` default changed from `prd` to `dev`. |
| `arangodbtopostgis/config.py` | Default env → `dev` | `arango_creds` default param changed `prd` → `dev`. |
| `arangodbtopostgis/db.py` | Arango request timeout | `get_arango_client` now passes `request_timeout=1800`. Default env → `dev`. |
| `arangodbtopostgis/setup_postgis_tables.py` | Deferred relation DDL | Added `RELATION_INDEX_DDL`, `RELATION_CONSTRAINTS`, `RELATION_SECONDARY_INDEXES`, plus `drop_relation_constraints_and_indexes` / `restore_relation_constraints_and_indexes`. `ensure_tables` gained `defer_relation_indexes` kwarg. |
| `sql/postgis_tables.sql` | Stripped relation FKs/indexes/views | Removed relation-table FK constraints, secondary indexes, and several materialized views (`assets_geometry`, `bevestiging_relaties`, etc.). `aantallen` changed from view to `MATERIALIZED VIEW IF NOT EXISTS`. Relation-type views switched from `relatietype_uuid` exact match to `type_uri ILIKE`. |
| `.gitignore` | Removed credentials ignore | Deleted the `config/*.json` ignore block (so real credential JSON is no longer ignored — see risk below). |
| `pyproject.toml` | Dropped `tqdm` | Removed `tqdm` dependency and its mypy override. |
| `uv.lock` | Lockfile regenerated | Reflects `tqdm` removal. |
| `README.md` | Updated docs | Expanded tables/collections listing and match tables. |

### Untracked (new) files

| File | Purpose |
|------|---------|
| `arangodbtopostgis/aql_cursor.py` | Resilient bounded AQL page iterator with cursor-expiry retry, heartbeat logging, and restart-from-last-key. |
| `tests/test_aql_cursor.py` | Tests for cursor paging and expiry retry. |
| `tests/test_main.py` | Tests for relation-group drop/restore window orchestration. |
| `tests/test_setup_postgis_tables.py` | Tests for relation DDL deferral, drop, and restore. |
| `tests/test_sync_assetrelaties.py` | Tests for COPY batch insert + fallback. |
| `tests/test_sync_assets.py` | Tests for bulk geometry insert and resumable paging. |
| `handoff.md` | Coworker's handoff notes from AWVInfraPostGISSyncer with porting recommendations (pooling, `execute_values`, constants, params typing, views, indexes, retry/backoff). |

---

## Risky / noteworthy items to review before committing

1. **`.gitignore` — credentials no longer ignored**
   The coworker removed:
   ```
   # Credentials (local only — real JSON files must not be committed)
   config/*.json
   !config/*.example.json
   ```
   Per `AGENTS.md`, real credential JSON belongs at `../config/` (outside the repo) and must never be committed. Re-add this ignore block before committing.

2. **Default environment switched to `dev`**
   `main.py`, `db.py`, `syncer.py`, `config.py` all default to `dev` instead of `prd`. Confirm this is intentional — running the pipeline against production data will now use dev credentials unless `--env` (removed) or an explicit env arg is passed.

3. **`--env` CLI flag removed from `main.py`**
   `fill_database()` and `setup_postgis_tables()` no longer accept an `env` parameter; both hardcode `"prd"` for PostGIS and `"dev"` for Arango internally. Any external callers passing `--env` will break.

4. **`sql/postgis_tables.sql` lost relation FKs, indexes, and views**
   DDL for relation constraints/indexes now lives only in `setup_postgis_tables.py` (applied at runtime). If you recreate the DB from SQL alone, relation FKs/secondary indexes and the `assets_geometry` / relation-type views will be missing.

---

## Practices to adapt (from `handoff.md`, ordered by impact)

The coworker included `handoff.md` documenting optimizations proven in the sibling repo `AWVInfraPostGISSyncer`. Recommended adoption order:

| # | Practice | Where to apply | Priority |
|---|----------|----------------|----------|
| 1 | **Connection pooling** | `arangodbtopostgis/db.py` — replace open/close-per-batch with a `ThreadedConnectionPool` (minconn=5, maxconn=20) with dead-connection detection and health-check retry | High |
| 2 | **Bulk `execute_values` inserts** | `sync_assets.py`, `sync_assetrelaties.py`, `sync_betrokkenerelaties.py`, `sync_bestekkoppelingen.py` — replace per-row `execute` + savepoints with `psycopg.extras.execute_values` | High |
| 3 | **Centralized constants module** | New `arangodbtopostgis/constants.py` — move `EXCLUDED_ATTRIBUTE_KEYS`, `NS_COLUMNS`, hardcoded UUIDs/URIs, batch sizes | Medium |
| 4 | **Strongly-typed params + TZ normalization** | `arangodbtopostgis/resume.py` — add `PARAM_TYPE_MAP` and normalize timestamps to Brussels TZ on read | Medium |
| 5 | **Database views** | `sql/postgis_tables.sql` — add `aantallen`, `assets_geometry`, and relation-type filtered views | Medium |
| 6 | **Comprehensive indexing** | `sql/postgis_tables.sql` — audit against AWVInfra indexes; add missing FK/filtered-column indexes (`relatietype_uuid`, `bron_*_uuid`, `doel_*_uuid`, `actief`) | Medium |
| 7 | **Retry + backoff** | `main.py` / `fill_database()` — add retry-with-backoff wrapper and max-consecutive-error threshold | Medium |
| 8 | **Fill vs Sync split** | Architectural — separate full fill from incremental event sync | Low |

## Suggested next steps

1. Re-add the `config/*.json` ignore to `.gitignore`.
2. Run `uv run pytest`, `uv run ruff check`, `uv run ruff format --check`, and `uv run mypy` to validate the new code.
3. Decide whether the `prd` → `dev` default and `--env` removal are intentional.
4. Adopt the handoff practices in the order above, starting with connection pooling and bulk inserts.
5. Commit once reviewed.
