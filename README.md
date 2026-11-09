# ArangoDbToPostGIS

Changed: 2026-11-09 — example config files live in the repo; real credential JSON goes outside the repo; dropped stale Tables/Collections reference (now points to sql/postgis_tables.sql, setup_postgis_tables.py, resume.py).

The idea of this repo is to copy the data from a ArangoDb to a PostGIS db.

## Project layout

```
arangodbtopostgis/
├── __init__.py        # Package exports
├── __main__.py        # python -m arangodbtopostgis
├── config.py          # Credential loading from JSON config files
├── db.py              # Connection helpers + batched upsert utility
├── syncer.py          # BaseSyncer base class (generic Arango→PostGIS sync)
├── main.py            # Entry point: checks, table setup, runs all syncers
├── setup_postgis_tables.py  # Ensures PostGIS tables exist
├── resume.py          # Resume/checkpoints via public.params
├── sync_parallel.py   # Parallel chunked COPY export (large tables)
├── sync_agents.py
├── sync_assettypes.py
├── sync_assetrelaties.py
├── sync_beheerders.py
├── sync_bestekkoppelingen.py
├── sync_bestekken.py
├── sync_betrokkenerelaties.py
├── sync_identiteiten.py
├── sync_relatietypes.py
└── sync_toezichtgroepen.py
sql/postgis_tables.sql
tests/                 # pytest unit tests
pyproject.toml         # Dependencies + ruff/pytest config
```

Each `sync_*.py` module defines a `*Syncer` subclass of `BaseSyncer` with a
`FIELD_MAP` (Arango field → PostGIS column). Subclasses may override
`_transform()` for custom field extraction. Large tables (assets, edge tables)
use the parallel COPY machinery in `sync_parallel.py` instead of `BaseSyncer`.

## Configuration

Example credential JSON files live in this repo under `config/` (`*.example.json`).
Copy them one level up, **outside** the repo (at `../config/`), rename to `*.json`,
and fill in your values:

```bash
cp config/settings_arangodb.example.json ../config/settings_arangodb.json
cp config/settings_postgis.example.json ../config/settings_postgis.json
# edit ../config/*.json with your host/user/password per environment
```

Real `config/*.json` files are git-ignored and never committed.

Each file needs a top-level `databases.<env>` object (e.g. `dev`, `prd`).
Arango config keys: `hosts`, `database`, `user`, `password`.
PostGIS config keys: `host`, `port`, `database`, `user`, `password`.


## Running

```bash
# Install dependencies
uv sync

# Run the full pipeline
python -m arangodbtopostgis

# Run a single syncer
python -m arangodbtopostgis.sync_toezichtgroepen

# Run tests
uv run pytest
```

## Adding a new table / syncer

See `AGENTS.md` for the full procedure. In short:

1. Add DDL to `sql/postgis_tables.sql` and the table name to `MANAGED_TABLES` in `setup_postgis_tables.py`.
2. Add the step name to `STEP_ORDER` (+ `STEP_TABLES`) in `resume.py`.
3. Create `arangodbtopostgis/sync_<table>.py`:
   - Small tables: subclass `BaseSyncer` (set `ARANGO_COLLECTION`, `POSTGIS_TABLE`, `FIELD_MAP`).
   - Large tables: call `parallel_export()` from `sync_parallel.py`.
4. Wire into `main.py` (`fill_database` `syncers_by_step` / `EXPORTERS`).
5. Add a test under `tests/`.

---

## Authoritative sources

The README intentionally does not duplicate the table/collection inventory — that lives in three places and this doc is not kept in sync with them:

- `sql/postgis_tables.sql` — DDL for all managed PostGIS tables + views (`aantallen`, `assets_geometry`, `*_relaties` views).
- `arangodbtopostgis/setup_postgis_tables.py` (`MANAGED_TABLES`) — which tables the pipeline ensures exist.
- `arangodbtopostgis/resume.py` (`STEP_ORDER`, `STEP_TABLES`) — pipeline step order and resume state.

Key cross-DB join fields (Arango `_from`/`_to` → PostGIS `bronuuid` → `doeluuid` on `*_relaties`, `betrokkenerelaties`, `bestekkoppelingen`) are documented in `AGENTS.md`.
