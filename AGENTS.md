# Agent guidance — ArangoDbToPostGIS

Changed: 2026-11-09 — example config files live in the repo; real credential JSON goes outside the repo.

## Purpose
Copy data from ArangoDB to PostGIS. One package, one pipeline.

## Layout
- `arangodbtopostgis/` — package
  - `main.py` — entry point: access checks → table setup → run all syncers
  - `config.py` — loads DB credentials from JSON files in `../config/` (outside repo)
  - `db.py` — Arango/Postgres connections + `execute_insert` (bulk insert)
  - `syncer.py` — `BaseSyncer`: generic Arango collection → PostGIS table sync (`FIELD_MAP`, `_transform`)
  - `resume.py` — resume/checkpoints via `public.params`; `STEP_ORDER` defines pipeline order
  - `setup_postgis_tables.py` — ensures managed tables exist (sentinel = `params`)
  - `sync_parallel.py` — parallel key-range chunked COPY export (used by large tables)
  - `sync_*.py` — per-table exporters/syncers
- `config/` — example credential JSON files (`*.example.json`); real JSON goes at `../config/`
- `sql/postgis_tables.sql` — DDL for all managed tables
- `tests/` — pytest unit tests

## Credentials (humans: configure here, no code change)
Example credential JSON files live in this repo under `config/` (`*.example.json`).
Copy them one level up, **outside** the repo (at `../config/`), rename to `*.json`,
then fill in host/user/password per environment (`dev`, `prd`, …). Real JSON is
git-ignored and never committed.

## Running
```bash
uv sync                          # install deps
python -m arangodbtopostgis      # full pipeline (env prd)
python -m arangodbtopostgis.sync_agents   # single table
```

## Adding a new table / syncer
1. Add DDL to `sql/postgis_tables.sql` and the table name to `MANAGED_TABLES` in `setup_postgis_tables.py`.
2. Add the step name to `STEP_ORDER` (+ `STEP_TABLES`) in `resume.py`.
3. Create `arangodbtopostgis/sync_<table>.py`:
   - Small tables: subclass `BaseSyncer` (set `ARANGO_COLLECTION`, `POSTGIS_TABLE`, `FIELD_MAP`, optional `_transform`).
   - Large tables: call `parallel_export()` from `sync_parallel.py` with `transform`, `columns`, `pg_table`.
4. Wire into `main.py` (`fill_database` `syncers_by_step` / `EXPORTERS`).
5. Add a test under `tests/`.

## Conventions
- Type hints on all public functions; docstrings on modules and classes.
- Lint: `uv run ruff check` ; `uv run ruff format --check`
- Typecheck: `uv run mypy`
- Tests: `uv run pytest`

## Where to look when something breaks
- Connection failures → `config.py` / `db.py`
- Missing tables → `setup_postgis_tables.py` / `sql/postgis_tables.sql`
- Resume/checkpoint state → `public.params` table; see `resume.py`
- A single table's transform logic → `arangodbtopostgis/sync_<table>.py`
