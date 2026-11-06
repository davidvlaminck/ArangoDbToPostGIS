# ArangoDbToPostGIS

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
├── sync_agents.py
├── sync_assettypes.py
├── sync_beheerders.py
├── sync_bestekken.py
├── sync_identiteiten.py
├── sync_relatietypes.py
└── sync_toezichtgroepen.py
sql/postgis_tables.sql
tests/                 # pytest unit tests
pyproject.toml         # Dependencies + ruff/pytest config
```

Each `sync_*.py` module defines a `*Syncer` subclass of `BaseSyncer` with a
`FIELD_MAP` (Arango field → PostGIS column). Subclasses may override
`_transform()` for custom field extraction.

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

---

## 1. Tables / Collections

### PostGIS — 24 user tables (+ `spatial_ref_sys` system table)

`agents`, `assetrelaties`, `assets`, `assettypes`, `attributen`, `attribuutkoppelingen`, `attribuutwaarden`, `beheerders`, `bestekken`, `bestekkoppelingen`, `betrokkenerelaties`, `elek_aansluitingen`, `gemeente`, `geometrie`, `identiteiten`, `locatie`, `params`, `regex`, `relatietypes`, `toezichtgroepen`, `vplan_koppelingen`, `weglocatie_aanduidingen`, `weglocatie_wegsegmenten`, `weglocaties`

System: `spatial_ref_sys` (PostGIS)

### ArangoDB — 20 collections (+ 8 `_*` system collections)

**Document (10):**
`agents`, `assets`, `assettypes`, `beheerders`, `bestekken`, `identiteiten`, `params`, `relatietypes`, `toezichtgroepen`, `aansluitingrefs`

**Edge (10):**
`aansluitingen`, `assetrelaties`, `bestekkoppelingen`, `betrokkenerelaties`, `bevestiging_relaties`, `heeftkeuring_relaties`, `hoortbij_relaties`, `sturing_relaties`, `voedt_relaties`, `gemigreerdnaar_relaties`

**Graphs** (edge definitions live here, not per-collection):
`assetrelaties_graph`, `voedt_graph`, `sturing_graph`, `bevestiging_graph`, `hoortbij_graph`, `heeftkeuring_graph`, `gemigreerdnaar_graph`, `betrokkenerelaties_graph`, `bestekkoppelingen_graph`, `aansluitingen_graph`

System collections: `_graphs`, `_appbundles`, `_frontend`, `_analyzers`, `_aqlfunctions`, `_queues`, `_jobs`, `_apps`

---

## 2. Common / matching names

### Direct name matches (PostGIS table ↔ Arango collection)

| PostGIS table | Arango collection | Arango type | Done |
|---------------|-------------------|-------------|---------|
| `agents` | `agents` | document | [x] |
| `assets` | `assets` | document | [ ] |
| `assettypes` | `assettypes` | document | [x] |
| `beheerders` | `beheerders` | document | [x] |
| `bestekken` | `bestekken` | document | [x] |
| `identiteiten` | `identiteiten` | document | [x] |
| `params` | `params` | document | [ ] |
| `relatietypes` | `relatietypes` | document | [x] |
| `toezichtgroepen` | `toezichtgroepen` | document | [x] |
| `assetrelaties` | `assetrelaties` | edge | [ ] |
| `bestekkoppelingen` | `bestekkoppelingen` | edge | [ ] |
| `betrokkenerelaties` | `betrokkenerelaties` | edge | [ ] |

afgeleide graphs

| PostGIS table | Arango collection | Arango type | Done |
|---------------|-------------------|-------------|---------|
| `bevestiging_relaties` | `bevestiging_relaties` | edge | [ ] |
| `heeftkeuring_relaties` | `heeftkeuring_relaties` | edge | [ ] |
| `hoortbij_relaties` | `hoortbij_relaties` | edge | [ ] |
| `sturing_relaties` | `sturing_relaties` | edge | [ ] |
| `voedt_relaties` | `voedt_relaties` | edge | [ ] |

### Near matches
- `elek_aansluitingen` (PG) ↔ `aansluitingen` + `aansluitingrefs` (Arango)
- `geometrie` (PG table) ↔ `geo` / `wkt` / `geometry` fields on Arango `assets`
- `gemigreerdnaar_relaties` (Arango edge) has no direct PG table counterpart

### PostGIS-only tables
`attributen`, `attribuutkoppelingen`, `attribuutwaarden`, `gemeente`, `geometrie`, `locatie`, `regex`, `vplan_koppelingen`, `weglocatie_aanduidingen`, `weglocatie_wegsegmenten`, `weglocaties`, `aantallen`

### Arango-only collections
`aansluitingrefs` (document), plus all 8 `_*` system collections

---

## 3. Common fields (how to join / link across DBs)

| Field | PostGIS type | Arango field | Notes |
|-------|-------------|--------------|-------|
| `uuid` | `uuid` (PK) | `uuid` | Primary identifier on both sides |
| `naam` | `text` | `naam` | Name, on most document tables/collections |
| `actief` | `boolean` | `actief` | Active flag |
| `type` | `typebeheerder`/`typeidentiteit`/`typegroep` | `type` | Type categorization |
| `relatietype` | `uuid` | `relatietype` / `relatietype_key` | Relationship type reference |
| `attributen` | `json` | `attributen` | JSON attributes |
| geometry | `geometry`/`USER-DEFINED` | `geo`, `wkt`, `geometry` | Spatial |

### Edge linking
Arango edge documents reference connected docs via `_from=<coll>/<key>` and `_to=<coll>/<key>` (e.g. `assets/<uuid>` → `bestekken/<uuid>`).

In PostGIS the equivalent link columns are `bronuuid` → `doeluuid` on the `*_relaties`, `betrokkenerelaties`, and `bestekkoppelingen` tables.

Example edge targets seen: `assets/*`, `bestekken/*`, `agents/*` — confirming edges connect the same core document entities that exist as PostGIS tables.
