# ArangoDbToPostGIS

The idea of this repo is to copy the data from a ArangoDb to a PostGIS db.

## Database access

Credentials live in the `config` folder (one level up from this repo).
**Non-remote** config files only (`*_remote.json` excluded).

| Database | Config file | Environment |
|----------|-------------|-------------|
| ArangoDB | `config/settings_arangodb.json` | `dev` |
| PostGIS  | `config/settings_postgis.json`  | `prd` |

### Connection parameters (secrets read from config, not stored here)

**ArangoDB (dev)** — from `config/settings_arangodb.json` → `databases.dev`
- endpoint: `http://127.0.0.1:8529`
- database: `infra_db`
- user: `sync_user`
- password: read from config at runtime

**PostGIS (prd)** — from `config/settings_postgis.json` → `databases.prd`
- host: `127.0.0.1`
- port: `5432`
- database: `awvinfra`
- user: `awvinfrasync`
- password: read from config at runtime

### Credential reader script

`scripts/read_creds.py` — loads credentials from the config JSON files (no secrets hard-coded):

```python
import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def load_json(name: str) -> dict:
    with open(CONFIG_DIR / name) as f:
        return json.load(f)


def arango_creds(env: str = "dev") -> dict:
    cfg = load_json("settings_arangodb.json")
    db = cfg["databases"][env]
    return {
        "hosts": db.get("hosts", "http://127.0.0.1:8529"),
        "database": db["database"],
        "username": db["user"],
        "password": db["password"],
    }


def postgis_creds(env: str = "prd") -> dict:
    cfg = load_json("settings_postgis.json")
    db = cfg["databases"][env]
    return {
        "host": db["host"],
        "port": db["port"],
        "database": db["database"],
        "user": db["user"],
        "password": db["password"],
    }


if __name__ == "__main__":
    print("ArangoDB:", arango_creds())
    print("PostGIS :", postgis_creds())
```

Usage:
```bash
python3 scripts/read_creds.py
```

Access commands (read-only, passwords supplied via the script above):
- PostGIS: `PGPASSWORD=<from script> psql -h 127.0.0.1 -p 5432 -U awvinfrasync -d awvinfra -c "\dt"`
- ArangoDB: Python `arango` driver, `ArangoClient(hosts='http://127.0.0.1:8529').db('infra_db', username='sync_user', password=...)`

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
