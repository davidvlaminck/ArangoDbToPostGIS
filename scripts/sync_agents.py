import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import psycopg
from arango import ArangoClient
from read_creds import arango_creds, postgis_creds

ARANGO_COLLECTION = "agents"
POSTGIS_TABLE = "public.agents"


def fetch_agents_arango() -> list[dict]:
    creds = arango_creds("dev")
    client = ArangoClient(hosts=creds["hosts"])
    db = client.db(
        creds["database"],
        username=creds["username"],
        password=creds["password"],
    )
    collection = db.collection(ARANGO_COLLECTION)
    docs = []
    for doc in collection.all():
        purl = doc.get("purl") or {}
        tz = doc.get("tz") or {}
        rows = {
            "uuid": doc.get("uuid"),
            "naam": purl.get("Agent_naam"),
            "void": tz.get("Agent_voId") or None,
            "actief": doc.get("AIMDBStatus_isActief"),
            "contact_info": json.dumps(purl) if purl else None,
            "ovo_code": tz.get("Agent_ovoCode"),
        }
        # actief has a NOT NULL constraint; default if missing.
        if rows["actief"] is None:
            rows["actief"] = True
        # Skip docs missing required NOT NULL fields.
        if not rows["uuid"]:
            continue
        docs.append(rows)
    print(f"Fetched {len(docs)} agents from ArangoDB")
    return docs


def upsert_agents_postgis(docs: list[dict]) -> None:
    if not docs:
        print("No agents to insert.")
        return

    creds = postgis_creds("prd")
    columns = ["uuid", "naam", "void", "actief", "contact_info", "ovo_code"]
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != "uuid"])

    sql = f"""
        INSERT INTO {POSTGIS_TABLE} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (uuid) DO UPDATE
        SET {update_set}
    """

    with psycopg.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["user"],
        password=creds["password"],
    ) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, [[doc[c] for c in columns] for doc in docs])
        conn.commit()
    print(f"Upserted {len(docs)} agents into PostGIS {POSTGIS_TABLE}")


def main() -> None:
    docs = fetch_agents_arango()
    upsert_agents_postgis(docs)


if __name__ == "__main__":
    main()
