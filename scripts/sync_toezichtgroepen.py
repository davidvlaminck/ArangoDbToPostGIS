import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import psycopg
from arango import ArangoClient
from read_creds import arango_creds, postgis_creds

ARANGO_COLLECTION = "toezichtgroepen"
POSTGIS_TABLE = "public.toezichtgroepen"

# Only map fields that exist in the PostGIS table; extra Arango fields are ignored.
FIELD_MAP = {
    "uuid": "uuid",
    "naam": "naam",
    "type": "typeGroep",
    "omschrijving": "referentie",
    "actief": "actief",
    # actiefInterval, contactFiche -> extra Arango data -> ignored
}


def fetch_toezichtgroepen_arango() -> list[dict]:
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
        rows = {}
        for arango_field, pg_column in FIELD_MAP.items():
            rows[pg_column] = doc.get(arango_field)
        docs.append(rows)
    print(f"Fetched {len(docs)} toezichtgroepen from ArangoDB")
    return docs


def upsert_toezichtgroepen_postgis(docs: list[dict]) -> None:
    if not docs:
        print("No toezichtgroepen to insert.")
        return

    creds = postgis_creds("prd")
    columns = list(FIELD_MAP.values())
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
    print(f"Upserted {len(docs)} toezichtgroepen into PostGIS {POSTGIS_TABLE}")


def main() -> None:
    docs = fetch_toezichtgroepen_arango()
    upsert_toezichtgroepen_postgis(docs)


if __name__ == "__main__":
    main()
