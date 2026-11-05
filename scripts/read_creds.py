import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


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
