"""Database connection helpers."""

from __future__ import annotations

from typing import Any

import psycopg
from arango import ArangoClient

from arangodbtopostgis.config import arango_creds, postgis_creds


def get_arango_client(env: str = "dev") -> ArangoClient:
    """Create an :class:`ArangoClient` configured for the given environment."""
    creds = arango_creds(env)
    return ArangoClient(hosts=creds["hosts"])


def get_arango_db(env: str = "dev"):
    """Return an authenticated Arango database connection."""
    creds = arango_creds(env)
    client = get_arango_client(env)
    return client.db(
        creds["database"],
        username=creds["username"],
        password=creds["password"],
    )


def get_postgis_connection(env: str = "prd") -> psycopg.Connection:
    """Open a PostGIS connection for the given environment."""
    creds = postgis_creds(env)
    return psycopg.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["user"],
        password=creds["password"],
    )


def execute_upsert(
    conn: psycopg.Connection,
    table: str,
    field_map: dict[str, str],
    rows: list[dict[str, Any]],
    *,
    pk: str = "uuid",
    batch_size: int = 1000,
) -> int:
    """Bulk upsert rows into ``table`` using ``field_map`` (arango_field -> pg_column).

    Returns the number of rows upserted.
    """
    if not rows:
        return 0

    columns = list(field_map.values())
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col != pk)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE
        SET {update_set}
    """

    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            params = [[row[col] for col in columns] for row in batch]
            cur.executemany(sql, params)
            total += len(batch)
    conn.commit()
    return total
