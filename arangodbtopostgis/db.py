"""Database connection helpers."""

from __future__ import annotations

import logging

from typing import Any

import psycopg
from arango import ArangoClient

from arangodbtopostgis.config import arango_creds, postgis_creds

logger = logging.getLogger(__name__)


def get_arango_client(env: str = "prd") -> ArangoClient:
    """Create an :class:`ArangoClient` configured for the given environment."""
    creds = arango_creds(env)
    return ArangoClient(hosts=creds["hosts"])


def get_arango_db(env: str = "prd") -> Any:
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


def execute_insert(
    conn: psycopg.Connection,
    table: str,
    field_map: dict[str, str],
    rows: list[dict[str, Any]],
    *,
    pk: str = "uuid",
    batch_size: int = 1000,
) -> int:
    """Bulk insert rows into ``table`` using ``field_map`` (arango_field -> pg_column).

    Plain INSERT for full reloads into an empty DB. ``pk`` is accepted for API
    compatibility. Returns the number of rows inserted.
    """
    if not rows:
        return 0

    def qident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def qtable(name: str) -> str:
        return ".".join(qident(p) for p in name.split("."))

    columns = list(field_map.values())
    col_list = ", ".join(qident(col) for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sql = f"""
        INSERT INTO {qtable(table)} ({col_list})
        VALUES ({placeholders})
    """

    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            params = [[_sanitize(row.get(col)) for col in columns] for row in batch]
            cur.executemany(sql, params)
            total += len(batch)
    conn.commit()
    return total


# Backwards-compatible name; syncers use insert-only behavior for empty-DB reloads.
execute_upsert = execute_insert


def _sanitize(value: Any) -> Any:
    """Convert empty strings to ``None`` so PostGIS does not reject them.

    Arango sometimes stores ``""`` where a UUID or other typed column expects
    a real value; PostGIS raises ``InvalidTextRepresentation`` in that case.
    """
    if value == "":
        return None
    return value
