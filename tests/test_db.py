"""Tests for the database connection helpers."""

from __future__ import annotations

from unittest import mock

import psycopg
from arango import ArangoClient

from arangodbtopostgis import db


def test_get_arango_client_uses_creds() -> None:
    with mock.patch.object(db, "arango_creds", return_value={"hosts": "http://h:8529"}) as m:
        client = db.get_arango_client("dev")
    m.assert_called_once_with("dev")
    assert isinstance(client, ArangoClient)


def test_get_arango_db_authenticates() -> None:
    fake_db = mock.Mock()
    fake_client = mock.Mock()
    fake_client.db.return_value = fake_db
    with (
        mock.patch.object(db, "get_arango_client", return_value=fake_client),
        mock.patch.object(
            db,
            "arango_creds",
            return_value={"database": "infra_db", "username": "u", "password": "p"},
        ),
    ):
        result = db.get_arango_db("dev")
    fake_client.db.assert_called_once_with("infra_db", username="u", password="p")
    assert result is fake_db


def test_get_postgis_connection_uses_creds() -> None:
    fake_conn = mock.Mock(spec=psycopg.Connection)
    with (
        mock.patch.object(
            db,
            "postgis_creds",
            return_value={"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
        ),
        mock.patch.object(db.psycopg, "connect", return_value=fake_conn) as mock_connect,
    ):
        result = db.get_postgis_connection("prd")
    mock_connect.assert_called_once_with(host="h", port=5432, dbname="d", user="u", password="p")
    assert result is fake_conn


def test_execute_insert_empty_rows() -> None:
    conn = mock.MagicMock()
    assert db.execute_insert(conn, "public.t", {"a": "a"}, []) == 0
    conn.commit.assert_not_called()


def test_execute_insert_builds_sql() -> None:
    conn = mock.MagicMock()
    cursor = mock.Mock()
    conn.cursor.return_value.__enter__.return_value = cursor

    rows = [{"uuid": "1", "naam": "a"}, {"uuid": "2", "naam": "b"}]
    count = db.execute_insert(conn, "public.t", {"uuid": "uuid", "naam": "naam"}, rows)

    assert count == 2
    # executemany called with the SQL and parameter list
    assert cursor.executemany.call_count == 1
    sql = cursor.executemany.call_args[0][0]
    params = cursor.executemany.call_args[0][1]
    assert "INSERT INTO" in sql
    assert "ON CONFLICT" not in sql
    assert params == [["1", "a"], ["2", "b"]]
    conn.commit.assert_called_once()


def test_execute_insert_sanitizes_empty_strings() -> None:
    conn = mock.MagicMock()
    cursor = mock.Mock()
    conn.cursor.return_value.__enter__.return_value = cursor

    rows = [{"uuid": "", "naam": ""}]
    db.execute_insert(conn, "public.t", {"uuid": "uuid", "naam": "naam"}, rows)

    params = cursor.executemany.call_args[0][1]
    assert params == [[None, None]]


def test_execute_insert_batching() -> None:
    conn = mock.MagicMock()
    cursor = mock.Mock()
    conn.cursor.return_value.__enter__.return_value = cursor

    rows = [{"uuid": str(i), "naam": f"n{i}"} for i in range(2500)]
    db.execute_insert(conn, "public.t", {"uuid": "uuid", "naam": "naam"}, rows, batch_size=1000)

    assert cursor.executemany.call_count == 3  # 1000 + 1000 + 500
