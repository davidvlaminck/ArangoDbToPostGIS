"""Tests for the BaseSyncer class."""

from __future__ import annotations

from unittest import mock

from arangodbtopostgis.syncer import BaseSyncer


class _DummySyncer(BaseSyncer):
    ARANGO_COLLECTION = "dummy"
    POSTGIS_TABLE = "public.dummy"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "actief": "actief",
    }


def test_field_map_defaults_applied() -> None:
    syncer = _DummySyncer()
    row = syncer._transform({"uuid": "abc", "naam": "Test"})
    assert row == {"uuid": "abc", "naam": "Test", "actief": True}


def test_field_map_keeps_existing_actief() -> None:
    syncer = _DummySyncer()
    row = syncer._transform({"uuid": "abc", "naam": "Test", "actief": False})
    assert row["actief"] is False


def test_is_valid_requires_uuid() -> None:
    syncer = _DummySyncer()
    assert syncer._is_valid({"uuid": "abc", "naam": "Test", "actief": True})
    assert not syncer._is_valid({"uuid": None, "naam": "Test", "actief": True})
    assert not syncer._is_valid({"uuid": "", "naam": "Test", "actief": True})


def test_fetch_skips_rows_with_empty_uuid() -> None:
    syncer = _DummySyncer()
    fake_collection = mock.Mock()
    fake_collection.all.return_value = iter(
        [
            {"uuid": "valid-1", "naam": "ok"},
            {"uuid": "", "naam": "empty-string"},
            {"uuid": None, "naam": "none"},
        ]
    )
    fake_db = mock.Mock()
    fake_db.collection.return_value = fake_collection
    with mock.patch("arangodbtopostgis.syncer.get_arango_db", return_value=fake_db):
        rows = syncer.fetch()
    assert len(rows) == 1
    assert rows[0]["uuid"] == "valid-1"


def test_run_calls_fetch_and_upsert() -> None:
    syncer = _DummySyncer()
    with (
        mock.patch.object(syncer, "fetch", return_value=[{"uuid": "x"}]) as mock_fetch,
        mock.patch.object(syncer, "upsert", return_value=1) as mock_upsert,
    ):
        result = syncer.run()
    mock_fetch.assert_called_once()
    mock_upsert.assert_called_once_with([{"uuid": "x"}])
    assert result == 1


def test_upsert_no_rows() -> None:
    syncer = _DummySyncer()
    with mock.patch("arangodbtopostgis.syncer.get_postgis_connection", side_effect=AssertionError):
        assert syncer.upsert([]) == 0


def test_required_columns_skip_invalid_rows() -> None:
    class _StrictSyncer(_DummySyncer):
        REQUIRED_COLUMNS = ("uuid", "naam")

    syncer = _StrictSyncer()
    docs = [
        {"uuid": "1", "naam": "ok"},
        {"uuid": "2", "naam": None},
        {"uuid": None, "naam": "no"},
    ]
    rows = [syncer._transform(d) for d in docs]
    valid = [r for r in rows if syncer._is_valid(r)]
    assert len(valid) == 1
    assert valid[0]["uuid"] == "1"
