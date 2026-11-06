"""Tests for the credential loading utilities."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from arangodbtopostgis import config


def _write_config(tmp_path: Path, name: str, data: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(data))
    return path


def test_arango_creds_defaults(tmp_path: Path) -> None:
    data = {"databases": {"dev": {"database": "infra_db", "user": "u", "password": "p"}}}
    _write_config(tmp_path, "settings_arangodb.json", data)
    with mock.patch.object(config, "CONFIG_DIR", tmp_path):
        creds = config.arango_creds("dev")
    assert creds == {
        "hosts": "http://127.0.0.1:8529",
        "database": "infra_db",
        "username": "u",
        "password": "p",
    }


def test_arango_creds_custom_host(tmp_path: Path) -> None:
    data = {
        "databases": {
            "dev": {
                "hosts": "http://example:8529",
                "database": "infra_db",
                "user": "u",
                "password": "p",
            }
        }
    }
    _write_config(tmp_path, "settings_arangodb.json", data)
    with mock.patch.object(config, "CONFIG_DIR", tmp_path):
        creds = config.arango_creds("dev")
    assert creds["hosts"] == "http://example:8529"


def test_postgis_creds(tmp_path: Path) -> None:
    data = {
        "databases": {
            "prd": {
                "host": "h",
                "port": 5432,
                "database": "awvinfra",
                "user": "u",
                "password": "p",
            }
        }
    }
    _write_config(tmp_path, "settings_postgis.json", data)
    with mock.patch.object(config, "CONFIG_DIR", tmp_path):
        creds = config.postgis_creds("prd")
    assert creds == {
        "host": "h",
        "port": 5432,
        "database": "awvinfra",
        "user": "u",
        "password": "p",
    }


def test_load_json_missing_file(tmp_path: Path) -> None:
    with mock.patch.object(config, "CONFIG_DIR", tmp_path):
        with pytest.raises(FileNotFoundError):
            config.load_json("does_not_exist.json")
