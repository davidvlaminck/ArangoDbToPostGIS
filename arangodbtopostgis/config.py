"""Credential loading from JSON config files.

Configuration files live in the ``config`` directory at the repository root.
Secrets are read at runtime; nothing is hard-coded here.
"""

from __future__ import annotations

import logging

import json
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CONFIG_DIR: Path = Path(__file__).resolve().parents[2] / "config"


def load_json(filename: str) -> dict[str, Any]:
    """Load a JSON configuration file from the config directory."""
    with (CONFIG_DIR / filename).open() as f:
        return json.load(f)


def arango_creds(env: str = "dev") -> dict[str, Any]:
    """Return ArangoDB connection parameters for the given environment."""
    cfg = load_json("settings_arangodb.json")
    db = cfg["databases"][env]
    return {
        "hosts": db.get("hosts", "http://127.0.0.1:8529"),
        "database": db["database"],
        "username": db["user"],
        "password": db["password"],
    }


def postgis_creds(env: str = "prd") -> dict[str, Any]:
    """Return PostGIS connection parameters for the given environment."""
    cfg = load_json("settings_postgis.json")
    db = cfg["databases"][env]
    return {
        "host": db["host"],
        "port": db["port"],
        "database": db["database"],
        "user": db["user"],
        "password": db["password"],
    }
