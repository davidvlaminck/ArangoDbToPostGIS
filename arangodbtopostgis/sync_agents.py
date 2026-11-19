"""Sync ``agents`` from ArangoDB to PostGIS."""

from __future__ import annotations

import json
import logging
from typing import Any

from arangodbtopostgis.syncer import BaseSyncer

logger = logging.getLogger(__name__)


class AgentsSyncer(BaseSyncer):
    ARANGO_COLLECTION = "agents"
    POSTGIS_TABLE = "public.agents"
    FIELD_MAP = {
        "uuid": "uuid",
        "naam": "naam",
        "void": "void",
        "actief": "actief",
        "contact_info": "contact_info",
        "ovo_code": "ovo_code",
        "agent_identificator": "agent_identificator",
    }
    REQUIRED_COLUMNS = ("uuid", "actief")

    def _transform(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        purl = doc.get("purl") or {}
        tz = doc.get("tz") or {}
        agent_id = purl.get("Agent_agentId") or {}

        row: dict[str, Any] = {
            "uuid": doc.get("uuid"),
            "naam": purl.get("Agent_naam"),
            "void": tz.get("Agent_voId"),
            "actief": doc.get("AIMDBStatus_isActief"),
            "contact_info": json.dumps(purl) if purl else None,
            "ovo_code": tz.get("Agent_ovoCode"),
            "agent_identificator": (
                agent_id.get("DtcIdentificator_identificator")
                if isinstance(agent_id, dict)
                else None
            ),
        }
        # actief has a NOT NULL constraint; default if missing.
        if row["actief"] is None:
            row["actief"] = True
        return row


def main() -> None:
    AgentsSyncer().run()


if __name__ == "__main__":
    main()
