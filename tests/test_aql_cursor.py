"""Tests for resilient bounded AQL cursor iteration."""

from __future__ import annotations

import logging
from unittest import mock

import pytest
from arango.exceptions import CursorNextError
from arango.request import Request
from arango.response import Response

from arangodbtopostgis import aql_cursor


def _cursor_not_found() -> CursorNextError:
    response = Response("put", "http://localhost/_api/cursor/id", {}, 404, "Not Found", "")
    response.error_code = 1600
    response.error_message = "cursor not found"
    response.is_success = False
    return CursorNextError(response, Request("put", "/_api/cursor/id"))


def test_iter_aql_page_restarts_after_last_yielded_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def execute(
        query: str,
        *,
        bind_vars: dict[str, object],
        batch_size: int,
        ttl: int,
    ):
        assert query == "query"
        assert batch_size == 2
        assert ttl == aql_cursor.AQL_CURSOR_TTL_SECONDS
        calls.append(dict(bind_vars))
        if bind_vars["after"] == "":
            yield {"_key": "001"}
            yield {"_key": "002"}
            raise _cursor_not_found()
        yield {"_key": "003"}

    db = mock.Mock()
    db.aql.execute.side_effect = execute
    checkpoints: list[str | None] = []
    monkeypatch.setattr(aql_cursor.time, "sleep", mock.Mock())

    docs = list(
        aql_cursor.iter_aql_page(
            db,
            "query",
            after=None,
            limit=3,
            batch_size=2,
            on_cursor_expired=checkpoints.append,
            logger=logging.getLogger(__name__),
            syncer_name="TestSyncer",
        )
    )

    assert [doc["_key"] for doc in docs] == ["001", "002", "003"]
    assert calls == [
        {"after": "", "limit": 3},
        {"after": "002", "limit": 1},
    ]
    assert checkpoints == ["002"]


def test_iter_aql_page_does_not_retry_other_arango_errors() -> None:
    error = _cursor_not_found()
    error.error_code = 1234
    db = mock.Mock()
    db.aql.execute.side_effect = error

    with pytest.raises(CursorNextError):
        list(
            aql_cursor.iter_aql_page(
                db,
                "query",
                after=None,
                limit=3,
                batch_size=2,
                on_cursor_expired=mock.Mock(),
                logger=logging.getLogger(__name__),
                syncer_name="TestSyncer",
            )
        )

    db.aql.execute.assert_called_once()
