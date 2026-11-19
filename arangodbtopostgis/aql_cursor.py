"""Helpers for safely reading large ArangoDB query results."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

from arango.exceptions import CursorNextError

AQL_PAGE_SIZE = 10_000
AQL_CURSOR_TTL_SECONDS = 1800
MAX_CURSOR_RETRIES = 5


class _CursorHeartbeat:
    def __init__(
        self,
        logger: logging.Logger,
        syncer_name: str,
        last_key: str | None,
        yielded: int,
        limit: int,
    ) -> None:
        self.logger = logger
        self.syncer_name = syncer_name
        self.last_key = last_key
        self.yielded = yielded
        self.limit = limit
        self.waiting = threading.Event()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._log_waits, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.waiting.clear()
        self.stop_event.set()
        self.thread.join()

    def update(self, last_key: str | None, yielded: int) -> None:
        self.last_key = last_key
        self.yielded = yielded

    def _log_waits(self) -> None:
        while not self.stop_event.is_set():
            if not self.waiting.wait(0.5):
                continue
            wait_started = time.monotonic()
            while self.waiting.is_set() and not self.stop_event.wait(30):
                if self.waiting.is_set():
                    self.logger.info(
                        "%s: still waiting for Arango response after cursor=%s "
                        "(received %s/%s; waiting %.0fs)",
                        self.syncer_name,
                        self.last_key,
                        self.yielded,
                        self.limit,
                        time.monotonic() - wait_started,
                    )


def iter_aql_page(
    db: Any,
    query: str,
    *,
    after: str | None,
    limit: int,
    batch_size: int,
    on_cursor_expired: Callable[[str | None], None],
    logger: logging.Logger,
    syncer_name: str,
) -> Iterator[dict[str, Any]]:
    """Yield one bounded AQL page, restarting expired cursors from the last key."""
    last_key = after
    yielded = 0
    retries = 0

    while yielded < limit:
        bind_vars = {"after": last_key or "", "limit": limit - yielded}
        logger.info(
            "%s: requesting Arango page after cursor=%s remaining=%s",
            syncer_name,
            last_key,
            limit - yielded,
        )
        heartbeat = _CursorHeartbeat(logger, syncer_name, last_key, yielded, limit)
        heartbeat.start()
        try:
            heartbeat.waiting.set()
            try:
                cursor = db.aql.execute(
                    query,
                    bind_vars=bind_vars,
                    batch_size=batch_size,
                    ttl=AQL_CURSOR_TTL_SECONDS,
                )
            finally:
                heartbeat.waiting.clear()
            iterator = iter(cursor)
            while True:
                heartbeat.waiting.set()
                try:
                    doc = next(iterator)
                except StopIteration:
                    return
                finally:
                    heartbeat.waiting.clear()
                last_key = doc.get("_key") or last_key
                yielded += 1
                heartbeat.update(last_key, yielded)
                yield doc
        except CursorNextError as exc:
            if exc.error_code != 1600:
                raise

            on_cursor_expired(last_key)
            retries += 1
            if retries > MAX_CURSOR_RETRIES:
                logger.error(
                    "%s: Arango cursor expired %s consecutive times after cursor=%s; stopping",
                    syncer_name,
                    retries,
                    last_key,
                )
                raise
            delay = min(2 ** (retries - 1), 30)
            logger.warning(
                "%s: Arango cursor expired; retry %s/%s from cursor=%s in %ss",
                syncer_name,
                retries,
                MAX_CURSOR_RETRIES,
                last_key,
                delay,
            )
            time.sleep(delay)
        finally:
            heartbeat.stop()
