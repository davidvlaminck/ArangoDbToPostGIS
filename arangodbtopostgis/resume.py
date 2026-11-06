"""Resume/checkpoint helpers using the PostGIS ``public.params`` table.

For small tables we allow restart-by-truncate. For large streaming tables
(assets, bestekkoppelingen, assetrelaties, later betrokkenerelaties) we store
a checkpoint cursor in ``params`` and update it in the same transaction as the
data batch so a stopped run can resume without duplicates.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

# Ordered pipeline steps. Small tables come first; resumable large tables last.
STEP_ORDER: tuple[str, ...] = (
    "agents",
    "assettypes",
    "beheerders",
    "bestekken",
    "identiteiten",
    "relatietypes",
    "toezichtgroepen",
    "assets",
    "bestekkoppelingen",
    "assetrelaties",
    "betrokkenerelaties",
)

STEP_NUMBERS: dict[str, int] = {name: i + 1 for i, name in enumerate(STEP_ORDER)}
SMALL_STEPS = set(STEP_ORDER[:7])
RESUMABLE_STEPS = set(STEP_ORDER[7:])

STEP_TABLES: dict[str, str] = {
    "agents": "public.agents",
    "assettypes": "public.assettypes",
    "beheerders": "public.beheerders",
    "bestekken": "public.bestekken",
    "identiteiten": "public.identiteiten",
    "relatietypes": "public.relatietypes",
    "toezichtgroepen": "public.toezichtgroepen",
    "assets": "public.assets",
    "bestekkoppelingen": "public.bestekkoppelingen",
    "assetrelaties": "public.assetrelaties",
    "betrokkenerelaties": "public.betrokkenerelaties",
}

CURRENT_STEP_KEY = "sync_current_step"
CURRENT_STEP_NUM_KEY = "sync_current_step_num"
CURSOR_KEY = "sync_cursor:{step}"
DONE_KEY = "sync_done:{step}"


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qtable(name: str) -> str:
    return ".".join(_qident(p) for p in name.split("."))


def get_param(conn: psycopg.Connection, key: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value_text, value_int, value_bool, value_timestamp "
            "FROM public.params WHERE key_name = %s",
            (key,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "value_text": row[0],
        "value_int": row[1],
        "value_bool": row[2],
        "value_timestamp": row[3],
    }


def set_param(
    conn: psycopg.Connection,
    key: str,
    *,
    value_text: str | None = None,
    value_int: int | None = None,
    value_bool: bool | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.params (key_name, value_text, value_int, value_bool)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (key_name) DO UPDATE
            SET value_text = EXCLUDED.value_text,
                value_int = EXCLUDED.value_int,
                value_bool = EXCLUDED.value_bool,
                value_timestamp = now()
            """,
            (key, value_text, value_int, value_bool),
        )


def clear_param(conn: psycopg.Connection, key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.params WHERE key_name = %s", (key,))


def truncate_table(conn: psycopg.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {_qtable(table)} CASCADE")
    conn.commit()


def get_resume_cursor(conn: psycopg.Connection, step: str) -> str | None:
    if step not in RESUMABLE_STEPS:
        return None
    done = get_param(conn, DONE_KEY.format(step=step))
    if done and done.get("value_bool") is True:
        return "__DONE__"
    param = get_param(conn, CURSOR_KEY.format(step=step))
    if not param:
        return None
    return param.get("value_text") or None


def mark_step_started(conn: psycopg.Connection, step: str) -> None:
    logger.info(f"{step}: marked started")
    set_param(conn, CURRENT_STEP_KEY, value_text=step)
    set_param(conn, CURRENT_STEP_NUM_KEY, value_int=STEP_NUMBERS.get(step))
    if step in RESUMABLE_STEPS:
        done = get_param(conn, DONE_KEY.format(step=step))
        if not (done and done.get("value_bool") is True):
            set_param(conn, DONE_KEY.format(step=step), value_bool=False)
    conn.commit()


def save_cursor(conn: psycopg.Connection, step: str, cursor: str | None) -> None:
    """Persist cursor; caller controls transaction so it can commit with data."""
    set_param(conn, CURSOR_KEY.format(step=step), value_text=cursor)


def save_cursor_and_commit(conn: psycopg.Connection, step: str, cursor: str | None) -> None:
    save_cursor(conn, step, cursor)
    conn.commit()


def mark_step_done(conn: psycopg.Connection, step: str) -> None:
    logger.info(f"{step}: marked done")
    if step in RESUMABLE_STEPS:
        set_param(conn, DONE_KEY.format(step=step), value_bool=True)
        clear_param(conn, CURSOR_KEY.format(step=step))
    else:
        clear_param(conn, CURSOR_KEY.format(step=step))
        clear_param(conn, DONE_KEY.format(step=step))
    conn.commit()


def prepare_step(conn: psycopg.Connection, step: str) -> str | None:
    """Prepare a step for execution.

    Returns:
      ``"__DONE__"`` if the resumable step is already complete.
      a cursor string for resumable steps that have progress.
      ``None`` to start from the beginning.
    """
    mark_step_started(conn, step)
    cursor = get_resume_cursor(conn, step)
    if cursor == "__DONE__":
        logger.info(f"{step}: already complete (resume marker in params)")
        return cursor
    if step in SMALL_STEPS:
        truncate_table(conn, STEP_TABLES[step])
        logger.info(f"{step}: truncated {STEP_TABLES[step]} for clean (re)start")
        return None
    if cursor:
        logger.info(f"{step}: resuming from cursor {cursor!r}")
    else:
        logger.info(f"{step}: starting from beginning")
    return cursor
