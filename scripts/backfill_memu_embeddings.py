#!/usr/bin/env python3
"""Backfill missing embedding_json values in the memU sqlite store.

Why this exists
---------------
memU writes ``embedding_json = NULL`` whenever the embedding profile
is unconfigured at memorize time. From 2026-05-06 to 2026-05-09 the
docker compose env var ``MEMU_EMBEDDING_BASE_URL`` was set on the
agent container, but the in-process code in ``nerve.memory.memu_bridge``
only consulted ``self.config.openai_api_key`` when deciding whether
to register the embedding profile, so memU saw no embedding provider
and stored every new memory with a NULL vector. The result: vector
search at recall time was disabled for those rows, and queries that
should have hit recent memories returned "No relevant memories
found." See ``notes/lessons/2026-05-09-memu-embeddings-not-wired.md``
for the full RCA.

The companion change in this PR teaches ``memu_bridge`` to read
``MEMU_EMBEDDING_BASE_URL`` first; this script catches up the rows
that were already written with NULL.

Targets
-------
Two tables get backfilled:

- ``memu_memory_items``: text source is the ``summary`` column.
- ``memu_resources``: text source is the ``caption`` column. Rows
  with NULL ``caption`` are skipped (nothing to embed).

``memu_memory_categories`` does NOT need backfill: those embeddings
were already populated on 2026-05-05 when the categories were first
created and never wiped.

Endpoint
--------
The script reads the same env vars memU does:

- ``MEMU_EMBEDDING_BASE_URL`` (e.g. ``http://embeddings:11434/v1``)
- ``MEMU_EMBEDDING_API_KEY`` (Ollama ignores this; OpenAI requires it)
- ``MEMU_EMBED_MODEL`` (e.g. ``nomic-embed-text``)

It POSTs to ``{base_url}/embeddings`` with the OpenAI-compatible
payload ``{"model": ..., "input": [text1, text2, ...]}``. Ollama and
OpenAI both accept this format.

Idempotent
----------
The script only selects rows where ``embedding_json IS NULL OR
embedding_json = ''``. Re-running it picks up exactly the rows that
still need work (e.g. if a previous run was interrupted or hit a
transient HTTP error).

Usage
-----
::

    # See what would happen, no DB writes:
    python3 scripts/backfill_memu_embeddings.py --dry-run

    # Backfill the first 100 rows (incremental):
    python3 scripts/backfill_memu_embeddings.py --limit 100

    # Backfill everything:
    python3 scripts/backfill_memu_embeddings.py

    # Custom DB path:
    python3 scripts/backfill_memu_embeddings.py \\
        --db /path/to/memu.sqlite

    # Backfill only one table:
    python3 scripts/backfill_memu_embeddings.py --table memu_memory_items
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable

import httpx

logger = logging.getLogger("backfill_memu_embeddings")

# Per-table backfill spec: (table name, text source column, log label)
TABLES = [
    ("memu_memory_items", "summary", "memory items"),
    ("memu_resources",    "caption", "resources"),
]

DEFAULT_BATCH_SIZE = 32
DEFAULT_DB = Path("~/.nerve/memu.sqlite").expanduser()
DEFAULT_BASE_URL = "http://embeddings:11434/v1"
DEFAULT_MODEL = "nomic-embed-text"
DEFAULT_API_KEY = "placeholder"
DEFAULT_TIMEOUT = 60.0  # seconds; nomic-embed on CPU is plenty fast within this


def _resolve_endpoint() -> tuple[str, str, str]:
    base_url = os.environ.get("MEMU_EMBEDDING_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    api_key = os.environ.get("MEMU_EMBEDDING_API_KEY", DEFAULT_API_KEY) or DEFAULT_API_KEY
    model = os.environ.get("MEMU_EMBED_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL
    return base_url, api_key, model


def _fetch_pending(
    cur: sqlite3.Cursor,
    table: str,
    text_col: str,
    limit: int | None,
) -> list[tuple[str, str]]:
    """Return rows that still need embeddings, as ``(id, text)`` tuples.

    Filters out rows where the text source is NULL or empty since
    there's nothing to embed for those, and the embedding endpoint
    rejects empty strings.
    """
    sql = (
        f"SELECT id, {text_col} FROM {table} "
        f"WHERE (embedding_json IS NULL OR embedding_json = '') "
        f"  AND {text_col} IS NOT NULL "
        f"  AND {text_col} != '' "
    )
    if limit is not None:
        sql += f"LIMIT {int(limit)}"
    return list(cur.execute(sql))


def _embed_batch(
    client: httpx.Client,
    base_url: str,
    api_key: str,
    model: str,
    texts: list[str],
) -> list[list[float]]:
    """POST a batch to ``/embeddings`` and return a list of vectors.

    Raises on non-2xx HTTP status. The caller controls retry policy.
    """
    response = client.post(
        f"{base_url}/embeddings",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"model": model, "input": texts},
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") or []
    if len(data) != len(texts):
        raise RuntimeError(
            f"Embedding endpoint returned {len(data)} vectors for "
            f"{len(texts)} inputs (model={model})"
        )
    # Order is documented to match input order; sort by index defensively.
    by_index = sorted(data, key=lambda d: d.get("index", 0))
    return [d["embedding"] for d in by_index]


def _backfill_table(
    conn: sqlite3.Connection,
    client: httpx.Client,
    base_url: str,
    api_key: str,
    model: str,
    table: str,
    text_col: str,
    label: str,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
) -> int:
    """Backfill one table. Returns the number of rows written."""
    cur = conn.cursor()
    pending = _fetch_pending(cur, table, text_col, limit)
    total = len(pending)
    if total == 0:
        logger.info("%s: nothing to backfill", label)
        return 0

    # Also report the truly-empty-text count so dry-run is honest.
    dropped = cur.execute(
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE (embedding_json IS NULL OR embedding_json = '') "
        f"  AND ({text_col} IS NULL OR {text_col} = '')"
    ).fetchone()[0]
    if dropped:
        logger.info(
            "%s: skipping %d rows with NULL/empty %s (nothing to embed)",
            label, dropped, text_col,
        )

    logger.info("%s: %d rows pending (batch=%d)", label, total, batch_size)
    if dry_run:
        return 0

    written = 0
    start = time.monotonic()
    for offset in range(0, total, batch_size):
        chunk = pending[offset : offset + batch_size]
        ids = [row[0] for row in chunk]
        texts = [row[1] for row in chunk]
        try:
            vectors = _embed_batch(client, base_url, api_key, model, texts)
        except (httpx.HTTPError, RuntimeError) as exc:
            logger.error(
                "%s: batch %d-%d failed (%s); skipping and continuing",
                label, offset, offset + len(chunk), exc,
            )
            continue

        # Single transaction per batch so an interrupt loses at most
        # one batch of work.
        with conn:
            for row_id, vector in zip(ids, vectors):
                conn.execute(
                    f"UPDATE {table} SET embedding_json = ? WHERE id = ?",
                    (json.dumps(vector), row_id),
                )
        written += len(chunk)

        if written % 100 < batch_size:
            elapsed = time.monotonic() - start
            rate = written / elapsed if elapsed > 0 else 0
            logger.info(
                "%s: %d/%d (%.1f rows/s, ~%.0fs remaining)",
                label, written, total, rate,
                (total - written) / rate if rate > 0 else 0,
            )

    elapsed = time.monotonic() - start
    logger.info(
        "%s: wrote %d embeddings in %.1fs",
        label, written, elapsed,
    )
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to memu.sqlite (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per embedding request (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many rows per table (default: no limit)",
    )
    parser.add_argument(
        "--table",
        choices=[t[0] for t in TABLES],
        default=None,
        help="Only backfill this table (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count pending rows; do not embed or write",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    base_url, api_key, model = _resolve_endpoint()
    logger.info(
        "endpoint: %s, model: %s%s",
        base_url, model,
        " (DRY RUN)" if args.dry_run else "",
    )

    db_path = args.db.expanduser()
    if not db_path.exists():
        logger.error("memu.sqlite not found at %s", db_path)
        return 2

    targets: Iterable[tuple[str, str, str]] = (
        [t for t in TABLES if t[0] == args.table] if args.table else TABLES
    )

    conn = sqlite3.connect(str(db_path))
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            grand_total = 0
            for table, text_col, label in targets:
                grand_total += _backfill_table(
                    conn,
                    client,
                    base_url,
                    api_key,
                    model,
                    table,
                    text_col,
                    label,
                    args.batch_size,
                    args.limit,
                    args.dry_run,
                )
        logger.info("%s%d rows", "WOULD WRITE " if args.dry_run else "wrote ", grand_total)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
