"""V40: Local code-review panel — reviews, line-anchored threads, comments.

Backs the ``code_review`` feature: a browser panel for reviewing on-disk git
worktrees before anything is committed/pushed, with comment threads anchored
to a file + line range that route into a Nerve session and back.

Purely additive — three new tables, no changes to existing schema — so this
migration is safe to apply to an existing database and trivially reversible by
dropping the tables.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS code_reviews (
            id                TEXT PRIMARY KEY,
            title             TEXT NOT NULL DEFAULT '',
            repo_root         TEXT NOT NULL,
            worktree          TEXT NOT NULL,
            branch            TEXT,
            base_ref          TEXT NOT NULL DEFAULT 'HEAD',
            target_session_id TEXT,
            created_by        TEXT NOT NULL DEFAULT 'human',   -- 'human' | 'agent'
            status            TEXT NOT NULL DEFAULT 'open',     -- 'open' | 'resolved'
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_code_reviews_status
            ON code_reviews(status);
        CREATE INDEX IF NOT EXISTS idx_code_reviews_session
            ON code_reviews(target_session_id);

        CREATE TABLE IF NOT EXISTS code_review_threads (
            id             TEXT PRIMARY KEY,
            review_id      TEXT NOT NULL REFERENCES code_reviews(id)
                               ON DELETE CASCADE ON UPDATE CASCADE,
            file_path      TEXT NOT NULL,                    -- repo-relative
            side           TEXT NOT NULL DEFAULT 'new',      -- 'new' | 'old'
            line_start     INTEGER,
            line_end       INTEGER,
            anchor_snippet TEXT,                             -- line text at creation
            status         TEXT NOT NULL DEFAULT 'open',     -- 'open' | 'answered' | 'resolved'
            created_at     TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_code_review_threads_review
            ON code_review_threads(review_id);

        CREATE TABLE IF NOT EXISTS code_review_comments (
            id         TEXT PRIMARY KEY,
            thread_id  TEXT NOT NULL REFERENCES code_review_threads(id)
                           ON DELETE CASCADE ON UPDATE CASCADE,
            author     TEXT NOT NULL,                        -- 'human' | 'agent'
            body       TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_code_review_comments_thread
            ON code_review_comments(thread_id);
        """
    )
    logger.info("v040: created code_reviews, code_review_threads, code_review_comments")
