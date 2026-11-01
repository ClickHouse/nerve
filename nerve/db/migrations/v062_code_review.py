"""Durable PR identities, immutable input versions and budget reservations."""

SQL = """
CREATE TABLE IF NOT EXISTS code_review_profiles (
    id TEXT PRIMARY KEY, paused INTEGER NOT NULL DEFAULT 0,
    last_polled_at TEXT, last_success_at TEXT, error TEXT NOT NULL DEFAULT '',
    error_count INTEGER NOT NULL DEFAULT 0, notified_error TEXT NOT NULL DEFAULT '',
    notified_budget_day TEXT NOT NULL DEFAULT '', last_digest_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS code_review_prs (
    id TEXT PRIMARY KEY, profile TEXT NOT NULL REFERENCES code_review_profiles(id),
    repository_id INTEGER NOT NULL, repository TEXT NOT NULL, number INTEGER NOT NULL,
    task_id TEXT NOT NULL UNIQUE, title TEXT NOT NULL, url TEXT NOT NULL,
    state TEXT NOT NULL, draft INTEGER NOT NULL DEFAULT 0,
    head TEXT NOT NULL, base TEXT NOT NULL, base_ref TEXT NOT NULL,
    current_version TEXT, reviewed_version TEXT, review_state TEXT NOT NULL DEFAULT 'queued',
    severity TEXT NOT NULL DEFAULT 'none', serious_keys TEXT NOT NULL DEFAULT '[]', ci TEXT NOT NULL DEFAULT '{}',
    last_polled_at TEXT NOT NULL, pr_updated_at TEXT NOT NULL,
    content_updated_at TEXT NOT NULL, queue_since TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
    UNIQUE(repository_id, number, profile)
);
CREATE TABLE IF NOT EXISTS code_review_versions (
    id TEXT PRIMARY KEY, pr_id TEXT NOT NULL REFERENCES code_review_prs(id),
    fingerprint TEXT NOT NULL, head TEXT NOT NULL, base TEXT NOT NULL,
    snapshot TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, retry_at TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
    UNIQUE(pr_id, fingerprint)
);
CREATE TABLE IF NOT EXISTS code_review_attempts (
    run_id TEXT PRIMARY KEY, version_id TEXT NOT NULL REFERENCES code_review_versions(id),
    profile TEXT NOT NULL, day TEXT NOT NULL, reserved REAL NOT NULL CHECK(reserved > 0),
    spent REAL NOT NULL DEFAULT 0 CHECK(spent >= 0), uncertain REAL NOT NULL DEFAULT 0 CHECK(uncertain >= 0), settled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL, end_day TEXT NOT NULL DEFAULT '', artifact_path TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS code_review_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, pr_id TEXT NOT NULL,
    kind TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL,
    notified INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_code_review_board
    ON code_review_prs(profile, state, content_updated_at DESC, id);
CREATE INDEX IF NOT EXISTS idx_code_review_queue
    ON code_review_versions(status, created_at, id);
CREATE INDEX IF NOT EXISTS idx_code_review_budget ON code_review_attempts(day, settled);
"""


async def up(db):
    await db.executescript(SQL)
