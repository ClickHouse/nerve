"""One explicit authorization per review attempt; historical ledgers stay intact."""

SQL = """
CREATE TABLE IF NOT EXISTS code_review_requests (
    id TEXT PRIMARY KEY,
    profile TEXT NOT NULL REFERENCES code_review_profiles(id),
    pr_id TEXT NOT NULL REFERENCES code_review_prs(id),
    version_id TEXT NOT NULL REFERENCES code_review_versions(id),
    actor_id TEXT NOT NULL REFERENCES actor_refs(id),
    idempotency_key TEXT NOT NULL,
    day TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','claimed','finished','cancelled','rejected')),
    run_id TEXT UNIQUE REFERENCES code_review_attempts(run_id),
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(pr_id, actor_id, idempotency_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_code_review_request_active
    ON code_review_requests(pr_id) WHERE state IN ('pending','claimed');
CREATE INDEX IF NOT EXISTS idx_code_review_request_queue
    ON code_review_requests(profile,state,created_at,id);
"""


async def up(db):
    await db.executescript(SQL)
