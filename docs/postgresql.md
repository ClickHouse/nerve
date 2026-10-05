# PostgreSQL storage

SQLite remains the default. Install the `postgres` extra and select PostgreSQL in `settings.yaml`:

```yaml
use_postgresql: true
postgresql_dsn: ${NERVE_POSTGRES_DSN}
tenant_id: example
workflow_id: assistant
```

Apply `nerve/db/postgres/schema.sql` once with a database owner before starting Nerve. It requires PostgreSQL 16 or newer and pgvector. Give the runtime role `USAGE` on schema `nerve_pg`, `SELECT, INSERT, UPDATE, DELETE` on its tables, and `USAGE` on its sequences. Grant only `SELECT` on `storage_version`. The runtime role must not own the schema, be a superuser, or have `BYPASSRLS`. Nerve does not run DDL. Keep the connection string in the environment; use TLS for remote connections.

Tasks (including Markdown content), sessions, messages, accounts, schedules, workflow/review state, usage, uploaded files, and memU memories and source bytes share this database. Each table is scoped by tenant and workflow, with composite keys and row-level policies. Different scopes can use the same identifiers. The trusted adapter sets scope; agents cannot choose it. A shared credential is not an isolation boundary against arbitrary host code that can change the PostgreSQL session setting.

Local task files, uploads and memory sources are reconstructed caches. Edit tasks through Nerve's task tools or task API; editing cached Markdown directly does not update PostgreSQL. Prompts, configuration and tools remain in the configuration repository. External agent subprocesses, their working trees, in-flight tool execution, and provider-owned resume files are not database snapshots. After losing local storage, Nerve's history remains available, but native provider sessions start fresh; interrupted external side effects still require reconciliation.

Use one daemon per tenant/workflow. Separate workflows can run concurrently against the same tables. PostgreSQL transactions protect store updates across connections; this option does not make the gateway's in-process scheduler a distributed scheduler.

Storage settings require a restart. A connection or schema error fails startup; there is no SQLite fallback. This release provisions new PostgreSQL scopes; it does not automatically import an existing SQLite installation. PostgreSQL text search uses prefix matching and weighted title/content ranking, rather than SQLite BM25 scores.

Use PostgreSQL backups/PITR and autovacuum. Local `nerve backup` bundles cannot back up PostgreSQL and are rejected in this mode; scheduled local bundles are disabled. Preserve the configuration repository separately.
