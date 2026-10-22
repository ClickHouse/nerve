# Fork migrations

This package is the second migration track. Upstream Nerve never ships a
migration here: the directory holds only `__init__.py` and this file.

A fork of Nerve that changes the database schema adds its migrations **here,
and only here**:

- Name them `vNNN_description.py` and give each an `async def up(db)`.
  Number them from `v001`. This numbering does not depend on the upstream
  migrations in the directory above.
- They are recorded in their own table, `fork_schema_version`. The runner
  creates it when the first fork migration runs.
- On every database open, the runner applies the pending upstream migrations
  first, then these. A fork migration therefore runs on the newest upstream
  schema, on a new database too.

Why a separate track: the runner skips every version at or below the
database's current one. Say a fork numbers its migration `v059` in the
upstream sequence. When upstream later ships its own `v059`, the fork's
database skips it forever. A version in this track can never hide an upstream
one, and rebasing the fork onto a newer upstream needs no renumbering.

The upstream rules still apply here:

- Number each new migration above the highest fork migration already in your
  fork. Never reuse or renumber one.
- Make each migration idempotent (`CREATE TABLE IF NOT EXISTS`, check a column
  before adding it). No transaction wraps a migration, so one that is
  interrupted runs again from the start.
- A Nerve refuses a database whose fork version is above the highest fork
  migration it ships, just as it refuses one that is ahead on the upstream
  track. So a build without your migrations does not start on a database they
  changed: restore the state backup taken before them, or set
  `NERVE_ALLOW_NEWER_SCHEMA=1` to accept the risk.

Give the tables and columns your migrations add a prefix of your own. An
upstream migration that later adds an object with the same name would fail on
your databases (`ADD COLUMN`) or skip its own (`CREATE TABLE IF NOT EXISTS`).
