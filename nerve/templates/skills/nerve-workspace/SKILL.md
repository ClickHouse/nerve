---
name: Nerve Workspace Config
description: >
  Change your own configuration — skills, cron jobs, sources, and settings that
  live in the git-synced workspace repo. Use when asked to add/edit/remove a cron
  job, create or change a skill, adjust settings, or "change your config", and
  especially when this instance is locked (remote-only) so direct edits don't
  apply. Triggers on "add a cron", "change your schedule", "edit your config",
  "update settings", "propose a config change".
version: 1.0.0
context: domain
---

# Managing Your Own Configuration

Your configuration lives in the **workspace** — a git repository synced from a
shared remote (the config repo). It contains:

- `config/settings.yaml` — shareable settings
- `config/cron/jobs.yaml`, `config/cron/system.yaml`, `config/cron/gates/` — cron
- `skills/<id>/SKILL.md` — skills
- `SOUL.md`, `IDENTITY.md`, `USER.md`, `AGENTS.md`, `TOOLS.md` — your standing
  instructions

This is **not** the Nerve application source code (that's the `nerve-dev` skill).

## How to change config

**Always propose config changes as a pull request** with the
`propose_config_change` tool — don't edit tracked config files directly. This
keeps every change reviewed, approved, and traceable, and it is the *only* way to
change config when the instance is **locked** (in lockdown, direct edits to
skills/cron/settings are blocked and wouldn't be synced anyway).

`propose_config_change` takes a `title`, an optional `body`, and a list of
`changes` — each the **full new content** of a file, path relative to the
workspace root. It:

1. stages your change on a branch off the remote's default branch (in an isolated
   worktree — your live workspace is untouched),
2. **validates** the resulting bundle (an invalid change is rejected with the
   errors to fix — no PR is opened),
3. pushes the branch and opens a PR via `gh` for a human to review and merge.

Once merged, workspace sync pulls it and it hot-reloads.

### What you can propose

Only reviewed configuration — anything under `config/` or `skills/`, plus the
workspace-root instruction files listed above. Everything else in the repo is
refused, including:

- **Runtime state** — `MEMORY.md`, `TASK.md`, `memory/`. You maintain these
  yourself as you work; they aren't reviewed and a PR per update helps nobody.
- **Anything that isn't config** — `.git/`, `.github/`, `scripts/`, application
  code, and `.gitattributes`/`.gitignore` (which decide whether a reviewer can
  see the diff at all). If you genuinely need one of those changed, ask.
- **Files named like code** — `.py`, `.sh`, `.js` and friends — with one
  exception: a cron gate plugin at `config/cron/gates/<name>.py`. Nothing
  validates a gate plugin, so keep it short and say in the PR body what it does
  and why a built-in gate won't do.

A proposal containing even one refused path is rejected whole; nothing is
dropped quietly. Fix the reported paths and re-submit.

`skills/<id>/scripts/` **is** proposable — it's a normal part of a skill — so a
script there reaches the instance through this route like anything else.

### Why this exists, and what it isn't

So that a change to your configuration is reviewed, attributable, and visible in
the repository's history. It is **not** a lock. You have a shell; you could write
these files another way. Doing that produces a running config nobody agreed to
and no record of who changed what, which is the thing this avoids — not something
the tool could stop you doing.

That's also why changes that alter *what runs* are flagged rather than refused.
When the tool can tell — a gate plugin, a script replacing an executable file, an
`mcp_servers` or `codex` or `proxy` entry in `settings.yaml` — it puts a notice at
the top of the PR. It can only recognise what it knows about, so **say it in your
own words too** whenever your change causes something new to execute. The
reviewer approving the PR is the only check there is.

### Examples

Add a cron job — read the current `config/cron/jobs.yaml`, add your job, and
submit the full file:

```
propose_config_change(
  title="Add nightly repo digest cron",
  body="Runs at 06:00 to summarize overnight PR activity.",
  changes=[{"path": "config/cron/jobs.yaml", "content": "<full updated jobs.yaml>"}],
)
```

Add or edit a skill — submit the full `skills/<id>/SKILL.md`:

```
propose_config_change(
  title="Add deploy-runbook skill",
  changes=[{"path": "skills/deploy-runbook/SKILL.md", "content": "<full SKILL.md>"}],
)
```

## Rules

- Read the current file first (so your submitted content is a correct full
  replacement, not a fragment).
- One logical change per PR; write a clear title/body — a human will review it.
- Never put secrets in tracked files; reference them as `${ENV_VAR}`.
- If validation fails, fix the reported errors and re-submit — don't try to
  bypass it by editing files directly.
