---
name: Nerve Workspace Config
description: >
  Change your own configuration — skills, cron jobs, sources, and settings that
  live in the workspace. Covers both routes: a pull request when the workspace is
  a shared config repo (and the only route when this instance is locked), or a
  direct edit when the workspace is purely local. Use when asked to add/edit/remove
  a cron job, create or change a skill, adjust settings, or "change your config".
  Triggers on "add a cron", "change your schedule", "edit your config",
  "update settings", "propose a config change".
version: 1.0.0
context: domain
---

# Managing Your Own Configuration

Your configuration lives in the **workspace**. It contains:

- `config/settings.yaml` — shareable settings
- `config/cron/jobs.yaml`, `config/cron/system.yaml`, `config/cron/gates/` — cron
- `skills/<id>/SKILL.md` — skills
- `SOUL.md`, `IDENTITY.md`, `USER.md`, `AGENTS.md`, `TOOLS.md` — your standing
  instructions

This is **not** the Nerve application source code (that's the `nerve-dev` skill).

## First: is this workspace shared?

The answer decides how you change config, so establish it before you start.

The workspace **may** be a git repository synced from a shared remote (a config
repo), in which case config is something a human reviews and merges, and your job
is to propose rather than to edit. It may equally be a plain local directory that
belongs to this instance alone — that is what an ordinary install looks like.

- **Locked instance** (lockdown / remote-only) — a PR is the *only* thing that
  works. Direct edits to tracked config are blocked, and would be overwritten by
  the next sync even if they weren't.
- **Workspace has a remote, not locked** — route every change to the reviewed
  surface through `propose_config_change`, even though nothing forces you to. A
  direct write leaves the workspace diverged from the reviewed revision, and sync
  refuses to merge while anything in that surface — `config/`, `skills/`, and the
  root instruction files — has uncommitted local state (untracked, modified,
  staged or deleted). So the edit also stops every later config change from
  reaching this instance until someone commits or drops it.
  `propose_config_change` stages in an isolated worktree, so it leaves the live
  workspace clean — only a direct write dirties it. That covers every route that
  writes those files, not just an editor: `skill_create` and `skill_update`, the
  `POST`/`PUT`/`DELETE /api/skills` endpoints, and a shell command.
- **No remote, not locked** — there is nothing to open a PR against and no
  review to route through. **Edit the files directly** and tell the user what you
  changed. `propose_config_change` will refuse, and it is right to.

If you don't know which you're in, try `propose_config_change` and read the
refusal — it says whether the problem is a missing remote or a locked instance.

## How to change config

When the workspace is shared, **propose config changes as a pull request** with
the `propose_config_change` tool rather than editing tracked config files. That
keeps every change reviewed, approved, and traceable.

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

Proposals carry **text**, so a binary file cannot go through this tool at all.
That matters for one thing in practice: the instance's favicon
(`config/favicon.svg` / `.png` / `.ico`, served at `/favicon.ico`). You can
propose an `.svg`, since SVG is text. A `.png` or `.ico` has to be committed by a
human — say so rather than submitting something that will be rejected.

### Why this exists, and what it isn't

So that a change to your configuration is reviewed, attributable, and visible in
the repository's history. It is **not** a lock. You have a shell; you could write
these files another way. On a workspace someone else reviews, doing that produces
a running config nobody agreed to and no record of who changed what, which is the
thing this avoids — not something the tool could stop you doing. On a local
workspace there is nobody to route around, which is why editing directly there is
the normal thing rather than a workaround.

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
- If **validation** fails, fix the reported errors and re-submit. Writing the
  same content straight to the file instead only moves an invalid config onto the
  instance — that is the one case where editing directly is the wrong answer even
  on a local workspace.

## Setting up a config repo (when the user wants review)

On a local workspace you edit directly, as above — that is not a stopgap. But if
the user wants config changes reviewed before they take effect, or wants one repo
to serve several instances, the workspace has to become a git repo synced from a
remote. That is an **operator** task, not something you do autonomously — walk the
human through it:

1. Run `nerve config init-repo` — scaffolds the CI workflow, `.gitignore`, a
   README and a `config/settings.yaml` into the workspace, and prints the
   remaining git/`gh` steps.
2. Create the private GitHub repo and push (`git init` → commit → `gh repo create`).
3. Enable `workspace_sync` (and, when ready, `lockdown`) in `settings.yaml`.

Full runbook: `docs/config.md` → "Setting up the config repo". Once the remote
exists and sync is on, use `propose_config_change` for all further changes.
