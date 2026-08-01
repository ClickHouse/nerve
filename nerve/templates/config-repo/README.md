# Nerve config repo

This repository is the **shared configuration** for a
[Nerve](https://github.com/ClickHouse/nerve) instance — its skills, cron jobs,
sources, and settings. It maps to the instance's **workspace**, so the repo root
holds:

- `config/settings.yaml` — shareable settings (secrets are `${ENV_VAR}` refs)
- `config/cron/` — cron jobs, system jobs, and drop-in gate plugins
- `skills/<id>/SKILL.md` — skills

## How changes are made

1. Open a PR with your change. CI (`.github/workflows/validate-config.yml`)
   validates the bundle and scans for committed secrets; both must pass.
2. A human reviews and merges.
3. The instance pulls the merged result (workspace sync) and hot-reloads — no
   restart.

The agent proposes its own changes the same way via the `nerve-workspace` skill
(`propose_config_change`). When the instance is **locked** (`lockdown: true`),
this PR flow is the *only* way to change config.

## Rules

- **Never commit secrets.** They live in the environment and are referenced with
  `${ENV_VAR}` from `settings.yaml`. See `.gitignore`. CI runs a secret scanner
  over every PR, but treat it as a backstop: it recognizes common credential
  shapes, not every string that happens to be one. It also errs the other way —
  a long random-looking value in a prompt or an MCP argument can trip it. Add a
  trailing `# gitleaks:allow` on that line, or a `.gitleaks.toml` for a
  repo-wide rule, and say why in the PR.
- Gate plugins under `config/cron/gates/*.py` are **executable code** run by the
  daemon — review them like any other code.
