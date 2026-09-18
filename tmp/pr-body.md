## Problem

When a Codex **workflow** run ends — completed, cancelled, budget-stopped, or the daemon crashed — some of its descendant processes can survive as orphans. Codex wraps each sandboxed command in `codex-linux-sandbox`, which `setsid`s into its own session/process group (and may double-fork). `CodexAppServerClient._signal_process_tree` signals a single process group (`os.killpg(proc.pid, sig)`), so those escaped descendants are never reached; the workflow teardown also assumes the in-memory client still covers every worker. The result is `codex-linux-sandbox` trees (and the `python`/`bash`/`pytest` they were running) left parented to PID 1 after the run is durably `done`.

## Approach

Opt-in, **workflow-only** containment. In `strict` mode a workflow run's `codex app-server` is launched **inside a dedicated delegated systemd user scope**, created *before* the command execs:

```
systemd-run --user --scope --quiet --collect --expand-environment=no \
  -p Delegate=yes -p KillMode=control-group -p SendSIGKILL=yes -p TimeoutStopSec=<grace> \
  --description="<fixed literal>" --unit=<nonce unit> -- <app-server argv>
```

Everything the run forks inherits that cgroup (setsid/double-fork can't escape it), so a single bounded `systemctl --user stop` (TERM → grace → SIGKILL over the whole control group) reaps the entire tree. Cleanup is then verified — the scope's recursive `cgroup.events` `populated=0`, or the scope cgroup gone after a *successful* stop — never assumed.

Each run writes a durable, validated record (`<runs_dir>/<run>/lifecycle.json`, atomic write + file/dir fsync; no argv/env/secrets). It carries the systemd `InvocationID`, `boot_id`, and daemon generation, so cleanup can run from the record alone — from the terminal path (independent of the in-memory client) or from startup reconciliation after a crash — and refuses to signal a same-name unit that was replaced by a foreign instance, a cross-boot record, or a corrupt record. A per-owner lock plus a generation fence keep launch, terminal reap, retry, and reconciliation from racing. One supported retry entry (`nerve codex reap-descendants <run>` + a service method) re-drives a `pending_retry` reap idempotently.

## Scope / safety

- **Off by default.** `codex.lifecycle.mode`: `disabled` (inert; unchanged behaviour) · `observe` (unenforced — records intent, never claims a cleanup it didn't perform) · `strict` (contain; **fail before exec** if containment can't be established — no silent downgrade to `killpg`).
- **Codex workflow runs only.** Interactive/cron Codex sessions and Claude runs get no containment.
- **Platform:** requires Linux + a reachable systemd `--user` manager + cgroup v2. Everywhere else it stays inert (disabled/observe) or strict refuses to launch — it never runs a workflow un-contained and never breaks a non-systemd deployment.
- **Not a security boundary:** cgroup inheritance contains *trusted* tool processes that fork/setsid/double-fork; it is not a sandbox against a malicious same-UID process, and no privilege is broadened to make it one.
- No daemon deploy / privilege / linger / systemd service-layout change.

## Testing

`tests/test_codex_lifecycle.py` — 26 assertion-bearing cases. The real-process cases launch actual systemd user scopes running real setsid/double-fork escapers (`tests/fixtures/lifecycle_escaper.py`) and the real fake app-server through the actual launch wrapper: containment + foreign-run survival, nested-cgroup recursive `populated`, TERM-ignoring → SIGKILL escalation, fork-during-teardown, crash/fresh-process reconcile, replaced-unit/cross-boot/corrupt/fsync-fail/manager-down identity refusals (no foreign signal, no false completion), concurrent-reap serialisation, real durable retry, and disabled/observe/strict-unavailable behaviour. No paid model or network; every scope/process is torn down by the suite.

Existing suites unaffected: `test_codex_appserver.py` + `test_workflow_runs.py` (92), `-k codex` (182).

## Deferred (not in this PR)

Subreaper/pidfd fallback for non-systemd hosts, chat-session extension, and the `ultracode.py:_run` setup-CLI path.
