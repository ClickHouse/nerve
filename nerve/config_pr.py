"""Propose a config change as a PR against the workspace repo.

This is how Nerve changes its own configuration when it can't (or shouldn't) edit
the live workspace directly — notably under lockdown, where runtime edits to
tracked config are blocked. Instead of touching the running workspace, it stages
the change in a throwaway git worktree off the remote's default branch, validates it,
pushes a branch, and opens a PR via ``gh`` for human review. Nothing in the live
working tree is modified (so it never conflicts with ff-only sync).

**What this is for.** A change that goes through here is reviewed, attributable
and recorded in the repository's history. It is not a barrier and cannot be one:
the agent has a shell, and a determined one can write the same files directly.
Lockdown and this tool exist so that the audit trail is honest, not so that the
alternative is impossible. That is also why a change with an executable effect is
*flagged* rather than refused — the goal is that the human approving the pull
request knows what they are approving.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from nerve.config import _is_within
from nerve.sync_service import _git, is_git_repo

logger = logging.getLogger(__name__)

_GIT_TIMEOUT_SECONDS = 120

# Used only when git cannot work out a committer itself. A daemon on a server
# often has no global git identity, and `git commit` then fails outright with
# "empty ident name" — at which point the branch and the worktree already exist
# and a proposal that was entirely valid reports failure. `.invalid` is the
# reserved TLD (RFC 2606), so this can never resolve to a real mailbox.
_FALLBACK_COMMITTER = ("Nerve", "nerve@nerve.invalid")

# What a proposal may change, relative to the workspace root. An allowlist, not a
# list of forbidden paths, because the workspace repo is also the repo the daemon
# runs out of: ``.git/hooks/``, ``.github/workflows/`` (the CI that validates the
# bundle) and ``scripts/`` (which the notification handlers shell out to) are all
# reachable from a "config change" that is only checked for traversal, and none of
# them are config. Anything not named below stays out of scope.
_ALLOWED_DIRS = ("config", "skills")


def _committer_args(cwd: Path) -> list[str]:
    """``-c user.*`` overrides, but only when git cannot resolve an identity.

    Asked unconditionally, these would override a real operator identity and
    attribute every config PR to the daemon, so defer to whatever is configured
    and step in only when there is nothing at all. ``git var`` is the question
    git itself answers, rather than reading config keys and re-deriving its
    precedence rules here.
    """
    if _git(["var", "GIT_COMMITTER_IDENT"], cwd).returncode == 0:
        return []
    name, email = _FALLBACK_COMMITTER
    logger.info(
        "No git identity configured; committing the proposal as %s <%s>", name, email,
    )
    return ["-c", f"user.name={name}", "-c", f"user.email={email}"]

# Workspace-root files that are the agent's own reviewed instructions. Not
# MEMORY.md, TASK.md or memory/: those are runtime state the instance rewrites
# for itself, and routing them through review would mean a pull request per
# thought while telling the reviewer nothing.
_ALLOWED_ROOT_FILES = frozenset({
    "SOUL.md", "IDENTITY.md", "USER.md", "AGENTS.md", "TOOLS.md",
})

# Names that decide how git renders or tracks the rest of the subtree. A
# ``.gitattributes`` marking ``config/`` binary or generated collapses every
# later diff under it, which removes the only thing the reviewer has to look at.
_REFUSED_NAMES = frozenset({".gitattributes", ".gitignore"})

# Suffixes read as code rather than configuration.
_CODE_SUFFIXES = frozenset({
    ".py", ".pyc", ".pyo", ".sh", ".bash", ".zsh", ".fish", ".pl", ".rb",
    ".js", ".mjs", ".cjs", ".ts", ".ps1", ".php",
})

# The one place a proposal may carry code. Cron gate plugins are a supported
# feature — the daemon imports ``<workspace>/config/cron/gates/*.py``
# (non-recursively) at start-up and on every cron reload — so refusing them
# outright would make a legitimate config change inexpressible here and push it
# onto the unreviewed route. Everywhere else the proposable surface is YAML and
# Markdown, and a file of code in it is a code drop wearing a config change's
# title.
_GATE_PLUGIN_DIR = ("config", "cron", "gates")

# Two lists of watched settings keys, judged by two different rules. Do not merge
# them: each rule is a bug when applied to the other list.
#
# _EFFECTFUL_SETTINGS_KEYS is judged by *presence* in the proposed file. These
# keys name something the daemon will run, and their safe state is not being
# there at all — so the question is "does the file I am approving spawn a
# subprocess", which is about the proposed file alone. Approving a rewrite of
# settings.yaml is approving every line in it, and a re-stated MCP server is
# still an MCP server the reviewer should have their eye on.
#
# _SECURITY_SETTINGS_KEYS is judged by *change* against the revision in the base
# branch. These keys are protections whose safe state is being switched on and
# stated, so presence would fire on the steady state — every settings proposal a
# locked instance ever made would flag its own unchanged `lockdown: true`, and a
# notice that always cries wolf teaches reviewers to scroll past it. Presence
# also cannot see the case that matters most: deleting the line disarms the
# control exactly as well as setting it false, and there is nothing present to
# notice.

# Keys whose whole purpose is to name something the daemon will run: an MCP
# server it spawns, a CLI it shells out to, a directory of Python it imports.
# Deliberately short and deliberately incomplete — configuration is broadly
# effectful, and enumerating every key that eventually reaches a subprocess is a
# list that rots with the schema. This catches the ones a reviewer would most
# want called out; it is not a boundary and nothing depends on it being complete.
_EFFECTFUL_SETTINGS_KEYS = (
    ("mcp_servers",),
    ("external_agents",),
    ("codex",),
    ("proxy",),
    ("houseofagents",),
    ("cron", "gate_plugins_dir"),
)

# Key -> what turning it changes, quoted into the notice so the reviewer does not
# have to know the flag to judge the diff.
_SECURITY_SETTINGS_KEYS: dict[tuple[str, ...], str] = {
    ("lockdown",): (
        "decides whether the machine-local config layers are read at all, "
        "whether the legacy cron directory is honored, and whether the daemon "
        "may write tracked config at runtime"
    ),
}

_SETTINGS_FILE = "config/settings.yaml"

# Distinguishes "the settings do not mention this key" from a key stated with a
# null or empty value, which ``None`` alone cannot (see
# :func:`_effectful_settings_keys`).
_ABSENT = object()

# "The revision this is being compared against could not be established" — as
# opposed to a revision that was read and does not mention the key. See
# :func:`_current_settings`.
_UNKNOWN = object()


@dataclass
class ProposeResult:
    ok: bool
    pr_url: str = ""
    branch: str = ""
    message: str = ""
    validation_errors: list[str] = field(default_factory=list)
    #: Staged paths that change what runs (see :func:`_executable_effect`).
    code_paths: list[str] = field(default_factory=list)


def _gh(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Run a gh command; captured, never raises (missing gh → rc=1)."""
    try:
        return subprocess.run(
            ["gh", *args], cwd=str(cwd), capture_output=True, text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except FileNotFoundError:
        return subprocess.CompletedProcess(args, 1, "", "gh CLI not found on PATH")
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(args, 1, "", "gh command timed out")


def _remote_default_branch(workspace: Path) -> str:
    """The branch ``origin`` calls its default, or ``""`` if nothing can say.

    Deliberately not the local ``HEAD``. A proposal is a pull request against
    the branch the remote merges into, and the checkout the workspace happens to
    be sitting on says nothing about that: a workspace parked on a feature
    branch would silently branch off and target *that* branch, carrying its
    unmerged commits into the proposal's diff, and a detached HEAD — what a sync
    leaves behind while it validates a rev — makes ``rev-parse --abbrev-ref
    HEAD`` answer the literal string ``HEAD``, which then reaches ``git fetch
    origin HEAD`` and ``gh pr create --base HEAD``.

    The remote is asked first because it is the only authoritative answer:
    ``refs/remotes/origin/HEAD`` is a snapshot taken at clone time and nothing
    refreshes it when the default branch is renamed, so trusting the cache can
    target a branch that has not been the default for a year. The cache is the
    fallback for a remote that cannot be reached. Empty means neither could
    answer, which is a state a real workspace reaches: ``git remote add``
    followed by ``git fetch`` never writes the cached ref at all.
    """
    ls = _git(["ls-remote", "--symref", "origin", "HEAD"], workspace)
    if ls.returncode == 0:
        for line in ls.stdout.splitlines():
            # "ref: refs/heads/main<TAB>HEAD", ahead of the plain sha line.
            ref, _, name = line.partition("\t")
            if name.strip() == "HEAD" and ref.startswith("ref: refs/heads/"):
                return ref[len("ref: refs/heads/"):].strip()
    cached = _git(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"], workspace)
    if cached.returncode == 0 and cached.stdout.strip().startswith("origin/"):
        return cached.stdout.strip()[len("origin/"):]
    return ""


def _slug(text: str, ts: int) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40] or "change"
    return f"nerve-config/{base}-{ts}"


def _rel_escapes(rel: str) -> bool:
    """True if an agent-provided path is absolute or contains a ``..`` component."""
    rel = str(rel)
    if not rel or rel.startswith("/") or Path(rel).is_absolute():
        return True
    return ".." in Path(rel).parts


def _is_code_name(name: str) -> bool:
    """True if ``name`` names code.

    Not ``Path.suffix``, which calls a file named exactly ``.py`` suffix-less
    while the gate-plugin loader's ``*.py`` glob matches it perfectly happily.
    Trailing dots and spaces come off first: not every filesystem this can reach
    keeps them, so ``gate.py.`` and ``gate.py`` may be the same file by the time
    anything globs the directory.
    """
    trimmed = name.rstrip(". ").lower()
    return any(trimmed.endswith(suffix) for suffix in _CODE_SUFFIXES)


def _is_gate_plugin(parts: tuple[str, ...]) -> bool:
    """True for ``config/cron/gates/<name>.py`` — and only that.

    The loader globs the directory one level deep, so a nested path is not a gate
    plugin: it would never be imported, and accepting it would be accepting code
    on the strength of a directory prefix that means nothing. The name is matched
    as the loader sees it, so a spelling the glob would miss (``gate.py.``,
    ``gate.pyc``) is not a gate plugin and falls through to being refused.
    """
    depth = len(_GATE_PLUGIN_DIR)
    return (
        len(parts) == depth + 1
        and parts[:depth] == _GATE_PLUGIN_DIR
        and parts[-1].lower().endswith(".py")
    )


def _proposal_path_problem(rel: str) -> str | None:
    """Why a proposal may not write ``rel``, or ``None`` if it may.

    Scope, not containment. :func:`_rel_escapes` and :func:`_safe_dst` answer
    "does this stay inside the workspace"; this answers "is this a configuration
    change at all". Both are needed: a path can be perfectly contained and still
    be a git hook, a CI workflow, or a helper script the daemon shells out to,
    landed under a title that says it edits a cron schedule.

    Note what this is and is not. It does not stop the agent from writing the
    config subtree — a shell command still can, which is why lockdown is
    documented as a config-integrity control rather than a sandbox. It keeps the
    *reviewed* route honest: what arrives on a reviewer's screen labelled "config
    change" is config.
    """
    if _rel_escapes(rel):
        return "must be a path relative to the workspace root, with no '..' segments"
    parts = Path(rel).parts
    in_allowed_dir = len(parts) > 1 and parts[0] in _ALLOWED_DIRS
    is_root_file = len(parts) == 1 and parts[0] in _ALLOWED_ROOT_FILES
    if not (in_allowed_dir or is_root_file):
        return (
            "outside the proposable surface. A proposal may change "
            + ", ".join(f"{d}/…" for d in _ALLOWED_DIRS)
            + " and the workspace-root instruction files ("
            + ", ".join(sorted(_ALLOWED_ROOT_FILES))
            + "); runtime state (MEMORY.md, TASK.md, memory/) and the rest of the "
            "repository are not reviewed configuration"
        )
    if ".git" in parts:
        return "git's own metadata is not configuration, and git will not track a path through .git/"
    if parts[-1] in _REFUSED_NAMES:
        return (
            f"{parts[-1]} decides how git renders and tracks everything beside it; "
            "marking the subtree binary or generated would collapse the diff this "
            "whole flow exists to put in front of a reviewer"
        )
    if _is_code_name(parts[-1]) and not _is_gate_plugin(parts):
        return (
            "a proposal carries configuration, not code. The only executable file "
            "it may add is a cron gate plugin at config/cron/gates/<name>.py"
        )
    return None


def _safe_dst(root: Path, rel: str) -> Path | None:
    """Resolve ``rel`` under ``root``, or None if it must not be written there.

    Re-checks :func:`_proposal_path_problem` — which only ever sees a string —
    against the tree the change will actually land in. The worktree is checked
    out from the remote, git tracks symlinks, and a reviewed bundle carrying
    ``config/cron/gates`` as a symlink would make an allowed-looking path resolve
    somewhere else entirely. Containment is judged with the same rule lockdown
    uses for its own path guards.

    Landing inside the worktree is not enough — the path it lands *at* has to be
    one a proposal may write too, or a symlink between two tracked directories
    would let ``config/cron/gates/x.py`` (code, allowed there) be written as
    ``skills/x.py`` (code, refused there).
    """
    if _proposal_path_problem(rel):
        return None
    # One resolved root for both questions below. _is_within resolves whatever
    # it is handed, so the raw root answers the same today — but a guard that
    # judges containment against one spelling of the root and re-checks scope
    # relative to another holds only for as long as the helper keeps resolving
    # on the caller's behalf, and would go quiet, not loud, if it stopped.
    root_resolved = root.resolve()
    dst = (root / rel).resolve()
    if dst == root_resolved or not _is_within(dst, root_resolved):
        return None
    if _proposal_path_problem(str(dst.relative_to(root_resolved))):
        return None
    return dst


def _settings_mapping(text: str) -> dict | None:
    """``text`` read as a settings mapping, or ``None`` if it is not one.

    An empty file is a mapping with no keys, not a shape we failed to read: YAML
    parses it to ``None`` and the loader treats it as "sets nothing", which is a
    definite answer about every key in it.
    """
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError:
        return None
    if parsed is None:
        return {}
    return parsed if isinstance(parsed, dict) else None


def _lookup(parsed: dict, key: tuple[str, ...]):
    """The value at a dotted key path, or :data:`_ABSENT` if nothing states it."""
    node = parsed
    for part in key:
        if not isinstance(node, dict) or part not in node:
            return _ABSENT
        node = node[part]
    return node


def _effectful_settings_keys(content: str) -> list[str]:
    """Watched keys present in a proposed ``config/settings.yaml``.

    Presence, not truthiness. A watched key stated with a falsey value is still
    a change to what runs, and is often the direction that matters, because the
    loader does not read "empty" as "unset": ``cron: {gate_plugins_dir: ''}``
    resolves to ``Path('.')`` — the daemon's working directory, every ``*.py``
    in it imported and executed at start-up and on each cron reload — and
    ``mcp_servers: {}`` withdraws the servers the merged config used to name.
    Both parse and validate clean, so this notice is the only place a reviewer
    would hear about either. A bare ``key:`` counts as stated too: the proposal
    put the key there, and what the loader makes of the null is exactly what the
    review is for.

    Unparseable or oddly-shaped content yields nothing: validation is about to
    reject the change anyway, and guessing at a broken file would announce
    effects the merged config would never have.
    """
    parsed = _settings_mapping(content)
    if parsed is None:
        return []
    return [
        ".".join(key)
        for key in _EFFECTFUL_SETTINGS_KEYS
        if _lookup(parsed, key) is not _ABSENT
    ]


def _current_settings(dst: Path) -> dict | object:
    """What ``config/settings.yaml`` says *before* this proposal, or ``_UNKNOWN``.

    The staged worktree is checked out from the base branch and the file has not
    been overwritten yet, so what is on disk here is exactly the revision the
    pull request will be diffed against — which is the only thing that can say
    whether a proposal moves a setting or restates it.

    A file that is not there is not unknown: the base branch states none of these
    keys, and a proposal that also states none of them changes nothing. A file
    that *is* there and cannot be read or parsed is genuinely unknown, and says
    so — the revision nobody could read is precisely the one that might have had
    ``lockdown: true`` in it, and staying quiet about that is the failure this
    check exists to avoid.
    """
    if not dst.exists():
        return {}
    try:
        text = dst.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return _UNKNOWN
    parsed = _settings_mapping(text)
    return _UNKNOWN if parsed is None else parsed


def _shown(value) -> str:
    """A watched value as the notice should spell it.

    Booleans in the file's own spelling, not Python's, so the notice quotes what
    the reviewer will find on the line. "not stated" is not the same phrase as
    ``false`` on purpose: it is the case a presence check cannot see, and naming
    it is most of the point of reporting the transition at all.
    """
    if value is _ABSENT:
        return "not stated"
    if isinstance(value, bool):
        return "true" if value else "false"
    return "null" if value is None else repr(value)


def _security_settings_change(content: str, dst: Path) -> str | None:
    """Why a proposal's security keys need a reviewer's eye, or ``None``.

    Change against the base branch, not presence — see the note on
    :data:`_SECURITY_SETTINGS_KEYS`. "Change" is judged on the *stated* value, so
    absent and ``false`` are different even though both resolve to off: adding
    ``lockdown: false`` to a file that never mentioned it puts a new line in the
    diff about a control that is now pinned off in the tracked config, and that
    is worth one notice on the one proposal that does it. Restating it on every
    later proposal is not, and does not fire.

    Unreadable *proposed* content yields nothing, for the same reason
    :func:`_effectful_settings_keys` does: validation will reject it and no PR
    will be opened. Unreadable *current* content is the opposite case and is
    reported — see :func:`_current_settings`.
    """
    parsed = _settings_mapping(content)
    if parsed is None:
        return None
    current = _current_settings(dst)
    if current is _UNKNOWN:
        names = ", ".join(".".join(key) for key in _SECURITY_SETTINGS_KEYS)
        return (
            f"may change {names}: the revision in the base branch could not be "
            "read or parsed, so this could not be compared and is reported "
            "rather than assumed harmless"
        )
    moved = []
    for key, why in _SECURITY_SETTINGS_KEYS.items():
        before, after = _lookup(current, key), _lookup(parsed, key)
        if before != after:
            moved.append(
                f"{'.'.join(key)} ({_shown(before)} → {_shown(after)}), which {why}"
            )
    return "changes " + "; ".join(moved) if moved else None


def _executable_effect(staged: str, dst: Path, content: str) -> str | None:
    """Why the reviewer should look hard at this change, or ``None``.

    Judged against ``dst`` — where the file actually lands and what is already
    there — rather than the path the caller asked for, because a tracked symlink
    can make the two differ and the announcement has to describe the change that
    is really being made.

    Reasons accumulate rather than short-circuit: a settings file the repository
    happens to mark executable would otherwise be announced for its mode alone,
    and the lockdown flip inside it would go unmentioned.
    """
    reasons: list[str] = []
    if _is_code_name(Path(staged).name):
        reasons.append("the daemon imports and runs this file")
    elif dst.exists() and dst.stat().st_mode & 0o111:
        # write_text keeps an existing file's mode, so replacing a tracked 755
        # file ships new code under the old permissions with no mode change in
        # the diff to give it away.
        reasons.append("replaces a file the repository marks executable")
    if staged == _SETTINGS_FILE:
        keys = _effectful_settings_keys(content)
        if keys:
            reasons.append(
                f"declares {', '.join(keys)}, which name things the daemon runs"
            )
        changed = _security_settings_change(content, dst)
        if changed:
            reasons.append(changed)
    return "; ".join(reasons) or None


def _body_with_code_notice(body: str, effects: dict[str, str]) -> str:
    """Put the executable effects at the top of the PR description."""
    if not effects:
        return body
    notice = (
        "**This proposal changes what runs on the instance.** Config validation "
        "never executes or parses a bundle's own code, and never judges whether a "
        "protection should be switched off, so this review is the only check on "
        "the following:\n\n"
        + "\n".join(f"- `{p}` — {why}" for p, why in effects.items())
    )
    return f"{notice}\n\n---\n\n{body}" if body else notice


def propose_config_change(
    workspace: Path,
    config_dir: Path,
    title: str,
    body: str,
    changes: list[dict],
    now: int,
    branch: str | None = None,
) -> ProposeResult:
    """Open a PR against the workspace repo with the given file changes.

    ``changes`` is a list of ``{"path": <relative to workspace>, "content": <str>}``.
    Every path must be inside the proposable surface (see
    :func:`_proposal_path_problem`); one that is not rejects the whole proposal.
    The change is staged in a temp worktree off origin's default branch (see
    :func:`_remote_default_branch`, never the local ``HEAD``), validated, and —
    only if valid — pushed as a branch with a PR opened via ``gh``. ``now`` is a
    unix timestamp used to make the branch name unique. Never raises.
    """
    workspace = Path(workspace)
    if not is_git_repo(workspace):
        return ProposeResult(ok=False, message=f"{workspace} is not a git repository")
    if not _git(["remote"], workspace).stdout.strip():
        return ProposeResult(ok=False, message="workspace has no git remote configured")
    if not changes:
        return ProposeResult(ok=False, message="no changes provided")

    # Screen every path BEFORE touching git. Refused paths reject the whole
    # proposal rather than being dropped from it: dropping would open a PR whose
    # title and body describe a change it does not contain, and would let a
    # refused path go unnoticed by the agent and the reviewer both.
    refused: list[str] = []
    for ch in changes:
        rel = ch.get("path") if isinstance(ch, dict) else None
        if not rel or "content" not in ch:
            return ProposeResult(ok=False, message="each change needs a 'path' and 'content'")
        problem = _proposal_path_problem(str(rel))
        if problem:
            refused.append(f"{rel!r}: {problem}")
    if refused:
        return ProposeResult(
            ok=False,
            message=(
                f"refused {len(refused)} of {len(changes)} proposed path(s) — the "
                "proposal is rejected as a whole, nothing was dropped silently:\n"
                + "\n".join(f"- {r}" for r in refused)
            ),
        )

    # One resolved name for the base, used for the fetch, the worktree it is
    # branched from and the PR's --base, so the three can never disagree.
    base_branch = _remote_default_branch(workspace)
    if not base_branch:
        return ProposeResult(
            ok=False,
            message=(
                "cannot work out origin's default branch, so there is no base to "
                "propose against — origin is unreachable and the workspace has no "
                "cached refs/remotes/origin/HEAD. Guessing (say 'main') would "
                "branch off, and target, whatever that name happens to mean in "
                "this repository. Run 'git remote set-head origin -a' in the "
                "workspace, or make origin reachable, and retry."
            ),
        )
    branch = branch or _slug(title, now)

    fetch = _git(["fetch", "origin", base_branch], workspace)
    if fetch.returncode != 0:
        return ProposeResult(
            ok=False, message=f"git fetch failed: {fetch.stderr.strip()}",
        )

    tmp = Path(tempfile.mkdtemp(prefix=".nerve-pr-", dir=str(workspace.parent)))
    wt = tmp / "wt"
    add = _git(["worktree", "add", "-b", branch, str(wt), f"origin/{base_branch}"], workspace)
    if add.returncode != 0:
        shutil.rmtree(tmp, ignore_errors=True)
        return ProposeResult(ok=False, branch=branch, message=f"worktree add failed: {add.stderr.strip()}")

    try:
        # Apply the proposed file contents into the staged worktree, classifying
        # each change by where it lands and what was already there.
        effects: dict[str, str] = {}
        for ch in changes:
            dst = _safe_dst(wt, ch["path"])
            if dst is None:  # already screened upfront; defense-in-depth
                return ProposeResult(
                    ok=False, branch=branch,
                    message=(
                        f"{ch['path']!r} does not land where it claims to inside the "
                        "staged worktree (a symlink in the repo?) — no PR opened"
                    ),
                )
            staged = dst.relative_to(wt.resolve()).as_posix()
            content = str(ch["content"])
            effect = _executable_effect(staged, dst, content)
            if effect:
                effects[staged] = effect
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(content, encoding="utf-8")

        code_paths = list(effects)
        body = _body_with_code_notice(body, effects)
        if effects:
            logger.warning(
                "propose_config_change: %s changes what runs on this instance: %s",
                branch, ", ".join(f"{p} ({why})" for p, why in effects.items()),
            )

        # Validate the proposed bundle — never open a PR for a broken config.
        from nerve.config_validate import validate_config_bundle
        errors = validate_config_bundle(config_dir, workspace_override=wt).errors
        if errors:
            return ProposeResult(
                ok=False, branch=branch, validation_errors=errors, code_paths=code_paths,
                message=f"proposed change is invalid ({len(errors)} error(s)) — no PR opened",
            )

        _git(["add", "-A"], wt)
        commit_args = [*_committer_args(wt), "commit", "-m", title]
        if body:
            commit_args += ["-m", body]
        commit = _git(commit_args, wt)
        if commit.returncode != 0:
            return ProposeResult(
                ok=False, branch=branch,
                message=f"nothing to commit or commit failed: {commit.stderr.strip() or commit.stdout.strip()}",
            )

        push = _git(["push", "-u", "origin", branch], wt)
        if push.returncode != 0:
            return ProposeResult(ok=False, branch=branch, message=f"git push failed: {push.stderr.strip()}")

        pr = _gh(
            ["pr", "create", "--title", title, "--body", body or title,
             "--head", branch, "--base", base_branch],
            wt,
        )
        if pr.returncode != 0:
            return ProposeResult(
                ok=False, branch=branch,
                message=(
                    f"branch pushed but 'gh pr create' failed: {pr.stderr.strip()}. "
                    "Open the PR manually."
                ),
            )
        pr_url = pr.stdout.strip().splitlines()[-1] if pr.stdout.strip() else ""
        return ProposeResult(
            ok=True, branch=branch, pr_url=pr_url, code_paths=code_paths,
            message=f"opened PR for {branch}: {pr_url}",
        )
    except Exception as e:  # noqa: BLE001 — keep the "never raises" contract
        logger.error("propose_config_change failed: %s", e)
        return ProposeResult(ok=False, branch=branch, message=f"internal error: {e}")
    finally:
        # Remove the worktree AND the local branch it created — otherwise a
        # dangling nerve-config/* branch accumulates and blocks a same-name retry.
        # (The pushed remote branch, which the PR targets, is unaffected.)
        _git(["worktree", "remove", "--force", str(wt)], workspace)
        _git(["branch", "-D", branch], workspace)
        shutil.rmtree(tmp, ignore_errors=True)
