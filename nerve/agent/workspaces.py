"""Which git checkouts a session works in, inferred from the tool calls it makes."""

from __future__ import annotations

import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path

FILE_TOOLS = frozenset({"Edit", "Write", "NotebookEdit"})

# Git subcommands that change a checkout's files, index or HEAD.
_MUTATING_GIT = frozenset({
    "add", "am", "apply", "checkout", "cherry-pick", "clean", "commit", "merge",
    "mv", "pull", "rebase", "reset", "restore", "revert", "rm", "stash", "switch",
})
_READ_ONLY_STASH = frozenset({"list", "show"})
_GIT_OPTS_WITH_VALUE = frozenset({"-c", "--git-dir", "--work-tree", "--namespace"})
_WORKTREE_ADD_OPTS_WITH_VALUE = frozenset({"-b", "-B", "--reason"})
_SHELLS = frozenset({"sh", "bash", "zsh"})
_OPERATORS = "();<>|&\n"
_ENTERED_WORKTREE_RE = re.compile(r"worktree at:?\s+(\S+)")


@dataclass(frozen=True)
class Checkout:
    toplevel: str
    branch: str | None


def resolve_checkout(path: str) -> Checkout | None:
    """The git working tree containing ``path``; ``None`` outside git."""
    if not path or not os.path.isabs(path):
        return None
    start = Path(path).resolve()
    for directory in (start, *start.parents):
        dot_git = directory / ".git"
        if dot_git.is_dir():
            return Checkout(str(directory), read_branch(dot_git))
        if dot_git.is_file():
            git_dir = _linked_git_dir(dot_git)
            return Checkout(str(directory), read_branch(git_dir)) if git_dir else None
    return None


def read_branch(git_dir: Path) -> str | None:
    """The checked-out branch, or ``None`` for a detached or unreadable HEAD."""
    try:
        head = (git_dir / "HEAD").read_text().strip()
    except (OSError, ValueError):
        return None
    prefix = "ref: refs/heads/"
    return head[len(prefix):] if head.startswith(prefix) else None


def _linked_git_dir(dot_git: Path) -> Path | None:
    try:
        content = dot_git.read_text().strip()
    except (OSError, ValueError):
        return None
    if not content.startswith("gitdir:"):
        return None
    return (dot_git.parent / content[len("gitdir:"):].strip()).resolve()


def is_nerve_workspace(toplevel: str, workspace: str | Path) -> bool:
    """A checkout that is, or contains, Nerve's own workspace, where every session works."""
    return _contains(toplevel, str(Path(workspace).expanduser().resolve()))


def _contains(outer: str, inner: str) -> bool:
    return inner == outer or inner.startswith(outer.rstrip(os.sep) + os.sep)


def describe_rows(rows: list[dict], workspace: str | Path) -> list[dict]:
    """The API shape of a session's rows: ``branch`` is the one the session last saw, ``current_branch`` the checkout's now."""
    root = str(Path(workspace).expanduser().resolve())
    described = []
    for row in rows:
        if _contains(row["toplevel"], root):
            continue
        current = resolve_checkout(row["toplevel"]) if os.path.isdir(row["toplevel"]) else None
        exists = current is not None and current.toplevel == row["toplevel"]
        described.append({
            "path": row["toplevel"],
            "branch": row["branch"],
            "current_branch": current.branch if exists else None,
            "exists": exists,
        })
    return described


def tool_paths(tool: str, tool_input: dict) -> list[str]:
    """Absolute paths a successful tool call wrote to or changed git state in."""
    if tool in FILE_TOOLS:
        path = str(tool_input.get("file_path") or tool_input.get("notebook_path") or "")
        return [path] if os.path.isabs(path) else []
    if tool == "Bash" and isinstance(tool_input.get("command"), str):
        return [t for t in git_targets(tool_input["command"], tool_input.get("cwd")) if os.path.isdir(t)]
    return []


def entered_worktree(result: str) -> str | None:
    """The worktree path an ``EnterWorktree`` result reports."""
    match = _ENTERED_WORKTREE_RE.search(result)
    return match.group(1).rstrip(".,;") if match else None


def git_targets(command: str, cwd: str | None) -> list[str]:
    """Directories in which ``command`` runs a state-changing git subcommand; ``cwd`` is where it starts, when known."""
    if "git" not in command:
        return []
    targets: list[str] = []
    current = cwd
    for segment in _segments(command):
        if os.path.basename(segment[0]) in _SHELLS and len(segment) > 2 and segment[1] in ("-c", "-lc"):
            if len(segment) == 3:
                targets += git_targets(segment[2], current)
                continue
            segment = segment[2:]
        if segment[0] == "cd":
            current = _absolute(segment[1], current) if len(segment) > 1 else os.path.expanduser("~")
            continue
        git_at = next((i for i, tok in enumerate(segment) if os.path.basename(tok) == "git"), None)
        if git_at is not None:
            target = _git_target(segment[git_at + 1:], current)
            if target:
                targets.append(target)
    return list(dict.fromkeys(targets))


def _segments(command: str) -> list[list[str]]:
    lexer = shlex.shlex(command.replace("\\\n", " "), posix=True, punctuation_chars=_OPERATORS)
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    # shlex would swallow a comment's newline, merging the next command into the commented one.
    lexer.commenters = ""
    tokens: list[str] = []
    try:
        for tok in lexer:
            tokens.append(tok)
    except ValueError:
        pass
    segments: list[list[str]] = [[]]
    for tok in tokens:
        if tok and set(tok) <= set(_OPERATORS):
            segments.append([])
        else:
            segments[-1].append(tok)
    return [s for s in segments if s]


def _git_target(args: list[str], cwd: str | None) -> str | None:
    directory = cwd
    i = 0
    while i < len(args) and args[i].startswith("-"):
        if args[i] == "-C" and i + 1 < len(args):
            directory = _absolute(args[i + 1], directory)
            i += 2
        elif args[i] in _GIT_OPTS_WITH_VALUE:
            i += 2
        else:
            i += 1
    if i >= len(args):
        return None
    subcommand, rest = args[i], args[i + 1:]
    if subcommand == "stash" and rest[:1] and rest[0] in _READ_ONLY_STASH:
        return None
    if subcommand in _MUTATING_GIT:
        return directory
    if subcommand == "worktree" and rest[:1] == ["add"]:
        new_path = _first_positional(rest[1:])
        return _absolute(new_path, directory) if new_path else None
    return None


def _first_positional(args: list[str]) -> str | None:
    i = 0
    while i < len(args):
        if args[i] in _WORKTREE_ADD_OPTS_WITH_VALUE:
            i += 2
        elif args[i].startswith("-"):
            i += 1
        else:
            return args[i]
    return None


def _absolute(path: str, base: str | None) -> str | None:
    expanded = os.path.expandvars(os.path.expanduser(path))
    if os.path.isabs(expanded):
        return os.path.normpath(expanded)
    return os.path.normpath(os.path.join(base, expanded)) if base else None
