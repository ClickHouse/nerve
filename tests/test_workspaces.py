"""nerve.agent.workspaces: checkout resolution and git-command targets."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from nerve.agent import workspaces


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=cwd, check=True, capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    main = tmp_path / "proj"
    (main / "src").mkdir(parents=True)
    _git(main, "init", "-b", "main")
    (main / "src" / "a.py").write_text("x = 1\n")
    _git(main, "add", ".")
    _git(main, "commit", "-m", "init")
    return main.resolve()


def test_resolve_main_linked_and_detached_checkouts(repo: Path):
    _git(repo, "worktree", "add", "-b", "feat", str(repo.parent / "proj-wt-feat"))
    _git(repo, "worktree", "add", "--detach", str(repo.parent / "proj-wt-detached"))

    main = workspaces.resolve_checkout(str(repo / "src" / "a.py"))
    assert main == workspaces.Checkout(str(repo), "main")

    linked = workspaces.resolve_checkout(str(repo.parent / "proj-wt-feat" / "src" / "new.py"))
    assert linked == workspaces.Checkout(str(repo.parent / "proj-wt-feat"), "feat")

    detached = workspaces.resolve_checkout(str(repo.parent / "proj-wt-detached"))
    assert detached is not None and detached.branch is None


def test_resolve_outside_git_and_relative_paths(tmp_path: Path):
    assert workspaces.resolve_checkout(str(tmp_path / "plain" / "f.txt")) is None
    assert workspaces.resolve_checkout("relative/f.txt") is None
    assert workspaces.resolve_checkout("") is None


@pytest.mark.parametrize("command, expected", [
    ("git -C /r/a commit -m 'x'", ["/r/a"]),
    ("cd /r/a && git rebase origin/main", ["/r/a"]),
    ("lockf -t 600 /r/a_agent.lock git -C /r/a switch -c topic", ["/r/a"]),
    ("GIT_EDITOR=true git rebase --continue 2>&1 | tail", ["/cwd"]),
    ("cd $HOME/dev/app && git commit -m x", ["/h/dev/app"]),
    ("git -C /r/app+v2 commit", ["/r/app+v2"]),
    ("/bin/zsh -lc 'cd /r/a && git commit -m x'", ["/r/a"]),
    ("/bin/zsh -lc cd /r/a && git commit -m x", ["/r/a"]),
    ("cd /r/a && git add . && git commit -m \"$(cat <<'EOF'\nit's cut", ["/r/a"]),
    ("git stash list; git -C /r/a stash show; git -C /r/b stash pop", ["/r/b"]),
    ("git -C /r/a worktree add -b topic \\\n  /r/a-wt-topic origin/main", ["/r/a-wt-topic"]),
    ("cd /r/a  # the repo\ngit commit -m x", ["/r/a"]),
    ("git -C /r/a worktree add -b topic ../a-wt-topic origin/main", ["/r/a-wt-topic"]),
    ("git -C /r/a log --oneline; git -C /r/a status", []),
    ("cd /r/a\ngit add -A\ncd /r/b && git stash", ["/r/a", "/r/b"]),
    ("git -C sub -c core.x=1 checkout main", ["/cwd/sub"]),
    ("git -C /r/a worktree list", []),
    ("echo 'unbalanced", []),
    ("ls -la", []),
])
def test_git_targets(command: str, expected: list[str], monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HOME", "/h")
    assert workspaces.git_targets(command, "/cwd") == expected


def test_git_targets_without_a_known_cwd_need_an_explicit_directory():
    assert workspaces.git_targets("git commit -m x", None) == []
    assert workspaces.git_targets("git -C sub commit -m x", None) == []
    assert workspaces.git_targets("cd /r/a && git -C sub commit", None) == ["/r/a/sub"]


def test_tool_paths(tmp_path: Path):
    assert workspaces.tool_paths("Edit", {"file_path": "/r/a/x.py"}) == ["/r/a/x.py"]
    assert workspaces.tool_paths("NotebookEdit", {"notebook_path": "n.ipynb"}) == []
    assert workspaces.tool_paths("Bash", {"command": "git commit -m x", "cwd": str(tmp_path)}) == [str(tmp_path)]
    assert workspaces.tool_paths("Bash", {"command": "git -C \"$d\" pull", "cwd": str(tmp_path)}) == []
    assert workspaces.tool_paths("Bash", {"command": "git commit -m x"}) == []
    assert workspaces.tool_paths("Read", {"file_path": "/r/a/x.py"}) == []


def test_entered_worktree():
    result = "Created worktree at: /r/a/.claude/worktrees/topic on branch worktree-topic."
    assert workspaces.entered_worktree(result) == "/r/a/.claude/worktrees/topic"
    assert workspaces.entered_worktree("Already in a worktree session.") is None


def test_describe_rows_reads_current_branch_and_skips_the_nerve_workspace(repo: Path, tmp_path: Path):
    _git(repo, "worktree", "add", "-b", "feat", str(repo.parent / "proj-wt-feat"))
    linked = str(repo.parent / "proj-wt-feat")
    rows = [
        {"toplevel": linked, "branch": "stale"},
        {"toplevel": str(tmp_path / "gone"), "branch": "old"},
        {"toplevel": str(repo), "branch": "main"},
    ]

    described = workspaces.describe_rows(rows, repo / "workspace")

    assert described == [
        {"path": linked, "branch": "stale", "current_branch": "feat", "exists": True},
        {"path": str(tmp_path / "gone"), "branch": "old", "current_branch": None, "exists": False},
    ]
