"""Tests for git-backed workspace sync."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

import nerve.sync_service as sync
from nerve.config import WorkspaceSyncConfig
from nerve.config_reload import reload_failures
from nerve.config_validate import ValidationResult
from nerve.sync_service import sync_workspace


def _cp(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def _valid(*_a, **_k):
    return ValidationResult()


def _invalid(*errors):
    return lambda *_a, **_k: ValidationResult(errors=list(errors))


class _FakeGit:
    """Scripts git responses for the fetch → rev-parse → merge flow."""

    def __init__(self, head, upstream, *, fetch_rc=0, merge_rc=0):
        self.head = head
        self.upstream = upstream
        self.fetch_rc = fetch_rc
        self.merge_rc = merge_rc
        self.calls = []
        self.merged = False

    def __call__(self, args, cwd):
        self.calls.append(args)
        if args[0] == "rev-parse":
            ref = args[1]
            if ref == "HEAD":
                return _cp(stdout=self.upstream if self.merged else self.head)
            return _cp(stdout=self.upstream)  # origin/<branch> or @{u}
        if args[0] == "fetch":
            return _cp(returncode=self.fetch_rc, stderr="" if not self.fetch_rc else "fetch err")
        if args[0] == "merge":
            self.merged = True
            return _cp(returncode=self.merge_rc, stderr="" if not self.merge_rc else "not ff")
        return _cp()

    def did(self, verb):
        return any(a and a[0] == verb for a in self.calls)


class TestSyncWorkspaceOrchestration:
    def _repo(self, tmp_path):
        ws = tmp_path / "ws"
        (ws / ".git").mkdir(parents=True)
        return ws

    def test_not_a_git_repo(self, tmp_path):
        result = sync_workspace(tmp_path / "ws", tmp_path / "cfg")
        assert not result.ok and "not a git repository" in result.message

    def test_up_to_date(self, tmp_path, monkeypatch):
        ws = self._repo(tmp_path)
        monkeypatch.setattr(sync, "_git", _FakeGit(head="abc", upstream="abc"))
        result = sync_workspace(ws, tmp_path / "cfg")
        assert result.ok and not result.changed and result.message == "up to date"

    def test_fetch_failure(self, tmp_path, monkeypatch):
        ws = self._repo(tmp_path)
        monkeypatch.setattr(sync, "_git", _FakeGit(head="a", upstream="a", fetch_rc=1))
        result = sync_workspace(ws, tmp_path / "cfg")
        assert not result.ok and "git fetch failed" in result.message

    def test_no_upstream(self, tmp_path, monkeypatch):
        ws = self._repo(tmp_path)
        monkeypatch.setattr(sync, "_git", _FakeGit(head="a", upstream=""))
        result = sync_workspace(ws, tmp_path / "cfg")
        assert not result.ok and "upstream" in result.message

    def test_changed_valid_merges(self, tmp_path, monkeypatch):
        ws = self._repo(tmp_path)
        fake = _FakeGit(head="aaaaaaaa", upstream="bbbbbbbb")
        monkeypatch.setattr(sync, "_git", fake)
        monkeypatch.setattr(sync, "_validate_rev", _valid)
        result = sync_workspace(ws, tmp_path / "cfg", validate=True)
        assert result.ok and result.changed and "updated" in result.message
        assert fake.did("merge")  # working tree fast-forwarded

    def test_changed_invalid_does_not_merge(self, tmp_path, monkeypatch):
        """The core guarantee: an invalid fetched bundle is never merged into the
        live working tree."""
        ws = self._repo(tmp_path)
        fake = _FakeGit(head="aaaaaaaa", upstream="bbbbbbbb")
        monkeypatch.setattr(sync, "_git", fake)
        monkeypatch.setattr(sync, "_validate_rev", _invalid("bad backend"))
        result = sync_workspace(ws, tmp_path / "cfg", validate=True)
        assert not result.ok
        # `changed` reports whether the live tree moved, and it did not.
        assert not result.changed
        assert result.new_rev != result.old_rev  # the remote did have something new
        assert result.validation_errors == ["bad backend"]
        assert not fake.did("merge")  # NOT applied — tree untouched

    def test_branch_passed_through(self, tmp_path, monkeypatch):
        ws = self._repo(tmp_path)
        fake = _FakeGit(head="a", upstream="b")
        monkeypatch.setattr(sync, "_git", fake)
        monkeypatch.setattr(sync, "_validate_rev", _valid)
        sync_workspace(ws, tmp_path / "cfg", branch="main")
        assert ["fetch", "origin", "main"] in fake.calls
        assert ["rev-parse", "origin/main"] in fake.calls

    def test_ff_merge_failure(self, tmp_path, monkeypatch):
        ws = self._repo(tmp_path)
        monkeypatch.setattr(sync, "_git", _FakeGit(head="a", upstream="b", merge_rc=1))
        monkeypatch.setattr(sync, "_validate_rev", _valid)
        result = sync_workspace(ws, tmp_path / "cfg")
        assert not result.ok and "ff-only merge failed" in result.message


class _RealGit:
    """Drives real ``git`` against local repositories only — never a network
    remote. ``_pair`` builds an origin and a clone of it under ``tmp_path``.
    """

    def _git(self, *args, cwd):
        return subprocess.run(
            ["git", *args], cwd=str(cwd), check=True,
            capture_output=True, text=True,
            env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                 "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                 "HOME": str(cwd), "PATH": os.environ["PATH"]},
        )

    def _pair(self, tmp_path, settings="timezone: UTC\n"):
        """A local origin repo with a config bundle, plus a clone of it."""
        origin = tmp_path / "origin"
        origin.mkdir()
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config").mkdir()
        (origin / "config" / "settings.yaml").write_text(settings)
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)
        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)
        return origin, ws


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestSyncWorkspaceRealGit(_RealGit):
    def test_end_to_end_pull_valid(self, tmp_path):
        origin = tmp_path / "origin"
        origin.mkdir()
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config").mkdir()
        (origin / "config" / "settings.yaml").write_text("timezone: UTC\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)

        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)

        # New commit lands on the remote.
        (origin / "config" / "settings.yaml").write_text("timezone: Europe/Berlin\n")
        self._git("commit", "-am", "change tz", cwd=origin)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert result.ok and result.changed, result.message
        assert "Europe/Berlin" in (ws / "config" / "settings.yaml").read_text()

    def test_end_to_end_invalid_not_applied(self, tmp_path):
        origin = tmp_path / "origin"
        origin.mkdir()
        self._git("init", "-b", "main", cwd=origin)
        (origin / "config" / "cron").mkdir(parents=True)
        (origin / "config" / "settings.yaml").write_text("timezone: UTC\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "init", cwd=origin)

        ws = tmp_path / "ws"
        self._git("clone", str(origin), str(ws), cwd=tmp_path)

        # Push a broken cron file to the remote (new file → needs add).
        (origin / "config" / "cron" / "jobs.yaml").write_text("jobs: [ broken: yaml\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "break cron", cwd=origin)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert not result.ok and result.validation_errors
        # The live working tree was NOT fast-forwarded.
        assert not (ws / "config" / "cron" / "jobs.yaml").exists()

    def test_unset_env_ref_blocks_the_merge(self, tmp_path, monkeypatch):
        """A bundle the daemon could not load must not reach the working tree.

        ``${VAR}`` (no ``:-default``) is the *required* form: load_config raises
        on an unresolved one. Validation is lenient about those by default,
        because CI has no secrets — but sync runs in the daemon, with the
        daemon's environment, so leniency here just moves the failure to the
        next restart, after the checkout has already moved.
        """
        monkeypatch.delenv("NERVE_SYNC_TEST_SECRET", raising=False)
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text(
            "timezone: UTC\ntelegram:\n  bot_token: ${NERVE_SYNC_TEST_SECRET}\n"
        )
        self._git("commit", "-am", "needs a secret", cwd=origin)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert not result.ok
        assert any("NERVE_SYNC_TEST_SECRET" in e for e in result.validation_errors)
        assert "NERVE_SYNC_TEST_SECRET" not in (
            ws / "config" / "settings.yaml"
        ).read_text()

    def test_env_ref_resolved_in_the_environment_merges(self, tmp_path, monkeypatch):
        """The same bundle is fine once the variable is actually set."""
        monkeypatch.setenv("NERVE_SYNC_TEST_SECRET", "s3cret")
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text(
            "timezone: UTC\ntelegram:\n  bot_token: ${NERVE_SYNC_TEST_SECRET}\n"
        )
        self._git("commit", "-am", "needs a secret", cwd=origin)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert result.ok and result.changed, result.message

    def test_no_strict_env_lets_an_operator_pull_anyway(self, tmp_path, monkeypatch):
        """The escape hatch for a shell that lacks the daemon's environment."""
        monkeypatch.delenv("NERVE_SYNC_TEST_SECRET", raising=False)
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text(
            "timezone: UTC\ntelegram:\n  bot_token: ${NERVE_SYNC_TEST_SECRET}\n"
        )
        self._git("commit", "-am", "needs a secret", cwd=origin)

        result = sync_workspace(
            ws, tmp_path / "cfg", branch="main", validate=True, strict_env=False,
        )
        assert result.ok and result.changed, result.message

    def test_unverifiable_gate_type_merges_but_is_reported(self, tmp_path):
        """Validation does not load gate plugins, so an unrecognized gate type
        is a warning, not an error — the bundle merges. Sync has to say so:
        if nothing registers that type at run time the gate is dropped and the
        job runs unconditionally, and this is the only place anyone would see
        it before that happens."""
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "cron").mkdir()
        (origin / "config" / "cron" / "jobs.yaml").write_text(
            "jobs:\n"
            "  - id: nightly\n"
            "    schedule: '0 3 * * *'\n"
            "    prompt: go\n"
            "    run_if:\n"
            "      - type: not_a_builtin_gate\n"
        )
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "plugin gate", cwd=origin)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert result.ok and result.changed, result.message
        assert any("not_a_builtin_gate" in w for w in result.validation_warnings)

    def test_cli_reports_the_missing_var_and_honors_no_strict_env(
        self, tmp_path, monkeypatch,
    ):
        from click.testing import CliRunner

        import nerve.config as nerve_config
        from nerve.cli import main

        monkeypatch.setattr(nerve_config, "_config", None, raising=False)
        monkeypatch.delenv("NERVE_SYNC_TEST_SECRET", raising=False)
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text(
            "timezone: UTC\ntelegram:\n  bot_token: ${NERVE_SYNC_TEST_SECRET}\n"
        )
        self._git("commit", "-am", "needs a secret", cwd=origin)
        config_dir = tmp_path / "cfg"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n")
        args = ["-c", str(config_dir), "config", "sync", "--branch", "main"]

        blocked = CliRunner().invoke(main, args)
        assert blocked.exit_code == 1, blocked.output
        assert "NERVE_SYNC_TEST_SECRET" in blocked.output
        assert "Traceback" not in blocked.output

        pulled = CliRunner().invoke(main, [*args, "--no-strict-env"])
        assert pulled.exit_code == 0, pulled.output

    def test_non_utf8_git_output_does_not_raise(self, tmp_path):
        """git hands back the bytes it was given; a strict decode would crash."""
        _origin, ws = self._pair(tmp_path)
        git_config = ws / ".git" / "config"
        git_config.write_bytes(
            git_config.read_bytes() + b"\n[nervetest]\n\tval = \xff\xfe\n"
        )
        r = sync._git(["config", "--get", "nervetest.val"], ws)
        assert r.returncode == 0 and "�" in r.stdout

    def test_merge_without_validation_says_so(self, tmp_path):
        """validate=False can be reached from a config key, a CLI flag, or an
        env var exported empty. However it happened, the merge that skipped the
        check has to leave a trace."""
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text("timezone: Europe/Berlin\n")
        self._git("commit", "-am", "change tz", cwd=origin)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=False)
        assert result.ok and result.changed, result.message
        assert any("validation is disabled" in w for w in result.validation_warnings)


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestLocalChangesBlockTheMerge(_RealGit):
    """Validation judges a clean checkout of the fetched commit; the merge lands
    in the live tree. Everything that can differ between the two is covered here,
    because each one makes the bundle on disk something other than the bundle
    that passed — and ``--ff-only`` only objects when the incoming commit happens
    to touch the same path.
    """

    def _upstream_commit(self, origin, text="timezone: Europe/Berlin\n"):
        """A remote commit touching only settings.yaml, so a fast-forward of a
        workspace dirty *elsewhere* would otherwise succeed."""
        (origin / "config" / "settings.yaml").write_text(text)
        self._git("commit", "-am", "change tz", cwd=origin)

    def _sync(self, ws, tmp_path):
        return sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)

    def test_locally_modified_tracked_file_elsewhere_in_subtree(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "cron").mkdir()
        (origin / "config" / "cron" / "jobs.yaml").write_text("jobs: []\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "add cron", cwd=origin)
        self._git("pull", "-q", "--ff-only", "origin", "main", cwd=ws)
        self._upstream_commit(origin)
        # Dirty a tracked file the incoming commit does not touch, so git itself
        # would fast-forward happily.
        (ws / "config" / "cron" / "jobs.yaml").write_text("jobs: not-a-list\n")

        result = self._sync(ws, tmp_path)
        assert not result.ok and not result.changed
        assert "local changes" in result.message
        assert "jobs.yaml" in result.message
        assert "Europe/Berlin" not in (ws / "config" / "settings.yaml").read_text()

    def test_locally_deleted_tracked_file(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "extra.yaml").write_text("timezone: UTC\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "add extra", cwd=origin)
        self._git("pull", "-q", "--ff-only", "origin", "main", cwd=ws)
        self._upstream_commit(origin)
        (ws / "config" / "extra.yaml").unlink()

        result = self._sync(ws, tmp_path)
        assert not result.ok and "extra.yaml" in result.message

    def test_staged_but_uncommitted_change(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        (ws / "config" / "staged.yaml").write_text("timezone: UTC\n")
        self._git("add", "config/staged.yaml", cwd=ws)

        result = self._sync(ws, tmp_path)
        assert not result.ok and "staged.yaml" in result.message

    def test_tracked_file_swapped_for_a_symlink_out_of_the_repo(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        outside = tmp_path / "outside.yaml"
        outside.write_text("timezone: Pacific/Auckland\n")
        target = ws / "config" / "settings.yaml"
        target.unlink()
        target.symlink_to(outside)

        result = self._sync(ws, tmp_path)
        assert not result.ok and "settings.yaml" in result.message

    def test_untracked_gate_plugin_blocks_and_never_lands(self, tmp_path):
        """The one with teeth. Gate plugins are imported and executed by the
        daemon, validation never loads them by design, and an untracked one is
        invisible to the validation checkout — so a box that is supposed to run
        only reviewed remote config would run this, with sync reporting success.
        """
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        gates = ws / "config" / "cron" / "gates"
        gates.mkdir(parents=True)
        plugin = gates / "unreviewed.py"
        plugin.write_text("raise SystemExit('this would have run')\n")

        result = self._sync(ws, tmp_path)
        assert not result.ok and not result.changed
        assert "unreviewed.py" in result.message
        # Nothing was applied, so the operator is not left believing the box is
        # running the reviewed bundle.
        assert "Europe/Berlin" not in (ws / "config" / "settings.yaml").read_text()

    def test_ignored_file_warns_but_does_not_block(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        (origin / ".gitignore").write_text("config/*.local.yaml\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "ignore", cwd=origin)
        self._git("pull", "-q", "--ff-only", "origin", "main", cwd=ws)
        self._upstream_commit(origin)
        (ws / "config" / "secrets.local.yaml").write_text("token: xyz\n")

        result = self._sync(ws, tmp_path)
        assert result.ok and result.changed, result.message
        assert any("secrets.local.yaml" in w for w in result.validation_warnings)

    def test_ignored_gate_plugin_blocks_a_locked_instance(self, tmp_path):
        """A locked box promises that only reviewed remote config runs on it.

        An ignored file in the config subtree is invisible to the reviewer, to
        the validation checkout, and to the blocking check above — but a
        ``cron/gates/*.py`` there is imported and executed by the daemon all the
        same. On an ordinary box refusing over it would be sync policing what the
        machine may hold locally; a locked box has already settled that question,
        so the same file is a refusal there and a warning here.
        """
        origin, ws = self._pair(tmp_path)
        (origin / ".gitignore").write_text("config/cron/gates/local_*.py\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "ignore", cwd=origin)
        self._git("pull", "-q", "--ff-only", "origin", "main", cwd=ws)
        self._upstream_commit(origin)
        gates = ws / "config" / "cron" / "gates"
        gates.mkdir(parents=True)
        (gates / "local_unreviewed.py").write_text("MARKER = 1\n")

        before = self._git("rev-parse", "HEAD", cwd=ws).stdout.strip()
        unlocked = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert unlocked.ok, unlocked.message
        assert any("local_unreviewed.py" in w for w in unlocked.validation_warnings)

        # Same tree, same commit — only lockdown differs.
        self._git("reset", "-q", "--hard", before, cwd=ws)
        locked = sync_workspace(
            ws, tmp_path / "cfg", branch="main", validate=True, locked=True,
        )
        assert not locked.ok and not locked.changed
        assert "local_unreviewed.py" in locked.message
        assert "Europe/Berlin" not in (ws / "config" / "settings.yaml").read_text()

    def test_untracked_skill_blocks_the_merge(self, tmp_path):
        """A skill is instructions the model can invoke, with its own
        ``allowed-tools``, indexed on the next reload. Scoped to ``config/``, the
        check merged straight over one and reported the workspace clean.
        """
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        (ws / "skills" / "backdoor").mkdir(parents=True)
        (ws / "skills" / "backdoor" / "SKILL.md").write_text(
            "---\nname: backdoor\nallowed-tools: Bash\n---\n",
        )

        result = self._sync(ws, tmp_path)
        assert not result.ok and not result.changed
        assert "SKILL.md" in result.message
        assert "Europe/Berlin" not in (ws / "config" / "settings.yaml").read_text()

    def test_locally_edited_instruction_file_blocks_the_merge(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        (origin / "SOUL.md").write_text("reviewed\n")
        self._git("add", "-A", cwd=origin)
        self._git("commit", "-m", "add soul", cwd=origin)
        self._git("pull", "-q", "--ff-only", "origin", "main", cwd=ws)
        self._upstream_commit(origin)
        (ws / "SOUL.md").write_text("do whatever you like\n")

        result = self._sync(ws, tmp_path)
        assert not result.ok and not result.changed
        assert "SOUL.md" in result.message

    def test_submodule_blocks_a_locked_instance(self, tmp_path):
        """Same rule as the ignored file above, and for a sharper reason: the
        working copy is whatever this box happens to have checked out, validation
        saw an empty directory, and no fast-forward will ever move it."""
        origin, ws = self._pair(tmp_path)
        sha = self._git("rev-parse", "HEAD", cwd=origin).stdout.strip()
        self._git(
            "update-index", "--add", "--cacheinfo", f"160000,{sha},config/vendored",
            cwd=origin,
        )
        self._git("commit", "-m", "vendor config", cwd=origin)

        result = sync_workspace(
            ws, tmp_path / "cfg", branch="main", validate=True, locked=True,
        )
        assert not result.ok and not result.changed
        assert "vendored" in result.message

    def test_unset_env_var_is_reported_even_when_tolerated(self, tmp_path):
        """With strict_env off the validator files this as *info*, which nothing
        propagated — so the gate saw the one signal that predicts a failed
        post-merge reload and dropped it. Tolerating it is what the setting asks
        for; saying nothing about it is not."""
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text(
            "timezone: ${NO_SUCH_VAR_FOR_SYNC}\n",
        )
        self._git("commit", "-am", "env ref", cwd=origin)

        result = sync_workspace(
            ws, tmp_path / "cfg", branch="main", validate=True, strict_env=False,
        )
        assert result.ok and result.changed, result.message
        assert any(
            "NO_SUCH_VAR_FOR_SYNC" in w for w in result.validation_warnings
        ), result.validation_warnings

    def test_submodule_in_the_subtree_is_flagged_as_unvalidated(self, tmp_path):
        origin, ws = self._pair(tmp_path)
        sha = self._git("rev-parse", "HEAD", cwd=origin).stdout.strip()
        self._git(
            "update-index", "--add", "--cacheinfo", f"160000,{sha},config/vendored",
            cwd=origin,
        )
        self._git("commit", "-m", "vendor config", cwd=origin)

        result = self._sync(ws, tmp_path)
        assert result.ok, result.message
        assert any("vendored" in w and "submodule" in w
                   for w in result.validation_warnings)

    def test_submodule_with_a_non_ascii_name_is_named_readably(self, tmp_path):
        """Naming the path is the whole job of that warning.

        git C-escapes non-ASCII bytes in paths unless told not to, which would
        turn the one actionable word in the message into ``\\303\\274`` noise.
        """
        origin, ws = self._pair(tmp_path)
        sha = self._git("rev-parse", "HEAD", cwd=origin).stdout.strip()
        self._git(
            "update-index", "--add", "--cacheinfo", f"160000,{sha},config/sübmodül",
            cwd=origin,
        )
        self._git("commit", "-m", "vendor config", cwd=origin)

        result = self._sync(ws, tmp_path)
        assert result.ok, result.message
        warning = next(
            (w for w in result.validation_warnings if "submodule" in w), ""
        )
        assert "config/sübmodül" in warning, warning
        assert "\\303" not in warning, warning

    def test_changes_outside_the_config_subtree_do_not_block(self, tmp_path):
        """A nerve workspace is also the agent's working directory. Refusing on
        any dirt anywhere would refuse on nearly every real box."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        (ws / "notes.md").write_text("scratch\n")
        (ws / "memory").mkdir()
        (ws / "memory" / "today.md").write_text("things\n")

        result = self._sync(ws, tmp_path)
        assert result.ok and result.changed, result.message

    def test_unreadable_status_fails_closed(self, tmp_path, monkeypatch):
        """Not being able to establish that the tree is clean is not the same as
        the tree being clean."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        real = sync._git

        def refuse_status(args, cwd):
            if "status" in args:
                return subprocess.CompletedProcess(args, 128, "", "fatal: nope")
            return real(args, cwd)

        monkeypatch.setattr(sync, "_git", refuse_status)
        result = self._sync(ws, tmp_path)
        assert not result.ok and "local changes" in result.message
        assert "Europe/Berlin" not in (ws / "config" / "settings.yaml").read_text()


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestWorktreeHousekeeping(_RealGit):
    def test_abandoned_validation_worktree_is_swept(self, tmp_path):
        """``_validate_rev`` cleans up in a ``finally``, which covers exceptions
        but not SIGKILL. What is left behind is a directory *and* a live entry in
        .git/worktrees, so ``git worktree prune`` will never clear it — on a
        five-minute cadence that grows without bound."""
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text("timezone: Europe/Berlin\n")
        self._git("commit", "-am", "change tz", cwd=origin)

        # A validation worktree from a process that died mid-check.
        stale = tmp_path / ".nerve-sync-crashed"
        stale.mkdir()
        self._git("worktree", "add", "--detach", str(stale / "wt"), "HEAD", cwd=ws)
        os.utime(stale, (0, 0))  # older than the abandonment threshold
        registrations = ws / ".git" / "worktrees"
        assert list(registrations.iterdir())

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert result.ok and result.changed, result.message
        assert not stale.exists()
        # git drops the whole directory once the last worktree is unregistered.
        assert not registrations.exists() or not list(registrations.iterdir())

    def test_a_recent_worktree_is_left_alone(self, tmp_path):
        """A sync running in another process (``nerve config sync`` alongside the
        daemon loop) owns a directory this sweep must not delete under it."""
        origin, ws = self._pair(tmp_path)
        (origin / "config" / "settings.yaml").write_text("timezone: Europe/Berlin\n")
        self._git("commit", "-am", "change tz", cwd=origin)
        live = tmp_path / ".nerve-sync-inflight"
        live.mkdir()
        self._git("worktree", "add", "--detach", str(live / "wt"), "HEAD", cwd=ws)

        result = sync_workspace(ws, tmp_path / "cfg", branch="main", validate=True)
        assert result.ok, result.message
        assert (live / "wt").exists()


class TestConcurrency:
    def test_overlapping_syncs_are_serialized(self, tmp_path, monkeypatch):
        """The periodic loop and POST /api/config/sync both drive syncs. Left to
        race they trip git's own ref locks, and the failure surfaces as an
        ff-only merge failure — blaming fast-forwardability for contention."""
        ws = tmp_path / "ws"
        (ws / ".git").mkdir(parents=True)
        fake = _FakeGit(head="a", upstream="a")
        active: list[int] = []
        high_water: list[int] = []

        def tracking_git(args, cwd):
            active.append(1)
            high_water.append(len(active))
            time.sleep(0.01)  # long enough to overlap if nothing serializes
            try:
                return fake(args, cwd)
            finally:
                active.pop()

        monkeypatch.setattr(sync, "_git", tracking_git)
        threads = [
            threading.Thread(
                target=lambda: sync_workspace(ws, tmp_path / "cfg"),
            )
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert max(high_water) == 1


class TestWorkspaceSyncConfig:
    def test_defaults_disabled(self):
        c = WorkspaceSyncConfig.from_dict({})
        assert c.enabled is False and c.interval_minutes == 1 and c.validate is True

    def test_from_dict(self):
        c = WorkspaceSyncConfig.from_dict(
            {"enabled": True, "branch": "main", "interval_minutes": 10, "validate": False}
        )
        assert c.enabled and c.branch == "main" and c.interval_minutes == 10 and not c.validate

    def test_string_flags_are_parsed_not_tested_for_truthiness(self):
        """``${VAR}`` interpolation yields a string, and ``bool("false")`` is
        ``True`` — so a builder that casts eagerly turns every kill switch into
        an on switch."""
        c = WorkspaceSyncConfig.from_dict(
            {"enabled": "false", "validate": "0", "strict_env": "no",
             "interval_minutes": "15"}
        )
        assert c.enabled is False and c.validate is False and c.strict_env is False
        assert c.interval_minutes == 15

    def test_env_refs_can_turn_sync_off(self, tmp_path, monkeypatch):
        """End to end through load_config: the operator sets the variable to
        ``false`` and sync stays off."""
        from nerve.config import load_config

        monkeypatch.setenv("NERVE_SYNC_ENABLED", "false")
        monkeypatch.setenv("NERVE_SYNC_VALIDATE", "no")
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        (ws / "config" / "settings.yaml").write_text(
            "workspace_sync:\n"
            "  enabled: ${NERVE_SYNC_ENABLED}\n"
            "  validate: ${NERVE_SYNC_VALIDATE}\n"
        )
        (tmp_path / "config.yaml").write_text(f"workspace: {ws}\n")

        loaded = load_config(tmp_path).workspace_sync
        assert loaded.enabled is False and loaded.validate is False

    def test_validation_actually_skipped_when_env_says_off(self, tmp_path, monkeypatch):
        """The flag has to reach the code, not just the dataclass: a truthy
        ``"false"`` would make sync validate a bundle the operator told it not
        to — and the mirror of that mistake is a sync that skips the check."""
        from nerve.config import load_config

        monkeypatch.setenv("NERVE_SYNC_VALIDATE", "false")
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        (ws / "config" / "settings.yaml").write_text(
            "workspace_sync:\n  validate: ${NERVE_SYNC_VALIDATE}\n"
        )
        (tmp_path / "config.yaml").write_text(f"workspace: {ws}\n")
        loaded = load_config(tmp_path).workspace_sync

        repo = tmp_path / "repo"
        (repo / ".git").mkdir(parents=True)
        calls = []
        monkeypatch.setattr(sync, "_git", _FakeGit(head="a", upstream="b"))
        monkeypatch.setattr(
            sync, "_validate_rev",
            lambda *a, **k: calls.append(a) or ValidationResult(),
        )
        result = sync_workspace(repo, tmp_path / "cfg", validate=loaded.validate)
        assert result.ok and not calls


class TestApplySync:
    @pytest.mark.asyncio
    async def test_apply_triggers_both_reloads(self, tmp_path):
        cron, engine = AsyncMock(), AsyncMock()
        await sync._apply_sync(engine, cron, tmp_path)
        cron.reload.assert_awaited_once()
        engine.reload_mcp_config.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_apply_reloads_sources_and_skills(self, tmp_path):
        """Sync applies the SAME subsystems as a manual reload (sources+skills)."""
        cron = AsyncMock()
        engine = MagicMock()
        engine.reload_mcp_config = AsyncMock(return_value=[])
        engine._skill_manager.discover = AsyncMock(return_value=[])
        await sync._apply_sync(engine, cron, tmp_path)
        cron.reload_sources.assert_awaited_once()
        engine._skill_manager.discover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_apply_survives_reload_error(self, tmp_path):
        cron = AsyncMock()
        cron.reload.side_effect = RuntimeError("boom")
        engine = AsyncMock()
        await sync._apply_sync(engine, cron, tmp_path)  # must not raise
        engine.reload_mcp_config.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_apply_reloads_config_singleton_engages_lockdown(self, tmp_path, monkeypatch):
        """A synced lockdown flip must engage without a restart."""
        import nerve.config as cfgmod
        from nerve.config import is_locked, workspace_settings_file

        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir(parents=True)
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        workspace_settings_file(ws).write_text(
            "lockdown: true\nauth:\n  jwt_secret: x\n", encoding="utf-8"
        )
        # A locked workspace has to be a repo with a remote or the load refuses;
        # this one is synced, so it is one.
        (ws / ".git").mkdir()
        monkeypatch.setattr(sync, "_git", lambda args, cwd: _cp(stdout="origin\n"))
        monkeypatch.setattr(cfgmod, "_config", cfgmod.NerveConfig(lockdown=False))
        assert not is_locked()
        summary = await sync._apply_sync(AsyncMock(), AsyncMock(), config_dir)
        assert "config" not in reload_failures(summary)
        assert is_locked()  # the reloaded config engaged lockdown

    @pytest.mark.asyncio
    async def test_apply_reports_when_lockdown_fails_to_engage(self, tmp_path, monkeypatch):
        """The merge landed `lockdown: true` and the daemon could not load it, so
        the box is *not* locked. That has to come back to the caller: reporting a
        clean success here tells an operator the write guards are closed while
        they are wide open."""
        import nerve.config as cfgmod
        from nerve.config import is_locked, workspace_settings_file

        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir(parents=True)
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        # Locks the box and names a variable this environment does not have, so
        # load_config raises on the merged result.
        workspace_settings_file(ws).write_text(
            "lockdown: true\nauth:\n  jwt_secret: ${NO_SUCH_SECRET_HERE}\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(cfgmod, "_config", cfgmod.NerveConfig(lockdown=False))
        summary = await sync._apply_sync(AsyncMock(), AsyncMock(), config_dir)
        failures = reload_failures(summary)
        assert "NO_SUCH_SECRET_HERE" in failures["config"]
        assert not is_locked()  # still running the old config — which the caller must say

    @pytest.mark.asyncio
    async def test_route_does_not_claim_success_when_apply_failed(self, tmp_path, monkeypatch):
        import nerve.gateway.server as srv

        import nerve.gateway.routes.config as route_mod
        from nerve.sync_service import SyncResult

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace = str(tmp_path / "ws")
        fake_cfg.config_dir = str(tmp_path / "cfg")
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True)
        fake_cfg.lockdown = False
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(
            route_mod, "get_deps", lambda: type("D", (), {"engine": None})(),
        )
        monkeypatch.setattr(
            sync, "sync_workspace",
            lambda *a, **k: SyncResult(ok=True, changed=True, message="updated"),
        )

        async def _failing_apply(engine, cron_service, config_dir):
            return {"config": "error: ConfigError: nope"}

        monkeypatch.setattr(sync, "_apply_sync", _failing_apply)
        body = await route_mod.sync_workspace_route(user={})
        assert body["ok"] is False
        assert body["changed"] is True and body["applied"] is False
        assert body["apply_error"] == "ConfigError: nope"

    @pytest.mark.asyncio
    async def test_route_reports_a_partly_applied_merge(self, tmp_path, monkeypatch):
        """The config loaded, so the merged settings *are* live — but cron didn't
        take it, so the daemon is on the merged config only in part. Reporting a
        clean `applied` for that hides the one outcome an operator cannot see
        from the outside.
        """
        import nerve.gateway.server as srv

        import nerve.gateway.routes.config as route_mod
        from nerve.sync_service import SyncResult

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace = str(tmp_path / "ws")
        fake_cfg.config_dir = str(tmp_path / "cfg")
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True)
        fake_cfg.lockdown = False
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(
            route_mod, "get_deps", lambda: type("D", (), {"engine": None})(),
        )
        monkeypatch.setattr(
            sync, "sync_workspace",
            lambda *a, **k: SyncResult(ok=True, changed=True, message="updated"),
        )

        async def _partial_apply(engine, cron_service, config_dir):
            return {"config": "reloaded", "cron": "error: bad jobs.yaml"}

        monkeypatch.setattr(sync, "_apply_sync", _partial_apply)
        body = await route_mod.sync_workspace_route(user={})
        assert body["ok"] is True  # the merged config itself is in effect
        assert body["applied"] is False  # ...but not everywhere
        assert body["apply_error"] is None
        assert body["reload_errors"] == {"cron": "bad jobs.yaml"}


class TestNeverRaises:
    """``sync_workspace`` promises a SyncResult for every outcome.

    Two callers depend on it absolutely: the HTTP route, which has no handler
    and would answer 500 with a stack trace, and the CLI, which would print
    one. The daemon loop catches, but then logs a traceback where a config
    problem belongs. So the guarantee is checked against the operations that
    can actually fail, not against the ones someone remembered to wrap.
    """

    def _repo(self, tmp_path):
        ws = tmp_path / "ws"
        (ws / ".git").mkdir(parents=True)
        return ws

    @pytest.mark.parametrize("exc", [
        FileNotFoundError(2, "No such file or directory: 'git'"),  # git not installed
        PermissionError(13, "Permission denied"),
        NotADirectoryError(20, "Not a directory"),                 # cwd vanished
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
        OSError("something the OS refused"),
    ])
    def test_subprocess_failures_become_a_result(self, tmp_path, monkeypatch, exc):
        ws = self._repo(tmp_path)

        def boom(*_a, **_k):
            raise exc

        monkeypatch.setattr(sync.subprocess, "run", boom)
        result = sync_workspace(ws, tmp_path / "cfg")
        assert not result.ok and "git" in result.message

    def test_git_timeout_becomes_a_result(self, tmp_path, monkeypatch):
        def slow(*_a, **_k):
            raise subprocess.TimeoutExpired(["git"], 120)

        monkeypatch.setattr(sync.subprocess, "run", slow)
        result = sync_workspace(self._repo(tmp_path), tmp_path / "cfg")
        assert not result.ok and "timed out" in result.message

    def test_validator_exception_fails_closed(self, tmp_path, monkeypatch):
        """The validator reads YAML and builds typed config; both can raise.
        A sync that can't reach a verdict must refuse the merge, not crash."""
        import nerve.config_validate as cv

        ws = self._repo(tmp_path)
        fake = _FakeGit(head="aaaaaaaa", upstream="bbbbbbbb")
        monkeypatch.setattr(sync, "_git", fake)

        def boom(*_a, **_k):
            raise RuntimeError("validator blew up")

        monkeypatch.setattr(cv, "validate_config_bundle", boom)
        result = sync_workspace(ws, tmp_path / "cfg", validate=True)
        assert not result.ok
        assert any("validator blew up" in e for e in result.validation_errors)
        assert not fake.did("merge")  # fail closed

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_unwritable_worktree_parent_fails_closed(self, tmp_path, monkeypatch):
        """The throwaway worktree is created next to the repo; a read-only
        parent (or a full disk) must not take the daemon down."""
        holder = tmp_path / "holder"
        ws = holder / "ws"
        (ws / ".git").mkdir(parents=True)
        fake = _FakeGit(head="aaaaaaaa", upstream="bbbbbbbb")
        monkeypatch.setattr(sync, "_git", fake)
        os.chmod(holder, 0o500)
        try:
            result = sync_workspace(ws, tmp_path / "cfg", validate=True)
        finally:
            os.chmod(holder, 0o700)
        assert not result.ok and result.validation_errors
        assert not fake.did("merge")

    def test_unexpected_internal_failure_becomes_a_result(self, tmp_path, monkeypatch):
        """The outer guard: the contract covers the whole call, including the
        parts nobody anticipated."""
        def boom(*_a, **_k):
            raise ValueError("something nobody thought of")

        monkeypatch.setattr(sync, "_rev", boom)
        result = sync_workspace(self._repo(tmp_path), tmp_path / "cfg")
        assert not result.ok and "something nobody thought of" in result.message

    @pytest.mark.asyncio
    async def test_route_reports_400_not_500(self, tmp_path, monkeypatch):
        """The end the contract exists for: a broken bundle is a client-visible
        config error, not an internal server error."""
        import nerve.gateway.server as srv
        from fastapi import HTTPException

        import nerve.gateway.routes.config as route_mod

        ws = self._repo(tmp_path)
        fake_cfg = type("C", (), {})()
        fake_cfg.workspace = str(ws)
        fake_cfg.config_dir = str(tmp_path / "cfg")
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True, validate=True)
        fake_cfg.lockdown = False
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(sync, "_git", _FakeGit(head="aaaaaaaa", upstream="bbbbbbbb"))
        monkeypatch.setattr(
            sync, "_validate_rev",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("kaboom")),
        )
        with pytest.raises(HTTPException) as ei:
            await route_mod.sync_workspace_route(user={})
        assert ei.value.status_code == 400

class _FakeClock:
    """Replaces the loop's interval wait so cycles run without real time passing.

    The loop's only observable use of ``interval_minutes`` is the timeout it
    hands to ``asyncio.wait_for``, so that is recorded rather than inferred.
    ``cycles`` syncs run, then the stop event is set and the wait reports
    "stopped" exactly as the real one would.

    ``hook`` is called with the wait's index each time, which is the point
    *between* two cycles with the loop already started — the only place a test
    can change the world the way an operator or another process does.
    """

    def __init__(self, cycles: int, stop: asyncio.Event, hook=None):
        self.cycles = cycles
        self.stop = stop
        self.hook = hook
        self.timeouts: list[float] = []

    async def wait_for(self, coro, timeout=None):
        self.timeouts.append(timeout)
        coro.close()  # the loop passed stop_event.wait(); we never await it
        if self.hook is not None:
            self.hook(len(self.timeouts) - 1)
        if self.cycles <= 0:
            self.stop.set()
            return True
        self.cycles -= 1
        raise asyncio.TimeoutError


def _run_cycles(monkeypatch, cycles, hook=None):
    """Wire a fake clock into the loop and return ``(stop_event, clock)``."""
    stop = asyncio.Event()
    clock = _FakeClock(cycles, stop, hook)
    monkeypatch.setattr(sync.asyncio, "wait_for", clock.wait_for)
    return stop, clock


def _sync_double(recorder, then=None):
    """A ``sync_workspace`` stand-in that records the keywords it was called with,
    then optionally runs ``then()`` to change the world between cycles.

    Takes ``**kwargs`` deliberately: the loop calls ``sync_workspace`` by keyword,
    and a double that pins the positional shape turns a new parameter into an
    empty recording — swallowed by the loop's own except-and-continue — instead of
    a visible failure.
    """
    def double(_workspace, _config_dir, **kwargs):
        recorder.append(kwargs)
        if then is not None:
            then()
        return sync.SyncResult(ok=True, changed=False)

    return double


def _loop_config(**overrides):
    cfg = type("C", (), {})()
    cfg.workspace = overrides.pop("workspace", "/tmp/ws")
    cfg.config_dir = overrides.pop("config_dir", None)
    cfg.lockdown = overrides.pop("lockdown", False)
    overrides.setdefault("enabled", True)
    overrides.setdefault("interval_minutes", 60)
    cfg.workspace_sync = WorkspaceSyncConfig(**overrides)
    return cfg


def _write_config(config_dir, workspace, **sync_keys):
    config_dir.mkdir(parents=True, exist_ok=True)
    lines = [f"workspace: {workspace}", "workspace_sync:"]
    lines += [f"  {k}: {v}" for k, v in sync_keys.items()]
    (config_dir / "config.yaml").write_text("\n".join(lines) + "\n")


class TestPeriodicLoop:
    @pytest.mark.asyncio
    async def test_stop_event_exits_promptly(self):
        stop = asyncio.Event()
        stop.set()  # already stopped → loop returns without syncing
        await asyncio.wait_for(
            sync.run_periodic_sync(_loop_config(), AsyncMock(), AsyncMock(), stop),
            timeout=2,
        )

    @pytest.mark.asyncio
    async def test_editing_config_on_disk_does_not_reach_the_loop_on_its_own(
        self, tmp_path, monkeypatch,
    ):
        """A file changing on disk is not a reload, and deliberately so.

        The loop re-reads the process-wide config object every cycle, but only an
        explicit reload replaces that object, so editing config.yaml on the box
        and waiting achieves nothing. The companion test below covers what
        happens once a reload is actually asked for; ``docs/config.md`` says the
        same thing to operators, under "What triggers a reload".
        """
        import nerve.config as nerve_config

        config_dir = tmp_path / "cfg"
        _write_config(config_dir, tmp_path / "ws", enabled="true", branch="OLD")
        monkeypatch.setattr(
            nerve_config, "_config", nerve_config.load_config(config_dir),
        )
        started_with = nerve_config.get_config()

        seen: list[dict] = []
        monkeypatch.setattr(sync, "sync_workspace", _sync_double(
            seen,
            # An operator edits the file between cycles.
            then=lambda: _write_config(
                config_dir, tmp_path / "ws", enabled="true", branch="NEW",
            ),
        ))
        stop, _clock = _run_cycles(monkeypatch, 3)
        await sync.run_periodic_sync(started_with, AsyncMock(), AsyncMock(), stop)

        assert [k["branch"] for k in seen] == ["OLD", "OLD", "OLD"]
        assert nerve_config.get_config() is started_with

    @pytest.mark.asyncio
    async def test_a_reload_makes_the_loop_see_the_edited_file(
        self, tmp_path, monkeypatch,
    ):
        """Drives the real config object, not a stand-in for it.

        The loop's per-cycle re-read was correct in shape long before anything
        replaced the object it reads, and a test that patched ``get_config``
        would have passed throughout. So this one edits a real config.yaml, runs
        a real reload, and checks the branch the next cycle actually pulls.
        """
        import nerve.config as nerve_config
        from nerve.config_reload import reload_all

        config_dir = tmp_path / "cfg"
        _write_config(config_dir, tmp_path / "ws", enabled="true", branch="OLD")
        monkeypatch.setattr(
            nerve_config, "_config", nerve_config.load_config(config_dir),
        )
        started_with = nerve_config.get_config()

        seen: list[dict] = []

        def edit_then_reload():
            """Stand in for a hand edit followed by POST /api/config/reload.

            The loop hands ``sync_workspace`` to ``asyncio.to_thread``, so this
            runs on a worker thread with no event loop of its own — which is what
            lets it drive the real coroutine rather than a substitute for it.
            """
            if len(seen) > 1:
                return
            _write_config(
                config_dir, tmp_path / "ws", enabled="true", branch="NEW",
            )
            asyncio.run(reload_all(None, None, config_dir))

        monkeypatch.setattr(
            sync, "sync_workspace", _sync_double(seen, then=edit_then_reload),
        )
        stop, _clock = _run_cycles(monkeypatch, 3)
        await sync.run_periodic_sync(started_with, AsyncMock(), AsyncMock(), stop)

        assert [k["branch"] for k in seen] == ["OLD", "NEW", "NEW"]
        assert nerve_config.get_config() is not started_with

    @pytest.mark.asyncio
    async def test_loop_follows_the_current_config_object(self, monkeypatch):
        """The loop holds no snapshot of its own: whatever replaces the process
        config is what the next cycle uses. Nothing replaces it today (see the
        test above), so this is the guarantee that the loop is not the obstacle.
        """
        import nerve.config as nerve_config

        first = _loop_config(branch="old", interval_minutes=60)
        monkeypatch.setattr(nerve_config, "_config", first)
        seen: list[dict] = []
        monkeypatch.setattr(sync, "sync_workspace", _sync_double(
            seen,
            then=lambda: nerve_config.set_config(_loop_config(
                branch="new", validate=False, strict_env=False, interval_minutes=1,
            )),
        ))
        stop, clock = _run_cycles(monkeypatch, 2)
        await sync.run_periodic_sync(first, AsyncMock(), AsyncMock(), stop)

        assert [k["branch"] for k in seen] == ["old", "new"]
        assert [k["validate"] for k in seen] == [True, False]
        assert [k["strict_env"] for k in seen] == [True, False]
        # interval_minutes 60 → 1 shows up as the next wait's timeout.
        assert clock.timeouts[:3] == [3600, 3600, 60]

    @pytest.mark.asyncio
    async def test_disabling_sync_stops_pulling(self, monkeypatch):
        import nerve.config as nerve_config

        first = _loop_config()
        monkeypatch.setattr(nerve_config, "_config", first)
        calls: list[dict] = []
        monkeypatch.setattr(sync, "sync_workspace", _sync_double(
            calls,
            then=lambda: nerve_config.set_config(_loop_config(enabled=False)),
        ))
        stop, _clock = _run_cycles(monkeypatch, 3)
        await sync.run_periodic_sync(first, AsyncMock(), AsyncMock(), stop)
        assert len(calls) == 1  # first cycle only; then the flag went false

    @pytest.mark.asyncio
    async def test_unreadable_config_does_not_kill_the_loop(self, monkeypatch):
        """Re-reading is part of every cycle, so it must not be able to end the
        task — a loop that dies silently never syncs again."""
        def boom():
            raise RuntimeError("config gone")

        monkeypatch.setattr("nerve.config.get_config", boom)
        calls: list[dict] = []
        monkeypatch.setattr(sync, "sync_workspace", _sync_double(calls))
        stop, clock = _run_cycles(monkeypatch, 3)
        await sync.run_periodic_sync(_loop_config(), AsyncMock(), AsyncMock(), stop)
        assert clock.cycles == 0 and not calls  # kept looping, synced nothing

    @pytest.mark.asyncio
    async def test_failure_while_applying_does_not_kill_the_loop(self, monkeypatch):
        """Everything a cycle does is inside the guard, applying included."""
        import nerve.config as nerve_config

        monkeypatch.setattr(nerve_config, "_config", _loop_config())
        calls: list[dict] = []

        def double(_workspace, _config_dir, **kwargs):
            calls.append(kwargs)
            return sync.SyncResult(ok=True, changed=True, message="updated")

        monkeypatch.setattr(sync, "sync_workspace", double)

        async def explode(*_a, **_k):
            raise RuntimeError("reload machinery is broken")

        monkeypatch.setattr(sync, "_apply_sync", explode)
        stop, clock = _run_cycles(monkeypatch, 3)
        await sync.run_periodic_sync(
            nerve_config.get_config(), AsyncMock(), AsyncMock(), stop,
        )
        assert clock.cycles == 0 and len(calls) == 3


class TestSyncRoute:
    @pytest.mark.asyncio
    async def test_route_400_on_invalid(self, monkeypatch):
        import nerve.gateway.server as srv
        from fastapi import HTTPException

        import nerve.gateway.routes.config as route_mod
        from nerve.sync_service import SyncResult

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace = "/tmp/ws"
        fake_cfg.config_dir = "/tmp/cfg"
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True)
        fake_cfg.lockdown = False
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        # The route imports sync_workspace from nerve.sync_service at call time.
        monkeypatch.setattr(
            sync, "sync_workspace",
            lambda *a, **k: SyncResult(ok=False, message="bad", validation_errors=["e"]),
        )
        with pytest.raises(HTTPException) as ei:
            await route_mod.sync_workspace_route(user={})
        assert ei.value.status_code == 400

    @pytest.mark.asyncio
    async def test_route_applies_a_changed_sync(self, monkeypatch, tmp_path):
        """The success path, which the 400 tests never reach: it resolves the
        config dir before applying, so a name that only exists on that branch
        would otherwise surface as a 500 on the first sync that changed anything.
        """
        import nerve.gateway.server as srv

        import nerve.gateway.routes.config as route_mod
        from nerve.sync_service import SyncResult

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace = str(tmp_path / "ws")
        fake_cfg.config_dir = str(tmp_path / "cfg")
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True)
        fake_cfg.lockdown = False
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(
            route_mod, "get_deps", lambda: type("D", (), {"engine": None})(),
        )
        monkeypatch.setattr(
            sync, "sync_workspace",
            lambda *a, **k: SyncResult(ok=True, changed=True, message="updated"),
        )
        applied: list = []

        async def _fake_apply(engine, cron_service, config_dir):
            applied.append(Path(config_dir))
            return {"config": "reloaded"}

        monkeypatch.setattr(sync, "_apply_sync", _fake_apply)
        body = await route_mod.sync_workspace_route(user={})
        assert body["ok"] and body["changed"] and body["applied"]
        assert body["status"] == "applied"
        assert applied == [tmp_path / "cfg"]

    @pytest.mark.asyncio
    async def test_status_separates_the_two_reasons_applied_is_false(
        self, tmp_path, monkeypatch,
    ):
        """``applied: false`` is the answer both when there was nothing to merge
        and when the merge reached no subsystem, and a script cannot act on the
        two the same way. ``status`` is what it reads instead.
        """
        import nerve.gateway.server as srv

        import nerve.gateway.routes.config as route_mod
        from nerve.sync_service import SyncResult

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace = str(tmp_path / "ws")
        fake_cfg.config_dir = str(tmp_path / "cfg")
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True)
        fake_cfg.lockdown = False
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(
            route_mod, "get_deps", lambda: type("D", (), {"engine": None})(),
        )

        monkeypatch.setattr(
            sync, "sync_workspace",
            lambda *a, **k: SyncResult(ok=True, changed=False, message="up to date"),
        )
        body = await route_mod.sync_workspace_route(user={})
        assert body["applied"] is False and body["status"] == "up-to-date"

        monkeypatch.setattr(
            sync, "sync_workspace",
            lambda *a, **k: SyncResult(ok=True, changed=True, message="updated"),
        )

        async def _no_subsystem_took_it(engine, cron_service, config_dir):
            return {"config": "error: ConfigError: nope"}

        monkeypatch.setattr(sync, "_apply_sync", _no_subsystem_took_it)
        body = await route_mod.sync_workspace_route(user={})
        assert body["applied"] is False and body["status"] == "not-applied"

        async def _cron_refused(engine, cron_service, config_dir):
            return {"config": "reloaded", "cron": "error: bad jobs.yaml"}

        monkeypatch.setattr(sync, "_apply_sync", _cron_refused)
        body = await route_mod.sync_workspace_route(user={})
        assert body["applied"] is False and body["status"] == "partial"


class _LoopAgainstRealGit(_RealGit):
    """A real origin/clone pair driven by the real periodic loop.

    The loop's whole subject here is what is on disk versus what this process
    applied, so a scripted git double cannot answer it: the revisions have to be
    real ones that move.
    """

    def _upstream_commit(self, origin, tz="Europe/Berlin"):
        (origin / "config" / "settings.yaml").write_text(f"timezone: {tz}\n")
        self._git("commit", "-am", "change tz", cwd=origin)

    def _config(self, ws, tmp_path, **overrides):
        (tmp_path / "cfg").mkdir(exist_ok=True)
        return _loop_config(
            workspace=str(ws), config_dir=str(tmp_path / "cfg"),
            branch="main", validate=False, interval_minutes=60, **overrides,
        )

    def _head(self, ws):
        return self._git("rev-parse", "HEAD", cwd=ws).stdout.strip()

    async def _run(self, cfg, monkeypatch, cycles, engine=None, hook=None):
        import nerve.config as nerve_config

        monkeypatch.setattr(nerve_config, "_config", cfg)
        stop, _clock = _run_cycles(monkeypatch, cycles, hook=hook)
        await sync.run_periodic_sync(cfg, engine, None, stop)


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestLoopAppliesWhatIsOnDisk(_LoopAgainstRealGit):
    """HEAD says where the config is, not that anything read it.

    It moves without this loop (`nerve config sync`, a bare `git pull`), and a
    reload can fail for one subsystem after a merge that did happen. Applying
    only when the loop's own pull merged something leaves both states reported as
    "up to date" for as long as the daemon runs.
    """

    @pytest.mark.asyncio
    async def test_a_head_moved_out_of_band_is_applied_once(self, tmp_path, monkeypatch):
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        applied: list = []

        async def _apply(engine, cron_service, config_dir):
            applied.append(config_dir)
            return {"config": "reloaded"}

        monkeypatch.setattr(sync, "_apply_sync", _apply)

        def fast_forward_from_a_shell(i):
            # Between cycles, with the daemon already running: the ordering
            # matters, since a daemon started afterwards reads the new config
            # from disk at start-up and has nothing to catch up on.
            if i == 0:
                sync_workspace(ws, tmp_path / "cfg", branch="main", validate=False)

        await self._run(
            self._config(ws, tmp_path), monkeypatch, 3,
            hook=fast_forward_from_a_shell,
        )
        assert len(applied) == 1  # applied once, then nothing left to do

    @pytest.mark.asyncio
    async def test_a_subsystem_that_refused_is_retried(self, tmp_path, monkeypatch):
        """The merge landed, cron did not take it, and the only trace was one
        warning. Nothing retried it, so the daemon ran the merged config in part
        until someone restarted it."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        attempts: list = []

        async def _apply(engine, cron_service, config_dir):
            attempts.append(config_dir)
            return {"config": "reloaded", "cron": "error: bad jobs.yaml"}

        monkeypatch.setattr(sync, "_apply_sync", _apply)
        await self._run(self._config(ws, tmp_path), monkeypatch, 4)

        assert len(attempts) == 4  # the merge, then a retry every cycle

    @pytest.mark.asyncio
    async def test_the_retry_stops_once_the_subsystem_takes_it(self, tmp_path, monkeypatch):
        """The other half: retrying for good would reload every subsystem once a
        minute forever."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        attempts: list = []

        async def _apply(engine, cron_service, config_dir):
            attempts.append(config_dir)
            if len(attempts) == 1:
                return {"config": "reloaded", "cron": "error: bad jobs.yaml"}
            return {"config": "reloaded", "cron": "reloaded"}

        monkeypatch.setattr(sync, "_apply_sync", _apply)
        await self._run(self._config(ws, tmp_path), monkeypatch, 4)

        assert len(attempts) == 2  # the merge, one retry, then quiet

    @pytest.mark.asyncio
    async def test_an_untouched_workspace_is_not_reapplied(self, tmp_path, monkeypatch):
        """The loop seeds from HEAD: a daemon that starts on the current revision
        has already read it, and must not reload on its first cycle."""
        _origin, ws = self._pair(tmp_path)
        applied: list = []

        async def _apply(engine, cron_service, config_dir):
            applied.append(config_dir)
            return {"config": "reloaded"}

        monkeypatch.setattr(sync, "_apply_sync", _apply)
        await self._run(self._config(ws, tmp_path), monkeypatch, 3)
        assert applied == []


class _ForgetsSyncState:
    @pytest.fixture(autouse=True)
    def _forget_previous_cycles(self, monkeypatch):
        """The retained state is module-level, as the daemon's own is."""
        monkeypatch.setattr(sync, "_last_sync", None)


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestRetainedStateFollowsTheLoop(_ForgetsSyncState, _LoopAgainstRealGit):
    @pytest.mark.asyncio
    async def test_a_partly_applied_merge_is_visible(self, tmp_path, monkeypatch):
        """The state a caller most needs and the log is worst at: the merge
        landed, the config loaded, and one subsystem is still running the old
        one."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        pinned = self._head(ws)

        async def _apply(engine, cron_service, config_dir):
            return {"config": "reloaded", "cron": "error: bad jobs.yaml"}

        monkeypatch.setattr(sync, "_apply_sync", _apply)
        await self._run(self._config(ws, tmp_path), monkeypatch, 2)

        state = sync.last_sync_state()
        assert state.reload_errors == {"cron": "bad jobs.yaml"}
        assert state.applied_rev == pinned          # not what the merge landed
        assert state.fetched_rev == self._head(ws)  # ...which is this
        assert state.ok and not state.blocked_paths


class _Notifier:
    """Records what the loop sends, with the notification service's signature."""

    def __init__(self):
        self.sent: list[dict] = []

    async def send_notification(self, session_id, title, body="", priority="normal"):
        self.sent.append({
            "session_id": session_id, "title": title,
            "body": body, "priority": priority,
        })
        return "notif-test"


class _Engine:
    def __init__(self, notification_service=None):
        self.notification_service = notification_service


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestBlockedSyncIsVisible(_ForgetsSyncState, _LoopAgainstRealGit):
    """A refused merge used to leave nothing but a repeating WARNING.

    The instance stays pinned to an old reviewed revision and every later config
    change stops arriving — on a locked box, the deployment defeated — while it
    answers every other question exactly like a healthy one.
    """

    def _dirty_a_skill(self, ws):
        (ws / "skills" / "backdoor").mkdir(parents=True, exist_ok=True)
        (ws / "skills" / "backdoor" / "SKILL.md").write_text(
            "---\nname: backdoor\nallowed-tools: Bash\n---\n",
        )

    @pytest.mark.asyncio
    async def test_entering_the_blocked_state_notifies_once(self, tmp_path, monkeypatch):
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        self._dirty_a_skill(ws)
        pinned = self._head(ws)
        notifier = _Notifier()

        await self._run(
            self._config(ws, tmp_path), monkeypatch, 3, engine=_Engine(notifier),
        )

        assert len(notifier.sent) == 1, notifier.sent  # not one per cycle
        sent = notifier.sent[0]
        assert sent["title"] == "Workspace sync blocked"
        assert "SKILL.md" in sent["body"]
        assert "propose_config_change" in sent["body"]
        assert sent["priority"] == "high"
        # ...and the merge really did not happen.
        assert self._head(ws) == pinned
        assert "Europe/Berlin" not in (ws / "config" / "settings.yaml").read_text()

    @pytest.mark.asyncio
    async def test_the_blocked_state_is_queryable(self, tmp_path, monkeypatch):
        """A log line cannot be asked a question. `nerve doctor` and
        GET /api/config/sync both read this."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        self._dirty_a_skill(ws)
        pinned = self._head(ws)

        await self._run(self._config(ws, tmp_path), monkeypatch, 2)

        state = sync.last_sync_state()
        assert state.blocked_paths and "SKILL.md" in state.blocked_paths[0]
        assert state.applied_rev == pinned
        assert state.fetched_rev != pinned  # what it would be running if it could
        assert state.ok is False and state.checked_at > 0

    @pytest.mark.asyncio
    async def test_clearing_the_block_notifies_once_more(self, tmp_path, monkeypatch):
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        self._dirty_a_skill(ws)
        notifier = _Notifier()

        def drop_the_local_change(i):
            if i == 2:
                shutil.rmtree(ws / "skills")

        await self._run(
            self._config(ws, tmp_path), monkeypatch, 4, engine=_Engine(notifier),
            hook=drop_the_local_change,
        )

        assert [n["title"] for n in notifier.sent] == [
            "Workspace sync blocked", "Workspace sync unblocked",
        ]
        assert notifier.sent[1]["priority"] == "low"
        assert not sync.last_sync_state().blocked_paths
        assert "Europe/Berlin" in (ws / "config" / "settings.yaml").read_text()

    @pytest.mark.asyncio
    async def test_a_cycle_that_never_reached_the_check_reports_no_recovery(
        self, tmp_path, monkeypatch,
    ):
        """A failed fetch establishes nothing about the local tree, and saying
        "unblocked" there would retract a warning that still stands — and arm the
        next block to notify all over again."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        self._dirty_a_skill(ws)
        notifier = _Notifier()

        def break_the_remote(i):
            if i == 1:
                self._git(
                    "remote", "set-url", "origin", str(tmp_path / "gone"), cwd=ws,
                )

        await self._run(
            self._config(ws, tmp_path), monkeypatch, 3, engine=_Engine(notifier),
            hook=break_the_remote,
        )

        assert [n["title"] for n in notifier.sent] == ["Workspace sync blocked"]
        assert sync.last_sync_state().blocked_paths

    @pytest.mark.asyncio
    async def test_a_later_failure_does_not_keep_reporting_the_old_block(
        self, tmp_path, monkeypatch,
    ):
        """The other side of the rule above: committing the local change clears
        the block *and* diverges the branch, so the merge fails for a new reason.
        Holding on to the old paths would name files that are no longer the
        problem, and no later block would ever be announced.
        """
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        self._dirty_a_skill(ws)

        def commit_it(i):
            if i == 1:
                self._git("add", "-A", cwd=ws)
                self._git("commit", "-m", "add skill", cwd=ws)

        await self._run(
            self._config(ws, tmp_path), monkeypatch, 3, engine=_Engine(_Notifier()),
            hook=commit_it,
        )

        state = sync.last_sync_state()
        assert not state.blocked_paths
        assert state.ok is False and "ff-only" in state.message

    @pytest.mark.asyncio
    async def test_no_notification_service_is_not_a_failure(
        self, tmp_path, monkeypatch, caplog,
    ):
        """Every other sender treats an absent service as "nothing to do here".
        Filing it as a failed delivery instead would put a warning in the log
        every time the CLI or a test crosses the transition, with nothing there
        to deliver to."""
        origin, ws = self._pair(tmp_path)
        self._upstream_commit(origin)
        self._dirty_a_skill(ws)

        with caplog.at_level(logging.WARNING, logger="nerve.sync_service"):
            await self._run(self._config(ws, tmp_path), monkeypatch, 2, engine=None)
            await self._run(
                self._config(ws, tmp_path), monkeypatch, 1, engine=_Engine(None),
            )
        assert "notification failed" not in caplog.text
        assert sync.last_sync_state().blocked_paths

    def test_doctor_reports_a_blocked_workspace(self, tmp_path, monkeypatch):
        """The CLI has no view of the daemon's record, and the shell is where an
        operator asks why config stopped arriving — so doctor runs the check
        itself."""
        from nerve.cli import doctor_report
        from nerve.config import NerveConfig

        _origin, ws = self._pair(tmp_path)
        config = NerveConfig(
            workspace=ws, config_dir=tmp_path / "cfg",
            workspace_sync=WorkspaceSyncConfig(enabled=True, branch="main"),
        )
        assert "Workspace sync: reviewed files clean" in doctor_report(config)

        self._dirty_a_skill(ws)
        report = doctor_report(config)
        assert "Workspace sync BLOCKED" in report
        assert "SKILL.md" in report
        assert "propose_config_change" in report


class TestSyncStatusRoute:
    @pytest.mark.asyncio
    async def test_reports_the_retained_state(self, monkeypatch):
        import nerve.gateway.routes.config as route_mod

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=True, branch="main")
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(sync, "_last_sync", sync.SyncState(
            applied_rev="a" * 40, fetched_rev="b" * 40,
            blocked_paths=["?? skills/backdoor/SKILL.md"],
            reload_errors={"cron": "bad jobs.yaml"},
            ok=False, message="fetched bbbbbbbb but ...", checked_at=time.time(),
        ))
        body = await route_mod.sync_status_route(user={})
        assert body["checked"] is True and body["blocked"] is True
        assert body["blocked_paths"] == ["?? skills/backdoor/SKILL.md"]
        assert body["applied_rev"] == "a" * 40
        assert body["reload_errors"] == {"cron": "bad jobs.yaml"}
        assert body["checked_at"].startswith("20")

    @pytest.mark.asyncio
    async def test_no_cycle_yet_is_not_a_clean_bill_of_health(self, monkeypatch):
        import nerve.gateway.routes.config as route_mod

        fake_cfg = type("C", (), {})()
        fake_cfg.workspace_sync = WorkspaceSyncConfig(enabled=False)
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(sync, "_last_sync", None)
        body = await route_mod.sync_status_route(user={})
        assert body["checked"] is False and body["enabled"] is False
        assert "blocked" not in body  # no answer, rather than a false negative
