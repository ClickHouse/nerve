"""Tests for config-repo scaffolding (nerve/config_repo.py + `config init-repo`)."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

import nerve
from nerve import config_repo
from nerve.config_pr import _ALLOWED_ROOT_FILES
from nerve.config_repo import _SCAFFOLD, _template_dir, scaffold_config_repo

_WORKFLOW = ".github/workflows/validate-config.yml"


def _workflow(ws: Path) -> dict:
    return yaml.safe_load((ws / _WORKFLOW).read_text(encoding="utf-8"))


def _steps(ws: Path) -> list[dict]:
    return _workflow(ws)["jobs"]["validate"]["steps"]


def _validate_step(ws: Path) -> dict:
    """The step that actually runs the validator."""
    return next(
        s for s in _steps(ws) if "config validate" in str(s.get("run", ""))
    )


def _install_step(ws: Path) -> dict:
    """The step that installs the nerve the validator comes from.

    Matched on the package spec rather than the installer, so swapping how it
    is installed does not silently match no step and skip the assertions that
    depend on this.
    """
    return next(s for s in _steps(ws) if "nerve @ git+" in str(s.get("run", "")))


def _run_ci_validation(ws: Path) -> subprocess.CompletedProcess:
    """Run the scaffolded workflow's validate step against ``ws``, as CI would.

    The command, its environment and its working directory are taken from the
    generated file rather than restated here, so this exercises the flags an
    operator's CI will actually run. The only substitution is where nerve comes
    from: CI installs it, here it is the tree the tests are running from.
    """
    step = _validate_step(ws)
    argv = shlex.split(step["run"])
    assert argv[0] == "nerve", step["run"]
    argv[:1] = [sys.executable, "-m", "nerve.cli"]

    env = dict(os.environ)
    env.update(step.get("env") or {})
    # The console script CI runs puts its own bin directory on sys.path, not the
    # working directory. `-m` would put the config repo there instead, so pin the
    # nerve under test explicitly and keep the repo off the path, or the
    # not-importable test below would pass for the wrong reason.
    env["PYTHONPATH"] = str(Path(nerve.__file__).resolve().parent.parent)
    env["PYTHONSAFEPATH"] = "1"
    return subprocess.run(argv, cwd=ws, env=env, capture_output=True, text=True)


class TestScaffold:
    def test_creates_all_files(self, tmp_path):
        result = scaffold_config_repo(tmp_path)
        assert set(result.created) == set(_SCAFFOLD)
        assert result.skipped == []
        for rel in _SCAFFOLD:
            assert (tmp_path / rel).is_file()

    def test_ci_workflow_lands_in_github_dir(self, tmp_path):
        scaffold_config_repo(tmp_path)
        wf = tmp_path / ".github/workflows/validate-config.yml"
        assert wf.is_file()
        assert "--workspace ." in wf.read_text(encoding="utf-8")

    def test_ci_workflow_is_valid_yaml(self, tmp_path):
        # A malformed template would ship and only blow up in the operator's CI.
        scaffold_config_repo(tmp_path)
        wf = yaml.safe_load(
            (tmp_path / ".github/workflows/validate-config.yml").read_text("utf-8")
        )
        steps = wf["jobs"]["validate"]["steps"]
        assert any("config validate" in str(s.get("run", "")) for s in steps)

    def test_ci_workflow_needs_no_secrets(self, tmp_path):
        # nerve is public and the validator needs no credentials, so a config repo
        # can run this without anyone provisioning an Actions secret first.
        scaffold_config_repo(tmp_path)
        assert "secrets." not in (
            tmp_path / ".github/workflows/validate-config.yml"
        ).read_text("utf-8")

    def test_gitignore_excludes_secrets(self, tmp_path):
        scaffold_config_repo(tmp_path)
        body = (tmp_path / ".gitignore").read_text(encoding="utf-8")
        assert "config.local.yaml" in body
        assert "*.migrated" in body

    def test_the_machine_local_layer_is_ignored_at_the_root_only(self, tmp_path):
        """Migration writes `config.yaml`, and the runbook then says `git add -A`.

        Asked of git rather than of the file's text, because the whole point is
        the leading slash: `config.yaml` unanchored would also swallow a
        `config/config.yaml` inside the tracked subtree, which is reviewed
        content and has to stay committable.
        """
        import shutil
        import subprocess

        if not shutil.which("git"):
            pytest.skip("git not available")
        scaffold_config_repo(tmp_path)
        subprocess.run(["git", "init", "-q"], cwd=str(tmp_path), check=True,
                       capture_output=True)

        def ignored(rel: str) -> bool:
            return subprocess.run(
                ["git", "check-ignore", "-q", rel], cwd=str(tmp_path),
            ).returncode == 0

        assert ignored("config.yaml"), "the machine-local layer must not be staged"
        assert not ignored("config/config.yaml"), "tracked config must stay committable"
        assert not ignored("config/settings.yaml")

    def test_agent_runtime_state_is_ignored_but_instructions_are_tracked(self, tmp_path):
        """`nerve init` writes MEMORY.md into every workspace, and the agent
        rewrites it and TASK.md as it works, so the runbook's `git add -A` would
        commit a scratchpad the instance then fights the repo over on each pull.

        The same line nerve draws for the agent's own proposals, where these are
        refused as runtime state while the instruction files are reviewable —
        so this pins both halves, not just the exclusion. Asked of git for the
        anchoring reason above: unanchored, `MEMORY.md` would also swallow a
        skill's own MEMORY.md inside the tracked subtree.
        """
        import shutil
        import subprocess

        if not shutil.which("git"):
            pytest.skip("git not available")
        scaffold_config_repo(tmp_path)
        subprocess.run(["git", "init", "-q"], cwd=str(tmp_path), check=True,
                       capture_output=True)

        def ignored(rel: str) -> bool:
            return subprocess.run(
                ["git", "check-ignore", "-q", rel], cwd=str(tmp_path),
            ).returncode == 0

        for rel in ("MEMORY.md", "TASK.md", "memory/entity.md"):
            assert ignored(rel), f"{rel} is runtime state and must not be staged"
        for rel in _ALLOWED_ROOT_FILES:
            assert not ignored(rel), f"{rel} is reviewed instruction and must stay committable"
        assert not ignored("skills/x/MEMORY.md"), "the exclusion must be root-anchored"

    def test_gitignore_covers_backup_secret_members(self, tmp_path):
        # If the workspace doubles as the state dir, `git add -A` (which the
        # runbook tells operators to run) must not sweep up nerve's credentials.
        from nerve.backup import SECRET_MEMBERS

        scaffold_config_repo(tmp_path)
        patterns = {
            ln.strip()
            for ln in (tmp_path / ".gitignore").read_text("utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")
        }
        for member in SECRET_MEMBERS:
            name = Path(member).name
            assert any(
                p in (name, f"{name}/", f"*{Path(name).suffix}")
                for p in patterns
            ), f"{name} (from backup.SECRET_MEMBERS) is not gitignored"

    def test_dry_run_writes_nothing(self, tmp_path):
        result = scaffold_config_repo(tmp_path, dry_run=True)
        assert set(result.created) == set(_SCAFFOLD)
        for rel in _SCAFFOLD:
            assert not (tmp_path / rel).exists()
        # Not even the parent dirs of nested targets.
        assert not (tmp_path / ".github").exists()
        assert list(tmp_path.iterdir()) == []

    def test_templates_are_packaged(self):
        # Guards the importlib-vs-source fallback in _template_dir(): every
        # scaffold source must actually resolve, or an installed nerve breaks.
        tmpl = _template_dir()
        for src_name in _SCAFFOLD.values():
            assert (tmpl / src_name).is_file(), f"missing template {src_name}"

    def test_idempotent_skips_existing(self, tmp_path):
        scaffold_config_repo(tmp_path)
        second = scaffold_config_repo(tmp_path)
        assert second.created == []
        assert set(second.skipped) == set(_SCAFFOLD)

    def test_never_overwrites_existing_file(self, tmp_path):
        gi = tmp_path / ".gitignore"
        gi.write_text("# my custom ignore\n", encoding="utf-8")
        result = scaffold_config_repo(tmp_path)
        assert ".gitignore" in result.skipped
        assert gi.read_text(encoding="utf-8") == "# my custom ignore\n"
        # The other two are still created.
        assert (tmp_path / "README.md").is_file()

    def test_detects_existing_git_repo(self, tmp_path):
        assert scaffold_config_repo(tmp_path).is_git_repo is False
        (tmp_path / ".git").mkdir()
        assert scaffold_config_repo(tmp_path).is_git_repo is True

    def test_seeds_a_portable_settings_layer(self, tmp_path):
        # The workflow validates the portable layer only, and a config/ it can
        # read nothing out of is an error there — so a repo scaffolded from a
        # bare directory needs one to be valid on its first commit.
        scaffold_config_repo(tmp_path)
        assert (tmp_path / "config" / "settings.yaml").is_file()

    def test_leaves_an_existing_settings_file_alone(self, tmp_path):
        settings = tmp_path / "config" / "settings.yaml"
        settings.parent.mkdir(parents=True)
        settings.write_text("timezone: UTC\n", encoding="utf-8")

        result = scaffold_config_repo(tmp_path)

        assert "config/settings.yaml" in result.skipped
        assert settings.read_text(encoding="utf-8") == "timezone: UTC\n"


class TestScaffoldedWorkflow:
    """What the generated workflow does, read off the generated file."""

    def test_the_validator_is_installed_from_the_nerve_repo(self, tmp_path):
        """Not from PyPI, where the bare name is an unrelated project."""
        scaffold_config_repo(tmp_path)
        run = _install_step(tmp_path)["run"]
        assert "git+https://github.com/ClickHouse/nerve" in run
        assert "install nerve\n" not in run and not run.endswith("install nerve")

    def test_the_validator_is_not_installed_into_the_system_interpreter(self, tmp_path):
        """`--system` is refused on the runner: its Python is externally managed.

        This failed every scaffolded repo on its first push, at the step after
        the secret scan, with "The interpreter at /usr is externally managed"
        (PEP 668). Only the CLI is needed, so it goes in its own environment.
        """
        scaffold_config_repo(tmp_path)
        run = _install_step(tmp_path)["run"]
        assert "--system" not in run, run

    def test_the_validate_step_runs_the_installed_cli(self, tmp_path):
        scaffold_config_repo(tmp_path)
        assert _validate_step(tmp_path)["run"].startswith("nerve config validate")

    def test_validates_the_portable_layer_strictly(self, tmp_path):
        scaffold_config_repo(tmp_path)
        run = _validate_step(tmp_path)["run"]
        assert "--portable-only" in run  # not the runner's machine config
        assert "--strict-keys" in run  # a typo'd key blocks the PR

    def test_does_not_assume_lockdown(self, tmp_path):
        # Correct default, and load-bearing: the locked view requires secrets a
        # fresh repo hasn't got, so passing it here would red-X every new repo.
        # The workflow comment points at it for repos that do serve a locked box.
        scaffold_config_repo(tmp_path)
        assert "--assume-lockdown" not in _validate_step(tmp_path)["run"]
        assert "--assume-lockdown" in (tmp_path / _WORKFLOW).read_text("utf-8")

    def test_scans_for_committed_secrets(self, tmp_path):
        scaffold_config_repo(tmp_path)
        assert any("gitleaks" in str(s.get("uses", "")) for s in _steps(tmp_path))

    def test_no_step_runs_code_from_the_config_repo(self, tmp_path):
        """The job reads the PR's files; it must never execute them.

        Validation itself declines to load the bundle's gate plugins, which is
        worth nothing if the job around it installs the repo, runs a script out
        of it, or resolves a local action from it.
        """
        scaffold_config_repo(tmp_path)
        for step in _steps(tmp_path):
            uses = str(step.get("uses", ""))
            assert not uses.startswith("./"), f"local action from the PR: {uses}"
            # A container step can point its entrypoint at the mounted checkout,
            # which `uses:` alone doesn't reveal.
            entrypoint = str((step.get("with") or {}).get("entrypoint", ""))
            assert "/github/workspace" not in entrypoint, entrypoint
            run = str(step.get("run", ""))
            for forbidden in (
                "-e .", "-r ", "install .", "bash ", "sh ", "make ",
                "pre-commit", "setup.py", "uv run", "npm ", "npx ", "tox",
            ):
                assert forbidden not in run, f"{forbidden!r} in step: {run}"

    def test_docs_show_the_generated_workflow_verbatim(self, tmp_path):
        """docs/config.md prints this file inline, comments and all.

        Compared as text, not as parsed YAML: every load-bearing instruction in
        that file — which flags matter, where nerve comes from, what silences a
        false positive — lives in a comment, so a structural comparison would
        wave through exactly the drift worth catching.
        """
        scaffold_config_repo(tmp_path)
        generated = (tmp_path / _WORKFLOW).read_text(encoding="utf-8").rstrip("\n")

        docs = Path(__file__).resolve().parent.parent / "docs" / "config.md"
        text = docs.read_text(encoding="utf-8")
        start = text.index("```yaml\nname: validate-config\n") + len("```yaml\n")
        documented = text[start:text.index("\n```", start)]

        assert documented == generated


class TestFreshRepoPassesItsOwnCI:
    """`init-repo` scaffolds the repo *and* the job that gates it, so the two
    have to agree on day one — a red X on the first commit teaches operators to
    ignore the check."""

    def test_a_fresh_scaffold_validates_clean(self, tmp_path):
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)

        proc = _run_ci_validation(ws)

        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert "Config OK" in proc.stdout

    def test_a_broken_bundle_fails(self, tmp_path):
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)
        (ws / "config" / "settings.yaml").write_text(
            "agent:\n  backend: bogus\n", encoding="utf-8",
        )

        proc = _run_ci_validation(ws)

        assert proc.returncode == 1
        assert "bogus" in proc.stdout

    def test_a_misspelled_key_blocks_the_pr(self, tmp_path):
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)
        (ws / "config" / "settings.yaml").write_text(
            "tiimezone: UTC\n", encoding="utf-8",
        )

        proc = _run_ci_validation(ws)

        assert proc.returncode == 1, proc.stdout
        assert "tiimezone" in proc.stdout

    def test_machine_config_beside_the_checkout_is_ignored(self, tmp_path):
        """A config.yaml in the working directory is picked up by the config-dir
        waterfall; merging it would let the runner's state decide the verdict."""
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)
        (ws / "config" / "settings.yaml").write_text(
            "agent:\n  backend: bogus\n", encoding="utf-8",
        )
        (ws / "config.yaml").write_text("agent:\n  backend: claude\n", encoding="utf-8")

        proc = _run_ci_validation(ws)

        assert proc.returncode == 1, proc.stdout
        assert "bogus" in proc.stdout

    def test_a_machine_local_cron_dir_cannot_fail_the_repo(
        self, tmp_path, monkeypatch,
    ):
        """The repo carries no cron jobs, so cron resolution would otherwise fall
        back to the machine-local directory — condemning a clean config repo over
        a file that isn't in it. Bites hardest where this command is most likely
        to be run by hand: the instance box, which has one."""
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)
        home = tmp_path / "nervehome"
        (home / "cron").mkdir(parents=True)
        (home / "cron" / "jobs.yaml").write_text("jobs: [oops\n", encoding="utf-8")
        monkeypatch.setenv("NERVE_HOME", str(home))

        proc = _run_ci_validation(ws)

        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert "oops" not in proc.stdout

    def test_a_gate_plugin_neither_runs_nor_fails_the_job(self, tmp_path):
        """--strict-keys must not turn "can't confirm" into "reject".

        A gate type only a plugin provides is unverifiable without importing the
        plugin, which the job declines to do — so it stays a warning, and the
        plugin's code never executes in CI.
        """
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)
        marker = tmp_path / "executed"
        gates = ws / "config" / "cron" / "gates"
        gates.mkdir(parents=True)
        (gates / "marker.py").write_text(
            f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')\n",
            encoding="utf-8",
        )
        (ws / "config" / "cron" / "jobs.yaml").write_text(
            "jobs:\n  - id: g\n    schedule: 1h\n    prompt: hi\n"
            "    run_if:\n      - type: marker_test\n",
            encoding="utf-8",
        )

        proc = _run_ci_validation(ws)

        assert not marker.exists(), "CI executed a gate plugin from the PR"
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert "marker_test" in proc.stdout

    def test_the_checkout_is_not_importable(self, tmp_path):
        """`python -m` puts the working directory first on sys.path, so a
        `yaml.py` committed to the config repo would be imported in place of the
        real one — running a pull request's code before anyone read it."""
        ws = tmp_path / "repo"
        ws.mkdir()
        scaffold_config_repo(ws)
        marker = tmp_path / "imported"
        (ws / "yaml.py").write_text(
            f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')\n",
            encoding="utf-8",
        )

        proc = _run_ci_validation(ws)

        assert not marker.exists(), "the config repo's yaml.py was imported"
        assert proc.returncode == 0, proc.stdout + proc.stderr


class TestInitRepoCommand:
    def _run(self, args):
        from nerve.cli import main

        return CliRunner().invoke(main, args, obj={"config": None, "config_dir": "."})

    def test_scaffolds_via_workspace_flag(self, tmp_path):
        result = self._run(["config", "init-repo", "--workspace", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert (tmp_path / ".github/workflows/validate-config.yml").is_file()
        assert "Created:" in result.output
        assert "Next steps:" in result.output

    def test_prints_git_init_when_not_a_repo(self, tmp_path):
        out = self._run(["config", "init-repo", "--workspace", str(tmp_path)]).output
        assert "git init" in out

    def test_omits_git_init_when_already_a_repo(self, tmp_path):
        (tmp_path / ".git").mkdir()
        out = self._run(["config", "init-repo", "--workspace", str(tmp_path)]).output
        assert "git init" not in out

    def test_dry_run_writes_nothing(self, tmp_path):
        out = self._run(
            ["config", "init-repo", "--workspace", str(tmp_path), "--dry-run"]
        ).output
        assert "Would create:" in out
        assert not (tmp_path / ".gitignore").exists()

    def test_leaves_an_existing_workflow_alone(self, tmp_path):
        wf = tmp_path / _WORKFLOW
        wf.parent.mkdir(parents=True)
        wf.write_text("name: mine\n", encoding="utf-8")

        out = self._run(["config", "init-repo", "--workspace", str(tmp_path)]).output

        assert "Skipped (exists)" in out
        assert wf.read_text(encoding="utf-8") == "name: mine\n"

    def test_the_verify_command_matches_what_ci_runs(self, tmp_path):
        """Before this, the runbook handed the operator a laxer command than the
        gate it feeds: a typo'd key passed locally and red-X'd on the PR."""
        out = self._run(["config", "init-repo", "--workspace", str(tmp_path)]).output
        printed = next(
            ln for ln in out.splitlines() if "nerve config validate" in ln
        )
        for flag in shlex.split(_validate_step(tmp_path)["run"]):
            if flag.startswith("--") and flag != "--workspace":
                assert flag in printed, f"{flag} missing from: {printed}"

    def test_missing_workspace_errors(self, tmp_path):
        result = self._run(
            ["config", "init-repo", "--workspace", str(tmp_path / "nope")]
        )
        assert result.exit_code != 0
        assert "does not exist" in result.output

    def test_no_config_no_workspace_errors(self, monkeypatch):
        # When config can't load (self-diagnosing → config=None) and no
        # --workspace is given, the command must fail with a clear message
        # rather than crash on the missing config.
        import nerve.cli as cli

        def _raise(config_dir=None):
            raise cli.ConfigError("boom")

        monkeypatch.setattr(cli, "load_config", _raise)
        result = CliRunner().invoke(cli.main, ["config", "init-repo"])
        assert result.exit_code != 0
        assert "Config could not be loaded" in result.output
