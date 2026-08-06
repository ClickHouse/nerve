"""Tests for lockdown / remote-only read-only mode."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

import nerve.config as cfg
from nerve.config import (
    ConfigError,
    LockdownError,
    NerveConfig,
    _resolve_cron_dir,
    ensure_not_locked,
    is_locked,
    load_config,
    lockdown_workspace_problems,
    tracked_config_write_refusal,
    workspace_settings_file,
)


def _repo(ws: Path, *, remote: str | None = "origin") -> Path:
    """Make ``ws`` what a locked workspace is on a real box: a git repository
    with a remote to receive reviewed config from.

    A locked instance with nowhere to receive it from refuses to start — see
    :class:`TestLockdownNeedsSomewhereToReceiveConfigFrom` — so every locked
    workspace here that is expected to load has one. The remote is never
    contacted; the check asks whether one is configured. ``remote=None`` leaves
    the repository without one.
    """
    if not shutil.which("git"):
        pytest.skip("git not available")
    runs = [["init", "-q"]]
    if remote:
        runs.append(["remote", "add", remote, "https://example.invalid/config.git"])
    for args in runs:
        subprocess.run(["git", *args], cwd=str(ws), check=True, capture_output=True)
    return ws


def _install(tmp_path, *, settings="", base="", local=""):
    config_dir = tmp_path / "cfg"
    workspace = tmp_path / "ws"
    config_dir.mkdir(parents=True)
    (workspace / "config").mkdir(parents=True)
    _repo(workspace)
    (config_dir / "config.yaml").write_text(f"workspace: {workspace}\n" + base, encoding="utf-8")
    if local:
        (config_dir / "config.local.yaml").write_text(local, encoding="utf-8")
    if settings:
        workspace_settings_file(workspace).write_text(settings, encoding="utf-8")
    return config_dir, workspace


_JWT = "auth:\n  jwt_secret: test-secret\n"


class TestLockdownResolution:
    _JWT = "auth:\n  jwt_secret: test-secret\n"

    def test_locked_drops_machine_overrides(self, tmp_path):
        # settings says UTC + locked; config.yaml tries to override timezone.
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\ntimezone: UTC\n" + self._JWT,
            base="timezone: America/New_York\n",
            local="timezone: Europe/Berlin\n",
        )
        c = load_config(config_dir)
        assert c.lockdown is True
        assert c.timezone == "UTC"  # machine layers ignored; only settings applies

    def test_local_cannot_override_lockdown(self, tmp_path):
        # Tamper attempt: config.yaml/local set lockdown:false, settings sets true.
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + self._JWT,
            base="lockdown: false\n",
            local="lockdown: false\n",
        )
        c = load_config(config_dir)
        assert c.lockdown is True  # the remote (settings.yaml) is authoritative

    def test_lockdown_not_settable_from_config_yaml(self, tmp_path):
        # Only the tracked settings file controls lockdown.
        config_dir, ws = _install(tmp_path, base="lockdown: true\n")
        c = load_config(config_dir)
        assert c.lockdown is False

    def test_not_locked_merges_normally(self, tmp_path):
        config_dir, ws = _install(
            tmp_path, settings="timezone: UTC\n", base="timezone: America/New_York\n"
        )
        c = load_config(config_dir)
        assert c.lockdown is False
        assert c.timezone == "America/New_York"  # config.yaml wins when unlocked

    def test_locked_still_resolves_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRET_TZ", "Asia/Tokyo")
        config_dir, ws = _install(
            tmp_path, settings="lockdown: true\ntimezone: ${SECRET_TZ}\n" + self._JWT,
        )
        c = load_config(config_dir)
        assert c.timezone == "Asia/Tokyo"  # secrets still come from env


class TestLockdownAuthFailClosed:
    @pytest.mark.asyncio
    async def test_require_auth_denies_when_locked_no_secret(self, monkeypatch):
        from fastapi import HTTPException

        from nerve.gateway.auth import require_auth

        c = NerveConfig(lockdown=True)  # jwt_secret empty
        monkeypatch.setattr(cfg, "_config", c)
        with pytest.raises(HTTPException) as ei:
            await require_auth(request=None)
        assert ei.value.status_code == 503

    @pytest.mark.asyncio
    async def test_websocket_denies_when_locked_no_secret(self, monkeypatch):
        from nerve.gateway.auth import authenticate_websocket

        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True))
        assert await authenticate_websocket(websocket=None) is False


class TestValidateRespectsLockdown:
    def test_validate_uses_locked_view(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        # settings locks + provides jwt; config.yaml has an unknown key that must
        # be ignored under lockdown (machine layers dropped).
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir(parents=True)
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(
            f"workspace: {ws}\ntiimezone: UTC\n", encoding="utf-8"
        )
        workspace_settings_file(ws).write_text(
            "lockdown: true\nauth:\n  jwt_secret: x\n", encoding="utf-8"
        )
        result = validate_config_bundle(config_dir, workspace_override=ws, strict_keys=True)
        # config.yaml's typo is dropped under lockdown → not reported.
        assert not any("tiimezone" in e for e in result.errors)


class TestLockdownCron:
    def test_locked_forces_workspace_cron_no_legacy(self, tmp_path):
        from nerve import paths

        workspace = tmp_path / "ws"
        legacy = paths.cron_dir()
        legacy.mkdir(parents=True, exist_ok=True)
        (legacy / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        # Unlocked would fall back to legacy (workspace has no jobs); locked won't.
        assert _resolve_cron_dir(workspace, locked=False) == legacy
        assert _resolve_cron_dir(workspace, locked=True) == workspace / "config" / "cron"


class TestLockdownGuards:
    def test_is_locked_reflects_config(self, monkeypatch):
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True))
        assert is_locked() is True
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False))
        assert is_locked() is False

    def test_ensure_not_locked_raises_when_locked(self, monkeypatch):
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True))
        with pytest.raises(LockdownError):
            ensure_not_locked("do a thing")

    def test_ensure_not_locked_noop_when_unlocked(self, monkeypatch):
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False))
        ensure_not_locked("do a thing")  # no raise

    def test_telegram_write_blocked_when_locked(self, tmp_path, monkeypatch):
        from nerve.config import append_telegram_allowed_user

        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True))
        with pytest.raises(LockdownError):
            append_telegram_allowed_user(tmp_path, 42)


class TestLockdownSkillWrites:
    @pytest.mark.asyncio
    async def test_skill_writes_blocked_when_locked(self, tmp_path, db, monkeypatch):
        from nerve.skills.manager import SkillManager

        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True))
        mgr = SkillManager(tmp_path / "ws", db)
        with pytest.raises(LockdownError):
            await mgr.create_skill("Test", "desc")
        with pytest.raises(LockdownError):
            await mgr.update_skill("x", "content")
        with pytest.raises(LockdownError):
            await mgr.delete_skill("x")
        with pytest.raises(LockdownError):
            await mgr.toggle_skill("x", True)

    @pytest.mark.asyncio
    async def test_skill_create_works_when_unlocked(self, tmp_path, db, monkeypatch):
        from nerve.skills.manager import SkillManager

        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False))
        mgr = SkillManager(tmp_path / "ws", db)
        meta = await mgr.create_skill("Test Skill", "a description")
        assert meta.id == "test-skill"


class TestLockdownFlagFromEnvironment:
    """The flag is read before the post-merge interpolation pass, so it has to
    resolve its own ``${VAR}`` — otherwise it is judged as the literal reference
    text, which is truthy no matter what the variable says.

    Both directions of getting that wrong are here. Reading ``True`` when the
    config says ``false`` locks a fleet by accident. Reading ``False`` when the
    config says ``true`` is an authentication and integrity bypass, so a value
    that cannot be read is refused rather than resolved to the safer-looking
    default.
    """

    def test_env_ref_false_leaves_the_instance_unlocked(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NERVE_LOCKDOWN", "false")
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: ${NERVE_LOCKDOWN}\ntimezone: UTC\n",
            base="timezone: America/New_York\n",
        )
        c = load_config(config_dir)
        assert c.lockdown is False
        # Not just the flag: the machine-local layer is merged again, which is
        # the behavior that was actually lost.
        assert c.timezone == "America/New_York"

    def test_env_ref_true_locks(self, tmp_path, monkeypatch):
        # NERVE_LOCKDOWN is also the environment anchor, so setting it truthy
        # requires NERVE_WORKSPACE alongside — see TestLockdownEnvironmentAnchor.
        monkeypatch.setenv("NERVE_LOCKDOWN", "true")
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "ws"))
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: ${NERVE_LOCKDOWN}\ntimezone: UTC\n" + _JWT,
            base="timezone: America/New_York\n",
        )
        c = load_config(config_dir)
        assert c.lockdown is True
        assert c.timezone == "UTC"

    def test_the_tracked_reference_and_the_anchor_are_one_switch(self, tmp_path, monkeypatch):
        """``lockdown: ${NERVE_LOCKDOWN}`` and the anchor read the same variable
        on purpose: "this box is locked" should have one spelling. With the anchor
        the tracked file need not mention the flag at all, and a fleet repo that
        does mention it gets the anchor's protection for free."""
        monkeypatch.setenv("NERVE_LOCKDOWN", "1")
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "ws"))
        config_dir, ws = _install(tmp_path, settings=_JWT)  # no lockdown key
        assert load_config(config_dir).lockdown is True

    def test_optional_ref_default_false_leaves_it_unlocked(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NERVE_LOCKDOWN", raising=False)
        config_dir, ws = _install(
            tmp_path, settings="lockdown: ${NERVE_LOCKDOWN:-false}\n",
        )
        assert load_config(config_dir).lockdown is False

    def test_unparseable_value_is_refused_not_read_as_unlocked(self, tmp_path):
        # jwt_secret is supplied so the only thing left to complain about is the
        # flag itself — an unreadable value must not resolve to either position.
        config_dir, ws = _install(tmp_path, settings="lockdown: yess\n" + _JWT)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "lockdown must be true or false" in str(ei.value)

    def test_unset_required_ref_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NERVE_LOCKDOWN", raising=False)
        config_dir, ws = _install(
            tmp_path, settings="lockdown: ${NERVE_LOCKDOWN}\n" + _JWT,
        )
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "NERVE_LOCKDOWN" in str(ei.value)

    def test_empty_env_var_is_refused(self, tmp_path, monkeypatch):
        """``FLAG=`` switches an ordinary feature off; it must not switch this
        one off, because a variable that failed to be populated would silently
        drop every restriction the flag imposes."""
        monkeypatch.setenv("NERVE_LOCKDOWN", "")
        config_dir, ws = _install(
            tmp_path, settings="lockdown: ${NERVE_LOCKDOWN}\n" + _JWT,
        )
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "lockdown must be true or false" in str(ei.value)

    def test_bare_key_is_off(self, tmp_path):
        config_dir, ws = _install(tmp_path, settings="lockdown:\ntimezone: UTC\n")
        assert load_config(config_dir).lockdown is False

    def test_machine_layer_cannot_unlock_with_an_env_ref(self, tmp_path, monkeypatch):
        """The tracked file stays the only authority once the flag is a reference:
        a local layer that resolves to false must not reach it."""
        monkeypatch.setenv("NERVE_LOCKDOWN", "false")
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT,
            base="lockdown: ${NERVE_LOCKDOWN}\n",
            local="lockdown: false\n",
        )
        assert load_config(config_dir).lockdown is True

    def test_from_dict_parses_a_string_flag(self):
        """``NerveConfig.from_dict`` is called with an already-merged dict by the
        validator and by tests, so it must agree with the loader."""
        assert NerveConfig.from_dict({"lockdown": "false"}).lockdown is False
        assert NerveConfig.from_dict({"lockdown": "1"}).lockdown is True
        with pytest.raises(ConfigError):
            NerveConfig.from_dict({"lockdown": "sure"})

    def test_validator_reports_an_unreadable_flag(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = _install(tmp_path, settings="lockdown: perhaps\n" + _JWT)
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert any("lockdown must be true or false" in e for e in result.errors)

    def test_machine_layer_lockdown_is_warned_about(self, tmp_path, caplog):
        """Ignoring it is the feature; ignoring it in silence leaves whoever put
        it in the wrong file believing the box is locked."""
        import logging

        config_dir, ws = _install(tmp_path, base="lockdown: true\n")
        with caplog.at_level(logging.WARNING, logger="nerve.config"):
            assert load_config(config_dir).lockdown is False
        assert any(
            "lockdown" in r.message and "settings.yaml" in r.message
            for r in caplog.records
        ), [r.message for r in caplog.records]

    def test_unreadable_settings_file_is_a_config_error(self, tmp_path):
        """``_read_yaml_mapping`` promises ConfigError rather than a traceback for
        a file it cannot use; that covered a parse failure but not a read one."""
        config_dir, ws = _install(tmp_path, settings="lockdown: false\n")
        settings = workspace_settings_file(ws)
        settings.unlink()
        settings.mkdir()  # a directory where the file should be
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "Cannot read" in str(ei.value)


class TestValidatingTheLockedViewInCi:
    """A fleet repo writes ``lockdown: ${NERVE_LOCKDOWN:-false}`` so one bundle can
    serve locked and unlocked boxes. CI has no such variable, so it resolved false
    and validated the view no locked box will ever run — leaving the lockdown
    checks with nothing to fire on and the locked instance to find out at boot.
    """

    _FLEET = "lockdown: ${NERVE_LOCKDOWN:-false}\ntimezone: UTC\n"

    def test_env_controlled_flag_passes_by_default(self, tmp_path, monkeypatch):
        from nerve.config_validate import validate_config_bundle

        monkeypatch.delenv("NERVE_LOCKDOWN", raising=False)
        config_dir, ws = _install(tmp_path, settings=self._FLEET)
        # No auth.jwt_secret anywhere: broken for a locked box, fine for this one.
        result = validate_config_bundle(
            config_dir, workspace_override=ws, strict_env=True,
        )
        assert result.ok
        # ...but the gap is at least named, which is how anyone learns to ask.
        assert any("locked view was NOT validated" in w for w in result.warnings)

    def test_assume_lockdown_catches_it(self, tmp_path, monkeypatch):
        """A locked-only error — here a config subtree symlinked out of the
        workspace — is invisible to the default run and fatal under the flag."""
        from nerve.config_validate import validate_config_bundle

        monkeypatch.delenv("NERVE_LOCKDOWN", raising=False)
        config_dir, ws = _install(tmp_path, settings=self._FLEET)
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        (outside / "settings.yaml").write_text(self._FLEET, encoding="utf-8")
        (ws / "config").rename(tmp_path / "discarded")
        (ws / "config").symlink_to(outside)

        assert validate_config_bundle(config_dir, workspace_override=ws).ok
        result = validate_config_bundle(
            config_dir, workspace_override=ws, assume_locked=True,
        )
        assert not result.ok
        assert any("outside the workspace" in e for e in result.errors), result.errors

    def test_assume_lockdown_says_it_is_assuming(self, tmp_path, monkeypatch):
        from nerve.config_validate import validate_config_bundle

        monkeypatch.delenv("NERVE_LOCKDOWN", raising=False)
        config_dir, ws = _install(tmp_path, settings=self._FLEET + _JWT)
        result = validate_config_bundle(
            config_dir, workspace_override=ws, assume_locked=True,
        )
        assert result.ok
        assert any("LOCKED view on request" in i for i in result.info)

    def test_a_literal_false_is_not_warned_about(self, tmp_path):
        """Only an env-controlled flag leaves a locked view unchecked. A bundle
        that says `lockdown: false` outright has no locked view to check."""
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = _install(tmp_path, settings="lockdown: false\n")
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert not any("locked view" in w for w in result.warnings)


class TestLockdownCronContainment:
    """Cron's three path keys decide which files a locked instance reads, and
    ``gate_plugins_dir`` decides which ``.py`` files it *executes*. A tracked
    ``settings.yaml`` is pure YAML that a reviewer may wave through, so pointing
    those keys out of the reviewed tree would turn a config edit into arbitrary
    on-disk code execution.
    """

    def _locked(self, tmp_path, cron_yaml, *, workspace=None):
        config_dir, ws = _install(
            tmp_path, settings="lockdown: true\n" + _JWT + cron_yaml,
        )
        (ws / "config" / "cron").mkdir(parents=True, exist_ok=True)
        return load_config(config_dir), ws

    def test_gate_plugins_dir_outside_the_workspace_is_dropped(self, tmp_path):
        outside = tmp_path / "outside"
        outside.mkdir()
        c, ws = self._locked(tmp_path, f"cron:\n  gate_plugins_dir: {outside}\n")
        assert c.cron.gate_plugins_dir == ws / "config" / "cron" / "gates"

    def test_the_redirect_no_longer_gets_code_executed(self, tmp_path):
        """The end that matters: with the path contained, the plugin loader never
        sees the outside directory, so nothing in it runs."""
        from nerve.cron.gate_plugins import load_gate_plugins

        outside = tmp_path / "outside"
        outside.mkdir()
        marker = tmp_path / "executed"
        (outside / "evil.py").write_text(
            f"open({str(marker)!r}, 'w').write('ran')\n", encoding="utf-8",
        )
        c, ws = self._locked(tmp_path, f"cron:\n  gate_plugins_dir: {outside}\n")
        assert load_gate_plugins(c.cron.gate_plugins_dir) == 0
        assert not marker.exists()

    def test_jobs_and_system_files_outside_are_dropped(self, tmp_path):
        c, ws = self._locked(
            tmp_path,
            f"cron:\n"
            f"  jobs_file: {tmp_path}/elsewhere/jobs.yaml\n"
            f"  system_file: {ws_escape(tmp_path)}\n",
        )
        assert c.cron.jobs_file == ws / "config" / "cron" / "jobs.yaml"
        assert c.cron.system_file == ws / "config" / "cron" / "system.yaml"

    def test_inside_the_workspace_but_outside_config_is_dropped(self, tmp_path):
        """Containment is to ``<workspace>/config``, not to the workspace: the
        workspace is also the agent's working directory, and a path it can write
        to freely is not a reviewed one."""
        c, ws = self._locked(
            tmp_path, f"cron:\n  gate_plugins_dir: {tmp_path}/ws/scratch/gates\n",
        )
        assert c.cron.gate_plugins_dir == ws / "config" / "cron" / "gates"

    def test_a_symlink_out_of_the_tree_is_dropped(self, tmp_path):
        """A path that *is* inside the subtree by name but resolves out of it —
        containment is judged on the resolved path for exactly this reason."""
        outside = tmp_path / "outside"
        outside.mkdir()
        config_dir, ws = _install(tmp_path, settings="lockdown: true\n" + _JWT)
        (ws / "config" / "cron").mkdir(parents=True)
        link = ws / "config" / "cron" / "borrowed-gates"
        link.symlink_to(outside, target_is_directory=True)
        workspace_settings_file(ws).write_text(
            "lockdown: true\n" + _JWT
            + f"cron:\n  gate_plugins_dir: {link}\n", encoding="utf-8",
        )
        c = load_config(config_dir)
        assert c.cron.gate_plugins_dir == ws / "config" / "cron" / "gates"

    def test_a_symlinked_cron_directory_refuses_to_load(self, tmp_path):
        """No substitution can fix this one — every default is derived from the
        escaping directory — so a locked instance declines to start."""
        outside = tmp_path / "outside"
        outside.mkdir()
        config_dir, ws = _install(tmp_path, settings="lockdown: true\n" + _JWT)
        (ws / "config" / "cron").symlink_to(outside, target_is_directory=True)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "outside the tracked config subtree" in str(ei.value)

    @pytest.mark.parametrize("name", ["gates", "jobs.yaml", "system.yaml"])
    def test_the_default_name_itself_as_a_symlink_refuses_to_load(self, tmp_path, name):
        """The case with no config key in it at all.

        The fallback for an escaping path is the in-workspace default, so when the
        *default's own name* is the symlink there is nothing contained to fall back
        to: substituting it hands back the path just rejected, with a warning that
        contradicts itself. Git tracks symlinks, so this needs no local write —
        a reviewed, merged config repo is enough.
        """
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "x.py").write_text("MARKER = 1\n", encoding="utf-8")
        config_dir, ws = _install(tmp_path, settings="lockdown: true\n" + _JWT)
        cron = ws / "config" / "cron"
        cron.mkdir(parents=True)
        target = outside if name == "gates" else outside / "x.py"
        (cron / name).symlink_to(target, target_is_directory=(name == "gates"))
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "outside the tracked config subtree" in str(ei.value)

    def test_a_symlinked_gates_default_never_gets_code_executed(self, tmp_path):
        """The same case, ended at the consequence rather than the exception."""
        from nerve.cron.gate_plugins import load_gate_plugins

        outside = tmp_path / "unreviewed_gates"
        outside.mkdir()
        marker = tmp_path / "executed"
        (outside / "evil.py").write_text(
            f"open({str(marker)!r}, 'w').write('ran')\n", encoding="utf-8",
        )
        config_dir, ws = _install(tmp_path, settings="lockdown: true\n" + _JWT)
        (ws / "config" / "cron").mkdir(parents=True)
        (ws / "config" / "cron" / "gates").symlink_to(outside, target_is_directory=True)
        with pytest.raises(ConfigError):
            config = load_config(config_dir)
            load_gate_plugins(config.cron.gate_plugins_dir)
        assert not marker.exists()


class TestLockdownTrackedSubtreeIsInTheWorkspace:
    """One level above cron: ``<workspace>/config`` itself.

    Every other containment check judges a path against that directory, so if it
    is a symlink out of the workspace they all pass while nothing underneath is in
    the reviewed repo — settings.yaml and the gate plugins included, with lockdown
    read from there and reporting that all is well.
    """

    def _elsewhere(self, tmp_path, settings):
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        elsewhere = tmp_path / "local-config"
        config_dir.mkdir()
        ws.mkdir()
        (elsewhere / "cron").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(
            f"workspace: {ws}\n", encoding="utf-8",
        )
        (ws / "config").symlink_to(elsewhere, target_is_directory=True)
        (elsewhere / "settings.yaml").write_text(settings, encoding="utf-8")
        return config_dir, ws

    def test_config_symlinked_out_of_the_workspace_refuses_to_load(self, tmp_path):
        config_dir, ws = self._elsewhere(tmp_path, "lockdown: true\n" + _JWT)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "outside the workspace" in str(ei.value)

    def test_unlocked_is_unaffected(self, tmp_path):
        """A symlinked config/ is a legitimate machine-local arrangement; only a
        locked instance, which claims that tree *is* the reviewed repo, cannot."""
        config_dir, ws = self._elsewhere(tmp_path, "timezone: UTC\n")
        assert load_config(config_dir).timezone == "UTC"

    def test_the_validator_reports_it(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = self._elsewhere(tmp_path, "lockdown: true\n" + _JWT)
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert any("outside the workspace" in e for e in result.errors)

    def test_a_symlinked_workspace_is_still_fine(self, tmp_path):
        """Where a machine keeps its workspace stays a machine-local decision."""
        real = tmp_path / "real-ws"
        (real / "config").mkdir(parents=True)
        _repo(real)
        link = tmp_path / "ws"
        link.symlink_to(real, target_is_directory=True)
        config_dir = tmp_path / "cfg"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text(
            f"workspace: {link}\n", encoding="utf-8",
        )
        (real / "config" / "settings.yaml").write_text(
            "lockdown: true\n" + _JWT, encoding="utf-8",
        )
        assert load_config(config_dir).lockdown is True

    def test_unlocked_overrides_are_still_honored(self, tmp_path):
        """An unmigrated install is entitled to point cron anywhere — that is what
        the machine-local layers are for."""
        outside = tmp_path / "outside"
        outside.mkdir()
        config_dir, ws = _install(
            tmp_path, base=f"cron:\n  gate_plugins_dir: {outside}\n",
        )
        assert load_config(config_dir).cron.gate_plugins_dir == outside

    def test_validation_contains_paths_against_the_candidate_workspace(self, tmp_path):
        """The validator pins the workspace to the tree under review, so the same
        containment has to be judged there — a candidate bundle must not be able
        to make validation read from outside its own checkout."""
        from nerve.config_validate import validate_config_bundle

        config_dir = tmp_path / "cfg"
        candidate = tmp_path / "candidate"
        outside = tmp_path / "outside"
        config_dir.mkdir()
        (candidate / "config" / "cron").mkdir(parents=True)
        _repo(candidate)
        outside.mkdir()
        (outside / "jobs.yaml").write_text("jobs: not-a-list\n", encoding="utf-8")
        (candidate / "config" / "cron" / "jobs.yaml").write_text(
            "jobs: []\n", encoding="utf-8",
        )
        workspace_settings_file(candidate).write_text(
            "lockdown: true\n" + _JWT
            + f"cron:\n  jobs_file: {outside}/jobs.yaml\n", encoding="utf-8",
        )
        result = validate_config_bundle(config_dir, workspace_override=candidate)
        assert result.ok, result.errors
        assert any(str(candidate) in i and "cron jobs" in i for i in result.info)

    def _sibling(self, tmp_path, settings):
        """``config`` as a symlink to a sibling directory *inside* the workspace."""
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir()
        (ws / "notes" / "cfg" / "cron" / "gates").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        (ws / "config").symlink_to(Path("notes") / "cfg", target_is_directory=True)
        (ws / "notes" / "cfg" / "settings.yaml").write_text(settings, encoding="utf-8")
        return config_dir, ws

    def test_config_symlinked_to_a_sibling_directory_refuses_to_load(self, tmp_path):
        """Containment alone does not make the subtree the reviewed one. The link
        target is inside the workspace, so every check that resolves passes."""
        config_dir, ws = self._sibling(tmp_path, "lockdown: true\n" + _JWT)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "must be a real directory" in str(ei.value)

    def test_a_symlinked_config_stays_fine_unlocked(self, tmp_path):
        config_dir, ws = self._sibling(tmp_path, "timezone: UTC\n")
        assert load_config(config_dir).timezone == "UTC"

    @pytest.mark.skipif(not shutil.which("git"), reason="git not available")
    def test_git_reports_nothing_under_a_symlinked_config(self, tmp_path):
        """Why the symlink is refused rather than tolerated: git does not descend
        into it, so sync's divergence check — the thing that keeps unreviewed
        ``cron/gates/*.py`` off a locked box — sees an empty subtree and calls the
        workspace clean while the daemon imports what is there.
        """
        from nerve.sync_service import _local_config_divergence

        config_dir, ws = self._sibling(tmp_path, "lockdown: true\n" + _JWT)
        env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
               "HOME": str(tmp_path), "PATH": os.environ["PATH"]}
        for args in (["init", "-b", "main"], ["add", "-A"], ["commit", "-m", "init"]):
            subprocess.run(["git", *args], cwd=str(ws), check=True,
                           capture_output=True, text=True, env=env)
        (ws / "notes" / "cfg" / "cron" / "gates" / "evil.py").write_text(
            "MARKER = 1\n", encoding="utf-8",
        )
        # Untracked code the daemon would import, and git says the subtree is clean.
        assert _local_config_divergence(ws, "HEAD", True) == ([], [])
        # So the layout has to be refused at the only place that can see it.
        with pytest.raises(ConfigError):
            load_config(config_dir)


class TestLockdownSettingsFileIsInTheSubtree:
    """The subtree can be the reviewed tree and ``settings.yaml`` still not be
    part of it. It carries the ``lockdown`` flag, the auth secret and the cron
    paths, so a symlink there is the whole tracked layer arriving from a file no
    reviewer saw — and, when it points at an ordinary workspace file, from one the
    agent may write with the tools lockdown leaves it.
    """

    def _linked(self, tmp_path, target, settings="lockdown: true\n" + _JWT):
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        (ws / "notes").mkdir()
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        real = ws / "notes" / "settings.yaml" if target == "inside" else tmp_path / "outside.yaml"
        real.write_text(settings, encoding="utf-8")
        link = Path("..") / "notes" / "settings.yaml" if target == "inside" else real
        workspace_settings_file(ws).symlink_to(link)
        return config_dir, ws

    def test_settings_symlinked_out_of_the_workspace_refuses_to_load(self, tmp_path):
        config_dir, ws = self._linked(tmp_path, "outside")
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "outside the tracked config subtree" in str(ei.value)

    def test_settings_symlinked_to_an_ordinary_workspace_file_refuses_to_load(self, tmp_path):
        """A relative link that never leaves the workspace, which is the harder
        case: nothing about it escapes, and the target is a file the agent writes
        the same way it writes its notes."""
        config_dir, ws = self._linked(
            tmp_path, "inside", "lockdown: true\nauth:\n  jwt_secret: attacker\n",
        )
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "outside the tracked config subtree" in str(ei.value)

    def test_the_validator_reports_it(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = self._linked(tmp_path, "inside")
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert any("outside the tracked config subtree" in e for e in result.errors)

    def test_unlocked_is_unaffected(self, tmp_path):
        config_dir, ws = self._linked(tmp_path, "outside", "timezone: UTC\n")
        assert load_config(config_dir).timezone == "UTC"

    def test_a_real_settings_file_loads(self, tmp_path):
        config_dir, ws = _install(tmp_path, settings="lockdown: true\ntimezone: UTC\n" + _JWT)
        assert lockdown_workspace_problems(ws) == []
        assert load_config(config_dir).timezone == "UTC"

    def test_an_absent_settings_file_is_not_a_problem(self, tmp_path):
        """Nothing to point anywhere. A locked instance can be anchored by the
        environment with no tracked settings file at all."""
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        _repo(ws)
        assert lockdown_workspace_problems(ws) == []

    def test_a_link_within_the_subtree_is_fine(self, tmp_path):
        """Both ends are tracked and reviewed, so the name is a layout choice the
        config repo is entitled to make. The check is containment, not a ban on
        symlinks."""
        config_dir, ws = _install(tmp_path)
        (ws / "config" / "shared.yaml").write_text(
            "lockdown: true\ntimezone: UTC\n" + _JWT, encoding="utf-8",
        )
        workspace_settings_file(ws).symlink_to(Path("shared.yaml"))
        assert load_config(config_dir).timezone == "UTC"

    def test_every_problem_is_reported_not_just_the_first(self, tmp_path):
        """An operator fixing a layout wants the list; the loader only needs one."""
        ws = tmp_path / "ws"
        (ws / "notes" / "cfg").mkdir(parents=True)
        _repo(ws)
        (ws / "notes" / "settings.yaml").write_text("lockdown: true\n", encoding="utf-8")
        (ws / "config").symlink_to(Path("notes") / "cfg", target_is_directory=True)
        (ws / "notes" / "cfg" / "settings.yaml").symlink_to(
            Path("..") / "settings.yaml",
        )
        problems = lockdown_workspace_problems(ws)
        assert len(problems) == 2
        assert any("must be a real directory" in p for p in problems)
        assert any("outside the tracked config subtree" in p for p in problems)


@pytest.mark.skipif(not shutil.which("git"), reason="git not available")
class TestLockdownNeedsSomewhereToReceiveConfigFrom:
    """A locked instance refuses every local change to the reviewed surface and
    tells the operator to open a PR instead. That flow ends in a git pull, so
    sync is the only route left in, and a workspace with no remote has no route
    at all: nothing local may change the config and nothing remote can arrive.

    Refused rather than ignored. Honoring the flag only where a remote happens to
    be configured would make ``git remote remove origin`` an unlock — a
    machine-local one, which is what ``lockdown`` being unreadable from
    config.yaml and ``NERVE_LOCKDOWN`` never unlocking already exist to prevent.
    """

    def _workspace(self, tmp_path, settings="lockdown: true\n" + _JWT):
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        workspace_settings_file(ws).write_text(settings, encoding="utf-8")
        return config_dir, ws

    def test_a_workspace_that_is_not_a_repository_refuses_to_load(self, tmp_path):
        config_dir, ws = self._workspace(tmp_path)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "is not a git repository" in str(ei.value)

    def test_a_repository_with_no_remote_refuses_to_load(self, tmp_path):
        config_dir, ws = self._workspace(tmp_path)
        _repo(ws, remote=None)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "has no git remote" in str(ei.value)

    def test_a_remote_is_all_it_takes(self, tmp_path):
        config_dir, ws = self._workspace(tmp_path)
        _repo(ws)
        assert load_config(config_dir).lockdown is True

    def test_the_remote_need_not_be_named_origin(self, tmp_path):
        """Which remote to fetch is sync's decision, not this check's: with
        ``workspace_sync.branch`` unset it follows the current branch's own
        upstream, which need not be ``origin``. Demanding the name would refuse a
        workspace that syncs today."""
        config_dir, ws = self._workspace(tmp_path)
        _repo(ws, remote="upstream")
        assert load_config(config_dir).lockdown is True

    def test_dropping_the_remote_does_not_unlock_the_box(self, tmp_path):
        """The fail-open version of this check, stated as what it would do. A box
        that came up locked must not come up unlocked after one local command."""
        config_dir, ws = self._workspace(tmp_path)
        _repo(ws)
        assert load_config(config_dir).lockdown is True
        subprocess.run(["git", "remote", "remove", "origin"], cwd=str(ws),
                       check=True, capture_output=True)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "has no git remote" in str(ei.value)

    def test_unlocked_is_unaffected(self, tmp_path):
        """Most workspaces are not config repos at all. Where the config came from
        is only a question for an instance that claims it came from review."""
        config_dir, ws = self._workspace(tmp_path, "timezone: UTC\n")
        assert load_config(config_dir).timezone == "UTC"

    def test_the_validator_reports_it(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = self._workspace(tmp_path)
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert any("no way to reach this instance" in e for e in result.errors)

    def test_git_being_unusable_is_not_a_pass(self, tmp_path, monkeypatch):
        """Not being able to tell is not a yes, and a box that cannot run git
        cannot sync either."""
        import nerve.sync_service as sync

        config_dir, ws = self._workspace(tmp_path)
        _repo(ws)
        monkeypatch.setattr(sync, "_git", lambda args, cwd: subprocess.CompletedProcess(
            args, 1, "", "could not run git: [Errno 2] No such file or directory: 'git'",
        ))
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "could not list the remotes" in str(ei.value)


class TestLockdownWriteGuardJudgesBothViewsOfAPath:
    """A symlink is where "where does this path end up" and "what does this path
    name" stop agreeing, and each direction of the disagreement is a way through.
    Both are checked, and both directions are pinned here so a later refactor
    cannot drop one for the other.
    """

    def _locked(self, tmp_path, monkeypatch):
        ws = tmp_path / "ws"
        (ws / "config" / "cron" / "gates").mkdir(parents=True)
        (ws / "notes").mkdir()
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        return ws

    def test_a_symlink_at_the_reviewed_name_is_refused(self, tmp_path, monkeypatch):
        """The lexical direction. Resolving alone puts the target outside the
        subtree, which reads as "not lockdown's business" — while the write goes
        through the link to the file the daemon loads as tracked config."""
        ws = self._locked(tmp_path, monkeypatch)
        outside = tmp_path / "outside.yaml"
        outside.write_text("lockdown: true\n", encoding="utf-8")
        (ws / "config" / "settings.yaml").symlink_to(outside)
        assert tracked_config_write_refusal("config/settings.yaml")
        assert tracked_config_write_refusal(ws / "config" / "settings.yaml")

    def test_a_relative_link_that_stays_in_the_workspace_is_refused_too(self, tmp_path, monkeypatch):
        ws = self._locked(tmp_path, monkeypatch)
        (ws / "notes" / "settings.yaml").write_text("lockdown: true\n", encoding="utf-8")
        (ws / "config" / "settings.yaml").symlink_to(
            Path("..") / "notes" / "settings.yaml",
        )
        assert tracked_config_write_refusal("config/settings.yaml")

    @pytest.mark.parametrize("path", [
        "notes/settings-link.yaml",          # a file symlink into the subtree
        "notes/cfgdir/settings.yaml",        # through a directory symlink
        "notes/cfgdir/cron/gates/new.py",    # a path that does not exist yet
        "notes/../config/settings.yaml",     # .. traversal back in
    ])
    def test_a_path_that_reaches_into_the_subtree_is_refused(self, tmp_path, monkeypatch, path):
        """The resolving direction, which the lexical check cannot see. Dropping
        it in favour of comparing names would reopen every one of these."""
        ws = self._locked(tmp_path, monkeypatch)
        (ws / "config" / "settings.yaml").write_text("lockdown: true\n", encoding="utf-8")
        (ws / "notes" / "settings-link.yaml").symlink_to(ws / "config" / "settings.yaml")
        (ws / "notes" / "cfgdir").symlink_to(ws / "config", target_is_directory=True)
        assert tracked_config_write_refusal(path)

    def test_ordinary_workspace_files_stay_writable(self, tmp_path, monkeypatch):
        """Including the target of the link above. The workspace is the agent's
        working directory and lockdown does not make it read-only; what keeps a
        link at ``config/settings.yaml`` from mattering is that a locked instance
        with one refuses to start — see
        :class:`TestLockdownSettingsFileIsInTheSubtree`.
        """
        ws = self._locked(tmp_path, monkeypatch)
        (ws / "config" / "settings.yaml").symlink_to(
            Path("..") / "notes" / "settings.yaml",
        )
        assert tracked_config_write_refusal("notes/settings.yaml") is None
        assert tracked_config_write_refusal("memory/notes.md") is None


class TestLockdownReviewedSurface:
    """``config/`` is not the whole of what a locked instance was reviewed for.

    ``skills/`` reaches the model as instructions with their own
    ``allowed-tools``, indexed by ``SkillManager.discover`` at startup and on
    every reload, and the root instruction files are the system prompt. The skill
    endpoints refuse under lockdown already, which only meant the 403 could be
    walked around with the plainest tool the agent has.
    """

    def _locked(self, tmp_path, monkeypatch):
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        return ws

    @pytest.mark.parametrize("path", [
        "skills/backdoor/SKILL.md",
        "skills/backdoor/scripts/run.sh",
        "skills",
        "AGENTS.md",
        "SOUL.md",
        "IDENTITY.md",
        "USER.md",
        "TOOLS.md",
    ])
    def test_the_reviewed_surface_is_refused(self, tmp_path, monkeypatch, path):
        self._locked(tmp_path, monkeypatch)
        refusal = tracked_config_write_refusal(path)
        assert refusal, path
        assert "pull request" in refusal

    @pytest.mark.parametrize("path", [
        "memory/notes.md",
        "tasks/active/t1.md",
        "README.md",
        # In PROMPT_FILES beside SOUL.md and read into the same prompt, and
        # deliberately not reviewed: it is the running task list the agent keeps.
        "TASK.md",
        # Neighbouring names, to pin that the match is on path components.
        "skills.md",
        "SOUL.md.bak",
        "memory/SOUL.md",
    ])
    def test_the_rest_of_the_workspace_stays_writable(self, tmp_path, monkeypatch, path):
        self._locked(tmp_path, monkeypatch)
        assert tracked_config_write_refusal(path) is None, path

    def test_a_symlink_at_a_reviewed_root_file_is_refused(self, tmp_path, monkeypatch):
        """The lexical half, one directory up from ``config/``: writing through
        the link changes the file the prompt is built from.

        The link's target goes with it, unlike the ``config/settings.yaml`` case
        where the reviewed root is the directory and a link at a file inside it
        leaves the target an ordinary workspace file. Here the reviewed root *is*
        the file, so resolving it names the target — and that is the answer this
        one needs: nothing refuses a locked instance whose ``SOUL.md`` is a
        symlink, so the write guard is the only thing standing between the agent
        and its own instructions.
        """
        ws = self._locked(tmp_path, monkeypatch)
        (ws / "notes").mkdir()
        (ws / "notes" / "soul.md").write_text("you are helpful\n", encoding="utf-8")
        (ws / "SOUL.md").symlink_to(Path("notes") / "soul.md")
        assert tracked_config_write_refusal("SOUL.md")
        assert tracked_config_write_refusal("notes/soul.md")

    def test_a_path_that_reaches_into_skills_is_refused(self, tmp_path, monkeypatch):
        ws = self._locked(tmp_path, monkeypatch)
        (ws / "skills" / "x").mkdir(parents=True)
        (ws / "notes").mkdir()
        (ws / "notes" / "skilldir").symlink_to(ws / "skills", target_is_directory=True)
        assert tracked_config_write_refusal("notes/skilldir/x/SKILL.md")
        assert tracked_config_write_refusal("notes/../skills/x/SKILL.md")

    def test_unlocked_writes_skills_freely(self, tmp_path, monkeypatch):
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False, workspace=ws))
        assert tracked_config_write_refusal("skills/x/SKILL.md") is None
        assert tracked_config_write_refusal("SOUL.md") is None

    def test_a_symlinked_skills_directory_refuses_to_load(self, tmp_path):
        """Same layout, same reason as a symlinked ``config/``: git does not
        descend into it, so nothing under it is tracked while ``discover`` indexes
        whatever is there."""
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        (ws / "notes" / "skills").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        workspace_settings_file(ws).write_text("lockdown: true\n" + _JWT, encoding="utf-8")
        (ws / "skills").symlink_to(Path("notes") / "skills", target_is_directory=True)

        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "must be a real directory" in str(ei.value)
        assert "skills" in str(ei.value)

    def test_skills_symlinked_out_of_the_workspace_refuses_to_load(self, tmp_path):
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        elsewhere = tmp_path / "local-skills"
        config_dir.mkdir()
        elsewhere.mkdir()
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        workspace_settings_file(ws).write_text("lockdown: true\n" + _JWT, encoding="utf-8")
        (ws / "skills").symlink_to(elsewhere, target_is_directory=True)

        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "outside the workspace" in str(ei.value)

    def test_an_absent_skills_directory_is_not_a_problem(self, tmp_path):
        """Most workspaces have no skills at all, and a missing directory is not
        a redirected one."""
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        _repo(ws)
        assert lockdown_workspace_problems(ws) == []

    def test_a_symlinked_skills_directory_stays_fine_unlocked(self, tmp_path):
        config_dir = tmp_path / "cfg"
        ws = tmp_path / "ws"
        elsewhere = tmp_path / "local-skills"
        config_dir.mkdir()
        elsewhere.mkdir()
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        workspace_settings_file(ws).write_text("timezone: UTC\n", encoding="utf-8")
        (ws / "skills").symlink_to(elsewhere, target_is_directory=True)
        assert load_config(config_dir).timezone == "UTC"


# Representative workspace-relative paths, and whether each is part of the
# reviewed surface. Hand-written on purpose: it is the statement the three
# implementations below are measured against, and deriving it from any of them
# would only make each agree with itself.
_SURFACE_TABLE: dict[str, bool] = {
    "config/settings.yaml": True,
    "config/cron/jobs.yaml": True,
    "config/cron/gates/gate.py": True,
    "skills/x/SKILL.md": True,
    "AGENTS.md": True,
    "SOUL.md": True,
    "IDENTITY.md": True,
    "USER.md": True,
    "TOOLS.md": True,
    "memory/notes.md": False,
    "tasks/active/t1.md": False,
    "TASK.md": False,
    "README.md": False,
    "../outside.md": False,
}


class TestReviewedSurfaceAgreement:
    """Three implementations, one surface, and nothing that made them agree.

    The write guard decides what a locked instance may not write, sync's
    divergence check decides what a local edit blocks a merge over, and the
    startup check decides which subtrees have to really be in the workspace.
    They were written at different times against ``config/`` alone, and the
    branch that started treating ``skills/`` as reviewed changed one of them:
    ``create_skill`` refused while ``Write`` did not, so the same file was
    forbidden through the endpoint, allowed through the tool, and invisible to
    sync.

    Each row of ``_SURFACE_TABLE`` is put to all three. What is asserted is
    agreement with the table, not agreement with each other — three
    implementations can be uniformly wrong.
    """

    def test_the_table_covers_the_declared_surface(self):
        """The table is the artifact the rest of this class trusts, so it is
        checked against the constants rather than against any of the guards."""
        from nerve.config import REVIEWED_DIRS, REVIEWED_ROOT_FILES

        reviewed = [p for p, want in _SURFACE_TABLE.items() if want]
        dirs_covered = {p.split("/")[0] for p in reviewed if "/" in p}
        files_covered = {p for p in reviewed if "/" not in p}
        assert set(REVIEWED_DIRS) <= dirs_covered, (
            "a reviewed directory no row of the table names — every guard below "
            "would pass without ever looking at it"
        )
        assert REVIEWED_ROOT_FILES <= files_covered, (
            "a reviewed root file no row of the table names"
        )
        stray = [
            p for p in reviewed
            if p.split("/")[0] not in REVIEWED_DIRS and p not in REVIEWED_ROOT_FILES
        ]
        assert not stray, f"rows claiming to be reviewed that nothing declares: {stray}"

    @pytest.mark.parametrize("path,reviewed", sorted(_SURFACE_TABLE.items()))
    def test_the_write_guard_agrees(self, tmp_path, monkeypatch, path, reviewed):
        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        assert (tracked_config_write_refusal(path) is not None) is reviewed, path

    @pytest.mark.skipif(not shutil.which("git"), reason="git not available")
    def test_the_sync_pathspec_agrees(self, tmp_path):
        """Asked of git, not of the pathspec: an entry that is spelled wrong, or
        that git reads as something other than a path, matches nothing and would
        pass a comparison against the list itself.
        """
        from nerve.sync_service import _local_config_divergence

        ws = tmp_path / "ws"
        ws.mkdir()
        env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
               "HOME": str(tmp_path), "PATH": os.environ["PATH"]}
        (ws / ".keep").write_text("", encoding="utf-8")
        for args in (["init", "-b", "main"], ["add", "-A"], ["commit", "-m", "init"]):
            subprocess.run(["git", *args], cwd=str(ws), check=True,
                           capture_output=True, text=True, env=env)

        # Outside the repository, so git has nothing to say about it either way.
        skipped = [p for p in _SURFACE_TABLE if p.startswith("../")]
        assert all(not _SURFACE_TABLE[p] for p in skipped), (
            "a reviewed path outside the workspace makes no sense; the sync check "
            "could never see it"
        )
        for path in _SURFACE_TABLE:
            if path in skipped:
                continue
            target = ws / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("local\n", encoding="utf-8")

        blocking, _warnings = _local_config_divergence(ws, "HEAD")
        named = {entry.split(" ", 1)[1] for entry in blocking}
        for path, reviewed in _SURFACE_TABLE.items():
            if path in skipped:
                continue
            assert (path in named) is reviewed, (path, sorted(named))

    def test_the_startup_check_agrees(self, tmp_path):
        """Redirect the subtree each path lives in and ask whether a locked
        instance still calls the layout sound.

        The construction needs a subtree to redirect, so rows naming a file at
        the workspace root, or a path outside it, have nothing to build — see the
        skip assertion. What that leaves uncovered is real and named in
        ``docs/config.md``: a locked instance whose ``SOUL.md`` is a symlink
        starts, and it is the write guard, refusing the link's target as well,
        that keeps the file the prompt is built from unwritable.
        """
        from nerve.config import REVIEWED_DIRS

        skipped = [p for p in _SURFACE_TABLE if "/" not in p or p.startswith("../")]
        assert all(
            "/" not in p or p.startswith("../") for p in skipped
        ), "only a path with no subtree of its own may be skipped here"
        assert set(REVIEWED_DIRS) <= {
            p.split("/")[0] for p, want in _SURFACE_TABLE.items()
            if want and p not in skipped
        }, "a reviewed directory that the skip rule would let past unchecked"

        for i, (path, reviewed) in enumerate(sorted(_SURFACE_TABLE.items())):
            if path in skipped:
                continue
            ws = tmp_path / f"ws{i}"
            (ws / "config").mkdir(parents=True)
            _repo(ws)
            workspace_settings_file(ws).write_text(
                "lockdown: true\n" + _JWT, encoding="utf-8",
            )
            subtree = path.split("/")[0]
            elsewhere = tmp_path / f"elsewhere{i}"
            elsewhere.mkdir()
            if subtree == "config":
                # Already a real directory; replace it with the redirect.
                shutil.rmtree(ws / "config")
                (elsewhere / "settings.yaml").write_text(
                    "lockdown: true\n" + _JWT, encoding="utf-8",
                )
            (ws / subtree).symlink_to(elsewhere, target_is_directory=True)

            problems = lockdown_workspace_problems(ws)
            assert bool(problems) is reviewed, (path, problems)


def ws_escape(tmp_path: Path) -> str:
    """A ``..``-traversal path that climbs out of the workspace config subtree."""
    return f"{tmp_path}/ws/config/cron/../../../system.yaml"


class TestLockdownTelegramDefault:
    """``telegram.enabled`` is a per-machine decision, so ``nerve init`` writes it
    to ``config.yaml`` — the layer lockdown drops. Left to its declared default
    that would read as "on", so a box where Telegram was switched off would start
    answering DMs, with full agent access, as soon as the shared settings carried
    a token its environment can resolve.
    """

    _TOKEN = "telegram:\n  bot_token: 12345:abc\n"

    def test_locked_leaves_telegram_off_when_the_tracked_file_is_silent(self, tmp_path):
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT + self._TOKEN,
            base="telegram:\n  enabled: false\n",
        )
        c = load_config(config_dir)
        assert c.telegram.enabled is False
        assert c.telegram.bot_token  # the token is there; the switch is not

    def test_locked_honors_an_explicit_enable(self, tmp_path):
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT
            + "telegram:\n  enabled: true\n  bot_token: 12345:abc\n",
        )
        assert load_config(config_dir).telegram.enabled is True

    def test_locked_honors_an_env_ref_per_machine(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TG_ON", "true")
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT
            + "telegram:\n  enabled: ${TG_ON}\n  bot_token: 12345:abc\n",
        )
        assert load_config(config_dir).telegram.enabled is True

    def test_unlocked_default_is_unchanged(self, tmp_path):
        config_dir, ws = _install(tmp_path, base=self._TOKEN)
        assert load_config(config_dir).telegram.enabled is True

    @pytest.mark.parametrize(
        "spelling", ["yess", "disabled", "${TG_ON:-nope}", "[]"],
    )
    def test_locked_reads_an_unparseable_value_as_off(self, tmp_path, spelling):
        """Being unstated was not the only way to reach the wrong answer.

        A value the parser cannot read falls back to a default, and coercion's
        default is the field's *declared* one — ``True``. So the whole guard was
        one typo wide, and `${TG_ON:-nope}` is the very spelling the docs
        recommend for a per-box fleet setting: one bad env value would have turned
        the bot on across the fleet.
        """
        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT
            + f"telegram:\n  enabled: {spelling}\n  bot_token: 12345:abc\n",
            base="telegram:\n  enabled: false\n",
        )
        assert load_config(config_dir).telegram.enabled is False

    def test_unlocked_unparseable_value_keeps_the_declared_default(self, tmp_path):
        """Only the fallback *direction* is lockdown's business. Unlocked, the
        machine layer is where this is normally stated and reverting to the
        documented default is what an operator expects."""
        config_dir, ws = _install(
            tmp_path, base="telegram:\n  enabled: yess\n  bot_token: 12345:abc\n",
        )
        assert load_config(config_dir).telegram.enabled is True

    def test_validator_names_the_setting_that_has_nowhere_to_live(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = _install(
            tmp_path, settings="lockdown: true\n" + _JWT + self._TOKEN,
        )
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert result.ok
        assert any("telegram.enabled" in w for w in result.warnings)

    def test_every_dropped_key_is_named_not_just_telegram(self, tmp_path):
        """The note previously covered one key of the seventeen on the list.

        The uncovered case that mattered was a locked box reverting from Bedrock
        to the Anthropic default: nothing reported it, and the only symptom was a
        missing API key the box would not otherwise have needed.
        """
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT,
            base=(
                "gateway:\n  port: 9100\n  ssl:\n    cert: /etc/tls/c.pem\n"
                "provider:\n  type: bedrock\n  aws_region: eu-west-1\n"
                "  aws_profile: prod\n"
                "sync:\n  gmail:\n    accounts: [me@example.com]\n"
            ),
        )
        notes = " ".join(
            validate_config_bundle(config_dir, workspace_override=ws).warnings
        )
        # Shareable values that should not have been stranded in config.yaml.
        for path in ("gateway.port", "provider.type", "provider.aws_region"):
            assert path in notes, path
        # ...reported separately from the ones that are local by design, because
        # the fix differs: move the first group, decide about the second.
        for path in ("gateway.ssl.cert", "provider.aws_profile",
                     "sync.gmail.accounts"):
            assert path in notes, path
        assert "move them to workspace/config/settings.yaml" in notes

    def test_a_key_the_tracked_settings_restate_is_not_reported(self, tmp_path):
        """Only silence is worth a warning. A tracked value that config.yaml also
        sets is a normal local override, and on a locked box it simply wins."""
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT + "gateway:\n  port: 9100\n",
            base="gateway:\n  port: 9100\n",
        )
        notes = " ".join(
            validate_config_bundle(config_dir, workspace_override=ws).warnings
        )
        assert "gateway.port" not in notes

    def test_a_falsey_tracked_value_counts_as_stated(self, tmp_path):
        """`enabled: false` is an answer, not an absence — presence is the test."""
        from nerve.config_validate import validate_config_bundle

        config_dir, ws = _install(
            tmp_path,
            settings="lockdown: true\n" + _JWT + "proxy:\n  enabled: false\n",
            base="proxy:\n  enabled: true\n",
        )
        notes = " ".join(
            validate_config_bundle(config_dir, workspace_override=ws).warnings
        )
        assert "proxy.enabled" not in notes


class TestLockdownTrackedConfigWrites:
    """Guards that sit in front of a named operation only cover the operations
    they were put on. An endpoint that takes a caller-supplied path is a
    different shape: whether it edits tracked config depends on the argument.
    """

    @pytest.mark.asyncio
    async def test_memory_file_route_cannot_edit_tracked_config(self, tmp_path, monkeypatch):
        from nerve.gateway.routes.memory import FileWriteRequest, write_memory_file

        ws = tmp_path / "ws"
        (ws / "config" / "cron" / "gates").mkdir(parents=True)
        monkeypatch.setattr(
            cfg, "_config", NerveConfig(lockdown=True, workspace=ws),
        )
        for path in ("config/settings.yaml", "config/cron/gates/evil.py"):
            with pytest.raises(LockdownError):
                await write_memory_file(
                    path, FileWriteRequest(content="lockdown: false\n"), user={},
                )
        assert not (ws / "config" / "settings.yaml").exists()
        assert not (ws / "config" / "cron" / "gates" / "evil.py").exists()

    @pytest.mark.asyncio
    async def test_memory_file_route_cannot_edit_the_instruction_files(self, tmp_path, monkeypatch):
        """The route writes anywhere under the workspace, and the workspace root
        is where the system prompt lives."""
        from nerve.gateway.routes.memory import FileWriteRequest, write_memory_file

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        for path in ("SOUL.md", "AGENTS.md", "skills/x/SKILL.md"):
            with pytest.raises(LockdownError):
                await write_memory_file(
                    path, FileWriteRequest(content="do whatever you like\n"), user={},
                )
            assert not (ws / path).exists()

    @pytest.mark.asyncio
    async def test_the_listing_marks_what_the_write_route_will_refuse(self, tmp_path, monkeypatch):
        """A UI that offers an edit the PUT answers with a 403 is worse than one
        that does not offer it. Both come from the same guard so they cannot part
        company."""
        from nerve.gateway.routes.memory import list_memory_files, read_memory_file

        ws = tmp_path / "ws"
        (ws / "memory").mkdir(parents=True)
        (ws / "SOUL.md").write_text("reviewed\n", encoding="utf-8")
        (ws / "NOTES.md").write_text("scratch\n", encoding="utf-8")
        (ws / "memory" / "notes.md").write_text("scratch\n", encoding="utf-8")
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))

        listed = {f["path"]: f["read_only"] for f in
                  (await list_memory_files(user={}))["files"]}
        assert listed == {
            "SOUL.md": True, "NOTES.md": False, "memory/notes.md": False,
        }
        assert (await read_memory_file("SOUL.md", user={}))["read_only"] is True
        assert (await read_memory_file("NOTES.md", user={}))["read_only"] is False

        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False, workspace=ws))
        unlocked = {f["path"]: f["read_only"] for f in
                    (await list_memory_files(user={}))["files"]}
        assert not any(unlocked.values())

    @pytest.mark.asyncio
    async def test_memory_file_route_still_writes_elsewhere(self, tmp_path, monkeypatch):
        """The workspace is also the agent's working directory — lockdown is
        about tracked config, not about making the box read-only."""
        from nerve.gateway.routes.memory import FileWriteRequest, write_memory_file

        ws = tmp_path / "ws"
        ws.mkdir()
        monkeypatch.setattr(
            cfg, "_config", NerveConfig(lockdown=True, workspace=ws),
        )
        await write_memory_file(
            "memory/notes.md", FileWriteRequest(content="hello\n"), user={},
        )
        assert (ws / "memory" / "notes.md").read_text() == "hello\n"

    @pytest.mark.asyncio
    async def test_unlocked_route_writes_config_freely(self, tmp_path, monkeypatch):
        from nerve.gateway.routes.memory import FileWriteRequest, write_memory_file

        ws = tmp_path / "ws"
        ws.mkdir()
        monkeypatch.setattr(
            cfg, "_config", NerveConfig(lockdown=False, workspace=ws),
        )
        await write_memory_file(
            "config/settings.yaml", FileWriteRequest(content="timezone: UTC\n"), user={},
        )
        assert (ws / "config" / "settings.yaml").exists()

    def test_memory_manager_write_file_is_guarded_too(self, tmp_path, monkeypatch):
        """The route's twin. Guarding one and not the other is not a guard."""
        from nerve.memory.manager import MemoryManager

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        mgr = MemoryManager(ws)
        with pytest.raises(LockdownError):
            mgr.write_file("config/settings.yaml", "lockdown: false\n")
        assert mgr.write_file("memory/notes.md", "hello\n") is True

    def test_save_jobs_is_guarded(self, tmp_path, monkeypatch):
        """No caller today, but the file it writes is tracked cron config."""
        from nerve.cron.jobs import save_jobs

        ws = tmp_path / "ws"
        (ws / "config" / "cron").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        with pytest.raises(LockdownError):
            save_jobs([], ws / "config" / "cron" / "jobs.yaml")

    @pytest.mark.asyncio
    async def test_task_route_cannot_be_pointed_at_tracked_config(self, tmp_path, monkeypatch):
        """``task["file_path"]`` is a stored path joined to the workspace. Today
        the indexer only ever fills it from a glob of ``tasks/``, so this is depth
        rather than a live hole — but it is the same shape as the memory PUT."""
        from nerve.gateway.routes import tasks as tasks_route

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        (ws / "config" / "settings.yaml").write_text("lockdown: true\n", encoding="utf-8")
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))

        class _Db:
            async def get_task(self, task_id):
                return {"file_path": "config/settings.yaml", "status": "pending"}

        monkeypatch.setattr(
            tasks_route, "get_deps", lambda: type("D", (), {"db": _Db()})(),
        )
        with pytest.raises(LockdownError):
            await tasks_route.update_task(
                "t1", tasks_route.TaskUpdateRequest(content="lockdown: false\n"),
                user={},
            )
        assert "lockdown: true" in (ws / "config" / "settings.yaml").read_text()


class TestLockdownTaskHandlersGuardWritesOnly:
    """The task tools join a stored ``file_path`` to the workspace, so each one
    that *changes* the file gets the guard. ``task_read`` must not: lockdown
    makes tracked config unwritable, not unreadable.
    """

    async def _locked_task(self, tmp_path, db, monkeypatch):
        """A locked instance whose task row points at tracked config."""
        from nerve.agent.tools.handlers import tasks as task_handlers
        from nerve.agent.tools.registry import ToolContext

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        (ws / "config" / "settings.yaml").write_text(
            "lockdown: true\n", encoding="utf-8",
        )
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        # Process-wide read-before-write set: give each test its own so the
        # write refusal below can only come from lockdown.
        monkeypatch.setattr(task_handlers, "_tasks_read", set())
        await db.upsert_task(
            task_id="t1", file_path="config/settings.yaml",
            title="T1", status="pending",
        )
        return ws, ToolContext(session_id="test", db=db, workspace=ws)

    @pytest.mark.asyncio
    async def test_read_is_not_refused_by_the_write_guard(self, tmp_path, db, monkeypatch):
        from nerve.agent.tools.handlers.tasks import task_read_handler

        _ws, ctx = await self._locked_task(tmp_path, db, monkeypatch)
        result = await task_read_handler(ctx, {"task_id": "t1"})
        assert "lockdown: true" in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_write_on_the_same_path_is_still_refused(self, tmp_path, db, monkeypatch):
        from nerve.agent.tools.handlers.tasks import (
            task_read_handler,
            task_write_handler,
        )

        ws, ctx = await self._locked_task(tmp_path, db, monkeypatch)
        await task_read_handler(ctx, {"task_id": "t1"})  # satisfies read-before-write
        with pytest.raises(LockdownError):
            await task_write_handler(
                ctx, {"task_id": "t1", "content": "lockdown: false\n"},
            )
        assert "lockdown: true" in (ws / "config" / "settings.yaml").read_text()

    @pytest.mark.asyncio
    async def test_update_is_still_refused(self, tmp_path, db, monkeypatch):
        from nerve.agent.tools.handlers.tasks import task_update_handler

        ws, ctx = await self._locked_task(tmp_path, db, monkeypatch)
        with pytest.raises(LockdownError):
            await task_update_handler(ctx, {"task_id": "t1", "note": "appended"})
        assert "lockdown: true" in (ws / "config" / "settings.yaml").read_text()

    @pytest.mark.asyncio
    async def test_done_is_refused_before_it_moves(self, tmp_path, db, monkeypatch):
        """``task_done`` appends to the file and renames it into ``done/`` —
        a rewrite and a move of tracked config, the most destructive of the
        three."""
        from nerve.agent.tools.handlers.tasks import task_done_handler

        ws, ctx = await self._locked_task(tmp_path, db, monkeypatch)
        with pytest.raises(LockdownError):
            await task_done_handler(ctx, {"task_id": "t1"})
        assert (ws / "config" / "settings.yaml").exists()
        # Refused before the DB was touched: no task left claiming to be done
        # with its file still sitting in the config subtree.
        assert (await db.get_task("t1"))["status"] == "pending"

    @pytest.mark.asyncio
    async def test_an_ordinary_task_is_untouched_by_any_of_them(self, tmp_path, db, monkeypatch):
        """The workspace is also the agent's working directory: a task file in
        its normal home is not config, locked or not."""
        from nerve.agent.tools.handlers import tasks as task_handlers
        from nerve.agent.tools.registry import ToolContext

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        active = ws / "memory" / "tasks" / "active"
        active.mkdir(parents=True)
        (active / "t2.md").write_text("# T2\n", encoding="utf-8")
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        monkeypatch.setattr(task_handlers, "_tasks_read", set())
        await db.upsert_task(
            task_id="t2", file_path="memory/tasks/active/t2.md",
            title="T2", status="pending",
        )
        ctx = ToolContext(session_id="test", db=db, workspace=ws)

        assert "# T2" in (
            await task_handlers.task_read_handler(ctx, {"task_id": "t2"})
        ).content[0]["text"]
        await task_handlers.task_write_handler(
            ctx, {"task_id": "t2", "content": "# T2 edited\n"},
        )
        assert (active / "t2.md").read_text() == "# T2 edited\n"
        await task_handlers.task_done_handler(ctx, {"task_id": "t2"})
        assert not (active / "t2.md").exists()
        assert (ws / "memory" / "tasks" / "done" / "t2.md").exists()


class TestLockdownTaskManagerGuardsTheMove:
    """``TaskManager.mark_done`` has the same append-and-rename shape as the
    ``task_done`` tool — a stored ``file_path`` joined to the workspace,
    appended to and then renamed into ``done/`` — so it needs the same guard,
    or it is a second route to rewriting and moving tracked config.

    Both cases run locked, so the second one is what stops the guard from being
    written as "refuse every move".
    """

    def _locked_manager(self, tmp_path, db, monkeypatch):
        from nerve.tasks.manager import TaskManager

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        return ws, TaskManager(ws, db)

    @pytest.mark.asyncio
    async def test_a_row_pointing_at_tracked_config_is_refused(self, tmp_path, db, monkeypatch):
        ws, manager = self._locked_manager(tmp_path, db, monkeypatch)
        (ws / "config" / "settings.yaml").write_text("lockdown: true\n", encoding="utf-8")
        await db.upsert_task(
            task_id="t1", file_path="config/settings.yaml",
            title="T1", status="pending",
        )

        with pytest.raises(LockdownError):
            await manager.mark_done("t1")

        assert (ws / "config" / "settings.yaml").read_text() == "lockdown: true\n"
        # Nothing half-done: no copy left in done/, no row claiming otherwise.
        assert not (ws / "memory" / "tasks" / "done" / "settings.yaml").exists()
        assert (await db.get_task("t1"))["status"] == "pending"

    @pytest.mark.asyncio
    async def test_an_ordinary_task_file_still_moves(self, tmp_path, db, monkeypatch):
        ws, manager = self._locked_manager(tmp_path, db, monkeypatch)
        active = ws / "memory" / "tasks" / "active"
        (active / "t2.md").write_text("# T2\n", encoding="utf-8")
        await db.upsert_task(
            task_id="t2", file_path="memory/tasks/active/t2.md",
            title="T2", status="pending",
        )

        assert await manager.mark_done("t2") is True
        assert not (active / "t2.md").exists()
        assert "DONE" in (ws / "memory" / "tasks" / "done" / "t2.md").read_text()
        assert (await db.get_task("t2"))["status"] == "done"


class TestSkillIdIsOnePathComponent:
    """``skill_id`` reaches ``skills_dir / skill_id`` straight from an HTTP path
    segment; ``_slugify`` only runs on create. Delete removes the whole tree it
    names, so ``../config`` took out the tracked config subtree."""

    @pytest.mark.asyncio
    async def test_delete_cannot_escape_the_skills_directory(self, tmp_path, db):
        from nerve.skills.manager import SkillIdError, SkillManager

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        (ws / "config" / "settings.yaml").write_text("lockdown: true\n", encoding="utf-8")
        mgr = SkillManager(ws, db)
        for bad in ("../config", "../../etc", "..", ".", "a/b", ".hidden"):
            with pytest.raises(SkillIdError):
                await mgr.delete_skill(bad)
        assert (ws / "config" / "settings.yaml").exists()

    @pytest.mark.asyncio
    async def test_existing_directory_names_stay_usable(self, tmp_path, db):
        """The filesystem is the source of truth and discover() adopts whatever
        directory names it finds, so the check has to be wider than _slugify."""
        from nerve.skills.manager import SkillManager

        ws = tmp_path / "ws"
        mgr = SkillManager(ws, db)
        skill_dir = ws / "skills" / "My_Skill.v2"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text(
            "---\nname: My Skill\ndescription: d\n---\nbody\n", encoding="utf-8",
        )
        await mgr.discover()
        assert await mgr.update_skill(
            "My_Skill.v2", "---\nname: My Skill\ndescription: e\n---\nbody\n",
        ) is not None


class TestLockdownEnvironmentAnchor:
    """Hardening the flag's value says nothing about which file supplies it.

    ``workspace:`` lives in the machine-local ``config.yaml`` and selects the
    ``settings.yaml`` that is then the only authority, so one local edit repoints
    the whole chain and lockdown evaluates to false. The anchor moves the decision
    out of every file on the box and into the service definition.
    """

    def _two_trees(self, tmp_path, *, machine_workspace):
        """A locked real workspace and an unlocked one an attacker points at."""
        config_dir = tmp_path / "cfg"
        real = tmp_path / "real-ws"
        attacker = tmp_path / "attacker-ws"
        config_dir.mkdir()
        (real / "config").mkdir(parents=True)
        (attacker / "config").mkdir(parents=True)
        _repo(real)
        workspace_settings_file(real).write_text(
            "lockdown: true\ntimezone: UTC\n" + _JWT, encoding="utf-8",
        )
        workspace_settings_file(attacker).write_text(
            "timezone: Europe/Berlin\n", encoding="utf-8",
        )
        (config_dir / "config.yaml").write_text(
            f"workspace: {machine_workspace}\nauth:\n  jwt_secret: attacker\n",
            encoding="utf-8",
        )
        return config_dir, real, attacker

    def test_repointing_the_workspace_unlocks_an_unanchored_box(self, tmp_path, monkeypatch):
        """The bypass, demonstrated. Nothing here is a bug on its own — this is
        what the anchor exists for, and it is why the anchor has to cover the
        workspace as well as the flag."""
        monkeypatch.delenv("NERVE_LOCKDOWN", raising=False)
        config_dir, real, attacker = self._two_trees(
            tmp_path, machine_workspace=tmp_path / "attacker-ws",
        )
        c = load_config(config_dir)
        assert c.lockdown is False
        assert c.workspace == attacker
        # ...and with the machine layers back, so is the machine's jwt_secret.
        assert c.auth.jwt_secret == "attacker"

    def test_the_anchor_closes_it(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NERVE_LOCKDOWN", "1")
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "real-ws"))
        config_dir, real, attacker = self._two_trees(
            tmp_path, machine_workspace=tmp_path / "attacker-ws",
        )
        c = load_config(config_dir)
        assert c.lockdown is True
        assert c.workspace == real            # the repoint is ignored
        assert c.timezone == "UTC"            # from the real tracked settings
        assert c.auth.jwt_secret == "test-secret"  # not the machine layer's

    def test_the_anchor_requires_a_workspace(self, tmp_path, monkeypatch):
        """Anchoring the flag alone would produce a box locked *onto* whatever
        tree config.yaml names — worse than unlocked, since it now trusts that
        tree exclusively."""
        monkeypatch.setenv("NERVE_LOCKDOWN", "true")
        monkeypatch.delenv("NERVE_WORKSPACE", raising=False)
        config_dir, real, attacker = self._two_trees(
            tmp_path, machine_workspace=tmp_path / "attacker-ws",
        )
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "NERVE_WORKSPACE" in str(ei.value)

    def test_a_relative_anchor_workspace_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NERVE_LOCKDOWN", "true")
        monkeypatch.setenv("NERVE_WORKSPACE", "relative/ws")
        config_dir, real, attacker = self._two_trees(
            tmp_path, machine_workspace=tmp_path / "real-ws",
        )
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "absolute" in str(ei.value)

    @pytest.mark.parametrize("value", ["false", "0", "off", "no", ""])
    def test_the_anchor_never_unlocks(self, tmp_path, monkeypatch, value):
        """Monotonic by design: the env can add lockdown, never remove it. A
        box whose reviewed config says locked cannot be unlocked from the
        environment — that still takes a merged change."""
        monkeypatch.setenv("NERVE_LOCKDOWN", value)
        monkeypatch.delenv("NERVE_WORKSPACE", raising=False)
        config_dir, ws = _install(tmp_path, settings="lockdown: true\n" + _JWT)
        assert load_config(config_dir).lockdown is True

    @pytest.mark.parametrize("value", ["false", "0", "off", ""])
    def test_no_opinion_leaves_an_unlocked_box_alone(self, tmp_path, monkeypatch, value):
        monkeypatch.setenv("NERVE_LOCKDOWN", value)
        monkeypatch.delenv("NERVE_WORKSPACE", raising=False)
        config_dir, ws = _install(tmp_path, base="timezone: UTC\n")
        c = load_config(config_dir)
        assert c.lockdown is False
        assert c.timezone == "UTC"  # machine layer still applies

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
    def test_accepted_spellings(self, tmp_path, monkeypatch, value):
        monkeypatch.setenv("NERVE_LOCKDOWN", value)
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "ws"))
        config_dir, ws = _install(tmp_path, settings=_JWT)
        assert load_config(config_dir).lockdown is True

    def test_an_unreadable_anchor_is_refused(self, tmp_path, monkeypatch):
        """The only other reading is "no opinion", which would silently discard
        an instruction to lock."""
        monkeypatch.setenv("NERVE_LOCKDOWN", "maybe")
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "ws"))
        config_dir, ws = _install(tmp_path, settings=_JWT)
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "NERVE_LOCKDOWN" in str(ei.value)

    def test_the_validator_judges_the_anchored_view(self, tmp_path, monkeypatch):
        from nerve.config_validate import validate_config_bundle

        monkeypatch.setenv("NERVE_LOCKDOWN", "1")
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "ws"))
        # The bundle never says lockdown, so an unlocked run would judge the
        # unlocked view; the environment is what makes this the locked one.
        config_dir, ws = _install(tmp_path, base="timezone: America/New_York\n")
        result = validate_config_bundle(config_dir)
        assert any("LOCKED view" in i for i in result.info)
        # config.yaml is what a locked box discards, so its keys are named.
        assert any("timezone" in w for w in result.warnings), result.warnings

    def test_an_explicit_workspace_still_wins_for_the_validator(self, tmp_path, monkeypatch):
        """CI is not the anchored box; --workspace points at a checkout."""
        from nerve.config_validate import validate_config_bundle

        monkeypatch.setenv("NERVE_LOCKDOWN", "1")
        monkeypatch.setenv("NERVE_WORKSPACE", str(tmp_path / "nonexistent"))
        config_dir, ws = _install(tmp_path, settings="timezone: UTC\n" + _JWT)
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert result.ok, result.errors


class TestAgentCannotWriteTrackedConfig:
    """``Write``/``Edit`` are auto-approved for every non-interactive tool, and
    never passed through the REST guards, so the agent's ordinary way of editing
    a file reached the config the box promises to run.

    Not a sandbox: ``Bash`` is auto-approved on the same path and is deliberately
    not filtered. See ``lockdown_denial``.
    """

    def _hub(self, session_id="s1"):
        class _Hub:
            snapshot_fn = None
            interactive_capable = False

            def __init__(self, sid):
                self.session_id = sid

            def mark_snapshotted(self, _p):
                return False

        return _Hub(session_id)

    def _locked(self, monkeypatch, tmp_path):
        ws = tmp_path / "ws"
        (ws / "config" / "cron" / "gates").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True, workspace=ws))
        return ws

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool", ["Write", "Edit", "NotebookEdit"])
    async def test_can_use_tool_denies_writes_into_config(self, tmp_path, monkeypatch, tool):
        from nerve.agent.backends.claude import ClaudeToolPermissions

        ws = self._locked(monkeypatch, tmp_path)
        perms = ClaudeToolPermissions(self._hub())
        result = await perms.can_use_tool(
            tool, {"file_path": str(ws / "config" / "settings.yaml")}, context=None,
        )
        assert type(result).__name__ == "PermissionResultDeny"
        assert "lockdown" in result.message
        # The refusal has to route the agent somewhere, not just say no.
        assert "pull request" in result.message

    @pytest.mark.asyncio
    async def test_a_gate_plugin_is_refused_too(self, tmp_path, monkeypatch):
        from nerve.agent.backends.claude import ClaudeToolPermissions

        ws = self._locked(monkeypatch, tmp_path)
        perms = ClaudeToolPermissions(self._hub())
        result = await perms.can_use_tool(
            "Write",
            {"file_path": str(ws / "config" / "cron" / "gates" / "evil.py")},
            context=None,
        )
        assert type(result).__name__ == "PermissionResultDeny"

    @pytest.mark.asyncio
    async def test_a_relative_path_is_resolved_against_the_workspace(self, tmp_path, monkeypatch):
        from nerve.agent.backends.claude import ClaudeToolPermissions

        self._locked(monkeypatch, tmp_path)
        perms = ClaudeToolPermissions(self._hub())
        result = await perms.can_use_tool(
            "Write", {"file_path": "config/settings.yaml"}, context=None,
        )
        assert type(result).__name__ == "PermissionResultDeny"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [
        "skills/backdoor/SKILL.md",
        "skills/existing/scripts/run.sh",
        "AGENTS.md",
        "SOUL.md",
        "IDENTITY.md",
        "USER.md",
        "TOOLS.md",
    ])
    async def test_can_use_tool_denies_writes_across_the_reviewed_surface(
        self, tmp_path, monkeypatch, path,
    ):
        """The plant-a-skill path, closed where the agent actually takes it.

        ``create_skill`` raises under lockdown, but nothing consults that on the
        way to a ``Write``: the file lands, ``SkillManager.discover`` indexes it
        on the next reload, and the model can invoke it with whatever
        ``allowed-tools`` its frontmatter claims. The root instruction files are
        the same shape one directory up — they are the system prompt.
        """
        from nerve.agent.backends.claude import ClaudeToolPermissions

        ws = self._locked(monkeypatch, tmp_path)
        perms = ClaudeToolPermissions(self._hub())
        result = await perms.can_use_tool(
            "Write", {"file_path": str(ws / path)}, context=None,
        )
        assert type(result).__name__ == "PermissionResultDeny"
        assert "lockdown" in result.message
        assert "pull request" in result.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path", ["memory/notes.md", "tasks/active/t1.md", "TASK.md", "scratch.md"],
    )
    async def test_ordinary_agent_writes_are_untouched(self, tmp_path, monkeypatch, path):
        from nerve.agent.backends.claude import ClaudeToolPermissions

        ws = self._locked(monkeypatch, tmp_path)
        perms = ClaudeToolPermissions(self._hub())
        result = await perms.can_use_tool(
            "Write", {"file_path": str(ws / path)}, context=None,
        )
        assert type(result).__name__ == "PermissionResultAllow"

    @pytest.mark.asyncio
    async def test_unlocked_writes_config_freely(self, tmp_path, monkeypatch):
        from nerve.agent.backends.claude import ClaudeToolPermissions

        ws = tmp_path / "ws"
        (ws / "config").mkdir(parents=True)
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False, workspace=ws))
        perms = ClaudeToolPermissions(self._hub())
        result = await perms.can_use_tool(
            "Write", {"file_path": str(ws / "config" / "settings.yaml")}, context=None,
        )
        assert type(result).__name__ == "PermissionResultAllow"

    def test_bash_is_deliberately_not_filtered(self, tmp_path, monkeypatch):
        """The documented gap, asserted so it cannot be quietly "fixed" with a
        command-string filter that looks like a boundary and is not one."""
        from nerve.agent.backends.claude import lockdown_denial

        ws = self._locked(monkeypatch, tmp_path)
        assert lockdown_denial(
            "Bash", {"command": f"echo x > {ws}/config/settings.yaml"},
        ) is None

    def test_the_background_subagent_hook_denies_too(self, tmp_path, monkeypatch):
        """For a background sub-agent the PreToolUse hook is the only thing that
        runs, so an allow issued there is the whole decision."""
        from nerve.agent.backends.claude import lockdown_denial

        ws = self._locked(monkeypatch, tmp_path)
        assert lockdown_denial(
            "Write", {"file_path": str(ws / "config" / "settings.yaml")},
        )
