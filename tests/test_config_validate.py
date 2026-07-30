"""Tests for config-bundle validation: validate_config_bundle + CLI."""

import textwrap
from pathlib import Path

import pytest
import yaml

from nerve.config import NerveConfig
from nerve.config_validate import _WORKING_DIR_PATH_KEYS, validate_config_bundle
from nerve.cron.gates import GATE_REGISTRY


def _cfg(tmp_path: Path, base: str = "", workspace: Path | None = None) -> Path:
    config_dir = tmp_path / "cfg"
    config_dir.mkdir(parents=True, exist_ok=True)
    ws = workspace or (tmp_path / "ws")
    ws.mkdir(parents=True, exist_ok=True)
    text = f"workspace: {ws}\n" + base
    (config_dir / "config.yaml").write_text(text, encoding="utf-8")
    return config_dir


def _settings(workspace: Path, body: str) -> None:
    """Write the portable ``<ws>/config/settings.yaml`` layer."""
    d = workspace / "config"
    d.mkdir(parents=True, exist_ok=True)
    (d / "settings.yaml").write_text(body, encoding="utf-8")


def _jobs(workspace: Path, jobs: list[dict]) -> None:
    cron = workspace / "config" / "cron"
    cron.mkdir(parents=True, exist_ok=True)
    (cron / "jobs.yaml").write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")


def _nested(key: tuple[str, ...], value) -> dict:
    """``("a", "b"), 1`` -> ``{"a": {"b": 1}}`` — one config key, written out."""
    out = {key[-1]: value}
    for part in reversed(key[:-1]):
        out = {part: out}
    return out


def _gate_plugin(workspace: Path, name: str, body: str) -> Path:
    gates = workspace / "config" / "cron" / "gates"
    gates.mkdir(parents=True, exist_ok=True)
    path = gates / name
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


class TestValidateBundle:
    def test_valid_bundle_ok(self, tmp_path):
        result = validate_config_bundle(_cfg(tmp_path, "timezone: UTC\n"))
        assert result.ok
        assert result.errors == []

    def test_unknown_key_is_warning_by_default(self, tmp_path):
        result = validate_config_bundle(_cfg(tmp_path, "tiimezone: UTC\n"))
        assert result.ok  # forward-compat / example keys shouldn't fail CI
        assert any("tiimezone" in w for w in result.warnings)

    def test_unknown_key_is_error_with_strict_keys(self, tmp_path):
        result = validate_config_bundle(
            _cfg(tmp_path, "tiimezone: UTC\n"), strict_keys=True
        )
        assert not result.ok
        assert any("tiimezone" in e for e in result.errors)
        # The version the check reflects, so CI disagreeing with the instance
        # reads as version skew rather than as a bad config.
        assert any("this nerve version knows" in i for i in result.info)

    def test_no_version_note_without_unknown_keys(self, tmp_path):
        result = validate_config_bundle(
            _cfg(tmp_path, "timezone: UTC\n"), strict_keys=True
        )
        assert result.ok
        assert not any("this nerve version knows" in i for i in result.info)

    def test_backend_misconfig_is_error(self, tmp_path):
        result = validate_config_bundle(_cfg(tmp_path, "agent:\n  backend: bogus\n"))
        assert not result.ok
        assert any("backend" in e.lower() or "config error" in e for e in result.errors)

    def test_unset_env_elsewhere_does_not_excuse_a_real_config_error(
        self, tmp_path, monkeypatch
    ):
        """A missing variable downgrades only the errors it causes.

        Secrets are unset in CI, so a bundle naming one would otherwise have
        every construction error demoted to a warning — disabling this check in
        the environment it exists for.
        """
        monkeypatch.delenv("SECRET_X", raising=False)
        result = validate_config_bundle(
            _cfg(tmp_path, "anthropic_api_key: ${SECRET_X}\nagent:\n  backend: bogus\n")
        )
        assert not result.ok
        assert any("bogus" in e for e in result.errors)

    def test_error_caused_by_the_unset_var_itself_stays_a_warning(
        self, tmp_path, monkeypatch
    ):
        """The literal ``${VAR}`` is not a known backend name, but that says
        nothing about the config — with the variable set it never happens."""
        monkeypatch.delenv("NERVE_BACKEND", raising=False)
        result = validate_config_bundle(
            _cfg(tmp_path, "agent:\n  backend: ${NERVE_BACKEND}\n")
        )
        assert result.ok
        assert any("NERVE_BACKEND" in w for w in result.warnings)

    def test_env_caused_error_that_does_not_quote_the_value_stays_a_warning(
        self, tmp_path, monkeypatch
    ):
        """Classification is by rebuilding without the unresolved refs, not by
        looking for ``${`` in the message — this one never mentions the value."""
        monkeypatch.delenv("UC_REV", raising=False)
        result = validate_config_bundle(_cfg(tmp_path, textwrap.dedent("""\
            agent:
              backend: codex
            codex:
              ultracode:
                revision: ${UC_REV}
        """)))
        assert result.ok
        assert any("UC_REV" in w for w in result.warnings)

    def test_unset_env_on_numeric_field_does_not_hard_fail(self, tmp_path, monkeypatch):
        """An unset ${VAR} on an int field must not be a hard error in lenient
        mode — it is a consequence of the missing variable, already reported."""
        monkeypatch.delenv("IDLE_T", raising=False)
        result = validate_config_bundle(
            _cfg(tmp_path, "agent:\n  cli_idle_timeout_seconds: ${IDLE_T}\n")
        )
        assert result.ok
        assert any("IDLE_T" in i for i in result.info)

    def test_strict_env_all_set_passes(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_KEY", "sk-real")
        result = validate_config_bundle(
            _cfg(tmp_path, "anthropic_api_key: ${MY_KEY}\n"), strict_env=True
        )
        assert result.ok

    def test_unset_env_is_info_by_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SECRET_X", raising=False)
        result = validate_config_bundle(
            _cfg(tmp_path, "anthropic_api_key: ${SECRET_X}\n")
        )
        assert result.ok  # unset secret is fine in CI
        assert any("SECRET_X" in i for i in result.info)

    def test_unset_env_is_error_when_strict(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SECRET_X", raising=False)
        result = validate_config_bundle(
            _cfg(tmp_path, "anthropic_api_key: ${SECRET_X}\n"), strict_env=True
        )
        assert not result.ok
        assert any("SECRET_X" in e for e in result.errors)

    def test_malformed_cron_is_error(self, tmp_path):
        ws = tmp_path / "ws"
        cron = ws / "config" / "cron"
        cron.mkdir(parents=True)
        (cron / "jobs.yaml").write_text("jobs: [ broken: yaml: here\n", encoding="utf-8")
        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)
        assert not result.ok
        assert any("jobs.yaml" in e or "parse" in e.lower() for e in result.errors)

    def test_invalid_job_is_error(self, tmp_path):
        ws = tmp_path / "ws"
        cron = ws / "config" / "cron"
        cron.mkdir(parents=True)
        # Missing prompt/prompt_file → CronJob.__post_init__ raises.
        (cron / "jobs.yaml").write_text(
            yaml.safe_dump({"jobs": [{"id": "bad", "schedule": "1h"}]}),
            encoding="utf-8",
        )
        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)
        assert not result.ok

    def test_valid_cron_reported(self, tmp_path):
        ws = tmp_path / "ws"
        cron = ws / "config" / "cron"
        cron.mkdir(parents=True)
        (cron / "jobs.yaml").write_text(
            yaml.safe_dump({"jobs": [{"id": "ok", "schedule": "1h", "prompt": "hi"}]}),
            encoding="utf-8",
        )
        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)
        assert result.ok
        assert any("1 job" in i for i in result.info)

    def test_workspace_override_reads_repo_settings(self, tmp_path):
        """--workspace points validation at a checked-out config repo."""
        repo = tmp_path / "repo"
        (repo / "config").mkdir(parents=True)
        (repo / "config" / "settings.yaml").write_text(
            "tiimezone: UTC\n", encoding="utf-8"  # typo in the shared settings
        )
        # config_dir has no config.yaml at all (just like a bare repo checkout)
        config_dir = tmp_path / "empty"
        config_dir.mkdir()
        result = validate_config_bundle(config_dir, workspace_override=repo, strict_keys=True)
        assert not result.ok
        assert any("tiimezone" in e for e in result.errors)


class TestGatePluginsAreNotExecuted:
    """Checking a bundle must not be the same act as running it.

    Gate plugins are ordinary ``.py`` files the daemon imports at startup. If
    validation imported them too, then by the time it decided a bundle was
    unfit, the bundle's Python would already have run — under the daemon's uid,
    env and network — so "an invalid bundle is refused" would guarantee nothing.
    """

    #: A plugin that records the fact it ran, then declares a perfectly good gate.
    MARKER_PLUGIN = '''
        import pathlib
        pathlib.Path({marker!r}).write_text("executed")

        from nerve.cron.gates import CronGate

        class MarkerGate(CronGate):
            type = "marker_test"

            async def is_satisfied(self, ctx):
                return True

            def describe(self):
                return "marker"

            @classmethod
            def from_config(cls, spec):
                return cls()
    '''

    def test_validating_does_not_run_plugin_code(self, tmp_path):
        marker = tmp_path / "EXECUTED"
        ws = tmp_path / "ws"
        _gate_plugin(ws, "marker.py", self.MARKER_PLUGIN.format(marker=str(marker)))
        _jobs(ws, [{
            "id": "g", "schedule": "1h", "prompt": "hi",
            "run_if": [{"type": "marker_test"}],
        }])

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert not marker.exists(), "validation executed the candidate plugin"
        assert "marker_test" not in GATE_REGISTRY
        # The gate type can't be confirmed without loading the plugin, so it is
        # reported — not accepted, and not failed.
        assert result.ok, result.errors
        assert any("marker_test" in w for w in result.warnings)


class TestWorkingDirectoryPaths:
    """An explicit ``.`` is refused; a blank value is the documented default.

    ``.``, ``./`` and ``./.`` survive ``config._expand_path`` as the path they
    are, so they really do point nerve at whichever directory the daemon happened
    to be started in. The sharp one is ``cron.gate_plugins_dir``, which the daemon
    imports every ``*.py`` from at startup and again on every reload: one
    character turns the working directory into a source of code the daemon
    executes.

    Blank is the opposite case and must not be reported at all. ``_expand_path``
    maps it to ``None`` before it can become ``Path(".")`` and the field takes its
    documented default, so flagging it would fail a bundle that runs correctly —
    ``jobs_file: ${CRON_JOBS_FILE:-}`` is the shape that turns up, and the docs
    promote that idiom.
    """

    def _validate(self, tmp_path: Path, settings: str):
        ws = tmp_path / "ws"
        _settings(ws, settings)
        return validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

    def test_dot_gate_plugins_dir_is_error(self, tmp_path):
        for value in (".", "./", "./."):
            result = self._validate(
                tmp_path, f"cron:\n  gate_plugins_dir: '{value}'\n"
            )
            [err] = [e for e in result.errors if "gate_plugins_dir" in e]
            # The message has to say what the setting would *do*. "Invalid path"
            # would leave the reader thinking they had a typo, not a
            # code-execution path into the daemon.
            assert "working directory" in err, value
            assert "executed" in err, value

    def test_blank_gate_plugins_dir_is_accepted(self, tmp_path):
        """Blank, whitespace-only and a bare key all mean the default."""
        for value in ("''", "'   '", ""):
            result = self._validate(
                tmp_path, f"cron:\n  gate_plugins_dir: {value}\n"
            )
            assert result.ok, (value, result.errors)

    def test_dot_workspace_is_error(self, tmp_path):
        """The worst one: the whole tree nerve reads and writes moves to the cwd.

        Flagged even though this run pins the workspace itself — the pin says
        where to look for the checkout, it does not excuse the value in the
        bundle.
        """
        ws = tmp_path / "ws"
        _settings(ws, "timezone: UTC\n")
        config_dir = tmp_path / "cfg"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "config.yaml").write_text("workspace: .\n", encoding="utf-8")
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert not result.ok
        assert any(e.startswith("workspace: ") for e in result.errors)

    def test_blank_workspace_is_accepted(self, tmp_path):
        ws = tmp_path / "ws"
        _settings(ws, "timezone: UTC\n")
        config_dir = tmp_path / "cfg"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "config.yaml").write_text("workspace: ''\n", encoding="utf-8")
        result = validate_config_bundle(config_dir, workspace_override=ws)
        assert result.ok, result.errors

    def test_dot_cron_file_names_the_key(self, tmp_path):
        """These already fail — as ``Is a directory: '.'``, which names neither
        the setting at fault nor what it meant."""
        for key in ("jobs_file", "system_file"):
            result = self._validate(tmp_path, f"cron:\n  {key}: '.'\n")
            assert any(e.startswith(f"cron.{key}: ") for e in result.errors), key

    def test_blank_cron_file_resolves_to_the_default(self, tmp_path):
        """The loader's contract, checked from the validator's side.

        A blank ``jobs_file`` loads ``<ws>/config/cron/jobs.yaml``; reporting it
        would have the two disagree about the same bundle.
        """
        for key in ("jobs_file", "system_file"):
            result = self._validate(tmp_path, f"cron:\n  {key}: ''\n")
            assert result.ok, (key, result.errors)

    def test_explicit_path_is_accepted(self, tmp_path):
        result = self._validate(
            tmp_path, "cron:\n  gate_plugins_dir: /opt/nerve/gates\n"
        )
        assert result.ok, result.errors

    def test_unset_env_ref_in_a_path_is_not_flagged(self, tmp_path, monkeypatch):
        """An unset ``${VAR}`` stays literal and is reported as a missing var —
        the lenient CI case, not a working-directory setting."""
        monkeypatch.delenv("GATES_DIR", raising=False)
        result = self._validate(
            tmp_path, "cron:\n  gate_plugins_dir: ${GATES_DIR}\n"
        )
        assert result.ok, result.errors
        assert any("GATES_DIR" in i for i in result.info)

    def test_env_default_expanding_to_empty_is_accepted(self, tmp_path, monkeypatch):
        """``${VAR:-}`` is how the docs say to make a fleet setting optional, and
        an unset variable then leaves the field on its default."""
        monkeypatch.delenv("GATES_DIR", raising=False)
        result = self._validate(
            tmp_path, "cron:\n  gate_plugins_dir: ${GATES_DIR:-}\n"
        )
        assert result.ok, result.errors

    def test_env_default_expanding_to_a_dot_is_error(self, tmp_path, monkeypatch):
        """A gate that only read literals would wave this one through."""
        monkeypatch.delenv("GATES_DIR", raising=False)
        result = self._validate(
            tmp_path, "cron:\n  gate_plugins_dir: ${GATES_DIR:-.}\n"
        )
        assert not result.ok
        [err] = [e for e in result.errors if "gate_plugins_dir" in e]
        # Both texts, or the author cannot see which of the two is at fault.
        assert "${GATES_DIR:-.}" in err
        assert "expands to '.'" in err


def _declared_path_keys(klass, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Dotted keys of every ``Path`` / ``Path | None`` field under *klass*.

    Read off ``typing.get_type_hints``, walking into nested config dataclasses
    (including ones reached through ``X | None`` or ``list[X]``), so the answer
    comes from what the config declares rather than from what any consumer of it
    happens to handle.
    """
    import dataclasses
    import typing

    keys: list[tuple[str, ...]] = []
    hints = typing.get_type_hints(klass)
    for f in dataclasses.fields(klass):
        declared = hints.get(f.name)
        key = (*prefix, f.name)
        if declared is Path or declared == (Path | None):
            keys.append(key)
            continue
        for arg in (declared, *typing.get_args(declared)):
            if isinstance(arg, type) and dataclasses.is_dataclass(arg):
                keys.extend(_declared_path_keys(arg, key))
    return keys


class TestWorkingDirPathCoverage:
    """The rule is a hand-written literal; the settings it must cover are not.

    Every ``Path`` setting missing from ``_WORKING_DIR_PATH_KEYS`` is one where an
    explicit ``.`` passes validation and lands the daemon on its working directory
    anyway — which is how ``gateway.ssl.cert`` and ``proxy.auth_dir`` went
    uncovered while the check's own tests stayed green. So the field set here is
    derived from the config dataclasses' annotations, never from the rule under
    test, which would only agree with itself.
    """

    # Path settings the working-directory rule cannot apply to, with the reason.
    EXEMPT = {
        # Not a bundle key at all: load_config overwrites it with the directory
        # the config was read from, so no written value ever reaches the check.
        ("config_dir",): "set by load_config, not by the config",
    }

    def test_every_path_setting_is_covered_or_exempt(self):
        found = _declared_path_keys(NerveConfig)
        assert len(found) >= 11, (
            f"the walk reached only {len(found)} Path settings — did it break?"
        )
        uncovered = sorted(set(found) - set(_WORKING_DIR_PATH_KEYS) - set(self.EXEMPT))
        assert not uncovered, (
            "Path settings that neither the working-directory rule nor the "
            "exemptions above account for — add a consequence to "
            "_WORKING_DIR_PATH_KEYS, or exempt it here with the reason:\n"
            + "\n".join(".".join(k) for k in uncovered)
        )

    def test_the_rule_names_only_real_settings(self):
        """A key that matches no field silently checks nothing forever."""
        found = set(_declared_path_keys(NerveConfig))
        unknown = sorted(set(_WORKING_DIR_PATH_KEYS) - found)
        assert not unknown, (
            "_WORKING_DIR_PATH_KEYS entries that are not Path settings of the "
            "config (renamed? misspelled?):\n"
            + "\n".join(".".join(k) for k in unknown)
        )

    def test_every_covered_key_refuses_an_explicit_dot(self, tmp_path):
        """The listing is only half of it; each key has to actually be read.

        ``gateway.ssl.cert`` is three levels deep and ``workspace`` is only ever
        set in the machine layer, so a rule entry can be correct and still never
        be reached.
        """
        for i, key in enumerate(_WORKING_DIR_PATH_KEYS):
            ws = tmp_path / f"ws{i}"
            _settings(ws, "timezone: UTC\n")
            config_dir = tmp_path / f"cfg{i}"
            config_dir.mkdir(parents=True, exist_ok=True)
            data = _nested(key, ".")
            data.setdefault("workspace", str(ws))
            (config_dir / "config.yaml").write_text(
                yaml.safe_dump(data), encoding="utf-8"
            )
            result = validate_config_bundle(config_dir, workspace_override=ws)
            dotted = ".".join(key)
            assert any(e.startswith(f"{dotted}: ") for e in result.errors), (
                dotted, result.errors,
            )


class TestScheduleChecking:
    """A schedule the daemon won't run as written must fail here, not there.

    This is the earliest and cheapest gate on the config repo. Everything it
    misses is caught — at best — after the change has merged and synced, with
    the instance stuck on its old config until someone reads a log line.
    """

    def _run(self, tmp_path, schedule, base=""):
        ws = tmp_path / "ws"
        _jobs(ws, [{"id": "j", "schedule": schedule, "prompt": "hi"}])
        return validate_config_bundle(
            _cfg(tmp_path, base, workspace=ws), workspace_override=ws,
        )

    def test_bad_crontab_field_is_an_error(self, tmp_path):
        result = self._run(tmp_path, "99 * * * *")

        assert not result.ok
        assert any("99 * * * *" in e for e in result.errors)

    def test_bad_day_of_week_is_an_error(self, tmp_path):
        result = self._run(tmp_path, "0 3 * * 9")

        assert not result.ok

    def test_the_error_names_the_job(self, tmp_path):
        result = self._run(tmp_path, "99 * * * *")

        assert any("job 'j'" in e for e in result.errors)

    def test_every_bad_job_is_reported_not_just_the_first(self, tmp_path):
        ws = tmp_path / "ws"
        _jobs(ws, [
            {"id": "a", "schedule": "99 * * * *", "prompt": "hi"},
            {"id": "b", "schedule": "* 99 * * *", "prompt": "hi"},
            {"id": "c", "schedule": "0 3 * * *", "prompt": "hi"},
        ])
        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws,
        )

        assert not result.ok
        assert sum("job 'a'" in e or "job 'b'" in e for e in result.errors) == 2
        assert not any("job 'c'" in e for e in result.errors)

    def test_a_bad_schedule_does_not_hide_the_job_s_other_faults(self, tmp_path):
        ws = tmp_path / "ws"
        _jobs(ws, [{
            "id": "j", "schedule": "99 * * * *", "prompt": "hi",
            "run_if": [{"type": "tasks", "min_count": "lots"}],
        }])
        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws,
        )

        assert any("99 * * * *" in e for e in result.errors)
        assert any("min_count" in e for e in result.errors)

    @pytest.mark.parametrize(
        "schedule", ["4h", "30m", "1h30m", "90s", "*/15 * * * *", "0 3 * * 1-5"],
    )
    def test_valid_schedules_pass(self, tmp_path, schedule):
        result = self._run(tmp_path, schedule)

        assert result.ok, result.errors

    @pytest.mark.parametrize("schedule", ["hourly", "every day", "@daily", "???"])
    def test_a_schedule_that_is_neither_form_is_an_error(self, tmp_path, schedule):
        """Not a warning: the runtime never complains about these either.

        The interval parser finds no h/m/s token and substitutes its default,
        so the job runs — just not on the cadence anyone wrote down. A warning
        would leave `ok` True and merge it.
        """
        result = self._run(tmp_path, schedule)

        assert not result.ok
        assert any("neither" in e and repr(schedule) in e for e in result.errors)

    def test_a_non_string_schedule_is_an_error(self, tmp_path):
        """`schedule: 4` reaches the scheduler as an int, whose first move is
        .split() — an AttributeError no runtime handler catches."""
        result = self._run(tmp_path, 4)

        assert not result.ok
        assert any("must be a string" in e for e in result.errors)

    def test_bad_source_schedule_is_an_error(self, tmp_path):
        result = self._run(
            tmp_path, "1h", "sync:\n  gmail:\n    schedule: '99 * * * *'\n",
        )

        assert not result.ok
        assert any("sync.gmail.schedule" in e for e in result.errors)

    def test_valid_source_schedule_passes(self, tmp_path):
        result = self._run(
            tmp_path, "1h", "sync:\n  telegram:\n    schedule: '*/5 * * * *'\n",
        )

        assert result.ok, result.errors

    def test_unset_env_ref_in_a_schedule_is_not_flagged(self, tmp_path, monkeypatch):
        """The missing variable is already reported; the literal ${VAR} text is
        not a second, separate mistake."""
        monkeypatch.delenv("SYNC_SCHED", raising=False)
        result = self._run(
            tmp_path, "1h", "sync:\n  gmail:\n    schedule: ${SYNC_SCHED}\n",
        )

        assert result.ok, result.errors
        assert any("SYNC_SCHED" in i for i in result.info)


class TestGateSpecChecking:
    """Three cases, and only three: structure is always enforced, a built-in
    type is checked in full, anything else is reported as unverified."""

    def _run(self, tmp_path, run_if):
        ws = tmp_path / "ws"
        _jobs(ws, [{"id": "g", "schedule": "1h", "prompt": "hi", "run_if": run_if}])
        return validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

    def test_builtin_gate_spec_is_checked_in_full(self, tmp_path):
        result = self._run(tmp_path, [{"type": "tasks", "min_count": "lots"}])

        assert not result.ok
        assert any("min_count" in e for e in result.errors)

    def test_valid_builtin_gate_passes(self, tmp_path):
        result = self._run(tmp_path, [{"type": "tasks", "status": "pending"}])

        assert result.ok, result.errors
        assert result.warnings == []

    def test_spec_that_is_not_a_mapping_is_an_error(self, tmp_path):
        result = self._run(tmp_path, ["tasks"])

        assert not result.ok
        assert any("mapping" in e for e in result.errors)

    def test_spec_without_a_type_is_an_error(self, tmp_path):
        result = self._run(tmp_path, [{"status": "pending"}])

        assert not result.ok
        assert any("'type'" in e for e in result.errors)

    def test_non_string_type_is_an_error(self, tmp_path):
        result = self._run(tmp_path, [{"type": 7}])

        assert not result.ok
        assert any("must be a string" in e for e in result.errors)

    def test_run_if_that_is_not_a_list_is_an_error(self, tmp_path):
        result = self._run(tmp_path, "tasks")

        assert not result.ok
        assert any("run_if" in e for e in result.errors)

    def test_plugin_provided_type_is_a_warning_not_an_error(self, tmp_path):
        """A type a plugin supplies is legitimate and common; failing it would
        make the gate unusable for anyone with a custom gate."""
        result = self._run(tmp_path, [{"type": "stale_tasks", "min_age_minutes": 60}])

        assert result.ok, result.errors
        assert any("stale_tasks" in w for w in result.warnings)

    def test_the_warning_states_the_runtime_consequence(self, tmp_path):
        """Warning, not error, was a deliberate call — so the text is the whole
        safety net. A typo'd built-in ('taks') costs the job: nothing registers
        the type, so it does not load at all."""
        result = self._run(tmp_path, [{"type": "taks"}])

        assert result.ok
        assert any("does not load" in w for w in result.warnings)

    def test_blank_type_is_an_error(self, tmp_path):
        result = self._run(tmp_path, [{"type": "  "}])

        assert not result.ok

    def test_unhashable_type_does_not_abort_the_rest_of_the_file(self, tmp_path):
        """A list under `type:` is unhashable; the registry lookup used to raise
        TypeError, failing the whole file and hiding every job after it."""
        ws = tmp_path / "ws"
        _jobs(ws, [
            {"id": "first", "schedule": "1h", "prompt": "hi",
             "run_if": [{"type": ["tasks"]}]},
            {"id": "second", "schedule": "1h", "prompt": "hi",
             "run_if": [{"type": "tasks", "min_count": "lots"}]},
        ])

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert any("first" in e for e in result.errors)
        assert any("second" in e and "min_count" in e for e in result.errors)

    def test_empty_run_if_key_is_not_an_error(self, tmp_path):
        """`run_if:` with nothing under it parses to None and used to take the
        whole job down with it."""
        result = self._run(tmp_path, None)

        assert result.ok, result.errors

    def test_a_falsy_wrong_shape_is_reported_not_treated_as_absent(self, tmp_path):
        """`run_if: {}` is not "no gates". Normalizing every falsy value to []
        made a mistyped gate block indistinguishable from an ungated job: the
        job ran unconditionally and this reported the bundle clean."""
        result = self._run(tmp_path, {})

        assert not result.ok
        assert any("run_if" in e and "dict" in e for e in result.errors)

    def test_a_scalar_run_if_does_not_abort_the_rest_of_the_file(self, tmp_path):
        """A non-iterable under `run_if:` used to raise out of job construction,
        failing the whole file and hiding every job after it."""
        ws = tmp_path / "ws"
        _jobs(ws, [
            {"id": "first", "schedule": "1h", "prompt": "hi", "run_if": 0},
            {"id": "second", "schedule": "1h", "prompt": "hi",
             "run_if": [{"type": "tasks", "min_count": "lots"}]},
        ])

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert any("first" in e and "run_if" in e for e in result.errors)
        assert any("second" in e and "min_count" in e for e in result.errors)

    def test_one_pass_reports_every_bad_job_in_the_file(self, tmp_path):
        """Validation collects per-job construction failures instead of raising
        on the first one.

        A job that cannot be constructed at all — no prompt, an invalid workflow
        block — makes ``load_jobs(strict=True)`` raise, and validation answers a
        raise by abandoning the file. So the first such job used to hide every
        problem after it, including problems in jobs that are perfectly loadable.
        One pass has to list everything, or fixing a bundle becomes a sequence of
        CI rounds that each reveal one more line.

        Two failures that stop construction, then two that only surface once the
        job exists, then a job with nothing wrong — all in one file.
        """
        ws = tmp_path / "ws"
        _jobs(ws, [
            {"id": "no-prompt", "schedule": "1h"},
            {"id": "bad-workflow", "schedule": "1h",
             "workflow": {"engine": "claude", "prompt": "go", "budget_usd": 0}},
            {"id": "bad-gate", "schedule": "1h", "prompt": "hi",
             "run_if": [{"type": "tasks", "min_count": "lots"}]},
            {"id": "bad-schedule", "schedule": "99 * * * *", "prompt": "hi"},
            {"id": "fine", "schedule": "1h", "prompt": "hi"},
        ])

        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws,
        )

        assert not result.ok
        for job_id in ("no-prompt", "bad-workflow", "bad-gate", "bad-schedule"):
            assert any(job_id in e for e in result.errors), (
                f"{job_id} went unreported — errors: {result.errors}"
            )
        # The three that did construct are still counted, so a failure to build
        # skips its job rather than derailing the file.
        assert any("3 job(s)" in i for i in result.info), result.info

    def test_unknown_field_in_a_builtin_gate_spec_is_reported(self, tmp_path):
        """The quietest gate mistake: from_config reads with .get(), so a typo
        takes the default and the gate checks something else entirely."""
        result = self._run(tmp_path, [{"type": "tasks", "statuz": "pending"}])

        assert result.ok  # a warning by default, like every other unknown key
        assert any("statuz" in w for w in result.warnings)

    def test_unknown_gate_field_is_an_error_under_strict_keys(self, tmp_path):
        ws = tmp_path / "ws"
        _jobs(ws, [{
            "id": "g", "schedule": "1h", "prompt": "hi",
            "run_if": [{"type": "tasks", "statuz": "pending"}],
        }])

        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws, strict_keys=True,
        )

        assert not result.ok
        assert any("statuz" in e for e in result.errors)


class TestLegacyIdleShorthand:
    """`skip_when_idle` is synthesized into a messages gate at load time, so it
    needs the same checking as run_if — and one shape more."""

    def _run(self, tmp_path, job_extra):
        ws = tmp_path / "ws"
        _jobs(ws, [{"id": "g", "schedule": "1h", "prompt": "hi", **job_extra}])
        return validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

    def test_a_bare_string_is_an_error(self, tmp_path):
        """`skip_when_idle: gmail` becomes sources ['g','m','a','i','l'] — no
        source ever matches, so the job silently never runs again."""
        result = self._run(tmp_path, {"skip_when_idle": "gmail"})

        assert not result.ok
        assert any("skip_when_idle" in e for e in result.errors)

    def test_a_list_of_non_strings_is_an_error(self, tmp_path):
        result = self._run(tmp_path, {"skip_when_idle": [1, 2]})

        assert not result.ok
        assert any("skip_when_idle" in e for e in result.errors)

    def test_a_proper_list_passes(self, tmp_path):
        result = self._run(tmp_path, {"skip_when_idle": ["gmail"]})

        assert result.ok, result.errors
        assert result.warnings == []

    def test_an_empty_list_is_not_an_error(self, tmp_path):
        """An explicit empty list, like a bare key, means "don't check"."""
        result = self._run(tmp_path, {"skip_when_idle": []})

        assert result.ok, result.errors

    def test_a_falsy_wrong_shape_is_reported(self, tmp_path):
        """`skip_when_idle: {}` used to be normalized to [] on the way in and
        then skipped here as "not set" — the mistyped block disappeared twice
        over, and the job ran with no idle check at all."""
        result = self._run(tmp_path, {"skip_when_idle": {}})

        assert not result.ok
        assert any("skip_when_idle" in e for e in result.errors)

    def test_one_warning_per_distinct_type(self, tmp_path):
        ws = tmp_path / "ws"
        _jobs(ws, [
            {"id": "a", "schedule": "1h", "prompt": "hi",
             "run_if": [{"type": "custom_one"}]},
            {"id": "b", "schedule": "1h", "prompt": "hi",
             "run_if": [{"type": "custom_one"}, {"type": "custom_two"}]},
        ])

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert result.ok, result.errors
        assert len([w for w in result.warnings if "custom_one" in w]) == 1
        assert len([w for w in result.warnings if "custom_two" in w]) == 1


class TestBadLayerIsReportedNotRaised:
    """Every layer is read before anything is type-checked, so a parse error
    there has to come back as a validation error and not a traceback."""

    def test_unparseable_config_yaml(self, tmp_path):
        config_dir = _cfg(tmp_path)
        (config_dir / "config.yaml").write_text("a: [1,\nb: }{\n", encoding="utf-8")

        result = validate_config_bundle(config_dir)

        assert not result.ok
        assert any("config.yaml" in e for e in result.errors)

    def test_unparseable_config_local_yaml(self, tmp_path):
        config_dir = _cfg(tmp_path)
        (config_dir / "config.local.yaml").write_text("x: [1,\ny: }{\n", encoding="utf-8")

        result = validate_config_bundle(config_dir)

        assert not result.ok
        assert any("config.local.yaml" in e for e in result.errors)

    def test_unparseable_settings_yaml(self, tmp_path):
        """The CI case: a YAML typo in the shared, reviewed layer."""
        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: [claude\n")

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert not result.ok
        assert any("settings.yaml" in e for e in result.errors)

    def test_settings_yaml_of_the_wrong_shape(self, tmp_path):
        ws = tmp_path / "ws"
        _settings(ws, "- not\n- a mapping\n")

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert not result.ok
        assert any("settings.yaml" in e for e in result.errors)

    def test_cli_prints_the_error_not_a_traceback(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: [claude\n")
        config_dir = _cfg(tmp_path, workspace=ws)

        result = CliRunner().invoke(
            main,
            ["-c", str(config_dir), "config", "validate", "--workspace", str(ws)],
        )

        assert result.exit_code == 1
        assert "Traceback" not in result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)
        assert "Config invalid" in result.output


class TestPortableOnly:
    """A change headed for a shared repo has to be judged on what it carries,
    not on what the machine reviewing it happens to have lying around."""

    def test_machine_override_masks_a_shared_error_by_default(self, tmp_path):
        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: bogus\n")
        config_dir = _cfg(tmp_path, "agent:\n  backend: claude\n", workspace=ws)

        result = validate_config_bundle(config_dir, workspace_override=ws)

        assert result.ok  # the local override wins the merge, as it does at load

    def test_portable_only_sees_the_shared_error(self, tmp_path):
        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: bogus\n")
        config_dir = _cfg(tmp_path, "agent:\n  backend: claude\n", workspace=ws)

        result = validate_config_bundle(
            config_dir, workspace_override=ws, portable_only=True,
        )

        assert not result.ok
        assert any("bogus" in e for e in result.errors)

    def test_portable_only_ignores_a_broken_machine_local_layer(self, tmp_path):
        """The inverse, and the commoner one: a host whose own secrets file is
        broken must still be able to tell that a shared bundle is fine."""
        ws = tmp_path / "ws"
        _settings(ws, "timezone: UTC\n")
        config_dir = _cfg(tmp_path, workspace=ws)
        (config_dir / "config.local.yaml").write_text("x: [1,\ny: }{\n", encoding="utf-8")

        overlaid = validate_config_bundle(config_dir, workspace_override=ws)
        assert not overlaid.ok  # today's behaviour: blames the incoming change

        portable = validate_config_bundle(
            config_dir, workspace_override=ws, portable_only=True,
        )
        assert portable.ok, portable.errors

    def test_overlaid_machine_layers_are_named_in_the_report(self, tmp_path):
        ws = tmp_path / "ws"
        config_dir = _cfg(tmp_path, workspace=ws)
        (config_dir / "config.local.yaml").write_text("timezone: UTC\n", encoding="utf-8")

        result = validate_config_bundle(config_dir, workspace_override=ws)

        note = "\n".join(result.info)
        assert "config.yaml" in note and "config.local.yaml" in note

    def test_portable_only_says_the_machine_layers_were_skipped(self, tmp_path):
        ws = tmp_path / "ws"
        _settings(ws, "timezone: UTC\n")
        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws, portable_only=True,
        )

        assert result.ok, result.errors
        assert any("machine-local" in i for i in result.info)

    def test_portable_only_on_a_workspace_with_no_portable_config_fails(self, tmp_path):
        """Even with the workspace named explicitly: judging the portable layer
        of a tree that has none is a gate that checked nothing."""
        ws = tmp_path / "ws"
        ws.mkdir()

        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws, portable_only=True,
        )

        assert not result.ok
        assert any("nothing to validate" in e for e in result.errors)

    def test_portable_only_fails_on_a_config_dir_that_holds_nothing_read(self, tmp_path):
        """The day-one layout mistakes: an empty config/, a .yml extension, or
        settings.yaml left at the repo root. The directory exists; the gate
        still read nothing, and a permanently green CI job is the worst of the
        available outcomes."""
        for name, place in (
            ("empty", None),
            ("wrong-ext", "config/settings.yml"),
            ("misplaced", "settings.yaml"),
        ):
            ws = tmp_path / name
            (ws / "config").mkdir(parents=True)
            if place:
                (ws / place).write_text("agent:\n  backend: bogus\n", encoding="utf-8")

            result = validate_config_bundle(
                _cfg(tmp_path, workspace=ws), workspace_override=ws, portable_only=True,
            )

            assert not result.ok, name
            assert any("nothing to validate" in e for e in result.errors), name

    def test_portable_only_passes_on_a_workspace_with_only_cron(self, tmp_path):
        """A repo may legitimately carry cron and no settings.yaml — that is a
        bundle, and it was read."""
        ws = tmp_path / "ws"
        _jobs(ws, [{"id": "ok", "schedule": "1h", "prompt": "hi"}])

        result = validate_config_bundle(
            _cfg(tmp_path, workspace=ws), workspace_override=ws, portable_only=True,
        )

        assert result.ok, result.errors

    def test_the_reported_paths_are_absolute(self, tmp_path, monkeypatch):
        """`--workspace .` must not report `config/settings.yaml`, which
        answers none of the questions the line exists to answer."""
        ws = tmp_path / "ws"
        _settings(ws, "timezone: UTC\n")
        config_dir = _cfg(tmp_path, workspace=ws)
        monkeypatch.chdir(ws)

        result = validate_config_bundle(config_dir, workspace_override=Path("."))

        assert any(str(ws.resolve() / "config" / "settings.yaml") in i for i in result.info)


class TestWrongShapedLayer:
    """A layer that parses to the wrong shape is dropped in lenient mode, and
    everything it was supposed to supply reverts to the layer below — including
    the workspace path, so validation walks off to a different tree."""

    def test_machine_layer_that_is_a_list_is_an_error(self, tmp_path):
        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: bogus\n")
        config_dir = _cfg(tmp_path, workspace=ws)
        (config_dir / "config.yaml").write_text("- a\n- b\n", encoding="utf-8")

        result = validate_config_bundle(config_dir)

        assert not result.ok
        assert any("config.yaml" in e for e in result.errors)

    def test_local_layer_that_is_a_scalar_is_an_error(self, tmp_path):
        config_dir = _cfg(tmp_path)
        (config_dir / "config.local.yaml").write_text("just a string\n", encoding="utf-8")

        result = validate_config_bundle(config_dir)

        assert not result.ok
        assert any("config.local.yaml" in e for e in result.errors)

    def test_an_unusable_layer_is_not_reported_as_overlaid(self, tmp_path):
        config_dir = _cfg(tmp_path)
        (config_dir / "config.yaml").write_text("- a\n", encoding="utf-8")

        result = validate_config_bundle(config_dir)

        assert not any("overlaid" in i for i in result.info)

    def test_the_workspace_that_was_read_is_always_named(self, tmp_path):
        """Whatever the flags, the report has to say which tree it looked at."""
        ws = tmp_path / "ws"
        _settings(ws, "timezone: UTC\n")

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert any(str(ws / "config" / "settings.yaml") in i for i in result.info)

    def test_a_missing_portable_layer_is_named_as_missing(self, tmp_path):
        ws = tmp_path / "ws"

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), workspace_override=ws)

        assert any("not present" in i for i in result.info)

    def test_portable_only_without_a_workspace_does_not_pass_silently(
        self, tmp_path, monkeypatch,
    ):
        """The worst outcome for a CI gate: green, and it read nothing. Dropping
        the machine layers also drops the workspace path they carried."""
        elsewhere = tmp_path / "not-the-checkout"
        monkeypatch.setattr(
            "nerve.paths.default_workspace", lambda: elsewhere,
        )
        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: bogus\n")

        result = validate_config_bundle(_cfg(tmp_path, workspace=ws), portable_only=True)

        assert not result.ok
        assert any("nothing to validate" in e for e in result.errors)
        assert any(str(elsewhere) in w for w in result.warnings)

    def test_cli_portable_only_flag(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        ws = tmp_path / "ws"
        _settings(ws, "agent:\n  backend: bogus\n")
        config_dir = _cfg(tmp_path, "agent:\n  backend: claude\n", workspace=ws)
        args = ["-c", str(config_dir), "config", "validate", "--workspace", str(ws)]

        assert CliRunner().invoke(main, args).exit_code == 0
        strict = CliRunner().invoke(main, [*args, "--portable-only"])
        assert strict.exit_code == 1
        assert "bogus" in strict.output


class TestFlagsAreDocumented:
    """`--strict-keys` shipped undocumented while the docs claimed its behaviour
    was the default. Keep every flag of the command visible in the docs."""

    def test_every_option_appears_in_docs_config_md(self):
        from nerve.cli import config_validate

        doc = (Path(__file__).resolve().parent.parent / "docs" / "config.md").read_text()
        flags = [
            opt for param in config_validate.params
            for opt in getattr(param, "opts", []) if opt.startswith("--")
        ]
        assert flags
        assert [f for f in flags if f not in doc] == []


class TestValidateCli:
    def test_cli_ok_exit_zero(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        config_dir = _cfg(tmp_path, "timezone: UTC\n")
        result = CliRunner().invoke(main, ["-c", str(config_dir), "config", "validate"])
        assert result.exit_code == 0, result.output
        assert "Config OK" in result.output

    def test_cli_bad_exit_nonzero(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        # A genuine error (bad backend) fails regardless of key strictness.
        config_dir = _cfg(tmp_path, "agent:\n  backend: bogus\n")
        result = CliRunner().invoke(main, ["-c", str(config_dir), "config", "validate"])
        assert result.exit_code != 0
        assert "[ERR]" in result.output

    def test_cli_strict_keys_flag(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        config_dir = _cfg(tmp_path, "tiimezone: UTC\n")
        ok = CliRunner().invoke(main, ["-c", str(config_dir), "config", "validate"])
        assert ok.exit_code == 0  # warning only by default
        strict = CliRunner().invoke(
            main, ["-c", str(config_dir), "config", "validate", "--strict-keys"]
        )
        assert strict.exit_code != 0

    def test_cli_unset_env_does_not_crash_group(self, tmp_path, monkeypatch):
        """The CI scenario: a required ${VAR} is unset, but `config validate`
        must still run (the group callback must not hard-fail)."""
        from click.testing import CliRunner

        from nerve.cli import main

        monkeypatch.delenv("MISSING_SECRET", raising=False)
        config_dir = _cfg(tmp_path, "anthropic_api_key: ${MISSING_SECRET}\n")
        result = CliRunner().invoke(main, ["-c", str(config_dir), "config", "validate"])
        assert result.exit_code == 0, result.output
        assert "MISSING_SECRET" in result.output

    def test_cli_doctor_reports_unloadable_config(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from nerve.cli import main

        monkeypatch.delenv("MISSING_SECRET", raising=False)
        config_dir = _cfg(tmp_path, "anthropic_api_key: ${MISSING_SECRET}\n")
        result = CliRunner().invoke(main, ["-c", str(config_dir), "doctor"])
        assert result.exit_code == 1
        assert "MISSING_SECRET" in result.output
        assert "Traceback" not in result.output
