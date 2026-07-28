"""Tests for workspace-aware cron config path resolution.

Cron config (jobs/system/gates) lives in workspace/config/cron, with a
fallback to the legacy ~/.nerve/cron for un-migrated installs, and honors an
explicit override in config.

Note: the autouse conftest fixture sets NERVE_HOME to a tmp dir, so
paths.cron_dir() (the legacy location) is already isolated per-test.
"""

from pathlib import Path

from nerve import paths
from nerve.config import CronConfig, _resolve_cron_dir


class TestResolveCronDir:
    def test_new_install_prefers_workspace(self, tmp_path):
        workspace = tmp_path / "ws"
        # Neither location exists yet → new install lands on the workspace.
        assert _resolve_cron_dir(workspace) == workspace / "config" / "cron"

    def test_prefers_workspace_when_it_has_jobs(self, tmp_path):
        workspace = tmp_path / "ws"
        ws_cron = workspace / "config" / "cron"
        ws_cron.mkdir(parents=True)
        (ws_cron / "system.yaml").write_text("jobs: []\n", encoding="utf-8")
        legacy = paths.cron_dir()
        legacy.mkdir(parents=True, exist_ok=True)
        (legacy / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        assert _resolve_cron_dir(workspace) == ws_cron

    def test_falls_back_to_legacy_when_only_legacy_has_jobs(self, tmp_path):
        workspace = tmp_path / "ws"  # no workspace/config/cron
        legacy = paths.cron_dir()
        legacy.mkdir(parents=True, exist_ok=True)
        (legacy / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        assert _resolve_cron_dir(workspace) == legacy

    def test_empty_ws_cron_does_not_shadow_legacy_jobs(self, tmp_path):
        """Regression: an empty workspace/config/cron (e.g. from a git checkout
        with only a gates/ placeholder) must NOT shadow real legacy jobs."""
        workspace = tmp_path / "ws"
        ws_cron = workspace / "config" / "cron"
        (ws_cron / "gates").mkdir(parents=True)  # exists, but no job files
        legacy = paths.cron_dir()
        legacy.mkdir(parents=True, exist_ok=True)
        (legacy / "jobs.yaml").write_text(
            "jobs:\n  - id: real\n    schedule: 1h\n    prompt: hi\n", encoding="utf-8"
        )
        assert _resolve_cron_dir(workspace) == legacy

    def test_none_workspace_uses_legacy(self):
        assert _resolve_cron_dir(None) == paths.cron_dir()


class TestCronConfigFromDict:
    def test_defaults_to_workspace_cron(self, tmp_path):
        workspace = tmp_path / "ws"
        cfg = CronConfig.from_dict({}, workspace=workspace)
        assert cfg.jobs_file == workspace / "config" / "cron" / "jobs.yaml"
        assert cfg.system_file == workspace / "config" / "cron" / "system.yaml"
        assert cfg.gate_plugins_dir == workspace / "config" / "cron" / "gates"

    def test_legacy_fallback(self, tmp_path):
        workspace = tmp_path / "ws"
        legacy = paths.cron_dir()
        (legacy).mkdir(parents=True, exist_ok=True)
        (legacy / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        cfg = CronConfig.from_dict({}, workspace=workspace)
        assert cfg.jobs_file == legacy / "jobs.yaml"

    def test_explicit_override_wins(self, tmp_path):
        workspace = tmp_path / "ws"
        ws_cron = workspace / "config" / "cron"
        ws_cron.mkdir(parents=True)
        (ws_cron / "system.yaml").write_text("jobs: []\n", encoding="utf-8")
        custom = tmp_path / "custom" / "myjobs.yaml"
        cfg = CronConfig.from_dict({"jobs_file": str(custom)}, workspace=workspace)
        assert cfg.jobs_file == custom
        # unspecified files still resolve to the workspace default
        assert cfg.system_file == ws_cron / "system.yaml"

    def test_gate_plugins_dir_override(self, tmp_path):
        workspace = tmp_path / "ws"
        custom_gates = tmp_path / "mygates"
        cfg = CronConfig.from_dict(
            {"gate_plugins_dir": str(custom_gates)}, workspace=workspace
        )
        assert cfg.gate_plugins_dir == custom_gates


class TestLoadConfigCron:
    def test_cron_resolved_relative_to_configured_workspace(self, tmp_path):
        from nerve.config import load_config

        config_dir = tmp_path / "cfg"
        workspace = tmp_path / "ws"
        config_dir.mkdir(parents=True)
        ws_cron = workspace / "config" / "cron"
        ws_cron.mkdir(parents=True)
        (ws_cron / "system.yaml").write_text("jobs: []\n", encoding="utf-8")
        (config_dir / "config.yaml").write_text(
            f"workspace: {workspace}\n", encoding="utf-8"
        )
        cfg = load_config(config_dir)
        assert cfg.cron.jobs_file == ws_cron / "jobs.yaml"

    def test_legacy_fallback_end_to_end(self, tmp_path):
        from nerve.config import load_config

        config_dir = tmp_path / "cfg"
        workspace = tmp_path / "ws"       # no workspace/config/cron
        config_dir.mkdir(parents=True)
        legacy = paths.cron_dir()
        legacy.mkdir(parents=True, exist_ok=True)
        (legacy / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        (config_dir / "config.yaml").write_text(
            f"workspace: {workspace}\n", encoding="utf-8"
        )
        cfg = load_config(config_dir)
        assert cfg.cron.jobs_file == legacy / "jobs.yaml"


class TestInitPreservesLegacyJobs:
    def test_reinit_migrates_legacy_custom_crons(self, tmp_path, monkeypatch):
        """Re-running init on an upgraded install must not drop custom crons."""
        import os

        from nerve.bootstrap import run_non_interactive

        # Legacy install with a real custom cron under the isolated NERVE_HOME.
        legacy = paths.cron_dir()
        legacy.mkdir(parents=True, exist_ok=True)
        legacy_jobs = (
            "jobs:\n"
            "  - id: my-custom\n"
            "    schedule: 1h\n"
            "    prompt: do the thing\n"
        )
        (legacy / "jobs.yaml").write_text(legacy_jobs, encoding="utf-8")

        config_dir = tmp_path / "cfg"
        config_dir.mkdir(parents=True, exist_ok=True)
        workspace = tmp_path / "ws"
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-testkey123",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(workspace),
        }
        with monkeypatch.context() as m:
            for k, v in env.items():
                m.setenv(k, v)
            run_non_interactive(config_dir)

        migrated = workspace / "config" / "cron" / "jobs.yaml"
        assert migrated.exists()
        assert "my-custom" in migrated.read_text(encoding="utf-8")
