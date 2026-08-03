"""Tests for legacy → workspace/config migration."""

import os
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

import nerve.migrate
from nerve import paths
from nerve.config import load_config, workspace_settings_file
from nerve.cron.jobs import load_jobs
from nerve.migrate import (
    _scrub_secrets,
    _suspect_values,
    is_migrated,
    maybe_migrate,
    migrate,
)


def _legacy_install(tmp_path, config_yaml: str, local_yaml: str | None = None):
    config_dir = tmp_path / "cfg"
    workspace = tmp_path / "ws"
    config_dir.mkdir(parents=True)
    workspace.mkdir(parents=True)
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\n" + config_yaml, encoding="utf-8"
    )
    if local_yaml is not None:
        (config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
    return config_dir, workspace


def _legacy_cron(tmp_path, files: dict[str, str]) -> Path:
    """A stand-in for the machine-global cron dir, inside the test's tmp tree."""
    d = tmp_path / "legacy-cron"
    d.mkdir(parents=True, exist_ok=True)
    for name, body in files.items():
        (d / name).write_text(body, encoding="utf-8")
    return d


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def _snapshot(root: Path) -> dict[str, bytes | None]:
    """Every path under ``root`` and its contents (``None`` for directories)."""
    return {
        str(p.relative_to(root)): None if p.is_dir() else p.read_bytes()
        for p in sorted(root.rglob("*"))
    }


@pytest.fixture
def permissive_umask():
    """Run with the common umask 022, under which a plain write lands 0644."""
    old = os.umask(0o022)
    yield
    os.umask(old)


class TestScrubSecrets:
    def test_moves_secret_keys(self):
        tracked, secrets, moved = _scrub_secrets({
            "anthropic_api_key": "sk-real",
            "auth": {"jwt_secret": "hunter2", "password_hash": "$2b$12$x"},
            "timezone": "UTC",
        })
        assert tracked["anthropic_api_key"] == "${ANTHROPIC_API_KEY}"
        assert tracked["auth"]["jwt_secret"] == "${AUTH_JWT_SECRET}"
        assert tracked["auth"]["password_hash"] == "${AUTH_PASSWORD_HASH}"
        assert tracked["timezone"] == "UTC"
        assert secrets["anthropic_api_key"] == "sk-real"
        assert secrets["auth"]["jwt_secret"] == "hunter2"
        assert set(moved) == {"anthropic_api_key", "auth.jwt_secret", "auth.password_hash"}

    def test_leaves_env_refs_and_env_name_keys(self):
        tracked, secrets, moved = _scrub_secrets({
            "openai_api_key": "${OPENAI_API_KEY}",   # already a ref
            "api_key_env": "OPENAI_API_KEY",          # a name, not a secret
        })
        assert tracked["openai_api_key"] == "${OPENAI_API_KEY}"
        assert tracked["api_key_env"] == "OPENAI_API_KEY"
        assert secrets == {}

    def test_sizes_and_timeouts_are_not_credentials(self):
        """Names that brush against the secret vocabulary but hold quantities."""
        tracked, secrets, moved = _scrub_secrets({
            "max_tokens": 4096,
            "default_token_budget": 120000,
            "client_idle_timeout_minutes": 30,
            "telegram_chat_id": 12345,
        })
        assert secrets == {}
        assert tracked["max_tokens"] == 4096
        assert tracked["default_token_budget"] == 120000
        assert tracked["client_idle_timeout_minutes"] == 30
        assert tracked["telegram_chat_id"] == 12345

    def test_numeric_credential_scrubbed(self):
        """Half a credential pair is no better than none: Telegram's api_id is an
        int, and scrubbing only its api_hash sibling left it in the clear."""
        tracked, secrets, moved = _scrub_secrets({
            "telegram": {"api_id": 2040123, "api_hash": "0123456789abcdef"}
        })
        assert tracked["telegram"]["api_id"] == "${TELEGRAM_API_ID}"
        assert secrets["telegram"]["api_id"] == 2040123
        assert moved == ["telegram.api_id", "telegram.api_hash"]

    def test_partial_env_ref_is_still_scrubbed(self):
        """One ${} reference anywhere used to exempt the whole value."""
        tracked, secrets, _ = _scrub_secrets({
            "github": {"token": "ghp_realsecretvalue${SUFFIX}"}
        })
        assert tracked["github"]["token"] == "${GITHUB_TOKEN}"
        assert secrets["github"]["token"] == "ghp_realsecretvalue${SUFFIX}"

    def test_scalar_list_items_scrubbed(self):
        """A list of strings is a leaf too — inside a sensitive subtree every
        item is a secret, and elsewhere the value's own shape decides."""
        tracked, secrets, moved = _scrub_secrets({
            "mcp": {
                "headers": ["Authorization: Bearer ghp_AAAABBBBCCCCDDDDEEEE"],
                "args": ["--stdio", "--api-key=sk-proj-AAAABBBBCCCCDDDDEEEE"],
            }
        })
        assert tracked["mcp"]["headers"] == ["${MCP_HEADERS_0}"]
        assert tracked["mcp"]["args"] == ["--stdio", "${MCP_ARGS_1}"]
        # Lists don't deep-merge — the overlay carries the real list wholesale.
        assert secrets["mcp"]["args"][1] == "--api-key=sk-proj-AAAABBBBCCCCDDDDEEEE"
        assert set(moved) == {"mcp.headers.0", "mcp.args.1"}

    def test_credential_after_a_flag_is_scrubbed(self):
        """``--token`` and its value are separate argv items, which is the form
        ``npx``/``uvx`` MCP servers are configured with. The value on its own has
        no vendor prefix and no separator to match on, so the flag in front of it
        is what marks it — otherwise it goes to the tracked file in plaintext."""
        tracked, secrets, moved = _scrub_secrets({
            "mcp_servers": [
                {
                    "name": "s",
                    "command": "npx",
                    "args": ["-y", "srv", "--token", "tok_abcdefghijklmnop"],
                },
            ]
        })
        assert moved == ["mcp_servers.0.args.3"]
        assert tracked["mcp_servers"][0]["args"] == [
            "-y", "srv", "--token", "${MCP_SERVERS_0_ARGS_3}",
        ]
        assert secrets["mcp_servers"][0]["args"][3] == "tok_abcdefghijklmnop"

    def test_a_flag_does_not_make_a_following_flag_a_secret(self):
        """``--token --verbose`` is a malformed command line, not a token whose
        value is ``--verbose``. Scrubbing it would put a required ``${VAR}``
        where a switch used to be, and the server would stop starting."""
        tracked, _, moved = _scrub_secrets(
            {"args": ["--token", "--verbose", "--port", "8080"]}
        )
        assert moved == []
        assert tracked["args"] == ["--token", "--verbose", "--port", "8080"]

    def test_only_a_credential_naming_flag_arms_the_next_item(self):
        """The flag has to name a credential. Every argv list has values after
        flags, and treating all of them as secrets would empty the tracked file
        into the overlay."""
        tracked, _, moved = _scrub_secrets(
            {"args": ["-y", "server-name", "--port", "8080", "--model", "opus"]}
        )
        assert moved == []
        assert tracked["args"] == ["-y", "server-name", "--port", "8080", "--model", "opus"]

    def test_credential_shaped_value_under_innocuous_key(self):
        tracked, secrets, moved = _scrub_secrets({
            "database": {"url": "postgres://admin:hunter2@db.internal:5432/nerve"},
            "feed": {"url": "https://example.com/all?token=abcdef1234567890"},
            "repo": {"url": "https://github.com/owner/repo.git"},
        })
        assert tracked["database"]["url"] == "${DATABASE_URL}"
        assert tracked["feed"]["url"] == "${FEED_URL}"
        assert tracked["repo"]["url"] == "https://github.com/owner/repo.git"

    def test_alternate_key_spellings(self):
        tracked, secrets, _ = _scrub_secrets({"apiKey": "abc", "API-TOKEN": "def"})
        assert tracked["apiKey"] == "${APIKEY}"
        assert tracked["API-TOKEN"] == "${API_TOKEN}"

    def test_sqlite_dsn_not_scrubbed(self):
        """"dsn" usually means a credential, but this one is a local file path."""
        tracked, secrets, _ = _scrub_secrets(
            {"memory": {"sqlite_dsn": "sqlite:////home/me/.nerve/memu.sqlite"}}
        )
        assert tracked["memory"]["sqlite_dsn"].startswith("sqlite:")
        assert secrets == {}

    def test_paths_are_not_pats(self):
        """Segment-anchored matching: ``pat`` must not swallow ``*_path``."""
        tracked, secrets, _ = _scrub_secrets({
            "archive_path": "/var/lib/nerve",
            "redact_patterns": ["sk-[a-z]+"],
            "public_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5",
        })
        assert secrets == {}

    def test_public_identifiers_are_not_scrubbed(self):
        """An OAuth client id is public by design. Scrubbing it turns a
        shareable value into a required ${VAR} that hard-fails load_config on
        the second machine — the one the tracked layer exists for."""
        tracked, secrets, _ = _scrub_secrets({
            "external_agents": {"codex": {"client_id": "app-12345", "client_secret": "shh"}}
        })
        codex = tracked["external_agents"]["codex"]
        assert codex["client_id"] == "app-12345"
        assert codex["client_secret"].startswith("${")

    def test_prose_mentioning_a_credential_is_not_scrubbed(self):
        """Category descriptions and help text are shareable. Only a value that
        *is* a credential counts, not a sentence that mentions the shape of one
        — the wizard generates descriptions like these."""
        tracked, secrets, _ = _scrub_secrets({
            "memory": {"categories": [
                {"name": "infra",
                 "description": "Connection strings like postgres://user:pass@host and "
                                "CLI flags such as --api-key=YOURKEYHERE live here"},
            ]},
            "help": "Append ?token=YOURTOKEN to the URL to authenticate",
        })
        assert secrets == {}
        assert "postgres://user:pass@host" in tracked["memory"]["categories"][0]["description"]
        assert tracked["help"].startswith("Append ?token=")

    def test_suspect_values_flags_what_the_rules_missed(self):
        """No rule matches an opaque blob under an innocuous key — but the
        operator gets told about it rather than a bare "scrubbed 0 secrets"."""
        tracked, _, _ = _scrub_secrets({
            "analytics": {"project": "5f4dcc3b5aa765d61d8327deb882cf99"},
            "agent": {"model": "claude-haiku-4-5-20251001"},
            "timezone": "America/New_York",
        })
        assert _suspect_values(tracked) == ["analytics.project"]

    def test_authorization_header_scrubbed(self):
        tracked, secrets, moved = _scrub_secrets({
            "mcp_servers": {"gh": {"headers": {"Authorization": "Bearer sk-leak"}}}
        })
        hdr = tracked["mcp_servers"]["gh"]["headers"]["Authorization"]
        assert hdr.startswith("${") and "sk-leak" not in hdr
        assert secrets["mcp_servers"]["gh"]["headers"]["Authorization"] == "Bearer sk-leak"

    def test_env_subtree_scrubbed_regardless_of_key_name(self):
        tracked, secrets, moved = _scrub_secrets({
            "mcp_servers": {"x": {"env": {"GH_PAT": "ghp_leak", "OPENAI_KEY": "sk-leak"}}}
        })
        env = tracked["mcp_servers"]["x"]["env"]
        assert env["GH_PAT"].startswith("${") and env["OPENAI_KEY"].startswith("${")
        assert secrets["mcp_servers"]["x"]["env"]["GH_PAT"] == "ghp_leak"

    def test_secret_inside_list_scrubbed(self):
        tracked, secrets, moved = _scrub_secrets({
            "external_agents": {"targets": [{"name": "codex", "token": "tok-leak"}]}
        })
        assert tracked["external_agents"]["targets"][0]["token"].startswith("${")
        # Full real list preserved in the overlay (lists don't deep-merge).
        assert secrets["external_agents"]["targets"] == [
            {"name": "codex", "token": "tok-leak"}
        ]

    def test_api_hash_scrubbed(self):
        tracked, secrets, _ = _scrub_secrets({"sync": {"telegram": {"api_hash": "H"}}})
        assert tracked["sync"]["telegram"]["api_hash"].startswith("${")

    def test_proxy_api_key_not_scrubbed(self):
        # A fixed local-loopback token — must stay in the shared file.
        tracked, secrets, _ = _scrub_secrets({"proxy": {"api_key": "sk-nerve-local-proxy"}})
        assert tracked["proxy"]["api_key"] == "sk-nerve-local-proxy"
        assert secrets == {}


class TestMigrateConfig:
    def test_creates_scrubbed_settings_and_moves_secrets(self, tmp_path):
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\nanthropic_api_key: sk-secret\n",
        )
        report = migrate(config_dir, workspace=workspace)
        assert report.migrated_config

        settings = yaml.safe_load(workspace_settings_file(workspace).read_text())
        assert settings["timezone"] == "UTC"
        assert settings["anthropic_api_key"] == "${ANTHROPIC_API_KEY}"
        assert "workspace" not in settings  # stripped (circular)

        local = yaml.safe_load((config_dir / "config.local.yaml").read_text())
        assert local["anthropic_api_key"] == "sk-secret"

        # Original kept as a breadcrumb; config.yaml no longer present.
        assert (config_dir / "config.yaml.migrated").exists()
        assert not (config_dir / "config.yaml").exists()

    def test_effective_config_unchanged_after_migration(self, tmp_path):
        """The whole point: values are preserved, just relocated."""
        config_dir, workspace = _legacy_install(
            tmp_path, "timezone: Europe/Berlin\nanthropic_api_key: sk-xyz\n"
        )
        before = load_config(config_dir)
        migrate(config_dir, workspace=workspace)
        after = load_config(config_dir)
        assert after.timezone == before.timezone == "Europe/Berlin"
        assert after.anthropic_api_key == before.anthropic_api_key == "sk-xyz"

    def test_lockdown_is_not_promoted_into_the_tracked_file(self, tmp_path):
        """``lockdown`` is ignored in config.yaml deliberately, so that a local
        edit cannot unlock or fake-lock an instance. Copying it into settings.yaml
        would hand a flag that has never had any effect the authority to drop the
        machine layers — including the config.local.yaml this migration just moved
        the secrets into, leaving the ${VAR}s in the tracked file resolving to
        nothing and the instance refusing to load."""
        config_dir, workspace = _legacy_install(
            tmp_path, "timezone: UTC\nlockdown: true\nanthropic_api_key: sk-xyz\n"
        )
        report = migrate(config_dir, workspace=workspace)

        settings = yaml.safe_load(workspace_settings_file(workspace).read_text())
        local = yaml.safe_load((config_dir / "config.local.yaml").read_text())
        assert "lockdown" not in settings
        assert "lockdown" not in local
        assert any("lockdown" in w for w in report.warnings)
        # Dropping it preserves what the box already did, so the install loads.
        assert load_config(config_dir).anthropic_api_key == "sk-xyz"

    def test_existing_local_secret_not_clobbered(self, tmp_path):
        config_dir, workspace = _legacy_install(
            tmp_path,
            "anthropic_api_key: sk-from-config\n",
            local_yaml="openai_api_key: sk-existing-local\n",
        )
        migrate(config_dir, workspace=workspace)
        local = yaml.safe_load((config_dir / "config.local.yaml").read_text())
        assert local["openai_api_key"] == "sk-existing-local"   # preserved
        assert local["anthropic_api_key"] == "sk-from-config"   # added

    def test_idempotent(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        assert migrate(config_dir, workspace=workspace).did_anything
        # Second run: settings.yaml now exists → no-op.
        assert not migrate(config_dir, workspace=workspace).did_anything
        assert is_migrated(config_dir, workspace=workspace)

    def test_settings_with_content_not_migrated(self, tmp_path):
        """A workspace whose settings file already carries configuration is left
        alone — migration must not rename config.yaml out from under it."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        (workspace / "config").mkdir(parents=True)
        workspace_settings_file(workspace).write_text("timezone: UTC\n", encoding="utf-8")
        report = migrate(config_dir, workspace=workspace)
        assert not report.migrated_config
        assert (config_dir / "config.yaml").exists()  # untouched

    def test_comments_only_scaffold_does_not_block_migration(self, tmp_path):
        """``nerve init`` scaffolds a settings.yaml that is nothing but comments.
        Treating its mere existence as "already migrated" made migration a
        permanent no-op for every install created after the scaffold shipped."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: Europe/Berlin\n")
        (workspace / "config").mkdir(parents=True)
        workspace_settings_file(workspace).write_text(
            "# Nerve — shareable workspace configuration.\n"
            "# Uncomment what you want to share.\n"
            "# timezone: UTC\n",
            encoding="utf-8",
        )
        report = migrate(config_dir, workspace=workspace)
        assert report.migrated_config
        settings = yaml.safe_load(workspace_settings_file(workspace).read_text())
        assert settings["timezone"] == "Europe/Berlin"
        assert (config_dir / "config.yaml.migrated").exists()

    def test_unreadable_settings_blocks_migration(self, tmp_path):
        """Never overwrite a tracked file we can't parse."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        (workspace / "config").mkdir(parents=True)
        workspace_settings_file(workspace).write_text("{ not: valid: yaml\n", encoding="utf-8")
        report = migrate(config_dir, workspace=workspace)
        assert not report.migrated_config
        assert (config_dir / "config.yaml").exists()

    def test_dry_run_writes_nothing(self, tmp_path):
        config_dir, workspace = _legacy_install(
            tmp_path, "anthropic_api_key: sk-secret\n"
        )
        legacy_cron = _legacy_cron(tmp_path, {"jobs.yaml": "jobs: []\n"})
        before = _snapshot(tmp_path)

        report = migrate(
            config_dir, workspace=workspace, dry_run=True, legacy_cron_dir=legacy_cron
        )

        assert report.migrated_config and report.migrated_cron  # would do both
        # Not one byte, and not one directory — the workspace config subtree in
        # particular must not be conjured into existence by a dry run.
        assert _snapshot(tmp_path) == before
        assert not (workspace / "config").exists()


class TestMigratePermissions:
    """The migrated files carry plaintext secrets; the modes have to say so."""

    def test_new_local_overlay_is_owner_only(self, tmp_path, permissive_umask):
        config_dir, workspace = _legacy_install(
            tmp_path, "telegram:\n  bot_token: 12345:AAisasecret\n"
        )
        migrate(config_dir, workspace=workspace)
        assert _mode(config_dir / "config.local.yaml") == 0o600

    def test_backup_of_the_unscrubbed_original_is_owner_only(self, tmp_path, permissive_umask):
        """The breadcrumb still holds *every* secret, unscrubbed, right next to
        the file we bothered to lock down."""
        config_dir, workspace = _legacy_install(
            tmp_path, "auth:\n  jwt_secret: hunter2\n"
        )
        os.chmod(config_dir / "config.yaml", 0o644)
        migrate(config_dir, workspace=workspace)
        backup = config_dir / "config.yaml.migrated"
        assert "hunter2" in backup.read_text()  # unscrubbed, as designed
        assert _mode(backup) == 0o600

    def test_pre_existing_wide_open_local_file_is_tightened(self, tmp_path, permissive_umask):
        config_dir, workspace = _legacy_install(
            tmp_path, "anthropic_api_key: sk-a\n", local_yaml="openai_api_key: sk-b\n"
        )
        os.chmod(config_dir / "config.local.yaml", 0o644)
        migrate(config_dir, workspace=workspace)
        assert _mode(config_dir / "config.local.yaml") == 0o600

    @pytest.mark.parametrize("umask,expected", [(0o022, 0o644), (0o077, 0o600), (0o002, 0o664)])
    def test_tracked_settings_follow_the_umask(self, tmp_path, umask, expected):
        """The shareable file is readable from a checkout, but an operator who
        set a restrictive umask does not get it widened behind their back — and
        scrubbing is explicitly not guaranteed to have emptied it of secrets."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        old = os.umask(umask)
        try:
            migrate(config_dir, workspace=workspace)
        finally:
            os.umask(old)
        assert _mode(workspace_settings_file(workspace)) == expected

    def test_scaffolded_settings_file_keeps_its_own_permissions(self, tmp_path, permissive_umask):
        """``nerve init`` scaffolds a comments-only settings.yaml, which migration
        is free to fill in — but filling it in is a *rewrite*, and a rewrite of an
        existing file does not re-permission it. An operator who locked the
        scaffold down must not have it opened up by a migration that happened to
        run under a laxer umask."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        settings = workspace_settings_file(workspace)
        settings.parent.mkdir(parents=True, exist_ok=True)
        settings.write_text("# Shared settings. No keys yet.\n", encoding="utf-8")
        os.chmod(settings, 0o600)

        migrate(config_dir, workspace=workspace)

        assert yaml.safe_load(settings.read_text())["timezone"] == "UTC"
        assert _mode(settings) == 0o600

    def test_secret_files_ignore_a_permissive_umask(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, "anthropic_api_key: sk-a\n")
        old = os.umask(0o000)
        try:
            migrate(config_dir, workspace=workspace)
        finally:
            os.umask(old)
        assert _mode(config_dir / "config.local.yaml") == 0o600
        assert _mode(config_dir / "config.yaml.migrated") == 0o600


class TestMigrateCrashSafety:
    def test_pre_existing_local_secrets_survive_a_failed_write(self, tmp_path, monkeypatch):
        """config.local.yaml holds secrets that exist in no backup — the wizard's
        API key, OAuth token and password hash. A migration that dies partway
        must not be what removes them."""
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\nanthropic_api_key: sk-from-config\n",
            local_yaml="openai_api_key: sk-only-copy\nclaude_oauth_token: tok-only-copy\n",
        )
        real_write = nerve.migrate.atomic_write_text

        def fail_on_settings(path, content, **kwargs):
            if Path(path) == workspace_settings_file(workspace):
                raise OSError("No space left on device")
            return real_write(path, content, **kwargs)

        monkeypatch.setattr(nerve.migrate, "atomic_write_text", fail_on_settings)
        with pytest.raises(OSError):
            migrate(config_dir, workspace=workspace)
        monkeypatch.undo()

        local = yaml.safe_load((config_dir / "config.local.yaml").read_text())
        assert local["openai_api_key"] == "sk-only-copy"
        assert local["claude_oauth_token"] == "tok-only-copy"

        # And the install is still in a state that a retry can finish, because
        # the source file was not renamed away first.
        assert (config_dir / "config.yaml").exists()
        assert migrate(config_dir, workspace=workspace).migrated_config
        local = yaml.safe_load((config_dir / "config.local.yaml").read_text())
        assert local["openai_api_key"] == "sk-only-copy"
        assert local["anthropic_api_key"] == "sk-from-config"

    def test_settings_is_written_before_the_source_is_renamed(self, tmp_path, monkeypatch):
        """Losing config.yaml before its replacement exists would drop every
        non-secret setting out of the live config, unrecoverably."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        seen = []
        real_rename = Path.rename

        def spy_rename(self, target):
            seen.append(workspace_settings_file(workspace).exists())
            return real_rename(self, target)

        monkeypatch.setattr(Path, "rename", spy_rename)
        migrate(config_dir, workspace=workspace)
        assert seen == [True]

    def test_wholesale_list_relocation_is_reported(self, tmp_path):
        """One secret in a list sends the whole list to the overlay, because a
        merge replaces a list rather than combining it element-wise. The copy
        left in settings.yaml is inert from then on — editing it does nothing,
        so the operator has to be told."""
        config_dir, workspace = _legacy_install(
            tmp_path,
            "mcp_servers:\n"
            "  targets:\n"
            "    - name: gh\n"
            "      token: ghp_AAAABBBBCCCCDDDDEEEEFFFF\n",
        )
        report = migrate(config_dir, workspace=workspace)
        assert any("mcp_servers.targets" in w and "no longer has any effect" in w
                   for w in report.warnings)

    def test_existing_breadcrumb_is_never_overwritten(self, tmp_path):
        """The cron half re-runs whenever the legacy dir has jobs again, so a
        fixed .migrated suffix could destroy the only copy of the first one."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = _legacy_cron(tmp_path, {"jobs.yaml": "jobs: []  # first\n"})
        migrate(config_dir, workspace=workspace, legacy_cron_dir=legacy_cron)

        # A later run of the daemon writes jobs to the legacy dir again, and the
        # workspace copy is gone (moved, or a fresh checkout).
        (legacy_cron / "jobs.yaml").write_text("jobs: []  # second\n", encoding="utf-8")
        (workspace / "config" / "cron" / "jobs.yaml").unlink()
        migrate(config_dir, workspace=workspace, legacy_cron_dir=legacy_cron)

        assert "first" in (legacy_cron / "jobs.yaml.migrated").read_text()
        assert "second" in (legacy_cron / "jobs.yaml.migrated.1").read_text()
        # ...and the breadcrumbs stay in the legacy dir. They're inert, but the
        # workspace cron dir is headed for a shared repo.
        assert not list((workspace / "config" / "cron").glob("*.migrated*"))


class TestAlreadySplitInstall:
    """A ``config.yaml`` written by the split layout holds only what must stay on
    this box. Copying it into the shareable file would publish exactly the
    values the split exists to keep local — and an absent or emptied
    ``settings.yaml`` is not evidence to the contrary, since a workspace loses
    one by being repointed, moved, or interrupted mid-init."""

    # Only the genuinely local half: certificate paths, a credential handle,
    # whose mailboxes, this box's helper process and mounts. gateway.host/port
    # and provider.type/aws_region are deliberately absent — they are shared
    # policy now, so a config.yaml holding them has something to migrate.
    MACHINE_ONLY = """
deployment: server
gateway:
  ssl:
    cert: /etc/nerve/tls/cert.pem
    key: /etc/nerve/tls/key.pem
provider:
  aws_profile: nerve-prod
sync:
  gmail:
    accounts:
      - alex@example.com
proxy:
  enabled: true
  port: 8317
docker:
  extra_mounts: []
"""

    def test_machine_local_config_is_not_migrated(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, self.MACHINE_ONLY)
        report = migrate(config_dir, workspace=workspace)
        assert not report.migrated_config
        assert (config_dir / "config.yaml").exists()
        assert not workspace_settings_file(workspace).exists()

    def test_one_shareable_key_is_enough_to_migrate(self, tmp_path):
        """The test is "is there anything here the shared layer should own", not
        "does this look tidy" — a legacy monolith always has something."""
        config_dir, workspace = _legacy_install(tmp_path, self.MACHINE_ONLY + "timezone: UTC\n")
        report = migrate(config_dir, workspace=workspace)
        assert report.migrated_config
        settings = yaml.safe_load(workspace_settings_file(workspace).read_text())
        assert settings["timezone"] == "UTC"

    def test_a_workflow_journal_dir_does_not_make_the_machine_half_look_legacy(
        self, tmp_path,
    ):
        """A runs directory alone is not a reason to migrate.

        The wizard never writes this section, so the coverage test above cannot
        see it. Left uncovered, one absolute path is enough to make a post-split
        machine-local ``config.yaml`` read as a legacy monolith — and then the
        whole machine half, provider and gateway included, is copied into the
        file the docs tell you to commit.
        """
        machine_half = self.MACHINE_ONLY + (
            "workflows:\n"
            "  runs_dir: /srv/box-42/workflow-runs\n"
        )
        config_dir, workspace = _legacy_install(tmp_path, machine_half)
        report = migrate(config_dir, workspace=workspace)

        assert not report.migrated_config
        assert (config_dir / "config.yaml").exists()
        assert not workspace_settings_file(workspace).exists()

    def test_machine_local_paths_cover_what_the_wizard_writes(self):
        """Keeps the list honest: if init starts routing another key to the
        machine layer, migration has to learn about it in the same change."""
        from nerve.bootstrap import SetupWizard

        uncovered = set()
        for mode in ("personal", "worker"):
            for provider in ("anthropic", "bedrock"):
                wizard = SetupWizard(Path("/nonexistent"))
                wizard.choices.mode = mode
                wizard.choices.provider_type = provider
                wizard.choices.aws_region = "us-east-1"
                wizard.choices.aws_profile = "nerve-prod"
                wizard.choices.use_proxy = True
                wizard.choices.deployment = "docker"
                machine, _portable, _shadowed = wizard._build_config_layers()
                uncovered |= {
                    p for p in nerve.migrate._leaf_paths(machine)
                    if not nerve.migrate._is_machine_local(p)
                }
        assert not uncovered, (
            "the wizard keeps these machine-local but migration would copy them "
            f"into the git-tracked settings file: {sorted(uncovered)}"
        )

    def test_machine_local_paths_claim_nothing_the_wizard_shares(self):
        """The direction nothing checked, and which had gone wrong.

        A path listed in _MACHINE_LOCAL_PATHS is one migration leaves behind in
        config.yaml, so an entry the wizard treats as shareable is not a harmless
        over-listing: it strands that value in a layer lockdown does not read.
        ``gateway`` and ``provider`` were listed as whole subtrees after the
        wizard had already moved host/port and type/aws_region to the tracked
        file.
        """
        from nerve.bootstrap import SetupWizard

        overclaimed = set()
        for provider in ("anthropic", "bedrock"):
            wizard = SetupWizard(Path("/nonexistent"))
            wizard.choices.mode = "personal"
            wizard.choices.provider_type = provider
            wizard.choices.aws_region = "us-east-1"
            wizard.choices.aws_profile = "nerve-prod"
            wizard.choices.use_proxy = True
            wizard.choices.deployment = "docker"
            _machine, portable, _shadowed = wizard._build_config_layers()
            overclaimed |= {
                p for p in nerve.migrate._leaf_paths(portable)
                if nerve.migrate._is_machine_local(p)
            }
        assert not overclaimed, (
            "the wizard shares these but migration would strand them in "
            f"config.yaml, where lockdown never reads them: {sorted(overclaimed)}"
        )

    def test_a_legacy_bind_port_and_provider_are_migrated(self, tmp_path):
        """Values the old classification stranded must now move across.

        A pre-split install with a non-default port, or one running on Bedrock,
        had both judged machine-local. Migration left them in config.yaml, so
        turning on lockdown reverted the box to 0.0.0.0:8900 and the Anthropic
        API with no indication that it had.
        """
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\n"
            "gateway:\n  host: 127.0.0.1\n  port: 9100\n"
            "  ssl:\n    cert: /etc/nerve/tls/cert.pem\n"
            "provider:\n  type: bedrock\n  aws_region: eu-west-1\n"
            "  aws_profile: nerve-prod\n",
        )
        report = migrate(config_dir, workspace=workspace)
        assert report.migrated_config

        settings = yaml.safe_load(
            workspace_settings_file(workspace).read_text(encoding="utf-8")
        )
        assert settings["gateway"]["host"] == "127.0.0.1"
        assert settings["gateway"]["port"] == 9100
        assert settings["provider"]["type"] == "bedrock"
        assert settings["provider"]["aws_region"] == "eu-west-1"

    def test_machine_local_keys_are_not_published(self, tmp_path):
        """A legacy monolith is split on ``_MACHINE_LOCAL_PATHS``, not copied.

        The machine half is rewritten into config.yaml and never reaches the
        git-tracked settings file: syncing a settings.yaml that names a
        certificate path present on one box makes every other box's gateway fail
        to bind. The split has to happen at the depth the path is listed at —
        ``gateway.ssl`` moving must not take ``gateway.port`` with it, which would
        strand the bind port in a layer lockdown does not read.
        """
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\n"
            "gateway:\n  port: 9100\n  ssl:\n    cert: /etc/nerve/tls/cert.pem\n"
            "provider:\n  type: bedrock\n  aws_profile: nerve-prod\n",
        )
        report = migrate(config_dir, workspace=workspace)
        settings = yaml.safe_load(
            workspace_settings_file(workspace).read_text(encoding="utf-8")
        )
        assert "ssl" not in settings["gateway"]
        assert "aws_profile" not in settings["provider"]
        assert settings["gateway"]["port"] == 9100          # portable sibling
        assert settings["provider"]["type"] == "bedrock"    # portable sibling

        machine = yaml.safe_load((config_dir / "config.yaml").read_text(encoding="utf-8"))
        assert machine["gateway"]["ssl"]["cert"] == "/etc/nerve/tls/cert.pem"
        assert machine["provider"]["aws_profile"] == "nerve-prod"
        assert "timezone" not in machine
        assert sorted(report.machine_local_kept) == [
            "gateway.ssl.cert", "provider.aws_profile",
        ]

    def test_the_split_preserves_the_effective_config(self, tmp_path):
        """Relocated, not dropped: config.yaml is a layer the loader still reads,
        so every machine-local value has to survive the split."""
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\n"
            "gateway:\n  port: 9100\n"
            "  ssl:\n    cert: /etc/nerve/tls/cert.pem\n    key: /etc/nerve/tls/key.pem\n"
            "docker:\n  extra_mounts: ['~/code:/code']\n"
            "workflows:\n  runs_dir: /srv/box-42/workflow-runs\n",
        )
        before = load_config(config_dir)
        migrate(config_dir, workspace=workspace)
        after = load_config(config_dir)

        assert after.gateway.ssl.cert == before.gateway.ssl.cert
        assert after.gateway.ssl.key == before.gateway.ssl.key
        assert after.gateway.port == before.gateway.port == 9100
        assert after.docker.extra_mounts == before.docker.extra_mounts
        assert after.workflows.runs_dir == before.workflows.runs_dir

    def test_the_rewritten_config_yaml_is_owner_only(self, tmp_path, permissive_umask):
        """It is the unscrubbed half — an ``external_agents`` target's token and a
        local service's credentials are in the subtrees that land here."""
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\n"
            "external_agents:\n  targets:\n    - name: codex\n      token: tok-secret\n",
        )
        migrate(config_dir, workspace=workspace)
        config_yaml = config_dir / "config.yaml"
        assert _mode(config_yaml) == 0o600
        assert "tok-secret" in config_yaml.read_text(encoding="utf-8")
        assert "tok-secret" not in workspace_settings_file(workspace).read_text()

    def test_the_split_half_does_not_look_legacy_on_a_re_run(self, tmp_path):
        """The file migration leaves behind is a post-split config.yaml, so a
        second pass has to read it as nothing to do rather than as a monolith
        shadowing the tracked file."""
        config_dir, workspace = _legacy_install(tmp_path, self.MACHINE_ONLY + "timezone: UTC\n")
        assert migrate(config_dir, workspace=workspace).migrated_config

        again = migrate(config_dir, workspace=workspace)
        assert not again.did_anything
        assert again.warnings == []
        assert is_migrated(config_dir, workspace=workspace)

    def test_no_config_yaml_is_written_when_there_is_no_machine_half(self, tmp_path):
        """A wholly portable legacy config leaves nothing behind to shadow the
        tracked file."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        report = migrate(config_dir, workspace=workspace)
        assert report.machine_local_kept == []
        assert not (config_dir / "config.yaml").exists()

    def test_the_external_agents_block_is_machine_local_too(self, tmp_path):
        """A second writer appends to config.yaml after the wizard has paired
        this box's external agents, so it has to be covered as well."""
        from types import SimpleNamespace

        from nerve.bootstrap import SetupWizard

        (tmp_path / "config.yaml").write_text("deployment: server\n", encoding="utf-8")
        wizard = SetupWizard(tmp_path)
        wizard._record_external_agents_in_config_yaml([SimpleNamespace(agent="codex")])

        raw = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert "external_agents" in raw  # the writer really did run
        assert not nerve.migrate._has_portable_content(raw)

    def test_lingering_config_yaml_is_reported_not_ignored(self, tmp_path):
        """What an interruption at the rename leaves behind, and what a
        hand-managed install can look like. Indistinguishable on disk, so say
        so instead of guessing."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        (workspace / "config").mkdir(parents=True)
        workspace_settings_file(workspace).write_text("timezone: Europe/Berlin\n", encoding="utf-8")

        report = migrate(config_dir, workspace=workspace)

        assert not report.migrated_config
        assert any("still present" in w for w in report.warnings)

    def test_no_warning_for_an_ordinary_split_install(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, self.MACHINE_ONLY)
        (workspace / "config").mkdir(parents=True)
        workspace_settings_file(workspace).write_text("timezone: UTC\n", encoding="utf-8")
        assert migrate(config_dir, workspace=workspace).warnings == []


_DOCS_CONFIG_MD = Path(__file__).resolve().parents[1] / "docs" / "config.md"
_LAYER_TABLE_HEADING = "## Which layer a key belongs in"
# A sentinel rather than getattr's default of None: a setting whose default *is*
# None (gateway.ssl.cert) has to read as present.
_MISSING = object()


def _documented_machine_local_paths() -> set[str]:
    """The ``config.yaml`` row of the layer table in ``docs/config.md``.

    Parsed out of the document rather than restated here. A copy of the table
    kept in this file would agree with the test forever and with the docs never,
    which is the failure mode the guard exists to catch.
    """
    text = _DOCS_CONFIG_MD.read_text(encoding="utf-8")
    parts = text.split(_LAYER_TABLE_HEADING, 1)
    assert len(parts) == 2, (
        f"{_LAYER_TABLE_HEADING!r} is no longer in {_DOCS_CONFIG_MD} — the layer "
        "table moved or was renamed, and this guard is now checking nothing"
    )
    rows = [
        line for line in parts[1].splitlines() if line.startswith("| `config.yaml` |")
    ]
    assert len(rows) == 1, (
        f"expected exactly one `config.yaml` row under {_LAYER_TABLE_HEADING!r}, "
        f"found {len(rows)}"
    )
    tokens = re.findall(r"`([^`]+)`", rows[0].split("|")[2])
    assert tokens, f"the `config.yaml` row names no keys: {rows[0]}"

    paths = set()
    for token in tokens:
        # ``gateway.ssl.*`` names a subtree; the code lists its root.
        assert re.fullmatch(r"[a-z_]+(?:\.[a-z_]+)*(?:\.\*)?", token), (
            f"{token!r} in the layer table is not a plain dotted path. The "
            "shorthand the settings.yaml row uses (`a`/`b` for two siblings) "
            "would be read as one key here, so spell it out instead"
        )
        paths.add(token[:-2] if token.endswith(".*") else token)
    return paths


class TestMachineLocalPathsMatchTheDocs:
    """The layer table in ``docs/config.md`` states which layer owns a key;
    ``_MACHINE_LOCAL_PATHS`` is what migration and the wizard actually do. The
    list's comment claims to mirror the table and nothing checked it.

    Both directions matter now that the list decides what migration publishes. A
    key the table calls machine-local but the list omits is copied into a file the
    docs tell you to commit; one the list claims but the table shares is stranded
    in a layer lockdown never reads.
    """

    def test_the_list_and_the_table_agree(self):
        documented = _documented_machine_local_paths()
        assert len(documented) >= 10, (
            f"the parse found only {len(documented)} keys in the layer table — "
            "did the table's formatting change?"
        )
        listed = set(nerve.migrate._MACHINE_LOCAL_PATHS)
        assert documented == listed, (
            "docs/config.md's `config.yaml` row and _MACHINE_LOCAL_PATHS "
            "disagree.\n"
            f"  in the docs, not in the code: {sorted(documented - listed)}\n"
            f"  in the code, not in the docs: {sorted(listed - documented)}"
        )

    def test_every_listed_path_names_a_real_setting(self):
        """A renamed or misspelled entry covers nothing and says nothing.

        It matches no key, so ``_has_portable_content`` counts the value as
        shareable and the split hands it to the tracked file — while the list
        still reads as though that key were accounted for.
        """
        from nerve.config import NerveConfig

        defaults = NerveConfig()
        unresolved = []
        for dotted in sorted(nerve.migrate._MACHINE_LOCAL_PATHS):
            node = defaults
            for part in dotted.split("."):
                node = getattr(node, part, _MISSING)
                if node is _MISSING:
                    unresolved.append(dotted)
                    break
        assert not unresolved, (
            "_MACHINE_LOCAL_PATHS entries that resolve to no setting on a default "
            f"NerveConfig (renamed? misspelled?): {unresolved}"
        )


class TestMigrationIsolation:
    """``workspace=`` alone never sandboxed the cron half — it resolved its
    source from the machine-global state dir and *renamed* files there."""

    def test_scoped_migration_leaves_the_machine_cron_dir_untouched(self, tmp_path):
        machine_cron = paths.cron_dir()
        machine_cron.mkdir(parents=True, exist_ok=True)
        (machine_cron / "jobs.yaml").write_text("jobs: []  # real\n", encoding="utf-8")
        (machine_cron / "system.yaml").write_text("jobs: []  # real\n", encoding="utf-8")

        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = _legacy_cron(tmp_path, {"jobs.yaml": "jobs: []  # scoped\n"})
        report = migrate(config_dir, workspace=workspace, legacy_cron_dir=legacy_cron)

        assert report.migrated_cron
        assert (machine_cron / "jobs.yaml").exists()
        assert (machine_cron / "system.yaml").exists()
        assert not list(machine_cron.glob("*.migrated"))
        # The scoped directory is the one that moved.
        assert "scoped" in (workspace / "config" / "cron" / "jobs.yaml").read_text()

    def test_override_threads_through_the_dry_run_helpers(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = _legacy_cron(tmp_path, {"jobs.yaml": "jobs: []\n"})
        empty = tmp_path / "no-cron-here"
        assert not is_migrated(config_dir, workspace=workspace, legacy_cron_dir=legacy_cron)
        maybe_migrate(config_dir, workspace=workspace, legacy_cron_dir=empty)
        # Config migrated, cron did not — the override was honored by both.
        assert workspace_settings_file(workspace).exists()
        assert not (workspace / "config" / "cron").exists()
        assert (legacy_cron / "jobs.yaml").exists()


class TestPlantedSecretCorpus:
    """A legacy config carrying one of every plausible secret shape. What
    matters is the bytes that land in the git-tracked file."""

    CORPUS = """
timezone: UTC
anthropic_api_key: sk-ant-api03-AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHH
auth:
  jwt_secret: hunter2hunter2hunter2
github:
  token: "ghp_MixedRefAAAABBBBCCCCDDDDEEEE${GH_SUFFIX}"
slack:
  webhook_url: https://hooks.slack.com/services/T000/B000/XXXXXXXXXXXXXXXXXXXX
database:
  url: postgres://admin:sup3rs3cr3t@db.internal:5432/nerve
mcp_servers:
  gh:
    args: ["--stdio", "--api-key=sk-proj-AAAABBBBCCCCDDDDEEEEFFFF"]
    headers: ["Authorization: Bearer ghp_AAAABBBBCCCCDDDDEEEEFFFFGGGG"]
  feed:
    url: https://feeds.example.com/all?token=abcdef1234567890abcdef
telegram_sync:
  api_id: 2040123
  api_hash: 0123456789abcdef0123456789abcdef
sentry:
  dsn: https://0123456789abcdef0123456789abcdef@o12345.ingest.sentry.io/9876
smtp:
  host: smtp.example.com
  pw: correct-horse-battery-staple
  pat: pat_AAAABBBBCCCCDDDDEEEE
telethon:
  session_string: 1BVtsOHYBu4XyZAbCdEfGhIjKlMnOpQrStUvWx0123456789
worker:
  env: ["GH_PAT=ghp_WWWWXXXXYYYYZZZZAAAABBBB"]
"""

    PLANTED = [
        "sk-ant-api03-AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHH",
        "hunter2hunter2hunter2",
        "ghp_MixedRefAAAABBBBCCCCDDDDEEEE",
        "T000/B000/XXXXXXXXXXXXXXXXXXXX",
        "sup3rs3cr3t",
        "sk-proj-AAAABBBBCCCCDDDDEEEEFFFF",
        "ghp_AAAABBBBCCCCDDDDEEEEFFFFGGGG",
        "token=abcdef1234567890abcdef",
        "2040123",
        "0123456789abcdef0123456789abcdef",
        "correct-horse-battery-staple",
        "pat_AAAABBBBCCCCDDDDEEEE",
        "1BVtsOHYBu4XyZAbCdEfGhIjKlMnOpQrStUvWx0123456789",
        "ghp_WWWWXXXXYYYYZZZZAAAABBBB",
    ]

    def test_no_planted_secret_reaches_the_tracked_file(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, self.CORPUS)
        migrate(config_dir, workspace=workspace)

        tracked = workspace_settings_file(workspace).read_text()
        leaked = [s for s in self.PLANTED if s in tracked]
        assert not leaked, f"{len(leaked)} secret(s) leaked into settings.yaml: {leaked}"

    def test_every_planted_secret_is_still_readable_after_migration(self, tmp_path):
        """Scrubbing must relocate, not destroy — the daemon still resolves the
        real values from the machine-local overlay."""
        config_dir, workspace = _legacy_install(tmp_path, self.CORPUS)
        migrate(config_dir, workspace=workspace)

        overlay = (config_dir / "config.local.yaml").read_text()
        missing = [s for s in self.PLANTED if s not in overlay]
        assert not missing, f"lost in migration: {missing}"


class TestMigrateCron:
    def test_copies_legacy_cron_to_workspace(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        legacy_cron.mkdir(parents=True, exist_ok=True)
        (legacy_cron / "jobs.yaml").write_text(
            "jobs:\n  - id: mine\n    schedule: 1h\n    prompt: hi\n", encoding="utf-8"
        )
        report = migrate(config_dir, workspace=workspace)
        assert report.migrated_cron

        ws_jobs = workspace / "config" / "cron" / "jobs.yaml"
        assert ws_jobs.exists()
        assert "mine" in ws_jobs.read_text()
        # Original kept as breadcrumb.
        assert (legacy_cron / "jobs.yaml.migrated").exists()

    def test_copies_cron_prompts_dir(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        (legacy_cron / "prompts").mkdir(parents=True, exist_ok=True)
        (legacy_cron / "prompts" / "daily.md").write_text("do it", encoding="utf-8")
        (legacy_cron / "jobs.yaml").write_text(
            "jobs:\n  - id: j\n    schedule: 1h\n    prompt_file: prompts/daily.md\n",
            encoding="utf-8",
        )
        migrate(config_dir, workspace=workspace)
        # The referenced prompt file came along, so the relative path still resolves.
        assert (workspace / "config" / "cron" / "prompts" / "daily.md").exists()

    def test_no_cron_migration_when_workspace_has_jobs(self, tmp_path):
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        legacy_cron.mkdir(parents=True, exist_ok=True)
        (legacy_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        ws_cron = workspace / "config" / "cron"
        ws_cron.mkdir(parents=True)
        (ws_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        report = migrate(config_dir, workspace=workspace)
        assert not report.migrated_cron
        assert not (legacy_cron / "jobs.yaml.migrated").exists()
        # Silence would leave the operator with two job files, one of which has
        # stopped running: the workspace copy wins and the legacy dir is dropped.
        assert any("still holds cron jobs" in w for w in report.warnings)

    def test_keeps_a_cron_path_outside_the_migrated_directory(self, tmp_path):
        """Only the paths this migration invalidates are dropped.

        A cron file somewhere else entirely is a deliberate choice about a
        location migration never touches, so it still resolves afterwards and
        must survive. Dropping it too would silently move that instance's jobs
        to a directory it never chose.
        """
        legacy_cron = paths.cron_dir()
        legacy_cron.mkdir(parents=True, exist_ok=True)
        (legacy_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
        elsewhere = tmp_path / "elsewhere" / "jobs.yaml"
        elsewhere.parent.mkdir(parents=True)
        elsewhere.write_text(
            "jobs:\n  - id: kept\n    schedule: 1h\n    prompt: hi\n", encoding="utf-8"
        )
        config_dir, workspace = _legacy_install(
            tmp_path, f"cron:\n  jobs_file: {elsewhere}\n"
        )

        migrate(config_dir, workspace=workspace)

        config = load_config(config_dir)
        assert config.cron.jobs_file == elsewhere
        assert [j.id for j in load_jobs(config.cron.jobs_file)] == ["kept"]

    def test_gates_and_prompts_migrate_past_the_files_nerve_init_wrote(self, tmp_path):
        """``nerve init`` writes ``system.yaml`` and an empty ``gates/`` into the
        workspace and copies only ``jobs.yaml`` across. A directory-level "the
        workspace already has cron config" test therefore skipped everything
        else permanently — and the legacy dir is never read again once the
        workspace has job files, so custom gates stopped loading. A job whose
        gate ``type`` no longer resolves does not run at all."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        (legacy_cron / "gates").mkdir(parents=True, exist_ok=True)
        (legacy_cron / "gates" / "stale_tasks.py").write_text("CUSTOM = 1\n", encoding="utf-8")
        (legacy_cron / "prompts").mkdir(parents=True, exist_ok=True)
        (legacy_cron / "prompts" / "daily.md").write_text("do it", encoding="utf-8")
        (legacy_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")

        ws_cron = workspace / "config" / "cron"
        (ws_cron / "gates").mkdir(parents=True)          # the init placeholder
        (ws_cron / "system.yaml").write_text("jobs: []\n", encoding="utf-8")
        (ws_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")

        report = migrate(config_dir, workspace=workspace)

        assert report.migrated_cron
        assert (ws_cron / "gates" / "stale_tasks.py").read_text() == "CUSTOM = 1\n"
        assert (ws_cron / "prompts" / "daily.md").exists()

    def test_an_existing_workspace_file_is_kept_and_the_rest_still_moves(self, tmp_path):
        """Never overwrite: the workspace copy is the reviewed one. That has to
        hold per file rather than per directory, or one same-named gate blocks
        every other gate in the directory."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        (legacy_cron / "gates").mkdir(parents=True, exist_ok=True)
        (legacy_cron / "gates" / "shared.py").write_text("LEGACY = 1\n", encoding="utf-8")
        (legacy_cron / "gates" / "only_legacy.py").write_text("OTHER = 1\n", encoding="utf-8")
        (legacy_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")

        ws_cron = workspace / "config" / "cron"
        (ws_cron / "gates").mkdir(parents=True)
        (ws_cron / "gates" / "shared.py").write_text("REVIEWED = 1\n", encoding="utf-8")

        migrate(config_dir, workspace=workspace)

        assert (ws_cron / "gates" / "shared.py").read_text() == "REVIEWED = 1\n"
        assert (ws_cron / "gates" / "only_legacy.py").read_text() == "OTHER = 1\n"

    def test_a_job_file_that_did_not_move_is_not_breadcrumbed(self, tmp_path):
        """Renaming a legacy ``jobs.yaml`` that lost to a workspace copy would
        hide the operator's only copy of those jobs behind a ``.migrated``
        suffix, while the jobs themselves are already not running."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        (legacy_cron / "gates").mkdir(parents=True, exist_ok=True)
        (legacy_cron / "gates" / "g.py").write_text("G = 1\n", encoding="utf-8")
        (legacy_cron / "jobs.yaml").write_text(
            "jobs:\n  - id: legacy-only\n    schedule: 1h\n    prompt: hi\n", encoding="utf-8"
        )
        ws_cron = workspace / "config" / "cron"
        ws_cron.mkdir(parents=True)
        (ws_cron / "jobs.yaml").write_text("jobs: []\n", encoding="utf-8")

        report = migrate(config_dir, workspace=workspace)

        assert (ws_cron / "gates" / "g.py").exists()       # the gate still moved
        assert (legacy_cron / "jobs.yaml").exists()        # ...and this stayed put
        assert not (legacy_cron / "jobs.yaml.migrated").exists()
        assert "legacy-only" in (legacy_cron / "jobs.yaml").read_text()
        assert any("still holds cron jobs" in w for w in report.warnings)

    def test_gates_migrate_even_with_no_legacy_job_files(self, tmp_path):
        """Job files are not the gate for the directory: an install that pinned
        ``cron.jobs_file`` elsewhere still keeps its gate plugins here, and they
        are imported from whichever cron dir resolution picks."""
        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        (legacy_cron / "gates").mkdir(parents=True, exist_ok=True)
        (legacy_cron / "gates" / "g.py").write_text("G = 1\n", encoding="utf-8")

        report = migrate(config_dir, workspace=workspace)

        assert report.migrated_cron
        assert (workspace / "config" / "cron" / "gates" / "g.py").exists()


class TestMaybeMigrate:
    def test_never_raises(self, tmp_path):
        # A config_dir with nothing — must not raise.
        result = maybe_migrate(tmp_path / "nonexistent")
        assert result is None or not result.did_anything

    def test_a_failure_partway_still_reports_what_was_applied(self, tmp_path, monkeypatch):
        """The config half commits before the cron half runs, so an exception can
        leave settings.yaml written, the secrets relocated and config.yaml renamed
        away. Reporting that as "nothing happened" is what made a half-applied
        migration a silent one: ``upgrade`` printed no review prompt for a tracked
        file that now exists and may hold an unscrubbed credential, and ``start``
        skipped the reload it needs because the files moved underneath it."""
        config_dir, workspace = _legacy_install(
            tmp_path, "timezone: UTC\nanthropic_api_key: sk-xyz\n"
        )
        legacy_cron = _legacy_cron(tmp_path, {"jobs.yaml": "jobs: []\n"})

        def boom(src, dst, *args, **kwargs):
            raise OSError("Read-only file system")

        monkeypatch.setattr(nerve.migrate.shutil, "copy2", boom)
        report = maybe_migrate(config_dir, workspace=workspace, legacy_cron_dir=legacy_cron)

        assert report is not None
        assert report.did_anything
        assert report.error and "Read-only file system" in report.error
        # ...and what it reports really did happen.
        assert workspace_settings_file(workspace).exists()
        assert (config_dir / "config.yaml.migrated").exists()
        assert report.secrets_moved == ["anthropic_api_key"]


class TestStartAfterMigration:
    def test_foreground_start_gets_the_post_migration_config(self, tmp_path):
        """``start`` loads config, migrates, then serves. Migration moves cron
        into the workspace and renames the legacy files away, so the object
        loaded beforehand points at job files that no longer exist — and
        ``--foreground`` is the deployed path, where that means the daemon runs
        for its whole lifetime with nothing scheduled."""
        from nerve.cli import start

        config_dir, workspace = _legacy_install(tmp_path, "timezone: UTC\n")
        legacy_cron = paths.cron_dir()
        legacy_cron.mkdir(parents=True, exist_ok=True)
        (legacy_cron / "jobs.yaml").write_text(
            "jobs:\n  - id: mine\n    schedule: 1h\n    prompt: hi\n", encoding="utf-8"
        )

        stale = load_config(config_dir)
        assert stale.cron.jobs_file == legacy_cron / "jobs.yaml"

        served = {}
        with (
            patch("nerve.gateway.server.run_server", side_effect=lambda c: served.update(config=c)),
            patch("nerve.cli._get_daemon_status", return_value=(False, None)),
            patch("nerve.cli._write_pid"),
            patch("nerve.cli._remove_pid"),
            patch("nerve.bootstrap.is_fresh_install", return_value=False),
        ):
            result = CliRunner().invoke(
                start,
                ["--foreground"],
                obj={"config": stale, "config_dir": str(config_dir), "verbose": False},
                standalone_mode=False,
            )

        assert result.exit_code == 0, result.output
        config = served["config"]
        assert config.cron.jobs_file == workspace / "config" / "cron" / "jobs.yaml"
        assert [j.id for j in load_jobs(config.cron.jobs_file)] == ["mine"]

    def test_cron_paths_naming_the_legacy_dir_do_not_outlive_it(self, tmp_path):
        """The same silence, for a config that spelled its cron paths out.

        The test above covers a config that never named them, where resolution
        follows the workspace on its own. A monolith that *did* name them
        carries absolute ``~/.nerve/cron`` paths into the migrated settings,
        where they keep pointing at files migration has moved and renamed to
        ``*.migrated``. Reloading is not enough to recover: the pointers are in
        the config now, so every subsequent start schedules nothing too.

        Nothing raises, here or in production — an absent cron file is a normal
        state for an install with no jobs — so the only trace is one INFO line
        per file and a daemon that quietly never runs anything again.
        """
        legacy_cron = paths.cron_dir()
        legacy_cron.mkdir(parents=True, exist_ok=True)
        (legacy_cron / "jobs.yaml").write_text(
            "jobs:\n  - id: mine\n    schedule: 1h\n    prompt: hi\n", encoding="utf-8"
        )
        config_dir, workspace = _legacy_install(
            tmp_path,
            "timezone: UTC\n"
            "cron:\n"
            f"  jobs_file: {legacy_cron / 'jobs.yaml'}\n"
            f"  system_file: {legacy_cron / 'system.yaml'}\n",
        )

        migrate(config_dir, workspace=workspace)

        # Nothing machine-local reached the file the docs say to commit, so the
        # next box to sync this repo does not inherit one box's ~/.nerve either.
        settings = yaml.safe_load(
            workspace_settings_file(workspace).read_text(encoding="utf-8")
        )
        assert "cron" not in (settings or {}), (
            "paths naming the files migration just moved were published to the "
            f"tracked settings: {(settings or {}).get('cron')!r}"
        )

        config = load_config(config_dir)
        assert config.cron.jobs_file == workspace / "config" / "cron" / "jobs.yaml"
        assert [j.id for j in load_jobs(config.cron.jobs_file)] == ["mine"]
