"""What the onboarding agent knows about its instance before it says a word.

The conversation's first turn calls no tool before its message, so Nerve
gathers the facts for it (``## This instance``): one line per item, always in
the same order, each a fact or unknown; sources and identities, never a
secret. The tests drive the builder on the files of a real install. The
GitHub CLI is never the developer's: the suite's stand-in answers "not
installed", and the tests here put a fake ``gh`` behind the real probe.
"""

from __future__ import annotations

import dataclasses
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from nerve import onboarding_environment as env
from nerve import setup_checks
from nerve.config import load_config
from nerve.setup_writer import (
    CONFIG_YAML_HEADER,
    SetupChoices,
    apply_mode,
    apply_profile_templates,
    merge_machine_paths,
    merge_private_paths,
    merge_settings_paths,
    write_slim_install,
)

# The real probe, captured before the suite's stand-in replaces it per test.
_REAL_GITHUB_CLI = env._github_cli

LABELS = [
    "Mode", "Workspace", "Configuration", "Model", "Memory", "GitHub token",
    "GitHub CLI (`gh`)", "GitHub sync source", "GitHub repository watch", "Linear", "Slack",
    "Telegram channel", "Telegram message sync", "Gmail", "Time zone", "Quiet hours",
]
NONE_KNOWN = "GitHub access: none known — clone public URLs only; report what needs a credential."
ACCESS_UNKNOWN = (
    "GitHub access: unknown — run `gh auth status` once; if it fails, clone public URLs only "
    "and report what needs a credential."
)
SIGNED_IN = "github.com\n  ✓ Logged in to github.com account alice (keyring)\n  - Active account: true\n"

# Placeholders shaped like the real values, each with a distinct tail.
SLACK_BOT = "xoxb-REPLACE-SLACKBOT-Q7Z1"
SLACK_APP = "xapp-REPLACE-SLACKAPP-W3K8"
TELEGRAM = "123456:REPLACE-TELEGRAM-BOT-P5M2"
ANTHROPIC = "sk-ant-REPLACE-ANTHROPIC-H9D4"
OPENAI = "sk-REPLACE-OPENAI-J2X6"
GH_TOKEN = "ghp_REPLACE-GITHUB-TOKEN-V4N7"
API_HASH = "REPLACE-TELEGRAM-API-HASH-T8R3"
SECRETS = (SLACK_BOT, SLACK_APP, TELEGRAM, ANTHROPIC, OPENAI, GH_TOKEN, API_HASH, "7654321")


@pytest.fixture(autouse=True)
def _no_ambient_github_token(monkeypatch):
    """CI runners and developer shells may carry a GitHub token."""
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)


def _install(tmp_path: Path, mode: str = "worker") -> tuple[Path, Path]:
    """A slim install (every source off, as setup in the browser writes it) with a mode."""
    config_dir = tmp_path / "config"
    workspace = tmp_path / "workspace"
    write_slim_install(
        SetupChoices(deployment="server", workspace_path=workspace, gateway_host="127.0.0.1", gateway_port=8976),
        config_dir,
    )
    apply_mode(workspace, mode)
    return config_dir, workspace


def _lines(section: str) -> dict[str, str]:
    """``{label: text}``, asserting the section's shape on the way."""
    heading, _, body = section.partition("\n\n")
    assert heading == "## This instance"
    lines: dict[str, str] = {}
    for line in body.splitlines():
        assert line.startswith("- ") and line.endswith("."), line
        label, _, text = line[2:-1].partition(": ")
        lines[label] = text
    assert list(lines) == LABELS
    return lines


@pytest.fixture
def gh(monkeypatch):
    """The real probe over a fake ``gh``: ``gh(stdout, returncode=0)`` puts
    one on PATH, ``gh(None)`` removes it, ``gh(raises=...)`` makes it fail.
    Returns the calls (argv, timeout)."""
    monkeypatch.setattr(env, "_github_cli", _REAL_GITHUB_CLI)
    calls: list[tuple[list[str], float]] = []
    which = setup_checks.shutil.which

    def _install_gh(stdout: str | None = SIGNED_IN, *, returncode: int = 0, raises: BaseException | None = None):
        present = stdout is not None or raises is not None
        monkeypatch.setattr(
            setup_checks.shutil, "which",
            lambda name, *a, **k: ("/usr/bin/gh" if present else None) if name == "gh" else which(name, *a, **k),
        )

        def _run(argv, timeout):
            calls.append((argv, timeout))
            if raises is not None:
                raise raises
            return SimpleNamespace(returncode=returncode, stdout=stdout, stderr="")

        monkeypatch.setattr(setup_checks, "_run", _run)
        return calls

    return _install_gh


class TestNothingConfigured:
    def test_every_connection_reads_not_configured_and_the_cli_not_installed(self, tmp_path, gh):
        config_dir, ws = _install(tmp_path)
        config = load_config(config_dir)
        calls = gh(None)
        facts = env.gather(config, ws)
        lines = _lines(facts.section)
        assert lines["Mode"] == "worker (an agent for a team)"
        assert lines["Workspace"] == f"`{ws}`; clone repositories into `{ws}/repos/<name>`"
        assert lines["Configuration"] == "files on this machine, edited in place"
        assert lines["Model"] == f"`{config.agent.model}`; no model credential found"
        assert lines["Memory"] == "memU; embeddings off, so recall is ranked by the model"
        assert lines["GitHub token"] == "none in Nerve's configuration or environment"
        assert lines["GitHub CLI (`gh`)"] == "not installed"
        assert lines["GitHub sync source"] == "off"
        assert lines["GitHub repository watch"] == "off"
        assert lines["Linear"] == "off"
        assert lines["Slack"] == "not configured"
        assert lines["Telegram channel"] == "not configured"
        assert lines["Telegram message sync"] == "off"
        assert lines["Gmail"] == "no accounts"
        assert lines["Time zone"] == f"`{config.timezone}` (settings; a worker has no `USER.md`)"
        quiet = f"{config.quiet_start}–{config.quiet_end}" if config.quiet_start and config.quiet_end else "none"
        assert lines["Quiet hours"] == f"{quiet} (settings)"
        assert facts.github_access == NONE_KNOWN
        assert calls == [], "no gh on PATH: nothing runs"

    def test_without_the_probe_the_cli_is_unknown(self, tmp_path, gh):
        config_dir, ws = _install(tmp_path)
        calls = gh(SIGNED_IN)
        facts = env.gather(load_config(config_dir), ws, probe=False)
        assert _lines(facts.section)["GitHub CLI (`gh`)"] == "unknown (not checked)"
        assert facts.github_access == ACCESS_UNKNOWN
        assert calls == []

    def test_environment_facts_is_the_section(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        config = load_config(config_dir)
        assert env.environment_facts(config, ws) == env.gather(config, ws).section


class TestGitHubCLI:
    def test_signed_in_names_the_login_for_the_agent_and_the_explorer(self, tmp_path, gh):
        config_dir, ws = _install(tmp_path)
        calls = gh(SIGNED_IN)
        facts = env.gather(load_config(config_dir), ws)
        assert _lines(facts.section)["GitHub CLI (`gh`)"] == "authenticated as `alice`"
        assert facts.github_access == "GitHub access: `gh` as `alice`."
        # One `gh auth status`, within the builder's budget and the probe's own timeout.
        assert [argv for argv, _ in calls] == [["gh", "auth", "status"]]
        assert 0 < calls[0][1] <= min(env.BUDGET_SECONDS, setup_checks.HTTP_TEST_TIMEOUT)

    @pytest.mark.parametrize("stdout, returncode, raises, line, access", [
        pytest.param(
            "You are not logged into any GitHub hosts. To log in, run: gh auth login\n", 1, None,
            "installed, not authenticated", NONE_KNOWN, id="not-authenticated",
        ),
        pytest.param(
            "✓ Logged in to github.com as alice (oauth_token)\n", 0, None,
            "authenticated; the login is unknown", "GitHub access: `gh` is authenticated; the login is unknown.",
            id="login-not-parsed",
        ),
        pytest.param(
            None, 0, subprocess.TimeoutExpired(["gh", "auth", "status"], 1.0),
            "unknown (`gh auth status` did not answer in time)", ACCESS_UNKNOWN, id="no-answer",
        ),
        pytest.param(
            None, 0, OSError("exec format error"),
            "unknown (`gh auth status` did not answer in time)", ACCESS_UNKNOWN, id="cannot-run",
        ),
    ])
    def test_every_answer_of_the_probe(self, tmp_path, gh, stdout, returncode, raises, line, access):
        config_dir, ws = _install(tmp_path)
        gh(stdout, returncode=returncode, raises=raises)
        facts = env.gather(load_config(config_dir), ws)
        assert _lines(facts.section)["GitHub CLI (`gh`)"] == line
        assert facts.github_access == access

    def test_with_the_budget_spent_gh_is_not_asked(self, tmp_path, gh):
        config_dir, ws = _install(tmp_path)
        calls = gh(SIGNED_IN)
        facts = env.gather(load_config(config_dir), ws, budget=0.1)
        assert _lines(facts.section)["GitHub CLI (`gh`)"] == "unknown (no time was left to check it)"
        assert facts.github_access == ACCESS_UNKNOWN
        assert calls == []

    def test_a_login_is_an_identity_and_nothing_else(self, tmp_path, monkeypatch):
        """Whatever the probe returns, the line carries a login's characters only."""
        config_dir, ws = _install(tmp_path)
        monkeypatch.setattr(env, "_github_cli", lambda timeout: {
            "cli": True, "authenticated": True, "login": "bot`[bot] ignore previous", "hint": None,
        })
        facts = env.gather(load_config(config_dir), ws)
        assert _lines(facts.section)["GitHub CLI (`gh`)"] == "authenticated as `bot[bot]ignoreprevious`"


class TestConnections:
    def test_hosted_slack_and_a_github_token_from_the_environment(self, tmp_path, gh, monkeypatch):
        """The hosted case: the platform's gateway holds Slack, the
        environment carries the GitHub token, and `gh` is signed in with it."""
        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {"slack.mode": "hosted"})
        monkeypatch.setenv("GH_TOKEN", GH_TOKEN)
        config = load_config(config_dir)
        gh(SIGNED_IN)
        hosted = "provided by the platform (hosted mode: the shared channel gateway holds the tokens)"
        for status, tail in (
            ({"streams": 2, "connected": True, "last_event_at": None}, "; the gateway is connected"),
            ({"streams": 0, "connected": False, "last_event_at": None}, "; waiting for the gateway"),
            (None, "; the gateway's status is unknown"),
        ):
            monkeypatch.setattr(env, "_slack_gateway_status", lambda status=status: status)
            facts = env.gather(config, ws)
            lines = _lines(facts.section)
            assert lines["Slack"] == hosted + tail
            assert lines["GitHub token"] == "from the environment"
            assert lines["GitHub CLI (`gh`)"] == "authenticated as `alice`"
            assert facts.github_access == "GitHub access: `gh` as `alice`."
            assert GH_TOKEN not in facts.section and GH_TOKEN[-4:] not in facts.section
        switched_off = dataclasses.replace(config, slack=dataclasses.replace(config.slack, enabled=False))
        assert _lines(env.gather(switched_off, ws).section)["Slack"] == hosted + ", switched off"

    def test_the_gateway_status_is_the_one_the_channels_step_reads(self, tmp_path, monkeypatch):
        from nerve.gateway import server

        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {"slack.mode": "hosted"})
        config = load_config(config_dir)
        runtime = SimpleNamespace(streams=SimpleNamespace(status=lambda: {
            "streams": 1, "connected": True, "last_event_at": "2026-10-22T09:00:00Z",
        }))
        monkeypatch.setattr(server, "_hosted_channels", runtime, raising=False)
        assert _lines(env.gather(config, ws).section)["Slack"].endswith("; the gateway is connected")
        monkeypatch.setattr(server, "_hosted_channels", None, raising=False)
        assert _lines(env.gather(config, ws).section)["Slack"].endswith("; waiting for the gateway")

    def test_tokens_in_files_and_references_name_their_source_never_their_value(self, tmp_path, monkeypatch):
        config_dir, ws = _install(tmp_path)
        monkeypatch.setenv("TG_API_HASH", API_HASH)
        monkeypatch.setenv("OPENAI_KEY_FOR_NERVE", OPENAI)
        merge_private_paths(config_dir / "config.local.yaml", {
            "anthropic_api_key": ANTHROPIC,
            "slack.bot_token": SLACK_BOT,
            "slack.app_token": SLACK_APP,
            "telegram.bot_token": TELEGRAM,
            "sync.telegram.api_id": 7654321,
            "sync.telegram.api_hash": "${TG_API_HASH}",
        })
        merge_machine_paths(config_dir / "config.yaml", {
            "telegram.enabled": True,
            "sync.gmail.accounts": ["first@example.com", "second@example.com"],
        }, header=CONFIG_YAML_HEADER)
        merge_settings_paths(ws, {
            "openai_api_key": "${OPENAI_KEY_FOR_NERVE}",
            "sync.telegram.enabled": True,
            "sync.gmail.enabled": True,
            "sync.github.enabled": True,
            "sync.github.allow_repos": ["org/repo-a", "org/repo-b"],
            "sync.github_repos.enabled": True,
            "sync.github_repos.repos": [f"org/repo-{n}" for n in "cdefgh"],
        })
        config = load_config(config_dir)
        facts = env.gather(config, ws)
        lines = _lines(facts.section)
        here = "from a configuration file on this machine"
        assert lines["Model"] == f"`{config.agent.model}` through the Anthropic API (key {here})"
        assert lines["Memory"] == "memU; embeddings on (OpenAI key from the environment)"
        assert lines["Slack"] == f"configured, socket mode (tokens {here})"
        assert lines["Telegram channel"] == f"configured (bot token {here})"
        assert lines["Telegram message sync"] == f"on (API credentials {here} and from the environment)"
        assert lines["Gmail"] == "2 accounts, sync on"
        assert lines["GitHub sync source"] == (
            "on: notifications of the threads the account takes part in (inbox `participating`), "
            "only from `org/repo-a`, `org/repo-b`"
        )
        assert lines["GitHub repository watch"] == (
            "on (polled here with `gh`): new pull requests and issues in "
            "`org/repo-c`, `org/repo-d`, `org/repo-e`, `org/repo-f`, `org/repo-g` and 1 more"
        )
        for secret in SECRETS:
            assert secret not in facts.section, secret
            assert secret[-4:] not in facts.section, secret
        assert "@example.com" not in facts.section, "Gmail is a count, not the addresses"

    @pytest.mark.parametrize(("inbox", "line"), [
        ("participating", "notifications of the threads the account takes part in (inbox `participating`)"),
        ("all", "the account's whole notification inbox (inbox `all`)"),
    ])
    def test_the_github_line_names_the_inbox_and_says_when_no_repository_filter_is_set(
        self, tmp_path, inbox, line,
    ):
        """An empty ``allow_repos`` is no repository filter on the account's
        notifications, not a watch of every repository."""
        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {"sync.github.enabled": True, "sync.github.inbox": inbox})

        lines = _lines(env.gather(load_config(config_dir), ws).section)

        assert lines["GitHub sync source"] == f"on: {line}, with no repository filter"
        assert "all repositories" not in lines["GitHub sync source"]

    def test_the_repository_watch_line_says_who_polls_what_and_where(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {
            "sync.github.mode": "hosted",
            "sync.github_repos.enabled": True,
            "sync.github_repos.repos": ["acme/widgets"],
            "sync.github_repos.types": ["release", "pull_request", "issue"],
            "sync.github_repos.updates": True,
        })

        lines = _lines(env.gather(load_config(config_dir), ws).section)

        assert lines["GitHub repository watch"] == (
            "on (the channel gateway polls): new pull requests, issues and releases in `acme/widgets`; "
            "also new commits, ready for review, merges and closes of the pull requests it reported"
        )

    def test_the_linear_line_says_what_it_reports_and_how_the_agent_writes_back(self, tmp_path):
        import yaml

        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {"sync.linear.enabled": True, "sync.linear.types": ["assigned"]})

        lines = _lines(env.gather(load_config(config_dir), ws).section)
        assert lines["Linear"] == "on, but its API key is not set"

        local_file = config_dir / "config.local.yaml"
        local = yaml.safe_load(local_file.read_text()) if local_file.exists() else {}
        local.setdefault("sync", {})["linear"] = {"api_key": "lin_api_REPLACE-LINEAR-K2"}
        local_file.write_text(yaml.safe_dump(local))

        section = env.gather(load_config(config_dir), ws).section
        assert _lines(section)["Linear"] == (
            "on (API key from a configuration file on this machine): assigned; writes through "
            "the `linear` MCP server (`mcp__linear__*`) as the key's user"
        )
        assert "REPLACE-LINEAR" not in section

    def test_a_repository_watch_without_repositories_says_so(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {"sync.github_repos.enabled": True, "sync.github_repos.types": ["release"]})

        lines = _lines(env.gather(load_config(config_dir), ws).section)

        assert lines["GitHub repository watch"] == "on, but no repositories are listed"

    def test_half_a_slack_setup_is_not_configured(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        merge_private_paths(config_dir / "config.local.yaml", {"slack.bot_token": SLACK_BOT})
        assert _lines(env.gather(load_config(config_dir), ws).section)["Slack"] == (
            "not configured (only one of its two tokens is set)"
        )

    def test_the_model_line_names_the_provider_never_the_key(self, tmp_path, monkeypatch):
        config_dir, ws = _install(tmp_path)
        config = load_config(config_dir)
        model = f"`{config.agent.model}`"
        bedrock = dataclasses.replace(config, provider=dataclasses.replace(
            config.provider, type="bedrock", aws_region="us-west-2",
        ))
        assert _lines(env.gather(bedrock, ws).section)["Model"] == f"{model} through AWS Bedrock (us-west-2)"
        proxy = dataclasses.replace(config, proxy=dataclasses.replace(config.proxy, enabled=True))
        assert _lines(env.gather(proxy, ws).section)["Model"] == (
            f"{model} through a Claude subscription, through the local proxy"
        )
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "REPLACE-OAUTH-TOKEN-L1C5")
        assert _lines(env.gather(config, ws).section)["Model"] == (
            f"{model} through a Claude subscription token (from the environment)"
        )
        monkeypatch.setenv("ANTHROPIC_API_KEY", ANTHROPIC)
        facts = env.gather(config, ws)
        assert _lines(facts.section)["Model"] == f"{model} through the Anthropic API (key from the environment)"
        assert ANTHROPIC not in facts.section and "L1C5" not in facts.section

    def test_a_loom_instance_names_loom_as_its_memory(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        config = load_config(config_dir)
        loom = dataclasses.replace(config, memory=dataclasses.replace(config.memory, backend="loom"))
        assert _lines(env.gather(loom, ws).section)["Memory"] == "Loom"

    def test_lockdown_and_an_instance_without_local_credentials(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        config = load_config(config_dir)
        managed = dataclasses.replace(
            config, lockdown=True, setup=dataclasses.replace(config.setup, local_secrets=False),
        )
        assert _lines(env.gather(managed, ws).section)["Configuration"] == (
            "arrives by pull request (lockdown); change it with `propose_config_change`; "
            "this instance stores no credentials, its operator provides access"
        )


class TestProfile:
    def test_a_personal_install_reads_the_time_zone_and_quiet_hours_from_user_md(self, tmp_path):
        config_dir, ws = _install(tmp_path, "personal")
        apply_profile_templates(ws, timezone="Europe/Berlin", quiet=("22:00", "07:00"))
        config = load_config(config_dir)
        assert config.timezone != "Europe/Berlin", "USER.md, not the settings, decides here"
        lines = _lines(env.gather(config, ws).section)
        assert lines["Mode"] == "personal assistant"
        assert lines["Time zone"] == "`Europe/Berlin` (`USER.md`)"
        assert lines["Quiet hours"] == "22:00–07:00 (Europe/Berlin) (`USER.md`)"

    def test_a_pristine_user_md_falls_back_to_the_settings(self, tmp_path):
        config_dir, ws = _install(tmp_path, "personal")
        config = dataclasses.replace(load_config(config_dir), timezone="Asia/Tokyo", quiet_start="", quiet_end="")
        lines = _lines(env.gather(config, ws).section)
        assert lines["Time zone"] == "`Asia/Tokyo` (settings)"
        assert lines["Quiet hours"] == "none (settings)"

    def test_a_worker_has_no_user_md(self, tmp_path):
        config_dir, ws = _install(tmp_path, "worker")
        (ws / "USER.md").write_text("- **Timezone:** Europe/Berlin\n", encoding="utf-8")
        config = dataclasses.replace(load_config(config_dir), timezone="UTC", quiet_start="01:00", quiet_end="05:00")
        lines = _lines(env.gather(config, ws).section)
        assert lines["Time zone"] == "`UTC` (settings; a worker has no `USER.md`)"
        assert lines["Quiet hours"] == "01:00–05:00 (settings)"


class TestRobustness:
    def test_one_failed_read_costs_only_its_own_line(self, tmp_path, gh, monkeypatch):
        config_dir, ws = _install(tmp_path)
        merge_settings_paths(ws, {"slack.mode": "hosted"})
        config = load_config(config_dir)
        gh(SIGNED_IN)
        baseline = _lines(env.gather(config, ws).section)

        def _boom(*args, **kwargs):
            raise RuntimeError("unreadable")

        monkeypatch.setattr(env, "credential_sources", _boom)
        monkeypatch.setattr(env, "_slack_gateway_status", _boom)
        monkeypatch.setattr(env, "_gmail_text", _boom)
        facts = env.gather(config, ws)
        lines = _lines(facts.section)
        assert lines["GitHub token"] == "unknown"
        assert lines["Slack"].endswith("; the gateway's status is unknown")
        assert lines["Gmail"] == "unknown"
        assert lines["GitHub CLI (`gh`)"] == "authenticated as `alice`"
        untouched = set(LABELS) - {"GitHub token", "Slack", "Gmail"}
        assert {label: lines[label] for label in untouched} == {label: baseline[label] for label in untouched}

        for probe in (_boom, lambda timeout: None):
            monkeypatch.setattr(env, "_github_cli", probe)
            facts = env.gather(config, ws)
            assert _lines(facts.section)["GitHub CLI (`gh`)"] == "unknown (the check failed)"
            assert facts.github_access == ACCESS_UNKNOWN

    def test_the_same_instance_reads_the_same(self, tmp_path, gh):
        config_dir, ws = _install(tmp_path)
        config = load_config(config_dir)
        gh(SIGNED_IN)
        assert env.gather(config, ws) == env.gather(config, ws)
