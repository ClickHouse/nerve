"""What the onboarding agent knows about its instance before it says a word.

The onboarding conversation's first turn opens with the visible message, with
no tool call before it (:func:`nerve.agent.prompts.onboarding_prompt`), so the
agent cannot look anything up before it asks. Nerve looks first and puts the
answers into the hidden first message, as the section ``## This instance``:
the mode, the workspace, how configuration changes, the model, memory, and the
connections: what is configured and where it comes from
(:func:`nerve.setup_provenance.credential_sources`), the GitHub CLI's login
(the probe of the setup page's sources preflight,
:func:`nerve.setup_checks._gh_status`), the GitHub sync source (its inbox and
repository filter), the repository watch and its repositories, Slack (with the
platform's gateway in hosted mode), Telegram, Gmail, and the time zone and
quiet hours.

One line per item, always in the same order, each a fact or ``unknown``.
Sources and identities only: never a secret, a part of one, or a hash.
Blocking I/O (a few small files and one ``gh auth status``), for a worker
thread. Every line is guarded, so a failed read costs only its own line, and
the GitHub CLI check gets what is left of :data:`BUDGET_SECONDS`.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from nerve import setup_checks
from nerve.setup_checklist import _line_value, _read_lines
from nerve.setup_provenance import (
    SOURCE_CONFIG_REPO,
    SOURCE_ENVIRONMENT,
    SOURCE_FILE,
    SOURCE_GATEWAY,
    credential_sources,
)
from nerve.setup_writer import workspace_mode

logger = logging.getLogger(__name__)

HEADING = "## This instance"
UNKNOWN = "unknown"
#: The whole build. The GitHub CLI check is its only slow part and gets what
#: is left, at most the sources preflight's own timeout.
BUDGET_SECONDS = 8.0
#: With less time left than this, the GitHub CLI is not asked at all.
_MIN_PROBE_SECONDS = 0.5
#: Repositories named in one line, at most; the rest are counted.
_MAX_NAMES = 5
#: A value read from ``USER.md``, at most this long.
_MAX_VALUE = 80

#: Where a configured value comes from, after the noun it describes.
_FROM = {
    SOURCE_FILE: "from a configuration file on this machine",
    SOURCE_ENVIRONMENT: "from the environment",
    SOURCE_CONFIG_REPO: "from the configuration repository",
    SOURCE_GATEWAY: "provided by the platform",
}
_FROM_UNKNOWN = "of unknown origin"

_LOGIN_RE = re.compile(r"[^A-Za-z0-9._\-\[\]]")


@dataclass(frozen=True)
class InstanceFacts:
    """The facts for the onboarding conversation's prompt."""

    #: The ``## This instance`` section: the heading and one line per item.
    section: str
    #: One line for the explorer's brief: how it can reach GitHub.
    github_access: str


@dataclass(frozen=True)
class _GitHubCLI:
    """The GitHub CLI check: ``checked`` with the probe's answer, or why not.

    ``state`` is ``checked``, ``not_checked`` (the caller asked for no probe),
    ``no_time`` (the budget was spent), ``no_answer`` (``gh auth status``
    timed out or could not run) or ``failed`` (the check itself raised).
    """

    state: str
    installed: bool = False
    authenticated: bool = False
    login: str | None = None


def _github_cli(timeout: float) -> dict[str, Any]:
    """The setup page's GitHub CLI probe, bounded by ``timeout``.

    The one place this module runs a subprocess; the test suite replaces it.
    """
    return setup_checks._gh_status(timeout)


def _check_github_cli(probe: bool, deadline: float) -> _GitHubCLI:
    """The probe's answer within what is left of the budget (:func:`gather`
    turns anything this raises into the ``failed`` state)."""
    if not probe:
        return _GitHubCLI("not_checked")
    timeout = min(setup_checks.HTTP_TEST_TIMEOUT, deadline - time.monotonic())
    if timeout < _MIN_PROBE_SECONDS:
        return _GitHubCLI("no_time")
    found = _github_cli(timeout)
    installed = bool(found.get("cli"))
    authenticated = bool(found.get("authenticated"))
    if installed and not authenticated and found.get("hint") == setup_checks.GH_NO_ANSWER:
        return _GitHubCLI("no_answer", installed=True)
    login = _LOGIN_RE.sub("", str(found.get("login") or ""))[:64] or None
    return _GitHubCLI("checked", installed=installed, authenticated=authenticated, login=login)


def _slack_gateway_status() -> dict[str, Any] | None:
    """The platform gateway's streams, as the setup page's Channels step reads
    them for ``slack.mode: hosted``: the hosted channel runtime's
    ``streams.status()``, its quiet default while the runtime has not started,
    or ``None`` outside the gateway process, where there is nothing to ask.
    Read from the builder's thread: a stream that opens or closes at that
    instant can make the read fail, and the status then reads unknown."""
    server = sys.modules.get("nerve.gateway.server")
    if server is None:
        return None
    runtime = getattr(server, "_hosted_channels", None)
    if runtime is None:
        return {"streams": 0, "connected": False, "last_event_at": None}
    return runtime.streams.status()


def _guard(label: str, read: Callable[[], Any], default: Any) -> Any:
    try:
        return read()
    except Exception:  # noqa: BLE001 - one unknown line, never a failed start
        logger.debug("Onboarding facts: %s could not be read", label, exc_info=True)
        return default


def _origin(sources: dict[str, str | None] | None, path: str) -> str:
    return _FROM.get((sources or {}).get(path) or "", _FROM_UNKNOWN)


def _origins(sources: dict[str, str | None] | None, paths: tuple[str, ...]) -> str:
    phrases: list[str] = []
    for path in paths:
        phrase = _origin(sources, path)
        if phrase not in phrases:
            phrases.append(phrase)
    return " and ".join(phrases)


def _names(items: list[Any]) -> str:
    shown = [f"`{str(item).replace('`', '')}`" for item in items[:_MAX_NAMES]]
    more = len(items) - len(shown)
    return ", ".join(shown) + (f" and {more} more" if more > 0 else "")


# --------------------------------------------------------------------------- #
#  One function per line                                                       #
# --------------------------------------------------------------------------- #


def _mode_text(mode: str | None) -> str:
    return {"worker": "worker (an agent for a team)", "personal": "personal assistant"}.get(
        mode or "", UNKNOWN,
    )


def _workspace_text(ws: Path) -> str:
    return f"`{ws}`; clone repositories into `{ws / 'repos'}/<name>`"


def _configuration_text(config: Any) -> str:
    if config.lockdown:
        text = "arrives by pull request (lockdown); change it with `propose_config_change`"
    else:
        text = "files on this machine, edited in place"
    if not config.setup.local_secrets:
        text += "; this instance stores no credentials, its operator provides access"
    return text


def _model_text(config: Any, sources: dict[str, str | None] | None) -> str:
    model = f"`{config.agent.model}`" if config.agent.model else "the backend's default model"
    if config.provider.is_bedrock:
        how = f"AWS Bedrock ({config.provider.aws_region or 'the default region'})"
    elif config.proxy.enabled:
        how = "a Claude subscription, through the local proxy"
    elif config.anthropic_api_key:
        how = f"the Anthropic API (key {_origin(sources, 'anthropic_api_key')})"
    elif config.claude_oauth_token:
        how = f"a Claude subscription token ({_origin(sources, 'claude_oauth_token')})"
    elif os.environ.get("ANTHROPIC_API_KEY"):
        how = f"the Anthropic API (key {_FROM[SOURCE_ENVIRONMENT]})"
    elif os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
        how = f"a Claude subscription token ({_FROM[SOURCE_ENVIRONMENT]})"
    else:
        return f"{model}; no model credential found"
    return f"{model} through {how}"


def _memory_text(config: Any, sources: dict[str, str | None] | None) -> str:
    if config.memory.backend == "loom":
        text = "Loom"
    elif config.openai_api_key:
        text = f"memU; embeddings on (OpenAI key {_origin(sources, 'openai_api_key')})"
    else:
        text = "memU; embeddings off, so recall is ranked by the model"
    if config.xmemory.enabled:
        text += "; xmemory alongside"
    return text


def _github_token_text(sources: dict[str, str | None] | None) -> str:
    if sources is None:
        return UNKNOWN
    source = sources.get("github_token")
    if source is None:
        return "none in Nerve's configuration or environment"
    return _FROM.get(source, UNKNOWN)


def _github_cli_text(cli: _GitHubCLI) -> str:
    if cli.state == "checked":
        if not cli.installed:
            return "not installed"
        if not cli.authenticated:
            return "installed, not authenticated"
        return f"authenticated as `{cli.login}`" if cli.login else "authenticated; the login is unknown"
    return {
        "not_checked": "unknown (not checked)",
        "no_time": "unknown (no time was left to check it)",
        "no_answer": "unknown (`gh auth status` did not answer in time)",
    }.get(cli.state, "unknown (the check failed)")


def _github_access(cli: _GitHubCLI) -> str:
    """The explorer brief's line: how the explorer reaches GitHub."""
    if cli.state == "checked":
        if cli.installed and cli.authenticated:
            if cli.login:
                return f"GitHub access: `gh` as `{cli.login}`."
            return "GitHub access: `gh` is authenticated; the login is unknown."
        return "GitHub access: none known — clone public URLs only; report what needs a credential."
    return (
        "GitHub access: unknown — run `gh auth status` once; if it fails, clone public URLs "
        "only and report what needs a credential."
    )


#: What each ``sync.github.inbox`` lists.
_INBOXES = {
    "participating": "notifications of the threads the account takes part in (inbox `participating`)",
    "all": "the account's whole notification inbox (inbox `all`)",
}
#: How the repository watch names each of its types.
_WATCHED_TYPES = {"pull_request": "pull requests", "issue": "issues", "release": "releases"}


def _github_sync_text(config: Any) -> str:
    sync = config.sync
    parts: list[str] = []
    if sync.github.enabled:
        inbox = str(sync.github.inbox)
        repos = list(sync.github.allow_repos or [])
        parts.append(
            _INBOXES.get(inbox, f"notifications (inbox `{inbox}`)")
            + (f", only from {_names(repos)}" if repos else ", with no repository filter")
        )
    if sync.github_events.enabled:
        repos = list(sync.github_events.repos or [])
        parts.append("the account's own activity" + (f" in {_names(repos)}" if repos else ""))
    return "on: " + "; ".join(parts) if parts else "off"


def _github_repos_text(config: Any) -> str:
    """The repository watch: who polls, what it reports, and where."""
    watch = config.sync.github_repos
    if not watch.enabled:
        return "off"
    repos = list(watch.repos or [])
    if not repos:
        return "on, but no repositories are listed"
    kinds = [_WATCHED_TYPES[kind] for kind in watch.types or () if kind in _WATCHED_TYPES]
    what = " and ".join([", ".join(kinds[:-1]), kinds[-1]] if len(kinds) > 1 else kinds)
    how = "the channel gateway polls" if config.sync.github.mode == "hosted" else "polled here with `gh`"
    text = f"on ({how}): new {what} in {_names(repos)}"
    if watch.updates:
        text += "; also new commits, ready for review, merges and closes of the pull requests it reported"
    return text


def _linear_text(config: Any, sources: dict[str, str | None] | None) -> str:
    """The Linear source: what it reports, and whether the agent can write back."""
    linear = config.sync.linear
    if not linear.enabled:
        return "off"
    if not linear.api_key:
        return "on, but its API key is not set"
    text = f"on (API key {_origin(sources, 'sync.linear.api_key')}): {', '.join(linear.types)}"
    if linear.mcp_server in config.mcp_servers:
        text += "; writes through the `linear` MCP server (`mcp__linear__*`) as the key's user"
    return text


def _slack_text(config: Any, sources: dict[str, str | None] | None) -> str:
    slack = config.slack
    if slack.mode == "hosted":
        text = "provided by the platform (hosted mode: the shared channel gateway holds the tokens)"
        if not slack.enabled:
            return text + ", switched off"
        status = _guard("the Slack gateway's status", _slack_gateway_status, None)
        if status is None:
            return text + "; the gateway's status is unknown"
        return text + ("; the gateway is connected" if status.get("connected") else "; waiting for the gateway")
    if slack.bot_token and slack.app_token:
        text = f"configured, socket mode (tokens {_origins(sources, ('slack.bot_token', 'slack.app_token'))})"
        return text if slack.enabled else text + ", switched off"
    if slack.bot_token or slack.app_token:
        return "not configured (only one of its two tokens is set)"
    return "not configured"


def _telegram_channel_text(config: Any, sources: dict[str, str | None] | None) -> str:
    telegram = config.telegram
    if not telegram.bot_token:
        return "not configured"
    text = f"configured (bot token {_origin(sources, 'telegram.bot_token')})"
    return text if telegram.enabled else text + ", switched off"


def _telegram_sync_text(config: Any, sources: dict[str, str | None] | None) -> str:
    sync = config.sync.telegram
    if not sync.enabled:
        return "off"
    if not (sync.api_id and sync.api_hash):
        return "on, but its API credentials are not set"
    return f"on (API credentials {_origins(sources, ('sync.telegram.api_id', 'sync.telegram.api_hash'))})"


def _gmail_text(config: Any) -> str:
    gmail = config.sync.gmail
    count = len(list(gmail.accounts or []))
    if not count:
        return "no accounts"
    accounts = "1 account" if count == 1 else f"{count} accounts"
    return f"{accounts}, sync {'on' if gmail.enabled else 'off'}"


def _user_md_value(user_md: list[str] | None, prefix: str) -> str | None:
    value = _line_value(user_md or [], prefix)
    return value[:_MAX_VALUE].replace("`", "") if value else None


def _time_zone_text(config: Any, mode: str | None, user_md: list[str] | None) -> str:
    if mode == "worker":
        return f"`{config.timezone}` (settings; a worker has no `USER.md`)"
    value = _user_md_value(user_md, "- **Timezone:**")
    return f"`{value}` (`USER.md`)" if value else f"`{config.timezone}` (settings)"


def _quiet_hours_text(config: Any, mode: str | None, user_md: list[str] | None) -> str:
    if mode != "worker":
        value = _user_md_value(user_md, "- **Quiet hours:**")
        if value:
            return f"{value} (`USER.md`)"
    start, end = str(config.quiet_start or ""), str(config.quiet_end or "")
    return (f"{start}–{end}" if start and end else "none") + " (settings)"


# --------------------------------------------------------------------------- #
#  The section                                                                 #
# --------------------------------------------------------------------------- #


def gather(
    config: Any, workspace: Path, *, probe: bool = True, budget: float = BUDGET_SECONDS,
) -> InstanceFacts:
    """The facts about this instance, for the onboarding conversation's prompt.

    ``probe`` runs ``gh auth status`` once, within what is left of ``budget``
    seconds; without it the GitHub CLI line reads unknown. Never raises.
    """
    deadline = time.monotonic() + budget
    ws = Path(workspace)
    mode = _guard("the mode", lambda: workspace_mode(ws), None)
    # ``{}`` means the layers could not be read: every source is unknown.
    sources = _guard("the credential sources", lambda: credential_sources(config) or None, None)
    user_md = _guard("USER.md", lambda: _read_lines(ws / "USER.md"), None) if mode != "worker" else None
    cli = _guard("the GitHub CLI", lambda: _check_github_cli(probe, deadline), _GitHubCLI("failed"))
    rows: tuple[tuple[str, Callable[[], str]], ...] = (
        ("Mode", lambda: _mode_text(mode)),
        ("Workspace", lambda: _workspace_text(ws)),
        ("Configuration", lambda: _configuration_text(config)),
        ("Model", lambda: _model_text(config, sources)),
        ("Memory", lambda: _memory_text(config, sources)),
        ("GitHub token", lambda: _github_token_text(sources)),
        ("GitHub CLI (`gh`)", lambda: _github_cli_text(cli)),
        ("GitHub sync source", lambda: _github_sync_text(config)),
        ("GitHub repository watch", lambda: _github_repos_text(config)),
        ("Linear", lambda: _linear_text(config, sources)),
        ("Slack", lambda: _slack_text(config, sources)),
        ("Telegram channel", lambda: _telegram_channel_text(config, sources)),
        ("Telegram message sync", lambda: _telegram_sync_text(config, sources)),
        ("Gmail", lambda: _gmail_text(config)),
        ("Time zone", lambda: _time_zone_text(config, mode, user_md)),
        ("Quiet hours", lambda: _quiet_hours_text(config, mode, user_md)),
    )
    lines = [f"- {label}: {_guard(label, build, UNKNOWN)}." for label, build in rows]
    return InstanceFacts(
        section=HEADING + "\n\n" + "\n".join(lines),
        github_access=_github_access(cli),
    )


def environment_facts(config: Any, workspace: Path, *, probe: bool = True) -> str:
    """The ``## This instance`` section alone (:func:`gather`)."""
    return gather(config, workspace, probe=probe).section


__all__ = ["BUDGET_SECONDS", "HEADING", "InstanceFacts", "environment_facts", "gather"]
