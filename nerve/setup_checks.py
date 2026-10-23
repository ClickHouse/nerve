"""Live checks the setup page offers: provider, OpenAI, Telegram, Slack, host
login detection, source preflights, external-agent detection, environment.

Every one of them shells out or talks to the network, so every one of them
runs in a worker thread with a timeout, behind one small semaphore — the
wizard must never block the gateway, and a person clicking "Test" five times
must not be able to open five connections to the Anthropic API at once.

Rules every subprocess call here follows: an argv *list*, ``timeout=``,
``capture_output=True``, never ``shell=True``, and never a value a person
typed as an argv element.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import time
import weakref
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from nerve import paths
from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

PROVIDER_TEST_TIMEOUT = 20.0
# Retries for the provider probe (the SDK default). They must end well inside
# PROVIDER_TEST_TIMEOUT: past it, every failure shows as "No answer in 20 s."
# and the real cause (a 429, an unreachable proxy) is lost. Without this, a
# proxy candidate gets PROXY_CLIENT_MAX_RETRIES, whose backoff alone is about
# 20 s.
PROVIDER_TEST_MAX_RETRIES = 2
HTTP_TEST_TIMEOUT = 10.0
HOST_LOGIN_TIMEOUT = 6.0
CLI_CHECK_TIMEOUT = 5.0
CACHE_SECONDS = 60.0
_CONCURRENT = 3


class Busy(Exception):
    """More checks are running than the semaphore allows."""


# One semaphore per event loop, for the same reason the setup routes keep
# one lock per loop: an asyncio primitive binds to the loop that first waits
# on it, and the test process runs many loops in one interpreter.
_semaphores: "weakref.WeakKeyDictionary[Any, asyncio.Semaphore]" = weakref.WeakKeyDictionary()


def _semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    sem = _semaphores.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(_CONCURRENT)
        _semaphores[loop] = sem
    return sem


async def run_blocking(fn: Callable[[], Any], timeout: float) -> Any:
    """Run ``fn`` in a thread under the shared semaphore, bounded by ``timeout``.

    Raises :class:`Busy` immediately when the semaphore is exhausted (the
    route answers 429) and :class:`asyncio.TimeoutError` past the timeout.
    """
    sem = _semaphore()
    if sem.locked():
        raise Busy()
    async with sem:
        return await asyncio.wait_for(asyncio.to_thread(fn), timeout)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _scrub(text: str, *secrets: str | None) -> str:
    """Replace every non-empty secret in ``text`` with ``<token>``.

    Applied at every boundary where live-service text (an exception message,
    an API description) leaves this module: a request URL that embeds the
    credential (``/bot<token>/getMe``), an SDK error echoing a header, a
    future client library quoting its input — none of it may reach a response
    body or a log line with the value intact.

    The ``repr`` forms are scrubbed too (``'…'``, ``b'…'``): h11 quotes an
    illegal header value as ``b'<bytes>'``, which the plain match misses.
    **Always scrub before truncating** — a cut taken first can leave a prefix
    of the value that no longer matches anything.
    """
    for secret in secrets:
        if not secret:
            continue
        for form in (secret, repr(secret), repr(secret.encode("utf-8", "surrogatepass"))):
            text = text.replace(form, "<token>")
    return text


#: The one scrubber for every sink — response bodies, log lines, the state
#: file. ``server.py`` scrubs the Telegram start failure with it too, so the
#: ``repr``/bytes forms are covered everywhere, not only in this module.
scrub_secrets = _scrub


def _scrubbed_exception_text(exc: BaseException, *secrets: str | None) -> str:
    """``str(exc)`` with every secret removed, before anything shortens it."""
    try:
        text = str(exc)
    except Exception:  # noqa: BLE001 — a broken __str__ must not leak anything
        text = type(exc).__name__
    return _scrub(text, *secrets)


# --------------------------------------------------------------------------- #
#  Provider                                                                    #
# --------------------------------------------------------------------------- #


def _classify_provider_error(
    exc: Exception, candidate: NerveConfig, text: str | None = None,
) -> tuple[str, str, str | None]:
    """``(code, detail, hint)`` for a failed connectivity probe.

    ``text`` is the exception's message **already scrubbed** of the
    candidate's secrets; the two truncations below (``[:120]``, ``[:200]``)
    therefore operate on text that cannot carry a prefix of a key. When it is
    not given, the message is scrubbed here from the candidate's own fields.
    """
    if text is None:
        text = _scrubbed_exception_text(
            exc, candidate.anthropic_api_key, candidate.claude_oauth_token,
            getattr(candidate.proxy, "api_key", None),
        )
    low = text.lower()
    name = type(exc).__name__
    status = getattr(exc, "status_code", None)
    if status == 401 or "authentication_error" in low or "invalid x-api-key" in low:
        return (
            "invalid_key",
            f"Anthropic rejected this key (401 authentication_error).",
            "Keys start with sk-ant-. Check for a missing last character or a revoked key.",
        )
    if status == 403 or "permission_error" in low:
        return (
            "model_access",
            "The credentials were accepted but this account may not use the model.",
            "Check the model access for this account or pick another model.",
        )
    if candidate.provider.is_bedrock and (
        status in (400, 404) or "model identifier" in low or "accessdenied" in low
        or "not authorized" in low or "could not resolve the model" in low
    ):
        region = candidate.provider.aws_region or "the default region"
        return (
            "model_access",
            f"AWS accepted the credentials but Claude is not enabled in {region}.",
            "Enable Anthropic model access in the Bedrock console for this region, or pick another region.",
        )
    if candidate.provider.is_bedrock and (
        "credential" in low or "token" in low or "profile" in low or "expired" in low
    ):
        return (
            "invalid_key",
            "AWS did not accept the credentials for Bedrock.",
            "Check the AWS profile or the IAM role on the server.",
        )
    if "timed out" in low or "timeout" in low or name in ("APITimeoutError", "ReadTimeout", "ConnectTimeout"):
        return ("timeout", f"No answer in {int(PROVIDER_TEST_TIMEOUT)} s.", "Try again; if it persists the server may be offline.")
    if name in ("APIConnectionError", "ConnectError") or "connect" in low or "name or service not known" in low or "nodename nor servname" in low:
        host = "api.anthropic.com" if not candidate.provider.is_bedrock else "the Bedrock endpoint"
        reason = text.split("\n")[0][:120] if text else "connection refused"
        return (
            "network",
            f"The server could not reach {host} ({reason}).",
            "This runs on the server, not in your browser. Check its internet access or proxy settings.",
        )
    short = text if len(text) <= 200 else text[:200] + "…"
    return ("unknown", f"{name}: {short}", None)


def _probe_provider_raw(candidate: NerveConfig) -> dict[str, Any]:
    """One ``max_tokens=1`` call; a failure is classified from its exception.

    The only provider probe: a variant that collapsed the exception through
    ``cli._check_api_connectivity`` returned its text unscrubbed and was
    removed so no future caller can reach for it.
    """
    model = candidate.agent.title_model
    started = time.monotonic()
    try:
        client = candidate.create_anthropic_client(
            timeout=PROVIDER_TEST_TIMEOUT - 2, max_retries=PROVIDER_TEST_MAX_RETRIES,
        )
        client.messages.create(
            model=model, max_tokens=1, messages=[{"role": "user", "content": "ping"}],
        )
    except Exception as exc:  # noqa: BLE001 — every failure becomes a result
        latency = int((time.monotonic() - started) * 1000)
        # Scrubbed *before* classification: the classifier truncates, and a
        # truncated prefix of a key would no longer match the scrub.
        text = _scrubbed_exception_text(
            exc, candidate.anthropic_api_key, candidate.claude_oauth_token,
            getattr(candidate.proxy, "api_key", None),
        )
        code, detail, hint = _classify_provider_error(exc, candidate, text)
        return {
            "ok": False, "checked": "api", "code": code, "detail": detail, "hint": hint,
            "latency_ms": None if code in ("network", "timeout") else latency,
        }
    latency = int((time.monotonic() - started) * 1000)
    return {
        "ok": True, "checked": "api", "code": "ok", "detail": f"{model} answered",
        "hint": None, "latency_ms": latency,
    }


async def provider_test(candidate: NerveConfig) -> dict[str, Any]:
    """Test a candidate configuration without saving it.

    OAuth tokens cannot be tested against the API (``checked: skipped``);
    everything else makes one ``max_tokens=1`` call.
    """
    if not candidate.provider.is_bedrock and not candidate.effective_api_key:
        if candidate.claude_oauth_token or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
            return {
                "ok": True, "checked": "skipped", "code": "ok",
                "detail": "Subscription tokens are verified on first use; the Anthropic API cannot test them.",
                "hint": "Background jobs (memory, titles, cron) also need an API key; add one later in Memory.",
                "latency_ms": None,
            }
        return {
            "ok": False, "checked": "api", "code": "invalid_key",
            "detail": "No credential to test.", "hint": "Enter an API key, or pick another provider.",
            "latency_ms": None,
        }
    try:
        return await run_blocking(lambda: _probe_provider_raw(candidate), PROVIDER_TEST_TIMEOUT)
    except asyncio.TimeoutError:
        return {
            "ok": False, "checked": "api", "code": "timeout",
            "detail": f"No answer in {int(PROVIDER_TEST_TIMEOUT)} s.",
            "hint": "Try again; if it persists the server may be offline.", "latency_ms": None,
        }


# --------------------------------------------------------------------------- #
#  OpenAI / Telegram / Slack                                                   #
# --------------------------------------------------------------------------- #


def _openai_probe(key: str) -> dict[str, Any]:
    from nerve.bootstrap import _check_openai_key

    ok, detail = _check_openai_key(key, timeout=HTTP_TEST_TIMEOUT - 1)
    if ok:
        return {"ok": True, "detail": None, "hint": None}
    detail = _scrub(detail, key)
    low = detail.lower()
    if "401" in low or "incorrect api key" in low or "invalid" in low:
        return {"ok": False, "detail": "OpenAI rejected the key (401).", "hint": "Check the key in the OpenAI dashboard."}
    return {"ok": False, "detail": f"OpenAI could not be reached: {detail[:160]}", "hint": "This runs on the server; check its internet access."}


async def openai_test(key: str) -> dict[str, Any]:
    try:
        return await run_blocking(lambda: _openai_probe(key), HTTP_TEST_TIMEOUT)
    except asyncio.TimeoutError:
        return {"ok": False, "detail": f"No answer from OpenAI in {int(HTTP_TEST_TIMEOUT)} s.", "hint": "Try again."}


def _telegram_probe(token: str) -> tuple[bool, str]:
    from nerve.bootstrap import _telegram_get_me

    ok, detail = _telegram_get_me(token, timeout=HTTP_TEST_TIMEOUT - 1)
    # The request URL embeds the token; an httpx error that quotes the URL
    # would otherwise carry it into the 400 body and the log.
    return ok, _scrub(str(detail), token)


async def telegram_get_me(token: str) -> tuple[bool, str]:
    """``(True, bot_username)`` or ``(False, description)``; never raises
    except :class:`Busy`. A timeout is a failure with a description. The
    description never contains the token."""
    try:
        return await run_blocking(lambda: _telegram_probe(token), HTTP_TEST_TIMEOUT)
    except asyncio.TimeoutError:
        return False, "timeout"


def _slack_probe(bot_token: str, app_token: str) -> dict[str, Any]:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    def clean(text: Any) -> str:
        return _scrub(str(text), bot_token, app_token)

    try:
        auth = WebClient(token=bot_token, timeout=int(HTTP_TEST_TIMEOUT - 1)).auth_test()
    except SlackApiError as e:
        return {"ok": False, "field": "bot_token", "detail": f"Slack rejected the bot token ({clean(e.response.get('error', 'error'))}).", "team": None, "bot_user": None}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "field": "bot_token", "detail": f"Slack could not be reached: {clean(e)[:120]}", "team": None, "bot_user": None}
    try:
        WebClient(token=app_token, timeout=int(HTTP_TEST_TIMEOUT - 1)).apps_connections_open()
    except SlackApiError as e:
        return {"ok": False, "field": "app_token", "detail": f"Slack rejected the app token ({clean(e.response.get('error', 'error'))}).", "team": auth.get("team"), "bot_user": auth.get("user")}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "field": "app_token", "detail": f"Slack could not be reached: {clean(e)[:120]}", "team": None, "bot_user": None}
    return {"ok": True, "field": None, "detail": None, "team": auth.get("team"), "bot_user": auth.get("user")}


async def slack_test(bot_token: str, app_token: str) -> dict[str, Any]:
    try:
        return await run_blocking(lambda: _slack_probe(bot_token, app_token), HTTP_TEST_TIMEOUT * 2)
    except asyncio.TimeoutError:
        return {"ok": False, "field": "bot_token", "detail": f"No answer from Slack in {int(HTTP_TEST_TIMEOUT * 2)} s.", "team": None, "bot_user": None}


# --------------------------------------------------------------------------- #
#  Host login (the Claude Code credential on the gateway host)                 #
# --------------------------------------------------------------------------- #

_API_KEY_SOURCES = ("ANTHROPIC_API_KEY env var", "macOS Keychain (API key)")


LOOKUP_FOUND = "found"
LOOKUP_NONE = "none"              # every source was readable and held no login
LOOKUP_UNREADABLE = "unreadable"  # a source (the Keychain) could not be consulted

_TERMINAL_FALLBACK = (
    "On the server, nerve init --terminal runs the same detection interactively "
    "and can use the login; or paste an API key here."
)


@dataclass(frozen=True)
class HostLogin:
    available: bool
    kind: str | None          # "api_key" | "oauth"
    source: str | None        # masked source label; never the value
    value: str = ""           # the credential itself; never leaves the server
    reason: str | None = None  # why nothing was found (short, no values)
    #: ``found`` | ``none`` | ``unreadable`` — "no login" and "the Keychain
    #: could not be read from the service" are different answers; the card
    #: copy and the fallback it offers depend on which one it is.
    lookup: str = LOOKUP_NONE

    def public(self) -> dict[str, Any]:
        return {
            "available": self.available, "kind": self.kind, "source": self.source,
            "reason": self.reason, "lookup": LOOKUP_FOUND if self.available else self.lookup,
        }


# ``security`` exit codes worth naming: 36 is errSecInteractionNotAllowed —
# the Keychain wanted to ask on the desktop and this process has none.
_KEYCHAIN_NO_INTERACTION = 36
_KEYCHAIN_NOT_FOUND = 44


def _keychain(service: str) -> tuple[str, int]:
    """``(secret, returncode)`` from the macOS Keychain.

    The exit code is logged at INFO (never the value): the lookup runs at
    most once per process, and the code is what explains an empty answer
    from a daemon without a GUI session.
    """
    result = subprocess.run(
        ["security", "find-generic-password", "-s", service, "-w"],
        capture_output=True, text=True, timeout=HOST_LOGIN_TIMEOUT - 1,
    )
    logger.info("host-login: security find-generic-password -s %r → rc=%s", service, result.returncode)
    if result.returncode == 0:
        return result.stdout.strip(), 0
    return "", result.returncode


def detect_host_login() -> HostLogin:
    """Find a Claude Code credential on this host, non-prompting sources first.

    Order: ``ANTHROPIC_API_KEY``, ``CLAUDE_CODE_OAUTH_TOKEN``,
    ``~/.claude/.credentials.json`` (``claudeAiOauth.accessToken``), then
    (macOS only) the two Keychain entries — which can raise an "allow access"
    dialog on the host's desktop, which is why this runs at most once per
    process (see :func:`host_login`). Always unavailable inside a container.
    Never raises. When nothing is found, ``reason`` says why in a sentence
    the UI can show under "Look again"; it never carries a value.
    """
    if os.environ.get("NERVE_DOCKER") == "1":
        return HostLogin(False, None, None, reason="Not looked up inside a container.")
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return HostLogin(True, "api_key", "ANTHROPIC_API_KEY env var", key, lookup=LOOKUP_FOUND)
    token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
    if token:
        return HostLogin(True, "oauth", "CLAUDE_CODE_OAUTH_TOKEN env var", token, lookup=LOOKUP_FOUND)
    reasons: list[str] = []
    unreadable = False
    creds = Path("~/.claude/.credentials.json").expanduser()
    if creds.exists():
        try:
            data = json.loads(creds.read_text(encoding="utf-8"))
            token = (data.get("claudeAiOauth") or {}).get("accessToken", "")
            if token:
                return HostLogin(True, "oauth", "~/.claude/.credentials.json", token, lookup=LOOKUP_FOUND)
            reasons.append("~/.claude/.credentials.json holds no Claude login")
        except (OSError, ValueError):
            reasons.append("~/.claude/.credentials.json could not be read")
    else:
        reasons.append("no ~/.claude/.credentials.json")
    if sys.platform == "darwin" and shutil.which("security"):
        try:
            raw, rc = _keychain("Claude Code-credentials")
            if raw:
                try:
                    token = (json.loads(raw).get("claudeAiOauth") or {}).get("accessToken", "")
                except ValueError:
                    token = raw
                if token:
                    return HostLogin(True, "oauth", "macOS Keychain (Claude Code)", token, lookup=LOOKUP_FOUND)
            raw2, rc2 = _keychain("Claude Code")
            if raw2:
                return HostLogin(True, "api_key", "macOS Keychain (API key)", raw2, lookup=LOOKUP_FOUND)
            if rc == _KEYCHAIN_NOT_FOUND and rc2 == _KEYCHAIN_NOT_FOUND:
                # Both lookups ran and answered "no such item": a real "none".
                reasons.append("no Claude Code entry in the Keychain")
            else:
                # 36 (interaction not allowed) is the daemon-without-a-desktop
                # case; any other code also means the Keychain was not
                # actually consulted, so "no login" would be a guess.
                unreadable = True
                if _KEYCHAIN_NO_INTERACTION in (rc, rc2):
                    reasons.append(
                        "the Keychain is not readable from a background process "
                        "(it would need to ask on the server's screen)"
                    )
                else:
                    reasons.append(f"the Keychain could not be read from the service (security answered {rc}/{rc2})")
        except subprocess.TimeoutExpired:
            logger.info("host-login: security find-generic-password timed out")
            unreadable = True
            reasons.append("the Keychain lookup did not answer in time (it may be waiting for a prompt on the server's screen)")
        except OSError as exc:
            logger.info("host-login: security could not run: %s", type(exc).__name__)
            unreadable = True
            reasons.append("the Keychain lookup could not run")
    if not reasons:
        return HostLogin(False, None, None)
    text = "; ".join(reasons)
    sentence = text[0].upper() + text[1:] + "."
    if unreadable:
        sentence += " " + _TERMINAL_FALLBACK
    logger.info("host-login: %s (%s)", LOOKUP_UNREADABLE if unreadable else LOOKUP_NONE, text)
    return HostLogin(
        False, None, None, reason=sentence,
        lookup=LOOKUP_UNREADABLE if unreadable else LOOKUP_NONE,
    )


_host_login_cache: HostLogin | None = None
_host_login_refreshed = False


async def host_login(*, refresh: bool = False) -> HostLogin:
    """The cached detection; evaluated once per process, plus one ``refresh``."""
    global _host_login_cache, _host_login_refreshed
    if _host_login_cache is not None and not (refresh and not _host_login_refreshed):
        return _host_login_cache
    if refresh and _host_login_cache is not None:
        _host_login_refreshed = True
    try:
        found = await run_blocking(detect_host_login, HOST_LOGIN_TIMEOUT)
    except asyncio.TimeoutError:
        found = HostLogin(
            False, None, None, lookup=LOOKUP_UNREADABLE,
            reason="The lookup did not answer in time. " + _TERMINAL_FALLBACK,
        )
    _host_login_cache = found
    return found


def reset_caches() -> None:
    """Drop every per-process cache (tests)."""
    global _host_login_cache, _host_login_refreshed, _sources_cache, _environment_cache
    _host_login_cache = None
    _host_login_refreshed = False
    _sources_cache = None
    _environment_cache = None


# --------------------------------------------------------------------------- #
#  Sources preflight                                                           #
# --------------------------------------------------------------------------- #

# (taken at, GitHub hosted, result)
_sources_cache: tuple[float, bool, dict[str, Any]] | None = None

# The gateway polls GitHub for an agent only while a GitHub credential is
# connected to it, so the hint says that is what GitHub waits for here.
GH_HOSTED_HINT = (
    "The channel gateway polls GitHub for this agent once a GitHub credential "
    "is connected to it; gh is not needed on the server."
)


def gh_needed(config: NerveConfig) -> bool:
    """Whether something in *config* runs the gh CLI.

    Hosted GitHub (``sync.github.mode: hosted``) does not; the GitHub events
    and repos sources still do.
    """
    sync = config.sync
    if sync.github.mode != "hosted":
        return True
    return bool(sync.github_events.enabled or sync.github_repos.enabled)

#: The GitHub CLI probe's hint when ``gh auth status`` timed out or could not run.
GH_NO_ANSWER = "gh auth status did not answer."


def _run(argv: list[str], timeout: float) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def _gh_status(timeout: float = HTTP_TEST_TIMEOUT) -> dict[str, Any]:
    """``{cli, authenticated, login, hint}`` from ``gh auth status``.

    Also the onboarding's GitHub fact (:mod:`nerve.onboarding_environment`),
    which passes a shorter ``timeout`` to stay within its own budget.
    """
    if not shutil.which("gh"):
        return {"cli": False, "authenticated": False, "login": None,
                "hint": "Install gh and run gh auth login on the server to enable the GitHub source."}
    try:
        result = _run(["gh", "auth", "status"], timeout)
    except (subprocess.TimeoutExpired, OSError):
        return {"cli": True, "authenticated": False, "login": None, "hint": GH_NO_ANSWER}
    text = (result.stdout or "") + (result.stderr or "")
    login = None
    for line in text.splitlines():
        if "Logged in to" in line and " account " in line:
            login = line.split(" account ", 1)[1].split()[0].strip("()")
            break
    authenticated = result.returncode == 0 and "Logged in" in text
    return {
        "cli": True, "authenticated": authenticated, "login": login if authenticated else None,
        "hint": None if authenticated else "Run gh auth login on the server.",
    }


def _gh_hosted_status() -> dict[str, Any]:
    """The GitHub preflight in hosted mode: nothing to sign in, so gh is not run."""
    return {
        "cli": bool(shutil.which("gh")), "authenticated": False, "login": None,
        "hint": GH_HOSTED_HINT, "hosted": True,
    }


def _sources_probe(github_hosted: bool = False) -> dict[str, Any]:
    gog = bool(shutil.which("gog"))
    return {
        "github": _gh_hosted_status() if github_hosted else _gh_status(),
        "gmail": {
            "cli": gog,
            "hint": None if gog else "Install gog, then run gog gmail setup <account> on the server.",
        },
        "checked_at": _now(),
    }


async def sources_preflight(config: NerveConfig) -> dict[str, Any]:
    global _sources_cache
    now = time.monotonic()
    hosted = config.sync.github.mode == "hosted"
    cached = _sources_cache
    if cached is not None and cached[1] == hosted and now - cached[0] < CACHE_SECONDS:
        result = dict(cached[2])
    else:
        try:
            result = await run_blocking(lambda: _sources_probe(hosted), HTTP_TEST_TIMEOUT + 2)
        except asyncio.TimeoutError:
            result = {
                "github": _gh_hosted_status() if hosted else {
                    "cli": bool(shutil.which("gh")), "authenticated": False, "login": None,
                    "hint": "gh did not answer.",
                },
                "gmail": {"cli": bool(shutil.which("gog")), "hint": None},
                "checked_at": _now(),
            }
        _sources_cache = (now, hosted, dict(result))
    result["telegram"] = {
        "credentials_configured": bool(config.sync.telegram.api_id and config.sync.telegram.api_hash),
    }
    return result


# --------------------------------------------------------------------------- #
#  External agents                                                             #
# --------------------------------------------------------------------------- #


def _agents_probe(enabled: set[str]) -> dict[str, Any]:
    from nerve.external_agents.registry import AGENT_REGISTRY

    out = []
    for name, agent in AGENT_REGISTRY.items():
        version = agent.smoke_check()
        config_paths = list(agent.default_config_paths())
        out.append({
            "id": name,
            "label": agent.display_name,
            "installed": version is not None,
            "version": version,
            "config_paths": [str(p).replace(str(Path.home()), "~") for p in config_paths],
            "config_exists": any(p.exists() for p in config_paths),
            "enabled": name in enabled,
        })
    return {"agents": out}


async def agents_detect(config: NerveConfig) -> dict[str, Any]:
    enabled = {t.name for t in config.external_agents.targets if t.enabled}
    try:
        return await run_blocking(lambda: _agents_probe(enabled), CLI_CHECK_TIMEOUT * 3)
    except asyncio.TimeoutError:
        return {"agents": []}


# --------------------------------------------------------------------------- #
#  Environment                                                                 #
# --------------------------------------------------------------------------- #

_environment_cache: tuple[float, list[dict[str, Any]]] | None = None


def _version_of(argv: list[str]) -> str | None:
    if not shutil.which(argv[0]):
        return None
    try:
        result = _run(argv, CLI_CHECK_TIMEOUT)
    except (subprocess.TimeoutExpired, OSError):
        return None
    text = (result.stdout or result.stderr or "").strip().splitlines()
    return text[0].strip()[:80] if text else "installed"


def _node_ok(version: str | None) -> bool:
    if not version:
        return False
    digits = version.lstrip("v").split(".")
    try:
        major, minor = int(digits[0]), int(digits[1]) if len(digits) > 1 else 0
    except ValueError:
        return False
    return (major, minor) >= (22, 12)


def _environment_probe() -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    claude = _version_of(["claude", "--version"])
    checks.append({
        "id": "claude_cli", "label": "Claude Code CLI",
        "status": "ok" if claude else "missing", "detail": claude,
        "hint": None if claude else "Install the Claude Code CLI on the server; the agent runs through it.",
    })
    node = _version_of(["node", "--version"])
    checks.append({
        "id": "node", "label": "Node.js",
        "status": "ok" if _node_ok(node) else ("warn" if node else "missing"), "detail": node,
        "hint": None if _node_ok(node) else "Nerve builds its web UI with Node 22.12 or newer.",
    })
    gh = _version_of(["gh", "--version"])
    checks.append({
        "id": "gh", "label": "GitHub CLI", "status": "ok" if gh else "missing", "detail": gh,
        "hint": None if gh else "Install gh and run gh auth login to enable the GitHub source.",
    })
    gog = _version_of(["gog", "--version"])
    checks.append({
        "id": "gog", "label": "Google Workspace CLI (gog)", "status": "ok" if gog else "missing",
        "detail": gog, "hint": None if gog else "Needed for Gmail sync: gog gmail setup <account>.",
    })
    try:
        usage = shutil.disk_usage(paths.nerve_home())
        free_gib = usage.free / (1024 ** 3)
        checks.append({
            "id": "disk", "label": "Free disk space",
            "status": "ok" if free_gib >= 2 else "warn",
            "detail": f"{free_gib:.1f} GiB free at the state directory",
            "hint": None if free_gib >= 2 else "Less than 2 GiB free; the database and logs live here.",
        })
    except OSError:
        checks.append({"id": "disk", "label": "Free disk space", "status": "warn", "detail": None, "hint": "Could not read the state directory."})
    web_dist = Path(__file__).resolve().parent.parent / "web" / "dist"
    checks.append({
        "id": "web_ui", "label": "Web UI build",
        "status": "ok" if web_dist.exists() else "warn",
        "detail": "web/dist present" if web_dist.exists() else "web/dist missing",
        "hint": None if web_dist.exists() else "Run cd web && npm ci && npm run build (or nerve upgrade) on the server.",
    })
    checks.append({
        "id": "python", "label": "Python", "status": "ok", "detail": platform.python_version(), "hint": None,
    })
    return checks


async def environment(config: NerveConfig) -> list[dict[str, Any]]:
    global _environment_cache
    now = time.monotonic()
    if _environment_cache is not None and now - _environment_cache[0] < CACHE_SECONDS:
        checks = [dict(c) for c in _environment_cache[1]]
    else:
        try:
            checks = await run_blocking(_environment_probe, CLI_CHECK_TIMEOUT * 5)
        except asyncio.TimeoutError:
            checks = []
        _environment_cache = (now, [dict(c) for c in checks])
    if not gh_needed(config):
        for check in checks:
            if check["id"] == "gh" and check["status"] == "missing":
                check.update(status="ok", detail="not needed", hint=GH_HOSTED_HINT)
    tz_ok = True
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(config.timezone)
    except Exception:  # noqa: BLE001
        tz_ok = False
    checks.append({
        "id": "timezone", "label": "Time zone", "status": "ok" if tz_ok else "warn",
        "detail": config.timezone,
        "hint": None if tz_ok else "Not an IANA time zone name; set one in the About-you step.",
    })
    return checks


def candidate_config(base: NerveConfig, **overrides: Any) -> NerveConfig:
    """A copy of ``base`` with top-level and provider fields replaced."""
    provider_keys = {k: v for k, v in overrides.items() if k in ("type", "aws_region", "aws_profile")}
    top = {k: v for k, v in overrides.items() if k not in provider_keys}
    candidate = replace(base, **top)
    if provider_keys:
        candidate = replace(candidate, provider=replace(base.provider, **provider_keys))
    return candidate
