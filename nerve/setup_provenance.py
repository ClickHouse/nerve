"""Where a configured connection value came from.

The setup checklist reports every credential-bearing value masked. This module
adds *where it came from*, so a value an operator provided reads as configured
and managed — not as something the page should ask for or let a person
replace:

* ``file`` — ``config.local.yaml`` or ``config.yaml`` on this machine, the
  normal case: the setup page may replace or remove it.
* ``environment`` — the effective value came from an environment variable:
  one Nerve reads directly (``ANTHROPIC_API_KEY``, ``CLAUDE_CODE_OAUTH_TOKEN``;
  ``GH_TOKEN``/``GITHUB_TOKEN`` for the GitHub CLI), or a ``${VAR}`` /
  ``${VAR:-default}`` reference in any configuration layer.
* ``config-repo`` — the value sits in the tracked ``settings.yaml`` under
  lockdown, where that file is the only configuration read.
* ``gateway`` — held by the shared channel gateway (``slack.mode: hosted``);
  there is no token on this machine at all.

Detection reads the **raw** layers, before interpolation: the loaded
configuration cannot tell a literal from a resolved reference. The layer that
wins is the one the loader would take the value from — ``config.local.yaml``
over ``config.yaml`` over ``settings.yaml``, or ``settings.yaml`` alone under
lockdown — and only its spelling decides. Values are never returned, only
their origin.

A switch the page writes to the workspace's ``settings.yaml`` has a second
question: whether a machine layer (``config.yaml`` / ``config.local.yaml``)
states it too. Those files outrank ``settings.yaml``, so the page's write
would change nothing the loader reads (:func:`machine_switches`).
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nerve.config import NerveConfig, _read_yaml_mapping, workspace_settings_file

logger = logging.getLogger(__name__)

SOURCE_FILE = "file"
SOURCE_ENVIRONMENT = "environment"
SOURCE_CONFIG_REPO = "config-repo"
SOURCE_GATEWAY = "gateway"
SOURCES = (SOURCE_FILE, SOURCE_ENVIRONMENT, SOURCE_CONFIG_REPO, SOURCE_GATEWAY)

#: A whole value that is one environment reference: ``${VAR}`` or ``${VAR:-default}``.
ENV_REF_RE = re.compile(r"^\$\{[A-Z0-9_]+(:-.*)?\}$")

#: Dotted config paths the checklist reports, and the environment variables
#: Nerve (or a tool it drives) reads directly for them.
DIRECT_ENV: dict[str, tuple[str, ...]] = {
    "anthropic_api_key": ("ANTHROPIC_API_KEY",),
    "claude_oauth_token": ("CLAUDE_CODE_OAUTH_TOKEN",),
    "github_token": ("GH_TOKEN", "GITHUB_TOKEN"),
}

#: Every credential-bearing value the checklist reports, by dotted path.
CREDENTIAL_PATHS: tuple[str, ...] = (
    "anthropic_api_key",
    "claude_oauth_token",
    "openai_api_key",
    "telegram.bot_token",
    "slack.bot_token",
    "slack.app_token",
    "sync.telegram.api_id",
    "sync.telegram.api_hash",
    "github_token",
    "sync.gmail.accounts",
    "sync.linear.api_key",
)

#: Held by the channel gateway when Slack runs in hosted mode.
GATEWAY_PATHS = frozenset({"slack.bot_token", "slack.app_token"})

#: Switches the setup page writes to the workspace's ``settings.yaml`` that an
#: operator may state in a machine layer instead, by dotted path.
MACHINE_SWITCH_PATHS: tuple[str, ...] = ("sync.github.enabled",)

#: The machine layers by their name in :meth:`RawLayers.winning`, as file names.
MACHINE_FILES: dict[str, str] = {"local": "config.local.yaml", "machine": "config.yaml"}


@dataclass(frozen=True)
class RawLayers:
    """The configuration files as written, before merging and interpolation."""

    settings: dict[str, Any]
    machine: dict[str, Any]
    local: dict[str, Any]
    locked: bool

    def winning(self) -> list[tuple[str, dict[str, Any]]]:
        """The layers in the order the loader lets them win, highest first."""
        if self.locked:
            return [("settings", self.settings)]
        return [("local", self.local), ("machine", self.machine), ("settings", self.settings)]


def _read(path: Path) -> dict[str, Any]:
    """A raw mapping, or ``{}`` for anything unreadable — provenance is a
    report, never a reason for the checklist to fail."""
    try:
        data = _read_yaml_mapping(path)
    except Exception as e:  # noqa: BLE001 — ConfigError, OSError, a parser error
        logger.debug("provenance: %s not readable (%s)", path, e)
        return {}
    return data if isinstance(data, dict) else {}


def read_layers(config: NerveConfig) -> RawLayers:
    """The three layers for ``config``, read now (two or three small files)."""
    config_dir = Path(getattr(config, "config_dir", "") or "")
    machine: dict[str, Any] = {}
    local: dict[str, Any] = {}
    if str(config_dir) and config_dir.is_dir():
        machine = _read(config_dir / "config.yaml")
        local = _read(config_dir / "config.local.yaml")
    settings = _read(workspace_settings_file(Path(config.workspace)))
    return RawLayers(settings=settings, machine=machine, local=local, locked=bool(config.lockdown))


def _lookup(layer: dict[str, Any], path: str) -> tuple[bool, Any]:
    """``(present, raw_value)`` for a dotted path in one raw layer."""
    node: Any = layer
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return False, None
        node = node[part]
    return True, node


def _stated(raw: Any) -> bool:
    """Whether a raw value says anything (``None``, ``""``, ``0`` and ``[]`` do not)."""
    if raw is None or raw == "" or raw == 0 or raw == [] or raw == {}:
        return False
    return True


def _is_env_reference(raw: Any) -> bool:
    if isinstance(raw, str):
        return bool(ENV_REF_RE.match(raw.strip()))
    if isinstance(raw, list):
        return any(isinstance(item, str) and ENV_REF_RE.match(item.strip()) for item in raw)
    return False


def source_of(path: str, layers: RawLayers, *, hosted: bool = False) -> str | None:
    """Where the value at ``path`` comes from, or ``None`` when nothing states it.

    ``hosted`` is ``slack.mode == "hosted"``: the Slack tokens are then the
    gateway's, whatever any file says.
    """
    if hosted and path in GATEWAY_PATHS:
        return SOURCE_GATEWAY
    for name, layer in layers.winning():
        present, raw = _lookup(layer, path)
        if not present or not _stated(raw):
            continue
        if _is_env_reference(raw):
            return SOURCE_ENVIRONMENT
        if name == "settings" and layers.locked:
            return SOURCE_CONFIG_REPO
        return SOURCE_FILE
    for variable in DIRECT_ENV.get(path, ()):
        if os.environ.get(variable):
            return SOURCE_ENVIRONMENT
    return None


def credential_sources(config: NerveConfig) -> dict[str, str | None]:
    """``{dotted path: source | None}`` for every path in :data:`CREDENTIAL_PATHS`.

    Never raises: an unreadable file reads as empty, so at worst a value's
    source falls back to what the checklist assumes (``file``).
    """
    try:
        layers = read_layers(config)
    except Exception as e:  # noqa: BLE001
        logger.debug("provenance unavailable: %s", e)
        return {}
    hosted = getattr(getattr(config, "slack", None), "mode", "") == "hosted"
    return {path: source_of(path, layers, hosted=hosted) for path in CREDENTIAL_PATHS}


def machine_layer_of(path: str, layers: RawLayers) -> str | None:
    """The machine file the loader takes ``path`` from — ``config.local.yaml``
    or ``config.yaml`` — or ``None`` when neither states it, and always on a
    locked instance, where the loader reads ``settings.yaml`` alone.

    Any value states a switch, ``false`` and a bare ``key:`` included: the
    loader takes the leaf from the highest layer that has the key, whatever
    it holds (a bare key reads as off), so the ``settings.yaml`` the page
    writes is shadowed all the same.
    """
    for name, layer in layers.winning():
        if name not in MACHINE_FILES:
            continue
        present, _raw = _lookup(layer, path)
        if present:
            return MACHINE_FILES[name]
    return None


def machine_switches(config: NerveConfig) -> dict[str, str]:
    """``{dotted path: machine file}`` for every switch in
    :data:`MACHINE_SWITCH_PATHS` that a machine layer states.

    Never raises: an unreadable file states nothing, so at worst a switch
    reads as the page's own, as it did before anything looked.
    """
    try:
        layers = read_layers(config)
    except Exception as e:  # noqa: BLE001
        logger.debug("provenance unavailable: %s", e)
        return {}
    found: dict[str, str] = {}
    for path in MACHINE_SWITCH_PATHS:
        layer = machine_layer_of(path, layers)
        if layer is not None:
            found[path] = layer
    return found


def describe(source: str | None) -> str:
    """A short operator-facing label: ``environment`` → ``(environment)``."""
    if not source or source == SOURCE_FILE:
        return ""
    return f" ({source})"


__all__ = [
    "CREDENTIAL_PATHS",
    "DIRECT_ENV",
    "ENV_REF_RE",
    "GATEWAY_PATHS",
    "MACHINE_FILES",
    "MACHINE_SWITCH_PATHS",
    "SOURCE_CONFIG_REPO",
    "SOURCE_ENVIRONMENT",
    "SOURCE_FILE",
    "SOURCE_GATEWAY",
    "SOURCES",
    "RawLayers",
    "credential_sources",
    "describe",
    "machine_layer_of",
    "machine_switches",
    "read_layers",
    "source_of",
]
