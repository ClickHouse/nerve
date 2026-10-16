"""The principal that Nerve names on each model request in external mode.

In external mode, a proxy in front of the model API can attribute model usage
to a person. Nerve gives it the principal that caused the work: one
``X-Nerve-Inference-Principal`` header with the actor ID, a canonical
lower-case UUID. The model requests of a session name the session's cause,
the person who started it (or the system actor for a session that Nerve
started for itself). Nerve's own model calls (session titles, prompt rewrite,
source condensing, memory, setup checks, the model catalogue) name the system
actor. Nerve does not prove the claim; the proxy decides what to accept.

Local mode sends no header. A NULL cause sends no header.

The Claude CLI reads ``ANTHROPIC_CUSTOM_HEADERS`` from its environment when it
builds the API client for a request: one ``Name: Value`` header per line,
lines split on ``\\n`` or ``\\r\\n``, name and value split on the first ``:``
and trimmed. The environment of a CLI process is fixed when it starts. That is
enough, because the session's cause never changes.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from uuid import UUID

logger = logging.getLogger(__name__)

INFERENCE_PRINCIPAL_HEADER = "X-Nerve-Inference-Principal"
CUSTOM_HEADERS_ENV = "ANTHROPIC_CUSTOM_HEADERS"

# The CLI splits the variable on these line ends (and on nothing else).
_LINE_END = re.compile(r"\r?\n")


def _is_external_mode() -> bool:
    # Imported at call time: nerve.gateway.auth imports nerve.config, which
    # imports this module.
    from nerve.gateway.auth import is_external_mode

    return is_external_mode()


def _canonical_uuid(value: str) -> bool:
    try:
        return str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def inference_principal(actor_id: str | None) -> str | None:
    """The principal to name on model requests for work that ``actor_id`` caused.

    ``None`` in local mode and when ``actor_id`` is ``None``. Also ``None``,
    with a warning, for an ID that is not a canonical lower-case UUID: actor
    IDs always are, and the value goes into a header line.
    """
    if actor_id is None or not _is_external_mode():
        return None
    if not _canonical_uuid(actor_id):
        logger.warning(
            "Actor ID %r is not a canonical lower-case UUID; model requests "
            "for it name no principal", actor_id,
        )
        return None
    return actor_id


def principal_headers(actor_id: str | None) -> dict[str, str]:
    """Default headers for a model client that works for ``actor_id``.

    Empty in local mode and when there is no principal to name.
    """
    principal = inference_principal(actor_id)
    return {INFERENCE_PRINCIPAL_HEADER: principal} if principal else {}


def system_principal_headers() -> dict[str, str]:
    """Default headers for Nerve's own model clients: the system actor.

    The system actor comes from the database that the server opened
    (:func:`nerve.db.init_db`). A process that opened none (for example a CLI
    command) names no principal.
    """
    if not _is_external_mode():
        return {}
    # Imported at call time: nerve.config imports this module, and must not
    # load the database package with it.
    from nerve.db import global_system_actor_id

    return principal_headers(global_system_actor_id())


def _header_name(line: str) -> str:
    index = line.find(":")
    return line[:index].strip().lower() if index >= 0 else ""


def custom_headers_env(
    principal: str | None, environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Environment entries that make a Claude CLI process name ``principal``.

    ``principal`` comes from :func:`inference_principal`. Local mode gives no
    entries, so the CLI inherits the environment of Nerve unchanged. In
    external mode, the value is the inherited ``ANTHROPIC_CUSTOM_HEADERS``
    without blank lines and without a line for the principal header, plus one
    line for ``principal`` when it is set. So the CLI sends exactly one
    principal header, or none. No entry when that value is the inherited
    value.
    """
    if not _is_external_mode():
        return {}
    env = os.environ if environ is None else environ
    inherited = env.get(CUSTOM_HEADERS_ENV, "")
    wanted = INFERENCE_PRINCIPAL_HEADER.lower()
    lines = [
        line for line in _LINE_END.split(inherited)
        if line.strip() and _header_name(line) != wanted
    ]
    # Checked again: the value goes into a header line.
    if principal and _canonical_uuid(principal):
        lines.append(f"{INFERENCE_PRINCIPAL_HEADER}: {principal}")
    value = "\n".join(lines)
    if value == inherited:
        return {}
    return {CUSTOM_HEADERS_ENV: value}
