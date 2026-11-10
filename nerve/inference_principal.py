"""The principal, trigger, cron job and origin Nerve names on model requests
in external mode.

In external mode, a proxy in front of the model API can attribute model usage
to a person. Nerve gives it the principal that caused the work: one
``X-Nerve-Inference-Principal`` header with the actor ID, a canonical
lower-case UUID. The model requests of a session name the session's cause,
the person who started it (or the system actor for a session that Nerve
started for itself). Nerve's own model calls (session titles, prompt rewrite,
source condensing, memory, setup checks, the model catalogue) name the system
actor. Nerve does not prove the claim; the proxy decides what to accept.

A second header, ``X-Nerve-Inference-Trigger``, names the kind of event that
started the work: the session's ``trigger`` from the closed vocabulary of
:mod:`nerve.session_origin`, or ``system`` for Nerve's own model calls. Like
the cause, the trigger of a session is set when the session is created.

A third header, ``X-Nerve-Inference-Cron-Job``, names the cron job of a
session that a cron job started (``origin_detail.cron_job``), so that the
proxy can attribute model usage to the job. Job IDs are free text, so Nerve
sends :func:`cron_job_tag_value` of the ID: 1 to 64 bytes of ASCII letters,
digits, ``.``, ``_`` and ``-``. Nerve's own model calls send no cron job.

Another header, ``X-Nerve-Inference-Origin``, names where a session comes
from: the cron job, the channel conversation, the hook, the plan, the
workflow run or the external client, as a tag such as ``cron=<job id>``
(:func:`nerve.session_origin.origin_tag`). Like the trigger, it comes from
the session's origin, which is set when the session is created. A session
without such a detail, and Nerve's own model calls, send no origin header.

Local mode sends no header, and a NULL cause sends no principal header: Nerve
also removes inherited principal, trigger, cron job and origin lines from
``ANTHROPIC_CUSTOM_HEADERS`` in the environment it gives the CLI.

The Claude CLI reads ``ANTHROPIC_CUSTOM_HEADERS`` from its environment when it
builds the API client for a request: one ``Name: Value`` header per line,
lines split on ``\\n`` or ``\\r\\n``, name and value split on the first ``:``
and trimmed. The environment of a CLI process is fixed when it starts. That is
enough, because the session's cause, trigger, cron job and origin never change.

Codex sends the principal header only to ``codex.base_url``, and sends no
trigger, cron job or origin header: the provider that Nerve gives Codex for it
reads the value from an environment variable of the Codex process
(``env_http_headers``). See ``nerve.agent.backends.codex.backend``. One Codex
process serves one session, so the same rule applies.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Mapping
from uuid import UUID

from nerve.session_origin import (
    TRIGGER_SYSTEM,
    TRIGGERS,
    Origin,
    is_origin_tag,
    origin_tag,
)

logger = logging.getLogger(__name__)

INFERENCE_PRINCIPAL_HEADER = "X-Nerve-Inference-Principal"
INFERENCE_TRIGGER_HEADER = "X-Nerve-Inference-Trigger"
INFERENCE_CRON_JOB_HEADER = "X-Nerve-Inference-Cron-Job"
INFERENCE_ORIGIN_HEADER = "X-Nerve-Inference-Origin"
CUSTOM_HEADERS_ENV = "ANTHROPIC_CUSTOM_HEADERS"

# The headers that Nerve sets, in lower case. Nerve removes an inherited line
# for each of them.
_NERVE_HEADERS = frozenset({
    INFERENCE_PRINCIPAL_HEADER.lower(), INFERENCE_TRIGGER_HEADER.lower(),
    INFERENCE_CRON_JOB_HEADER.lower(),
    INFERENCE_ORIGIN_HEADER.lower(),
})

# The value rule of the cron job header, which the proxy checks again.
MAX_CRON_JOB_BYTES = 64
_CRON_JOB_VALUE = re.compile(rf"[A-Za-z0-9._-]{{1,{MAX_CRON_JOB_BYTES}}}")
_CRON_JOB_OTHER = re.compile(r"[^A-Za-z0-9._-]+")
# Length of the hash part of a changed cron job value, in hex digits.
_CRON_JOB_HASH_HEX = 8

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


def inference_trigger(trigger: str | None) -> str | None:
    """The trigger to name on the model requests of a session with ``trigger``.

    ``None`` in local mode, and for a value outside the vocabulary: the value
    goes into a header line.
    """
    if trigger not in TRIGGERS or not _is_external_mode():
        return None
    return trigger


def cron_job_tag_value(job_id: str) -> str:
    """The value of the cron job header for cron job ``job_id``.

    Each run of characters other than ASCII letters, digits, ``.``, ``_``
    and ``-`` becomes one ``-``, and ``-`` at the start and the end is
    removed. An empty result becomes ``job-`` and the first 8 hex digits of
    the SHA-256 of ``job_id`` (UTF-8). A result longer than 64 bytes becomes
    its first 55 bytes, ``-`` and the same 8 hex digits. So the value is 1
    to 64 bytes, and two different IDs give the same value only rarely.
    """
    value = _CRON_JOB_OTHER.sub("-", job_id).strip("-")
    if value and len(value) <= MAX_CRON_JOB_BYTES:
        return value
    digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()[:_CRON_JOB_HASH_HEX]
    if not value:
        return f"job-{digest}"
    return f"{value[:MAX_CRON_JOB_BYTES - _CRON_JOB_HASH_HEX - 1]}-{digest}"


def inference_cron_job(job_id: str | None) -> str | None:
    """The cron job to name on the model requests of a session of ``job_id``.

    ``None`` in local mode and when the session has no cron job; else
    :func:`cron_job_tag_value` of the ID.
    """
    if not job_id or not _is_external_mode():
        return None
    return cron_job_tag_value(job_id)


def inference_origin(origin: Origin) -> str | None:
    """The origin to name on the model requests of a session with ``origin``.

    The tag of :func:`nerve.session_origin.origin_tag`. ``None`` in local
    mode, and for an origin without a tag.
    """
    if not _is_external_mode():
        return None
    return origin_tag(origin)


def _system_trigger_headers() -> dict[str, str]:
    trigger = inference_trigger(TRIGGER_SYSTEM)
    return {INFERENCE_TRIGGER_HEADER: trigger} if trigger else {}


def system_inference_headers() -> dict[str, str]:
    """Default headers for Nerve's own model clients.

    In external mode: the system actor as principal, and the trigger
    ``system``. The system actor comes from the database that the server
    opened (:func:`nerve.db.init_db`). A process that did not open it (a CLI
    command such as ``nerve sync``, which opens its own connection, or none)
    reads the stored system actor read-only from ``nerve.db``. No principal
    when neither gives one.
    """
    if not _is_external_mode():
        return {}
    # Imported at call time: nerve.config imports this module, and must not
    # load the database package with it.
    from nerve.db import global_system_actor_id

    actor_id = global_system_actor_id()
    if actor_id is None:
        from nerve import paths
        from nerve.db.accounts import read_system_actor_id

        actor_id = read_system_actor_id(paths.db_path())
    return {**principal_headers(actor_id), **_system_trigger_headers()}


def _header_name(line: str) -> str:
    index = line.find(":")
    return line[:index].strip().lower() if index >= 0 else ""


def direct_client_headers(
    environ: Mapping[str, str] | None = None, *, principal_id: str | None = None,
) -> dict[str, object]:
    """Default headers for Nerve's own Anthropic SDK clients.

    The Anthropic SDK also adds the lines of ``ANTHROPIC_CUSTOM_HEADERS`` in
    the environment of Nerve to its default headers, with the name as written.
    Each such name of the principal, trigger, cron job or origin header gets
    ``anthropic.omit``, which removes it. Then, in external mode, the header
    for ``principal_id`` is added, or for the system actor of the global
    database when it is ``None``, and the trigger ``system``
    (:func:`system_inference_headers`). So a request names one principal and
    one trigger in external mode, and neither in local mode. It never names
    a cron job or an origin.
    """
    from anthropic import omit

    env = os.environ if environ is None else environ
    headers: dict[str, object] = {
        line[:line.find(":")].strip(): omit
        for line in env.get(CUSTOM_HEADERS_ENV, "").split("\n")
        if _header_name(line) in _NERVE_HEADERS
    }
    headers.update(
        {**principal_headers(principal_id), **_system_trigger_headers()}
        if principal_id is not None
        else system_inference_headers()
    )
    return headers


def custom_headers_env(
    principal: str | None, environ: Mapping[str, str] | None = None,
    *, trigger: str | None = None, cron_job: str | None = None,
    origin: str | None = None,
) -> dict[str, str]:
    """Environment entries that make a Claude CLI process name ``principal``,
    ``trigger``, ``cron_job`` and ``origin``.

    ``principal`` comes from :func:`inference_principal`, ``trigger`` from
    :func:`inference_trigger`, ``cron_job`` from :func:`inference_cron_job`
    and ``origin`` from :func:`inference_origin`.
    The SDK merges the entries over the environment of Nerve, which the CLI
    inherits. The value is the inherited ``ANTHROPIC_CUSTOM_HEADERS`` without
    blank lines and without a line for the principal, the trigger or the
    cron job or origin header, plus, in external mode, one line for each of
    ``principal``, ``trigger``, ``cron_job`` and ``origin`` that is set. So
    the CLI sends at most one header of each in external mode, and none in
    local mode. No entry when that value is the inherited value, and in local
    mode when the inherited value has no line to remove.
    """
    env = os.environ if environ is None else environ
    inherited = env.get(CUSTOM_HEADERS_ENV, "")
    lines = _LINE_END.split(inherited) if inherited else []
    if not _is_external_mode():
        if not any(_header_name(line) in _NERVE_HEADERS for line in lines):
            return {}
        principal = trigger = cron_job = origin = None
    kept = [
        line for line in lines
        if line.strip() and _header_name(line) not in _NERVE_HEADERS
    ]
    # Checked again: the values go into header lines.
    if principal and _canonical_uuid(principal):
        kept.append(f"{INFERENCE_PRINCIPAL_HEADER}: {principal}")
    if trigger in TRIGGERS:
        kept.append(f"{INFERENCE_TRIGGER_HEADER}: {trigger}")
    if cron_job and _CRON_JOB_VALUE.fullmatch(cron_job):
        kept.append(f"{INFERENCE_CRON_JOB_HEADER}: {cron_job}")
    if is_origin_tag(origin):
        kept.append(f"{INFERENCE_ORIGIN_HEADER}: {origin}")
    value = "\n".join(kept)
    if value == inherited:
        return {}
    return {CUSTOM_HEADERS_ENV: value}
