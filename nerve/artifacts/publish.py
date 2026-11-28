"""Publish the bundle of a workspace path to the platform as an artifact.

In external mode the platform serves artifacts: static bundles of files
that the members of the agent see in a viewer, and that a person can share
with everyone in the tenant there. The runtime sends a bundle through the
artifacts MCP server of the platform (catalog server ``nerve-artifacts`` of
the MCP gateway) in three calls that only the runtime makes:

1. ``begin_artifact_version`` with the title, the entry and each file with
   its size and SHA-256. The platform checks its limits and keeps a pending
   version.
2. ``get_artifact_upload_links`` for at most ``link_batch_size`` paths at a
   time. An upload link is a short-lived right to write one object: a
   ``PUT`` with exactly the headers of the answer (the size, the checksum
   and "no second write" are signed) and the bytes of the file.
3. ``complete_artifact_version``: the platform checks the objects and gives
   the viewer address.

The model never sees these three tools (``RUNTIME_ONLY_TOOLS``; the
backends keep them out of its tool list). It calls ``publish_artifact``,
and the Publish button of the Preview tab calls the same function
(:func:`publish_artifact`).

An upload link goes to the ``PUT`` and nowhere else: not into a result, an
error text, a log line or an exception that leaves this module. An error of
an upload names the file and the HTTP status. The ``httpx`` logger names
the address of each request it sends, and the ``httpcore`` loggers the
headers of each answer; while a task uploads, a record factory empties each
record of these loggers (``_upload_record_factory``), whatever the levels.

The runtime sends file bytes only to the object store: every link of an
answer must have an origin of ``artifacts.upload_origins`` before any byte
goes to any link of that answer, and the client follows no redirect.

Every file is read through :func:`~nerve.artifacts.bundle.open_bundle_file`:
for the hash and again for the upload. A version is exactly the bytes that
were hashed: the link signs their size and checksum. A file that is not the
described file when it is opened for the upload fails the publish, and so
do bytes that differ from the hashed ones when they are sent (the store
refuses them). A change after the bytes were read is a newer state: the
version keeps the earlier one, and the result says so (``changed_after``).
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import json
import logging
import re
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Protocol

import httpx

from nerve.artifacts.bundle import (
    MAX_FILE_BYTES,
    MAX_FILES,
    MIB,
    Bundle,
    BundleError,
    BundleFile,
    BundleRefusal,
    describe_bundle,
    open_bundle_file,
)

from nerve.config import upload_origin

logger = logging.getLogger(__name__)

# --- The artifacts MCP server of the platform ------------------------------

#: The catalog server of the MCP gateway that has the artifact tools.
ARTIFACTS_SERVER_ID = "nerve-artifacts"
TOOL_BEGIN = "begin_artifact_version"
TOOL_UPLOAD_LINKS = "get_artifact_upload_links"
TOOL_COMPLETE = "complete_artifact_version"
#: The tools of catalog servers that only the runtime calls. The backends
#: keep them out of the tool list of the model, by server and tool name.
RUNTIME_ONLY_TOOLS: dict[str, frozenset[str]] = {
    ARTIFACTS_SERVER_ID: frozenset({TOOL_BEGIN, TOOL_UPLOAD_LINKS, TOOL_COMPLETE}),
}

#: The runtime tool of the model.
PUBLISH_TOOL = "publish_artifact"

#: The most paths of one ``get_artifact_upload_links`` call. The platform
#: says its own value (``link_batch_size``); the runtime takes the smaller.
MAX_LINK_BATCH = 100
#: The most characters of an address of an answer, such as the viewer
#: address of a published version.
MAX_URL_CHARS = 2048
#: The most characters of an upload link. A link signed with temporary
#: credentials carries their session token in its query, often more than
#: 1000 characters, besides the key and the signature; the platform bounds
#: a link at 8 KiB.
MAX_LINK_URL_CHARS = 8 * 1024
#: How long an upload link lives, when the answer has no valid ``expires_at``.
LINK_LIFETIME_SECONDS = 15 * 60
#: A link that has less than this left is not used: new links are asked
#: for the files that are left.
LINK_RENEW_MARGIN_SECONDS = 60.0
#: Uploads at the same time.
UPLOAD_CONCURRENCY = 4
#: The time to open a connection for an upload, and for the whole upload of
#: one file (a file has at most 50 MiB).
UPLOAD_CONNECT_TIMEOUT_SECONDS = 10.0
UPLOAD_TIMEOUT_SECONDS = 600.0
#: The bytes of one read of a file, for the hash and for the upload.
CHUNK_BYTES = 1 * MIB
#: The time of one call to the MCP gateway. Completion checks every object
#: of the version, so it gets the call limit of the gateway (two minutes)
#: and some more.
CALL_TIMEOUT_SECONDS = 60.0
COMPLETE_TIMEOUT_SECONDS = 150.0

#: The refusal reasons of the artifacts MCP server, each as a sentence for
#: the model and for a person.
REFUSAL_SENTENCES: dict[str, str] = {
    "artifacts_disabled": "Publishing artifacts is turned off on the platform.",
    "restricted_egress": "Agents with restricted internet access cannot publish artifacts.",
    "agent_not_active": "This agent is not active on the platform, so it cannot publish.",
    "invalid_path": "A file path of the bundle is not valid for an artifact.",
    "invalid_entry": "The entry file of the bundle is not valid for an artifact.",
    "invalid_title": "The title is not valid: it must have 1 to 200 characters and no control characters.",
    "invalid_file": "The size or the checksum of a file of the bundle is not valid.",
    "limit_upload_paths": "The runtime asked for too many upload links at a time.",
    "limit_file_size": f"A file of the bundle is larger than an artifact can have ({MAX_FILE_BYTES // MIB} MiB).",
    "limit_file_count": f"The bundle has more files than an artifact can have ({MAX_FILES}).",
    "limit_version_size": "The bundle is larger than one version of an artifact can be in this tenant.",
    "limit_tenant_storage": "The tenant has no storage left for artifacts. Unpublish old artifacts or ask an admin.",
    "limit_artifact_count": "This agent has as many artifacts as it can have. Unpublish one first.",
    "limit_version_count": "This artifact has as many versions as it can have. Publish a new artifact.",
    "limit_pending_versions": "This agent has too many publishes in progress. Try again later.",
    "not_found": "The artifact does not exist, or it is not an artifact of this agent.",
    "version_not_pending": "This version is complete or was removed already.",
    "upload_incomplete": "Some files did not arrive on the platform.",
}

#: Why a publish cannot start or did not end, as codes for the API of the
#: Publish button. The refusal reasons of the platform are codes too.
SERVER_MISSING = "server_missing"
PLATFORM_UNAVAILABLE = "platform_unavailable"
PLATFORM_ERROR = "platform_error"
UPLOAD_FAILED = "upload_failed"
INVALID_ARGUMENTS = "invalid_arguments"

CHANGED_AFTER_SENTENCE = "The files changed while they were published. This version has the earlier state."

UPLOAD_ORIGIN_REFUSED = "upload_origin_refused"
UPLOAD_ORIGINS_MISSING = "upload_origins_missing"
UPLOAD_ORIGIN_SENTENCE = (
    "The platform returned an upload address outside the configured object store "
    "(artifacts.upload_origins). Nothing was uploaded to it."
)
UPLOAD_ORIGINS_MISSING_SENTENCE = (
    "Publishing is not configured on this agent: artifacts.upload_origins names no object store."
)

SERVER_REFUSED = "server_refused"
PLATFORM_BUSY = "platform_busy"
SERVER_REFUSED_SENTENCE = (
    "The MCP gateway did not let this agent use the artifact tools: the policy of the tenant "
    "does not allow them, or the gateway has no working credential for them. An admin can check "
    "the artifact tools of the agent."
)
INVALID_ANSWER_SENTENCE = "The platform answer was not valid. Nothing more was done; try again later."
# The fixed texts of the artifacts MCP server for a tool error without a
# reason, each with the code and the sentence that the runtime gives.
FIXED_TEXTS: tuple[tuple[str, str, str], ...] = (
    ("The arguments are not valid.", PLATFORM_ERROR,
     "The platform did not take the arguments of the publish. Update Nerve or the platform."),
    ("The platform could not read this right now.", PLATFORM_UNAVAILABLE,
     "The platform could not finish the publish right now. Try again in a minute."),
    ("This agent already has calls to the artifact tools in progress.", PLATFORM_BUSY,
     "The platform is busy with other calls of this agent. Try again in a few seconds."),
)
OTHER_ERROR_SENTENCE = "The platform answered the publish with an error. Try again later."

SERVER_MISSING_SENTENCE = (
    "This agent has no artifact tools (the MCP server nerve-artifacts), so it cannot publish "
    "artifacts. Publishing may be off on the platform, or an admin turned the tools off for the agent."
)


class PublishError(Exception):
    """A publish that did not end. ``message`` is a sentence for the model and
    for a person; it never holds an upload link. ``bundle``: the bundle of
    the path was refused, and ``code`` is a code of the bundle (the codes of
    ``GET /api/preview/bundle``, and ``changed``), not one of the platform.
    The two sets share ``not_found``."""

    def __init__(self, code: str, message: str, *, bundle: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.bundle = bundle


@dataclass(frozen=True)
class Published:
    """A complete version of an artifact."""

    artifact_id: str
    version: int
    viewer_url: str
    expires_at: str
    title: str
    # A file of the bundle changed after the publish read it.
    changed_after: bool = False


class ToolCaller(Protocol):
    """What the publish needs of a server of the MCP gateway
    (:class:`nerve.mcp_gateway.McpGatewayServer`)."""

    async def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float | None = None) -> Any: ...


# --- Logs ------------------------------------------------------------------

# True while this task sends an upload.
_UPLOADING: contextvars.ContextVar[bool] = contextvars.ContextVar("nerve_artifact_uploading", default=False)


# The loggers of the HTTP client and of its transport, with their children.
# Their records name the address of a request and the headers of an
# answer, and the store can repeat the link in a header (``Location``).
HIDDEN_LOGGERS = ("httpx", "httpcore")
HIDDEN_RECORD_TEXT = "artifact upload: the details of this HTTP record are not logged"


def _hidden_logger(name: str) -> bool:
    return any(name == root or name.startswith(root + ".") for root in HIDDEN_LOGGERS)


def _upload_record_factory(previous: Callable[..., logging.LogRecord]) -> Callable[..., logging.LogRecord]:
    """A record factory that empties each record of ``HIDDEN_LOGGERS`` made
    while ``_UPLOADING`` is set in the task: a fixed text, no arguments, no
    exception. A record factory runs for every record of the process,
    whatever the levels, filters and handlers say, in the task that logs."""

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = previous(*args, **kwargs)
        if _UPLOADING.get() and _hidden_logger(record.name):
            record.msg, record.args = HIDDEN_RECORD_TEXT, ()
            record.exc_info, record.exc_text, record.stack_info = None, None, None
        return record

    factory.nerve_artifact_uploads = True  # type: ignore[attr-defined]
    return factory


if not getattr(logging.getLogRecordFactory(), "nerve_artifact_uploads", False):
    logging.setLogRecordFactory(_upload_record_factory(logging.getLogRecordFactory()))


# --- When and where it exists ----------------------------------------------


@dataclass(frozen=True)
class PublishSettings:
    """``artifacts.publish`` and ``artifacts.viewer_origin`` as they apply:
    both only in external mode."""

    publish: bool
    viewer_origin: str
    # ``artifacts.upload_origins`` as ``(scheme, host, port)``.
    upload_origins: frozenset[tuple[str, str, int]] = frozenset()


# The settings that the gateway pinned at its start (:func:`pin_publish_settings`).
_pinned: PublishSettings | None = None


def _settings_of(config: Any) -> PublishSettings:
    from nerve.gateway.auth import is_external_mode

    if not is_external_mode():
        return PublishSettings(publish=False, viewer_origin="")
    return PublishSettings(
        publish=bool(config.artifacts.publish),
        viewer_origin=config.artifacts.viewer_origin or "",
        upload_origins=allowed_upload_origins(config.artifacts.upload_origins),
    )


def allowed_upload_origins(origins: Any) -> frozenset[tuple[str, str, int]]:
    """The entries of ``artifacts.upload_origins`` as ``(scheme, host, port)``."""
    from nerve.config import upload_origin

    parsed = (upload_origin(str(origin)) for origin in (origins or ()))
    return frozenset(origin for origin in parsed if origin is not None)


def pin_publish_settings(config: Any) -> PublishSettings:
    """Decide the settings one time, when the gateway starts. The section
    needs a restart: after this, a reload of the configuration changes
    neither the route of the Publish button, nor ``GET /api/auth/me``, nor
    the tool list of the model."""
    global _pinned
    if _pinned is None:
        _pinned = _settings_of(config)
    return _pinned


def unpin_publish_settings() -> None:
    """Forget the pinned settings (tests)."""
    global _pinned
    _pinned = None


def publish_settings(config: Any) -> PublishSettings:
    """The pinned settings, or those of ``config`` before the gateway pinned
    them (a process without a gateway, tests)."""
    return _pinned if _pinned is not None else _settings_of(config)


def publish_active(config: Any) -> bool:
    """Whether the tool and the Publish button exist: external mode, with
    ``artifacts.publish`` on (pinned at the start of the gateway)."""
    return publish_settings(config).publish


def artifacts_server_url(servers: list[Any]) -> str | None:
    """The address of the ``nerve-artifacts`` server among the MCP servers of the
    catalog, or None."""
    for server in servers:
        if getattr(server, "name", "") == ARTIFACTS_SERVER_ID and getattr(server, "url", None):
            return str(server.url)
    return None


def on_behalf_of(cause_id: str | None) -> str | None:
    """The person to state as the publisher: the cause of the work when it is
    a person (a canonical UUID that is not the system principal), else None."""
    from nerve.identity import system_principal_id_from_env
    from nerve.inference_principal import inference_principal

    principal = inference_principal(cause_id)
    if principal is None:
        return None
    try:
        system = system_principal_id_from_env()
    except ValueError:
        system = None
    return None if principal == system else principal


# --- Answers of the server -------------------------------------------------


# A reason code of a later platform: lower-case words with ``_``.
_REASON_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")


def _refusal(text: str, known_paths: frozenset[str] = frozenset()) -> PublishError:
    """The error of a tool result with ``isError``: a refusal with a reason
    of the platform, or one of its fixed texts. No other text of the answer
    goes into the error: a reason only when it is a code, a path of
    ``upload_incomplete`` only when it is a file of the bundle."""
    try:
        data = json.loads(text)
    except ValueError:
        data = None
    if isinstance(data, dict) and isinstance(data.get("reason"), str):
        reason = data["reason"]
        sentence = REFUSAL_SENTENCES.get(reason)
        if sentence is None:
            if not _REASON_CODE.fullmatch(reason):
                return PublishError(PLATFORM_ERROR, INVALID_ANSWER_SENTENCE)
            sentence = f"The platform refused the publish ({reason})."
        paths = data.get("paths")
        if reason == "upload_incomplete" and isinstance(paths, list):
            names = ", ".join(repr(p) for p in paths[:20] if isinstance(p, str) and p in known_paths)
            if names:
                sentence = f"{sentence} Files: {names}."
        return PublishError(reason, sentence)
    # A fixed text of the server: arguments that it does not take, a failure
    # of the platform, or a busy agent. None of them is a refusal.
    for start, code, sentence in FIXED_TEXTS:
        if text.strip().startswith(start):
            return PublishError(code, sentence)
    return PublishError(PLATFORM_ERROR, OTHER_ERROR_SENTENCE)


async def _call(
    server: ToolCaller, name: str, arguments: dict[str, Any], timeout: float,
    known_paths: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Call ``name`` and return its result object, from the JSON of its text."""
    from nerve.mcp_gateway import POLICY_REFUSED, GatewayCallError

    try:
        result = await server.call_tool(name, arguments, timeout=timeout)
    except GatewayCallError as e:
        if e.code == POLICY_REFUSED:
            raise PublishError(SERVER_REFUSED, SERVER_REFUSED_SENTENCE) from None
        logger.warning("Artifact publish: %s did not answer (%s)", name, type(e).__name__)
        raise PublishError(
            PLATFORM_UNAVAILABLE,
            "The platform did not answer the publish. Try again in a minute.",
        ) from None
    if result.is_error:
        raise _refusal(result.text, known_paths)
    try:
        data = json.loads(result.text)
    except ValueError:
        data = None
    if not isinstance(data, dict):
        raise _invalid_answer(name)
    return data


def _invalid_answer(name: str) -> PublishError:
    logger.warning("Artifact publish: the answer of %s is not valid", name)
    return PublishError(PLATFORM_ERROR, INVALID_ANSWER_SENTENCE)


def _uuid(data: dict[str, Any], key: str, name: str) -> str:
    """A UUID of an answer, in its canonical lower-case form."""
    value = data.get(key)
    try:
        if isinstance(value, str) and str(uuid.UUID(value)) == value:
            return value
    except ValueError:
        pass
    raise _invalid_answer(name)


def _printable(value: str) -> bool:
    """Only printable ASCII without a space (0x21 to 0x7E): no C0 or C1
    control character, no DEL, nothing outside ASCII."""
    return all(0x21 <= ord(c) <= 0x7E for c in value)


def _timestamp(data: dict[str, Any], key: str, name: str) -> str:
    """A time of an answer: RFC 3339 with a time zone (``Z`` or an offset)."""
    value = data.get(key)
    if isinstance(value, str) and len(value) <= 40 and _printable(value):
        try:
            if datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None:
                return value
        except ValueError:
            pass
    raise _invalid_answer(name)


def _string(data: dict[str, Any], key: str, name: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise _invalid_answer(name)
    return value


def _number(data: dict[str, Any], key: str, name: str) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise _invalid_answer(name)
    return value


def _http_url(value: str, name: str, max_chars: int = MAX_URL_CHARS) -> str:
    """An absolute ``http`` or ``https`` address with a host and no user
    information, of at most ``max_chars`` printable ASCII characters (no
    space, no control character)."""
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(value)
        parts.port  # noqa: B018 - raises for a port that is not a number
    except ValueError:
        raise _invalid_answer(name) from None
    if (
        parts.scheme not in ("https", "http") or not parts.hostname or "@" in parts.netloc
        or len(value) > max_chars or not _printable(value)
    ):
        raise _invalid_answer(name)
    return value


def _deadline(expires_at: Any, asked_at: float) -> float:
    """The monotonic time from which a link is not used any more."""
    lifetime = float(LINK_LIFETIME_SECONDS)
    if isinstance(expires_at, str):
        try:
            when = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if when.tzinfo is not None:
                lifetime = min(lifetime, (when - datetime.now(timezone.utc)).total_seconds())
        except ValueError:
            pass
    return asked_at + lifetime - LINK_RENEW_MARGIN_SECONDS


@dataclass(frozen=True)
class _Link:
    url: str
    headers: dict[str, str]
    deadline: float


def _links(data: dict[str, Any], asked: list[str], asked_at: float) -> dict[str, _Link]:
    """The links of an answer of ``get_artifact_upload_links``, by path. Only
    paths that were asked for."""
    items = data.get("links")
    if not isinstance(items, list):
        raise _invalid_answer(TOOL_UPLOAD_LINKS)
    deadline = _deadline(data.get("expires_at"), asked_at)
    wanted = set(asked)
    links: dict[str, _Link] = {}
    for item in items:
        if not isinstance(item, dict):
            raise _invalid_answer(TOOL_UPLOAD_LINKS)
        path, url, headers = item.get("path"), item.get("url"), item.get("headers")
        if not isinstance(path, str) or path not in wanted or not isinstance(url, str):
            raise _invalid_answer(TOOL_UPLOAD_LINKS)
        if not isinstance(headers, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in headers.items()
        ):
            raise _invalid_answer(TOOL_UPLOAD_LINKS)
        links[path] = _Link(
            url=_http_url(url, TOOL_UPLOAD_LINKS, MAX_LINK_URL_CHARS), headers=dict(headers), deadline=deadline,
        )
    return links


# --- Files -----------------------------------------------------------------


def _changed(path: str) -> PublishError:
    return PublishError(
        BundleRefusal.CHANGED.value,
        f"The file {path!r} changed while it was published. Publish again when the files do not change.",
        bundle=True,
    )


def _hash_files(bundle: Bundle) -> list[tuple[BundleFile, str]]:
    """The SHA-256 (lowercase hexadecimal) of each file of ``bundle``, read
    through :func:`open_bundle_file`."""
    hashed: list[tuple[BundleFile, str]] = []
    for f in bundle.files:
        digest = hashlib.sha256()
        count = 0
        try:
            with open_bundle_file(bundle, f.path) as handle:
                while count <= f.size:
                    chunk = handle.read(CHUNK_BYTES)
                    if not chunk:
                        break
                    digest.update(chunk)
                    count += len(chunk)
        except BundleError as e:
            if e.code is BundleRefusal.CHANGED:
                raise _changed(f.path) from None
            raise PublishError(e.code.value, e.message, bundle=True) from None
        if count != f.size:
            raise _changed(f.path)
        hashed.append((f, digest.hexdigest()))
    return hashed


def checksum_header(sha256_hex: str) -> str:
    """The value of ``x-amz-checksum-sha256`` for a SHA-256 in hexadecimal:
    base64 of the 32 bytes."""
    return base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")


async def _chunks(handle: BinaryIO, size: int, path: str) -> AsyncIterator[bytes]:
    """The first ``size`` bytes of ``handle``, in chunks. Fewer bytes on disk
    end the upload with an error."""
    left = size
    while left > 0:
        chunk = await asyncio.to_thread(handle.read, min(CHUNK_BYTES, left))
        if not chunk:
            raise _changed(path)
        left -= len(chunk)
        yield chunk


def _header(headers: dict[str, str], name: str) -> str | None:
    return next((v for k, v in headers.items() if k.lower() == name.lower()), None)


async def _put(http: httpx.AsyncClient, link: _Link, bundle: Bundle, f: BundleFile) -> None:
    """Send the file ``f`` to its link: a ``PUT`` with the headers of the
    link and the bytes of the file, with a fixed ``Content-Length``."""
    headers = dict(link.headers)
    length = _header(headers, "Content-Length")
    if length is None:
        headers["Content-Length"] = str(f.size)
    elif length.strip() != str(f.size):
        raise _invalid_answer(TOOL_UPLOAD_LINKS)
    try:
        handle = await asyncio.to_thread(open_bundle_file, bundle, f.path)
    except BundleError as e:
        if e.code is BundleRefusal.CHANGED:
            raise _changed(f.path) from None
        raise PublishError(e.code.value, e.message, bundle=True) from None
    token = _UPLOADING.set(True)
    try:
        content: bytes | AsyncIterator[bytes] = b"" if f.size == 0 else _chunks(handle, f.size, f.path)
        async with asyncio.timeout(UPLOAD_TIMEOUT_SECONDS):
            response = await http.put(link.url, headers=headers, content=content)
    except PublishError:
        raise
    except TimeoutError:
        raise PublishError(
            UPLOAD_FAILED, f"The upload of {f.path!r} took longer than {int(UPLOAD_TIMEOUT_SECONDS)} seconds.",
        ) from None
    except Exception as e:  # noqa: BLE001 - the text of an error can hold the link
        raise PublishError(UPLOAD_FAILED, f"The upload of {f.path!r} failed ({type(e).__name__}).") from None
    finally:
        _UPLOADING.reset(token)
        handle.close()
    if not 200 <= response.status_code < 300:
        raise PublishError(
            UPLOAD_FAILED,
            f"The upload of {f.path!r} was refused with HTTP {response.status_code}. "
            "If the file changed while it was published, publish again.",
        )


class _Uploads:
    """Upload the files of a pending version: links in batches of at most
    ``batch`` paths, asked for the next files that wait, and asked again
    for a file whose link is too old. Each link is used one time."""

    def __init__(
        self, server: ToolCaller, http: httpx.AsyncClient, bundle: Bundle, version_id: str,
        files: list[BundleFile], batch: int, clock: Callable[[], float],
        origins: frozenset[tuple[str, str, int]],
    ) -> None:
        self._origins = origins
        self._server = server
        self._http = http
        self._bundle = bundle
        self._version_id = version_id
        self._batch = batch
        self._clock = clock
        self._waiting: deque[BundleFile] = deque(files)
        self._links: dict[str, _Link] = {}
        self._lock = asyncio.Lock()

    async def _link(self, f: BundleFile) -> _Link:
        async with self._lock:
            link = self._links.pop(f.path, None)
            if link is not None and self._clock() < link.deadline:
                return link
            asked = [f.path, *(w.path for w in list(self._waiting)[: self._batch - 1])]
            asked_at = self._clock()
            data = await _call(
                self._server, TOOL_UPLOAD_LINKS, {"version_id": self._version_id, "paths": asked},
                CALL_TIMEOUT_SECONDS,
            )
            links = _links(data, asked, asked_at)
            # Every link of the answer, before any byte goes to any of them.
            if any(upload_origin(link.url) not in self._origins for link in links.values()):
                logger.warning("Artifact publish: an upload link is outside artifacts.upload_origins")
                raise PublishError(UPLOAD_ORIGIN_REFUSED, UPLOAD_ORIGIN_SENTENCE)
            if f.path not in links:
                raise _invalid_answer(TOOL_UPLOAD_LINKS)
            link = links.pop(f.path)
            self._links.update(links)
            return link

    async def _worker(self) -> None:
        while self._waiting:
            f = self._waiting.popleft()
            await _put(self._http, await self._link(f), self._bundle, f)

    async def run(self) -> None:
        workers = [asyncio.create_task(self._worker()) for _ in range(min(UPLOAD_CONCURRENCY, len(self._waiting)))]
        try:
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                task.cancel()
            # Each task is awaited, so no error of a task is logged with its text.
            await asyncio.gather(*workers, return_exceptions=True)


def upload_client() -> httpx.AsyncClient:
    """The HTTP client of the uploads. The object store is a host on the
    internet: the client goes out through the proxy of the environment of the
    machine (``HTTPS_PROXY``), as the clients of the channels do. It never
    follows a redirect."""
    return httpx.AsyncClient(
        trust_env=True,
        follow_redirects=False,
        timeout=httpx.Timeout(UPLOAD_TIMEOUT_SECONDS, connect=UPLOAD_CONNECT_TIMEOUT_SECONDS),
    )


# --- The publish -----------------------------------------------------------


def _check_arguments(path: Any, title: Any, artifact_id: Any) -> tuple[str, str, str | None]:
    if not isinstance(path, str) or not path.strip():
        raise PublishError(INVALID_ARGUMENTS, "Give the path of the file or folder to publish.")
    if not isinstance(title, str) or not title.strip():
        raise PublishError(INVALID_ARGUMENTS, "Give a title for the artifact.")
    if artifact_id is None or artifact_id == "":
        return path, title.strip(), None
    try:
        canonical = str(uuid.UUID(artifact_id)) if isinstance(artifact_id, str) else None
    except ValueError:
        canonical = None
    if canonical is None:
        raise PublishError(INVALID_ARGUMENTS, "artifact_id is not the ID of an artifact.")
    return path, title.strip(), canonical


async def publish_bundle(
    workspace: Path | str,
    path: Any,
    title: Any,
    *,
    server: ToolCaller,
    upload_origins: frozenset[tuple[str, str, int]],
    artifact_id: Any = None,
    on_behalf_of: str | None = None,
    http: httpx.AsyncClient | None = None,
    kit_dir: Path | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> Published:
    """Publish the bundle of ``path`` (relative to ``workspace``, or absolute
    below it) as a new artifact, or as a new version of ``artifact_id``.
    ``on_behalf_of`` is the person of the session, when it has one. Raises
    :class:`PublishError`."""
    path, title, artifact_id = _check_arguments(path, title, artifact_id)
    if not upload_origins:
        raise PublishError(UPLOAD_ORIGINS_MISSING, UPLOAD_ORIGINS_MISSING_SENTENCE)
    try:
        bundle = await asyncio.to_thread(describe_bundle, workspace, path, kit_dir=kit_dir)
    except BundleError as e:
        raise PublishError(e.code.value, e.message, bundle=True) from None
    hashed = await asyncio.to_thread(_hash_files, bundle)

    begin: dict[str, Any] = {
        "title": title,
        "entry": bundle.entry,
        "files": [{"path": f.path, "size": f.size, "sha256": sha} for f, sha in hashed],
    }
    if artifact_id is not None:
        begin["artifact_id"] = artifact_id
    if on_behalf_of is not None:
        begin["on_behalf_of"] = on_behalf_of
    started = await _call(server, TOOL_BEGIN, begin, CALL_TIMEOUT_SECONDS)
    begun_artifact = _uuid(started, "artifact_id", TOOL_BEGIN)
    version_id = _uuid(started, "version_id", TOOL_BEGIN)
    if artifact_id is not None and begun_artifact != artifact_id:
        raise _invalid_answer(TOOL_BEGIN)
    batch = min(_number(started, "link_batch_size", TOOL_BEGIN), MAX_LINK_BATCH)

    own_client = http is None
    client = upload_client() if http is None else http
    try:
        await _Uploads(
            server, client, bundle, version_id, [f for f, _ in hashed], batch, clock, upload_origins,
        ).run()
    finally:
        if own_client:
            await client.aclose()

    # The version is the bytes that were hashed: the links sign their size
    # and checksum. A file that changed after its read is a newer state,
    # which the next publish sends; the result says so.
    try:
        later = await asyncio.to_thread(describe_bundle, workspace, path, kit_dir=kit_dir)
        changed_after = later.fingerprint != bundle.fingerprint
    except BundleError:
        changed_after = True

    done = await _call(
        server, TOOL_COMPLETE, {"version_id": version_id}, COMPLETE_TIMEOUT_SECONDS,
        frozenset(f.path for f, _ in hashed),
    )
    if _uuid(done, "artifact_id", TOOL_COMPLETE) != begun_artifact:
        raise _invalid_answer(TOOL_COMPLETE)
    return Published(
        artifact_id=begun_artifact,
        version=_number(done, "version", TOOL_COMPLETE),
        viewer_url=_http_url(_string(done, "viewer_url", TOOL_COMPLETE), TOOL_COMPLETE),
        expires_at=_timestamp(done, "expires_at", TOOL_COMPLETE),
        title=title,
        changed_after=changed_after,
    )


# What the tool and the route need of the engine: the MCP servers of the catalog.
ServerFactory = Callable[[str], Any]


def _gateway_server(url: str) -> Any:
    from nerve.mcp_gateway import McpGatewayServer

    return McpGatewayServer(url, timeout=CALL_TIMEOUT_SECONDS)


async def publish_artifact(
    *,
    workspace: Path | str,
    servers: list[Any],
    path: Any,
    title: Any,
    artifact_id: Any = None,
    cause_id: str | None = None,
    upload_origins: frozenset[tuple[str, str, int]] = frozenset(),
    server_factory: ServerFactory | None = None,
    http: httpx.AsyncClient | None = None,
    kit_dir: Path | None = None,
) -> Published:
    """The publish of the tool ``publish_artifact`` and of the Publish button:
    find the ``nerve-artifacts`` server in ``servers`` (the catalog of the
    engine), publish, and close the client. ``cause_id`` is the person for
    whom the work runs (the cause of the session, or the person of the
    button). Raises :class:`PublishError`."""
    if not upload_origins:
        raise PublishError(UPLOAD_ORIGINS_MISSING, UPLOAD_ORIGINS_MISSING_SENTENCE)
    url = artifacts_server_url(servers)
    if url is None:
        raise PublishError(SERVER_MISSING, SERVER_MISSING_SENTENCE)
    server = (server_factory or _gateway_server)(url)
    try:
        return await publish_bundle(
            workspace, path, title, server=server, upload_origins=upload_origins, artifact_id=artifact_id,
            on_behalf_of=on_behalf_of(cause_id), http=http, kit_dir=kit_dir,
        )
    finally:
        close: Callable[[], Awaitable[None]] | None = getattr(server, "close", None)
        if close is not None:
            await close()


def result_text(published: Published) -> str:
    """The text of the tool result: what the model can tell people. No
    upload link and no ID of a person."""
    return (
        f"Published {json.dumps(published.title, ensure_ascii=False)} as version {published.version}.\n"
        f"Viewer address: {published.viewer_url}\n"
        "Who can see it: the members of this agent. A member can share this version "
        "with everyone in the tenant with the Share button of the viewer.\n"
        f"Artifact ID: {published.artifact_id} (give it as artifact_id to publish a new version of this artifact)\n"
        + (f"Expires: {published.expires_at}\n" if published.expires_at else "")
        + (f"{CHANGED_AFTER_SENTENCE}\n" if published.changed_after else "")
    ).rstrip("\n")
