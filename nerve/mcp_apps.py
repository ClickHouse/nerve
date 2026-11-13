"""MCP Apps host support (MCP extension ``io.modelcontextprotocol/ui``).

The parts of the host that do not depend on a runtime or on HTTP: which tool
opens which view, the tool result a view is handed, the view's document, and
the on-disk cache of documents. The Claude backend supplies the access to the
MCP servers (``ClaudeClient.mcp_*``), and the gateway the routes
(``nerve/gateway/routes/mcp_apps.py``) and the sandbox document with its
policy (``nerve/gateway/mcp_apps_sandbox.py``). See docs/mcp-apps.md.

Everything a server sends is untrusted third-party data. Nothing here
interprets a view's HTML, and every size is bounded.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nerve import paths
from nerve.utils.fs import atomic_write_text

logger = logging.getLogger(__name__)

# The Claude CLI runs as an MCP Apps host when it starts with this variable
# set to "1": it tells MCP servers that the client shows ``ui://`` views, and
# keeps tools that are for a view only out of the model's tool list.
HOST_ENV = "CLAUDE_CODE_MCP_APPS_HOST"

UI_SCHEME = "ui://"
RESOURCE_MIME_TYPE = "text/html;profile=mcp-app"
# Nerve's own in-process MCP server. It is never part of MCP Apps: no view
# may call Nerve's tools.
NERVE_SERVER = "nerve"

DEFAULT_VISIBILITY = ("model", "app")

MAX_HTML_BYTES = 5 * 1024 * 1024
MAX_RESULT_BYTES = 2 * 1024 * 1024
# Set in ``_meta`` of a result that was cut to MAX_RESULT_BYTES.
TRUNCATED_META_KEY = "nerve/truncated"

MAX_CACHED_DOCUMENTS = 64

_MCP_NAME_UNSAFE = re.compile(r"[^a-zA-Z0-9_-]")
_LONE_SURROGATE = re.compile(r"[\ud800-\udfff]")
_CONTENT_TYPES = frozenset({"text", "image", "audio", "resource", "resource_link"})


class McpAppsError(Exception):
    """An MCP Apps request that cannot be carried out, with the HTTP status
    and the ``detail`` the routes answer."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


# --------------------------------------------------------------------------- #
#  Tools and their views                                                       #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class AppTool:
    """One tool of an MCP server, as an MCP Apps host sees it."""

    server: str                      # the server's name as Nerve configured it
    name: str                        # the tool's own name
    full_name: str                   # the name the model calls: mcp__<server>__<tool>
    resource_uri: str | None = None  # the ``ui://`` view the tool is linked to
    visibility: tuple[str, ...] = DEFAULT_VISIBILITY
    read_only: bool = False

    @property
    def app_visible(self) -> bool:
        """A view may call the tool."""
        return "app" in self.visibility

    def app(self) -> dict[str, str] | None:
        """The ``mcp_app`` field of a call of this tool, or None without a view."""
        if not self.resource_uri:
            return None
        return {"server": self.server, "tool": self.name, "resource_uri": self.resource_uri}

    def view(self) -> dict[str, Any]:
        """The tool as the tools route lists it."""
        return {
            "name": self.name,
            "read_only": self.read_only,
            "visibility": list(self.visibility),
            "resource_uri": self.resource_uri,
        }


@dataclass(frozen=True)
class AppServer:
    """One MCP server of a runtime: its connection status and its tools."""

    name: str
    status: str
    tools: tuple[AppTool, ...] = ()

    @property
    def connected(self) -> bool:
        return self.status == "connected"

    def tool(self, name: str) -> AppTool | None:
        return next((t for t in self.tools if t.name == name), None)


def mcp_tool_name(server: str, tool: str) -> str:
    """The name the model calls a server's tool by. The CLI replaces every
    character outside ``[a-zA-Z0-9_-]`` of both parts with ``_``."""
    return (
        f"mcp__{_MCP_NAME_UNSAFE.sub('_', server)}__{_MCP_NAME_UNSAFE.sub('_', tool)}"
    )


def is_third_party_tool(tool_name: str) -> bool:
    """A tool of an MCP server other than Nerve's own, by its name alone."""
    return tool_name.startswith("mcp__") and not tool_name.startswith(
        f"mcp__{NERVE_SERVER}__",
    )


def index_servers(status: Any) -> dict[str, AppServer]:
    """The MCP servers of a runtime's MCP status, by name.

    ``status`` is the Claude CLI's ``mcp_status`` answer:
    ``{"mcpServers": [{"name", "status", "config", "tools": [{"name",
    "annotations", "_meta"}]}]}``. Nerve's own server and other in-process
    (``sdk``) servers are left out. Entries of the wrong shape are skipped.
    """
    servers: dict[str, AppServer] = {}
    entries = status.get("mcpServers") if isinstance(status, dict) else None
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name or name == NERVE_SERVER:
            continue
        config = entry.get("config")
        if isinstance(config, dict) and config.get("type") == "sdk":
            continue
        raw_tools = entry.get("tools")
        tools = (
            tuple(
                tool for tool in (_app_tool(name, raw) for raw in raw_tools)
                if tool is not None
            )
            if isinstance(raw_tools, list) else ()
        )
        servers[name] = AppServer(
            name=name, status=str(entry.get("status") or ""), tools=tools,
        )
    return servers


def _app_tool(server: str, raw: Any) -> AppTool | None:
    if not isinstance(raw, dict):
        return None
    name = raw.get("name")
    if not isinstance(name, str) or not name:
        return None
    meta = raw.get("_meta")
    meta = meta if isinstance(meta, dict) else {}
    ui = meta.get("ui")
    ui = ui if isinstance(ui, dict) else {}
    # ``_meta["ui/resourceUri"]`` is the deprecated spelling of
    # ``_meta.ui.resourceUri``.
    uri = ui.get("resourceUri", meta.get("ui/resourceUri"))
    visibility = ui.get("visibility")
    annotations = raw.get("annotations")
    annotations = annotations if isinstance(annotations, dict) else {}
    return AppTool(
        server=server,
        name=name,
        full_name=mcp_tool_name(server, name),
        resource_uri=uri if isinstance(uri, str) and uri.startswith(UI_SCHEME) else None,
        visibility=(
            tuple(v for v in DEFAULT_VISIBILITY if v in visibility)
            if isinstance(visibility, list) else DEFAULT_VISIBILITY
        ),
        # The CLI reports the MCP ``readOnlyHint`` annotation as ``readOnly``.
        read_only=(
            annotations.get("readOnly") is True
            or annotations.get("readOnlyHint") is True
        ),
    )


def tools_by_full_name(servers: dict[str, AppServer]) -> dict[str, AppTool]:
    """The tools of ``servers`` by the name the model calls them.

    Two server names that differ only in characters the CLI replaces give
    their tools the same name. Such a name is left out: nothing says which of
    the two was called.
    """
    tools: dict[str, AppTool] = {}
    ambiguous: set[str] = set()
    for server in servers.values():
        for tool in server.tools:
            if tool.full_name in tools:
                ambiguous.add(tool.full_name)
            tools[tool.full_name] = tool
    for name in ambiguous:
        del tools[name]
    return tools


# --------------------------------------------------------------------------- #
#  Tool results                                                                #
# --------------------------------------------------------------------------- #


def call_tool_result(
    content: Any, *, structured: Any = None, is_error: bool = False, meta: Any = None,
) -> dict[str, Any]:
    """An MCP ``CallToolResult`` for a view, from what a runtime reports.

    ``content`` is a string, a list of content blocks (MCP's, or the model
    API's for images) or None; it always comes out as a list of MCP content
    blocks. ``structured`` and ``meta`` are kept when they are objects.
    The result has no unpaired surrogates and is at most MAX_RESULT_BYTES of
    JSON: beyond that ``structuredContent`` goes, then ``_meta``, then
    everything in ``content`` but the start of its text, and ``_meta`` says
    ``{"nerve/truncated": true}``.
    """
    result: dict[str, Any] = {"content": _content_blocks(content)}
    if isinstance(structured, dict):
        result["structuredContent"] = structured
    if is_error:
        result["isError"] = True
    if isinstance(meta, dict) and meta:
        result["_meta"] = meta
    return _capped(_without_lone_surrogates(result))


def tool_event_result(
    content: Any, structured: dict[str, Any] | None, is_error: bool,
) -> dict[str, Any]:
    """The ``CallToolResult`` of a tool call of a turn, from the backend's
    ``ToolResult`` event (``content``, ``structured``, ``is_error``).

    For an MCP tool the Claude CLI reports ``structured`` as
    ``{"content": <as the model got it>, "structuredContent"?, "_meta"?}``.
    Without it, the result is the event's own content.
    """
    if isinstance(structured, dict) and (
        "structuredContent" in structured or "content" in structured
    ):
        return call_tool_result(
            structured.get("content", content),
            structured=structured.get("structuredContent"),
            is_error=is_error,
            meta=structured.get("_meta"),
        )
    return call_tool_result(content, is_error=is_error)


def _content_blocks(content: Any) -> list[dict[str, Any]]:
    if content is None or content == "":
        return []
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if not isinstance(content, list):
        return [{"type": "text", "text": json.dumps(content, default=str)}]
    blocks: list[dict[str, Any]] = []
    for item in content:
        if isinstance(item, str):
            if item:
                blocks.append({"type": "text", "text": item})
            continue
        block = _content_block(item)
        if block is not None:
            blocks.append(block)
    return blocks


def _content_block(item: Any) -> dict[str, Any] | None:
    """``item`` as an MCP content block, or None when it is not one.

    A view validates what it is sent, so a block of another shape (the model
    API's ``tool_reference``, say) is dropped rather than passed on.
    """
    if not isinstance(item, dict) or item.get("type") not in _CONTENT_TYPES:
        return None
    kind = item["type"]
    if kind == "text":
        return item if isinstance(item.get("text"), str) else None
    if kind in ("image", "audio"):
        if isinstance(item.get("data"), str) and isinstance(item.get("mimeType"), str):
            return item
        # The model API's shape: {"source": {"type": "base64", "media_type", "data"}}.
        source = item.get("source")
        if (
            isinstance(source, dict)
            and source.get("type") == "base64"
            and isinstance(source.get("data"), str)
            and isinstance(source.get("media_type"), str)
        ):
            return {"type": kind, "data": source["data"], "mimeType": source["media_type"]}
        return None
    if kind == "resource":
        resource = item.get("resource")
        if isinstance(resource, dict) and isinstance(resource.get("uri"), str) and (
            isinstance(resource.get("text"), str) or isinstance(resource.get("blob"), str)
        ):
            return item
        return None
    # resource_link
    if isinstance(item.get("uri"), str) and isinstance(item.get("name"), str):
        return item
    return None


def _without_lone_surrogates(value: Any) -> Any:
    """``value`` with every unpaired surrogate replaced by U+FFFD.

    A runtime that cuts tool output in the middle of a character leaves one,
    and a string with one cannot be encoded as UTF-8 for a socket or a
    response.
    """
    if isinstance(value, str):
        return _LONE_SURROGATE.sub("�", value) if _LONE_SURROGATE.search(value) else value
    if isinstance(value, list):
        return [_without_lone_surrogates(v) for v in value]
    if isinstance(value, dict):
        return {
            _without_lone_surrogates(k) if isinstance(k, str) else k:
                _without_lone_surrogates(v)
            for k, v in value.items()
        }
    return value


def _json_size(value: Any) -> int:
    """Bytes of ``value`` as ASCII JSON, the largest of its serializations
    (the messages table stores that one)."""
    return len(json.dumps(value, default=str))


def _capped(result: dict[str, Any]) -> dict[str, Any]:
    if _json_size(result) <= MAX_RESULT_BYTES:
        return result
    meta = result.get("_meta")
    capped = {k: v for k, v in result.items() if k not in ("structuredContent", "_meta")}
    flag = {TRUNCATED_META_KEY: True}
    for kept_meta in ({**meta, **flag} if isinstance(meta, dict) else flag, flag):
        capped["_meta"] = kept_meta
        if _json_size(capped) <= MAX_RESULT_BYTES:
            return capped
    # The content alone is over the limit: keep the start of its text.
    text = "".join(
        block["text"] for block in capped["content"] if block.get("type") == "text"
    )
    capped["content"] = []
    budget = MAX_RESULT_BYTES - _json_size(capped) - 64
    while text and _json_size(text) > budget:
        text = text[: len(text) // 2]
    if text:
        capped["content"] = [{"type": "text", "text": text}]
    return capped


# --------------------------------------------------------------------------- #
#  View documents                                                              #
# --------------------------------------------------------------------------- #


def app_document(read_result: Any, uri: str) -> dict[str, Any]:
    """The view document in a ``resources/read`` result, as the resource
    route answers it (without ``cached``).

    Follows the reference host: exactly one content item, of the MCP Apps
    MIME type, with its HTML as ``text`` or as base64 in ``blob``. ``csp``,
    ``permissions`` and ``prefersBorder`` come from the item's ``_meta.ui``.
    Raises :class:`McpAppsError` (502) for anything else and for a document
    over MAX_HTML_BYTES.
    """
    contents = read_result.get("contents") if isinstance(read_result, dict) else None
    if not isinstance(contents, list) or len(contents) != 1 or not isinstance(contents[0], dict):
        raise McpAppsError(502, "the server did not return one document for this resource")
    item = contents[0]
    mime = item.get("mimeType")
    if not isinstance(mime, str) or "".join(mime.split()).lower() != RESOURCE_MIME_TYPE:
        raise McpAppsError(
            502, f"the resource is not an MCP App document ({RESOURCE_MIME_TYPE}): {mime!r}",
        )
    text, blob = item.get("text"), item.get("blob")
    if isinstance(text, str):
        html = text
    elif isinstance(blob, str):
        # Base64 is 4 characters for 3 bytes: refuse before decoding.
        if len(blob) > MAX_HTML_BYTES * 4 // 3 + 4:
            raise _too_large()
        try:
            html = base64.b64decode("".join(blob.split()), validate=True).decode("utf-8")
        except (binascii.Error, ValueError) as e:
            raise McpAppsError(502, f"the document is not base64 of UTF-8 text: {e}") from e
    else:
        raise McpAppsError(502, "the document has no content")
    html = _without_lone_surrogates(html)
    if len(html.encode("utf-8")) > MAX_HTML_BYTES:
        raise _too_large()

    # ``meta`` without the underscore is what the Python MCP SDK has sent.
    meta = item.get("_meta") or item.get("meta")
    ui = meta.get("ui") if isinstance(meta, dict) else None
    ui = ui if isinstance(ui, dict) else {}
    csp, permissions, border = ui.get("csp"), ui.get("permissions"), ui.get("prefersBorder")
    item_uri = item.get("uri")
    return {
        "uri": item_uri if isinstance(item_uri, str) and item_uri else uri,
        "mime_type": RESOURCE_MIME_TYPE,
        "html": html,
        "csp": _without_lone_surrogates(csp) if isinstance(csp, dict) else None,
        "permissions": (
            _without_lone_surrogates(permissions) if isinstance(permissions, dict) else None
        ),
        "prefers_border": border if isinstance(border, bool) else None,
    }


def _too_large() -> McpAppsError:
    return McpAppsError(
        502, f"the app document is larger than {MAX_HTML_BYTES // (1024 * 1024)} MiB",
    )


# --------------------------------------------------------------------------- #
#  Document cache                                                              #
# --------------------------------------------------------------------------- #
#
# A reloaded page shows the views of a session whose runtime is gone (after a
# restart, or once the idle sweep closed it), so the last document read for a
# server and URI is kept on disk. Small on purpose: at most
# MAX_CACHED_DOCUMENTS files of at most MAX_HTML_BYTES of HTML each.

_DOCUMENT_KEYS = ("uri", "mime_type", "html", "csp", "permissions", "prefers_border")


def documents_dir() -> Path:
    return paths.cache_dir() / "mcp_apps"


def _document_file(server: str, uri: str) -> Path:
    key = hashlib.sha256(f"{server}\x00{uri}".encode("utf-8", "surrogatepass")).hexdigest()
    return documents_dir() / f"{key}.json"


def load_document(server: str, uri: str) -> dict[str, Any] | None:
    """The cached document of ``server`` and ``uri``, or None. Never raises."""
    try:
        entry = json.loads(_document_file(server, uri).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    document = entry.get("document") if isinstance(entry, dict) else None
    if (
        not isinstance(document, dict)
        or entry.get("server") != server
        or entry.get("uri") != uri
        or not isinstance(document.get("html"), str)
    ):
        return None
    return {key: document.get(key) for key in _DOCUMENT_KEYS}


def store_document(server: str, uri: str, document: dict[str, Any]) -> None:
    """Keep ``document`` as the cached copy for ``server`` and ``uri``.

    A copy that is already the same is only marked as used: every view on a
    page reads its document, and that must not be megabytes written each
    time. Best-effort: a cache that cannot be written is logged and ignored.
    """
    directory = documents_dir()
    file = _document_file(server, uri)
    kept = {key: document.get(key) for key in _DOCUMENT_KEYS}
    try:
        if load_document(server, uri) == kept:
            os.utime(file)
            return
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_text(
            file,
            json.dumps({
                "server": server,
                "uri": uri,
                "fetched_at": int(time.time()),
                "document": kept,
            }),
        )
        _sweep_documents(directory)
    except (OSError, ValueError) as e:
        logger.debug("MCP Apps: could not cache the document of %r %r: %s", server, uri, e)


def _sweep_documents(directory: Path) -> None:
    """Keep the MAX_CACHED_DOCUMENTS newest documents. Symlinks and other
    entries are not touched."""
    files: list[tuple[float, str]] = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.name.endswith(".json") and entry.is_file(follow_symlinks=False):
                files.append((entry.stat(follow_symlinks=False).st_mtime, entry.path))
    for _, path in sorted(files, reverse=True)[MAX_CACHED_DOCUMENTS:]:
        try:
            os.unlink(path)
        except OSError:
            pass
