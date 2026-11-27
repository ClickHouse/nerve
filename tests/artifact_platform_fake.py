"""A fake of the platform side of an artifact publish, for the tests.

``FakePlatform`` answers JSON-RPC tool calls as the artifacts MCP server
does through the MCP gateway (the wire shapes of its tools), and
``FakeStore`` checks each upload as the object store checks an upload link:
the signed ``Content-Length`` (not for an empty file), the
``x-amz-checksum-sha256`` (base64 of the SHA-256 of the body), the
``If-None-Match: *``, and no second write of a key (412). Both are httpx
transports. All hosts and IDs are invented.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import quote

import httpx

GATEWAY_URL = "http://192.0.2.1:3130"
ARTIFACTS_URL = f"{GATEWAY_URL}/s/nerve-artifacts/mcp"
STORE = "https://store.example.test"
VIEWER = "https://artifacts.example.test"
TENANT = "0b9d6c1e-5c3f-4e43-9f0a-1d2c3b4a5f60"

INVALID_ARGUMENTS_TEXT = "The arguments are not valid. See the tool's input schema."


@dataclass
class Link:
    key: str
    size: int
    sha256: str
    used: bool = False


@dataclass
class Version:
    artifact_id: str
    number: int
    title: str
    entry: str
    files: dict[str, tuple[int, str]]
    state: str = "pending"


@dataclass
class FakePlatform:
    store: "FakeStore | None" = None
    batch: int = 100
    # name -> function(args) that returns a result object, or a refusal dict
    # (``{"reason": ...}``), or None for the normal answer.
    hooks: dict[str, Callable[[dict], Any]] = field(default_factory=dict)
    calls: list[tuple[str, dict]] = field(default_factory=list)
    versions: dict[str, Version] = field(default_factory=dict)
    links: dict[str, Link] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    # Structured content that is not the text (to show that the text is read).
    lying_structure: bool = False

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]

    def args(self, name: str) -> list[dict]:
        return [args for called, args in self.calls if called == name]

    # --- JSON-RPC ---------------------------------------------------------

    def __call__(self, request: httpx.Request) -> httpx.Response:
        message = json.loads(request.content)
        name = message["params"]["name"]
        args = message["params"]["arguments"]
        self.calls.append((name, args))
        hook = self.hooks.get(name)
        outcome = hook(args) if hook else None
        if isinstance(outcome, httpx.Response):
            return outcome
        if outcome is None:
            outcome = getattr(self, f"_{name}")(args)
        if isinstance(outcome, str):  # a fixed text of the server
            result = {"isError": True, "content": [{"type": "text", "text": outcome}]}
        elif "reason" in outcome:
            result = {
                "isError": True, "content": [{"type": "text", "text": json.dumps(outcome)}],
                "structuredContent": outcome,
            }
        else:
            structured = {"note": "not the text"} if self.lying_structure else outcome
            result = {"content": [{"type": "text", "text": json.dumps(outcome)}], "structuredContent": structured}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": message["id"], "result": result})

    # --- Tools ------------------------------------------------------------

    def _begin_artifact_version(self, args: dict) -> Any:
        allowed = {"title", "entry", "files", "artifact_id", "on_behalf_of"}
        if set(args) - allowed or any(value is None for value in args.values()):
            return INVALID_ARGUMENTS_TEXT
        if "on_behalf_of" in args and not isinstance(args["on_behalf_of"], str):
            return INVALID_ARGUMENTS_TEXT
        files = {f["path"]: (f["size"], f["sha256"]) for f in args["files"]}
        if args["entry"] not in files:
            return {"reason": "invalid_entry", "message": "The entry is not a file of the version."}
        artifact_id = args.get("artifact_id") or str(uuid.uuid4())
        number = self.counts.get(artifact_id, 0) + 1
        self.counts[artifact_id] = number
        version_id = str(uuid.uuid4())
        self.versions[version_id] = Version(artifact_id, number, args["title"], args["entry"], files)
        return {"artifact_id": artifact_id, "version_id": version_id, "version": number, "link_batch_size": self.batch}

    def _get_artifact_upload_links(self, args: dict) -> Any:
        version = self.versions.get(args["version_id"])
        if version is None:
            return {"reason": "not_found", "message": "No such version."}
        if len(args["paths"]) > self.batch:
            return {"reason": "limit_upload_paths", "message": "Too many paths."}
        links = []
        for path in args["paths"]:
            size, sha = version.files[path]
            key = f"/bucket/t/{TENANT}/a/{version.artifact_id}/v/{args['version_id']}/f/{quote(path)}"
            signature = secrets.token_hex(16)
            self.links[signature] = Link(key=key, size=size, sha256=sha)
            headers = {"If-None-Match": "*", "x-amz-checksum-sha256": base64.b64encode(bytes.fromhex(sha)).decode()}
            if size:
                headers["Content-Length"] = str(size)
            links.append({
                "path": path,
                "url": f"{STORE}{key}?X-Amz-Expires=900&X-Amz-Signature={signature}",
                "headers": headers,
            })
        expires = datetime.now(timezone.utc) + timedelta(minutes=15)
        return {"links": links, "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}

    def _complete_artifact_version(self, args: dict) -> Any:
        version = self.versions.get(args["version_id"])
        if version is None:
            return {"reason": "not_found", "message": "No such version."}
        if version.state != "pending":
            return {"reason": "version_not_pending", "message": "The version is not pending."}
        missing = [
            path for path, (size, _) in version.files.items()
            if self.store is None or len(self.store.objects.get(self._key(args["version_id"], path), b"x" * (size + 1))) != size
        ]
        if missing:
            return {
                "reason": "upload_incomplete",
                "message": "Some files are missing or have another size. Upload them and complete again.",
                "paths": missing[:20],
            }
        version.state = "complete"
        return {
            "artifact_id": version.artifact_id, "version": version.number,
            "viewer_url": f"{VIEWER}/t/{TENANT}/a/{version.artifact_id}",
            "expires_at": "2026-12-25T10:00:00Z",
        }

    def _key(self, version_id: str, path: str) -> str:
        version = self.versions[version_id]
        return f"/bucket/t/{TENANT}/a/{version.artifact_id}/v/{version_id}/f/{quote(path)}"


@dataclass
class FakeStore:
    platform: FakePlatform
    objects: dict[str, bytes] = field(default_factory=dict)
    # Each upload: the headers and the body length.
    requests: list[httpx.Request] = field(default_factory=list)
    # Called with each request before the checks; a response it gives is the answer.
    hook: Callable[[httpx.Request], Any] | None = None

    def __post_init__(self) -> None:
        self.platform.store = self

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        self.requests.append(request)
        if self.hook is not None:
            answer = self.hook(request)
            if hasattr(answer, "__await__"):
                answer = await answer
            if answer is not None:
                return answer
        if request.method != "PUT":
            return httpx.Response(405)
        link = self.platform.links.get(request.url.params.get("X-Amz-Signature", ""))
        if link is None or request.url.path != link.key:
            return httpx.Response(403, text="SignatureDoesNotMatch")
        if request.headers.get("If-None-Match") != "*":
            return httpx.Response(403, text="SignatureDoesNotMatch")
        length = request.headers.get("Content-Length")
        if length is None or int(length) != len(body) or (link.size and int(length) != link.size):
            return httpx.Response(403, text="SignatureDoesNotMatch")
        expected = base64.b64encode(bytes.fromhex(link.sha256)).decode()
        if request.headers.get("x-amz-checksum-sha256") != expected:
            return httpx.Response(403, text="SignatureDoesNotMatch")
        if base64.b64encode(hashlib.sha256(body).digest()).decode() != expected:
            return httpx.Response(400, text="BadDigest")
        if link.key in self.objects:
            return httpx.Response(412, text="PreconditionFailed")
        self.objects[link.key] = body
        link.used = True
        return httpx.Response(200)
