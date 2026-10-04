"""anthropic_base_url: one Anthropic-compatible endpoint for every model request."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from nerve.agent.backends.claude import ClaudeBackend
from nerve.config import NerveConfig, load_config

ENDPOINT = "http://10.200.0.1:3128"


def _cli_env(cfg: NerveConfig) -> dict[str, str]:
    return ClaudeBackend(SimpleNamespace(config=lambda: cfg))._build_env()


class TestDefault:
    """Without the key, every caller keeps today's endpoint."""

    def test_direct_anthropic(self) -> None:
        cfg = NerveConfig.from_dict({"anthropic_api_key": "sk-ant-test"})
        assert cfg.anthropic_base_url == ""
        assert cfg.anthropic_api_base_url == "https://api.anthropic.com/v1/"
        assert str(cfg.create_anthropic_client().base_url).rstrip("/") == (
            "https://api.anthropic.com"
        )
        env = _cli_env(cfg)
        assert "ANTHROPIC_BASE_URL" not in env
        assert env["ANTHROPIC_API_KEY"] == "sk-ant-test"

    def test_proxy(self) -> None:
        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "host": "10.0.0.5", "port": 4000},
        })
        assert cfg.anthropic_api_base_url == "http://10.0.0.5:4000/v1/"
        assert _cli_env(cfg)["ANTHROPIC_BASE_URL"] == "http://10.0.0.5:4000"


class TestEndpoint:
    def test_every_client_uses_it(self) -> None:
        cfg = NerveConfig.from_dict({
            "anthropic_base_url": ENDPOINT,
            "anthropic_api_key": "placeholder",
        })
        assert cfg.anthropic_api_base_url == f"{ENDPOINT}/v1/"
        assert str(cfg.create_anthropic_client().base_url).rstrip("/") == ENDPOINT
        assert (
            str(cfg.create_async_anthropic_client().base_url).rstrip("/") == ENDPOINT
        )
        env = _cli_env(cfg)
        assert env["ANTHROPIC_BASE_URL"] == ENDPOINT
        assert env["ANTHROPIC_API_KEY"] == "placeholder"

    @pytest.mark.parametrize(
        ("raw", "stored"),
        [
            (ENDPOINT, ENDPOINT),
            (f"{ENDPOINT}/", ENDPOINT),
            (f"{ENDPOINT}/v1", ENDPOINT),
            (f"{ENDPOINT}/v1/", ENDPOINT),
            ("  https://gw.example.com/anthropic/v1/ ", "https://gw.example.com/anthropic"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_normalized(self, raw: str | None, stored: str) -> None:
        cfg = NerveConfig.from_dict({"anthropic_base_url": raw})
        assert cfg.anthropic_base_url == stored

    @pytest.mark.parametrize(
        "raw",
        [
            "gw.example.com",
            "ftp://gw.example.com",
            "http://",
            "http://:3128",
            "https://user:secret@gw.example.com",
            "https://gw.example.com/?key=1",
            "https://gw.example.com/#top",
        ],
    )
    def test_rejected(self, raw: str) -> None:
        with pytest.raises(ValueError, match="anthropic_base_url"):
            NerveConfig.from_dict({"anthropic_base_url": raw})

    def test_not_with_proxy(self) -> None:
        with pytest.raises(ValueError, match="proxy.enabled"):
            NerveConfig.from_dict({
                "anthropic_base_url": ENDPOINT,
                "proxy": {"enabled": True},
            })

    def test_not_with_bedrock(self) -> None:
        with pytest.raises(ValueError, match="bedrock"):
            NerveConfig.from_dict({
                "anthropic_base_url": ENDPOINT,
                "provider": {"type": "bedrock"},
            })

    def test_from_local_layer(self, tmp_path: Path) -> None:
        (tmp_path / "config.yaml").write_text("workspace: ~/ws\n")
        (tmp_path / "config.local.yaml").write_text(yaml.dump({
            "anthropic_api_key": "placeholder",
            "anthropic_base_url": f"{ENDPOINT}/",
        }))
        assert load_config(tmp_path).anthropic_base_url == ENDPOINT


class _Recorder(BaseHTTPRequestHandler):
    """Answers the Messages and chat completions APIs, and records each path."""

    paths: list[str] = []

    def do_POST(self) -> None:  # noqa: N802 - the http.server name
        self.rfile.read(int(self.headers.get("content-length", "0")))
        type(self).paths.append(self.path)
        if self.path.endswith("/chat/completions"):
            body = {
                "id": "c", "object": "chat.completion", "created": 0, "model": "m",
                "choices": [{
                    "index": 0, "finish_reason": "stop",
                    "message": {"role": "assistant", "content": "ok"},
                }],
            }
        else:
            body = {
                "id": "m", "type": "message", "role": "assistant", "model": "m",
                "content": [{"type": "text", "text": "ok"}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(name, raising=False)
    _Recorder.paths = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/prefix"
    finally:
        server.shutdown()
        server.server_close()


class TestRequestPaths:
    """Requests reach the endpoint, under its path prefix, with one /v1."""

    def test_messages(self, recorder: str) -> None:
        cfg = NerveConfig.from_dict({
            "anthropic_base_url": recorder, "anthropic_api_key": "placeholder",
        })
        cfg.create_anthropic_client().messages.create(
            model="m", max_tokens=1, messages=[{"role": "user", "content": "hi"}],
        )
        assert _Recorder.paths == ["/prefix/v1/messages"]

    @pytest.mark.asyncio
    async def test_memu_chat(self, recorder: str) -> None:
        from memu.llm.openai_sdk import OpenAISDKClient

        cfg = NerveConfig.from_dict({
            "anthropic_base_url": recorder, "anthropic_api_key": "placeholder",
        })
        # The same base URL and key the memU bridge gives its chat profiles.
        client = OpenAISDKClient(
            base_url=cfg.anthropic_api_base_url,
            api_key=cfg.effective_api_key,
            chat_model="m",
            embed_model="e",
        )
        text, _ = await client.chat("hi")
        assert text == "ok"
        assert _Recorder.paths == ["/prefix/v1/chat/completions"]
