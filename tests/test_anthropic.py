"""Tests for Anthropic model discovery (``nerve/anthropic.py``)."""

from __future__ import annotations

import io
import json
import urllib.error
from unittest.mock import patch

from nerve.anthropic import discover_models, latest_per_family


class _FakeResponse(io.BytesIO):
    """Minimal stand-in for the ``urlopen`` context-manager response."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _response(payload) -> _FakeResponse:
    return _FakeResponse(json.dumps(payload).encode("utf-8"))


def test_discover_models_requires_api_key():
    assert discover_models("") == []


def test_discover_models_parses_ids_in_api_order():
    payload = {
        "data": [
            {"id": "claude-sonnet-5", "display_name": "Claude Sonnet 5"},
            {"id": "claude-fable-5", "display_name": "Claude Fable 5"},
            {"id": "claude-fable-5"},          # duplicate dropped
            {"display_name": "no id, skipped"},
        ],
        "has_more": False,
    }
    with patch(
        "nerve.anthropic.urllib.request.urlopen", return_value=_response(payload),
    ):
        assert discover_models("sk-test") == ["claude-sonnet-5", "claude-fable-5"]


def test_discover_models_swallows_network_errors():
    with patch(
        "nerve.anthropic.urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        assert discover_models("sk-test") == []


def test_discover_models_tolerates_malformed_payload():
    with patch(
        "nerve.anthropic.urllib.request.urlopen", return_value=_response([1, 2]),
    ):
        assert discover_models("sk-test") == []


def test_latest_per_family_keeps_newest_of_each_family():
    ids = [
        "claude-sonnet-5",
        "claude-fable-5",
        "claude-opus-4-8",
        "claude-opus-4-7",
        "claude-sonnet-4-6",
        "claude-haiku-4-5-20251001",
        "claude-opus-4-1-20250805",
    ]
    assert latest_per_family(ids) == [
        "claude-sonnet-5",
        "claude-fable-5",
        "claude-opus-4-8",
        "claude-haiku-4-5-20251001",
    ]


def test_latest_per_family_handles_legacy_version_first_ids():
    ids = [
        "claude-3-5-sonnet-20241022",
        "claude-3-sonnet-20240229",
        "claude-3-haiku-20240307",
    ]
    assert latest_per_family(ids) == [
        "claude-3-5-sonnet-20241022",
        "claude-3-haiku-20240307",
    ]


def test_latest_per_family_keeps_unrecognizable_ids():
    assert latest_per_family(["weird-id-123", "claude-opus-4-8"]) == [
        "weird-id-123",
        "claude-opus-4-8",
    ]
