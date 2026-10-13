"""Text from direct Messages API responses (nerve.utils.llm)."""

from types import SimpleNamespace

import pytest

from nerve.sources.models import SourceRecord
from nerve.sources.runner import SourceRunner
from nerve.utils.llm import response_text


def _thinking() -> SimpleNamespace:
    # A thinking block has no ``text`` attribute.
    return SimpleNamespace(type="thinking", thinking="", signature="sig")


def _text(value: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=value)


def test_skips_thinking_blocks_before_the_text():
    response = SimpleNamespace(content=[_thinking(), _text("Short title")])
    assert response_text(response) == "Short title"


def test_joins_text_blocks():
    response = SimpleNamespace(content=[_text("a"), _thinking(), _text("b")])
    assert response_text(response) == "ab"


@pytest.mark.parametrize(
    "response",
    [
        SimpleNamespace(content=[_thinking()]),
        SimpleNamespace(content=[]),
        SimpleNamespace(content=None),
        SimpleNamespace(),
    ],
)
def test_empty_when_there_is_no_text(response):
    assert response_text(response) == ""


class _FakeClient:
    def __init__(self, response):
        self.messages = SimpleNamespace(create=self._create)
        self._response = response

    async def _create(self, **kwargs):
        return self._response


def _runner(response) -> SourceRunner:
    source = SimpleNamespace(source_name="test-condense")
    return SourceRunner(
        source, db=None, condense=True, condense_model="claude-haiku-5-5",
        condense_client_factory=lambda: _FakeClient(response),
    )


def _long_record() -> SourceRecord:
    return SourceRecord(
        id="r1", source="test-condense", record_type="test", summary="s",
        content="x" * 1000, timestamp="2026-10-13T00:00:00Z",
    )


@pytest.mark.asyncio
async def test_condense_uses_the_text_after_thinking():
    response = SimpleNamespace(
        content=[_thinking(), _text("condensed")], stop_reason="end_turn",
    )
    records = await _runner(response)._condense_long_content([_long_record()])
    assert records[0].content == "condensed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        SimpleNamespace(content=[_thinking()], stop_reason="max_tokens"),
        SimpleNamespace(
            content=[_thinking(), _text("cut")], stop_reason="max_tokens",
        ),
    ],
)
async def test_condense_keeps_the_original_without_complete_text(response):
    records = await _runner(response)._condense_long_content([_long_record()])
    assert records[0].content == "x" * 1000
