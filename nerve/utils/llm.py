"""Helpers for direct Anthropic Messages API calls."""

from __future__ import annotations

from typing import Any


def response_text(response: Any) -> str:
    """Return the text of a Messages API response, with its text blocks joined.

    A model with adaptive thinking (the Claude 5.5 models, for example) can
    put thinking blocks before the text, so the first block is not always a
    text block. The result is empty when the response has no text, for
    example when thinking used all of ``max_tokens``.
    """
    return "".join(
        text
        for block in getattr(response, "content", None) or []
        if isinstance(text := getattr(block, "text", None), str)
    )
