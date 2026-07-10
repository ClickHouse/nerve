"""reverse_apply_unified_diff — pre-image reconstruction unit tests."""

from __future__ import annotations

import difflib

import pytest

from nerve.agent.backends.codex.diffs import reverse_apply_unified_diff


def _diff(before: str, after: str) -> str:
    return "".join(difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile="a", tofile="b",
    ))


@pytest.mark.parametrize("before,after", [
    ("ORIGINAL\n", "CHANGED\n"),
    ("a\nb\nc\n", "a\nB\nc\n"),
    ("a\nb\nc\n", "a\nb\nc\nd\n"),                    # pure addition
    ("a\nb\nc\nd\n", "a\nd\n"),                       # deletion
    ("one\ntwo\nthree\nfour\nfive\nsix\nseven\n",
     "one\nTWO\nthree\nfour\nFIVE\nsix\nseven\nEIGHT\n"),  # multi-hunk
    ("", "hello\nworld\n"),                           # created content
    ("x\n" * 50, "x\n" * 20 + "y\n" + "x\n" * 30),    # mid-file insert
])
def test_roundtrip(before: str, after: str):
    diff = _diff(before, after)
    assert reverse_apply_unified_diff(diff, after) == before


def test_wrong_after_text_fails_verification():
    diff = _diff("ORIGINAL\n", "CHANGED\n")
    # Pre-apply timing: disk still holds the before-text — the reverse
    # must FAIL (caller then uses the disk content directly).
    assert reverse_apply_unified_diff(diff, "ORIGINAL\n") is None
    assert reverse_apply_unified_diff(diff, "SOMETHING ELSE\n") is None


def test_garbage_and_empty_diffs_fail_closed():
    assert reverse_apply_unified_diff("", "x\n") is None
    assert reverse_apply_unified_diff("not a diff at all", "x\n") is None
    assert reverse_apply_unified_diff("--- a\n+++ b\n", "x\n") is None  # headers only


def test_git_style_headers_are_tolerated():
    before, after = "a\nb\n", "a\nc\n"
    diff = (
        "diff --git a/f b/f\nindex 000..111 100644\n"
        + _diff(before, after)
    )
    assert reverse_apply_unified_diff(diff, after) == before
