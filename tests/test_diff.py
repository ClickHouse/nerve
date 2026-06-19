"""Tests for unified-diff computation, including the git-style ``patch`` string
consumed by the @pierre/diffs frontend renderer."""

from __future__ import annotations

import difflib

from nerve.gateway.diff import (
    MAX_DIFF_LINES,
    _build_patch,
    compute_file_diff,
)


def test_modified_file_patch_is_git_style():
    orig = "line1\nline2\nline3\nline4\n"
    curr = "line1\nline2 changed\nline3\nline4\nline5\n"
    d = compute_file_diff(orig, curr, "/ws/src/app.py", workspace="/ws")

    assert d["status"] == "modified"
    assert d["short_path"] == "src/app.py"
    patch = d["patch"]
    # git-style header lets the parser strip a/ b/ prefixes
    assert patch.startswith("diff --git a/src/app.py b/src/app.py\n")
    assert "--- a/src/app.py\n" in patch
    assert "+++ b/src/app.py\n" in patch
    # hunk + the actual change
    assert "@@ -1,4 +1,5 @@" in patch
    assert "-line2\n" in patch
    assert "+line2 changed\n" in patch
    assert "+line5\n" in patch
    assert not d["truncated"]


def test_created_file_patch():
    d = compute_file_diff(None, "a\nb\nc\n", "/ws/new.ts", workspace="/ws")

    assert d["status"] == "created"
    patch = d["patch"]
    assert "diff --git a/new.ts b/new.ts\n" in patch
    assert "new file mode 100644\n" in patch
    assert "--- /dev/null\n" in patch
    assert "+++ b/new.ts\n" in patch
    assert patch.count("\n+") >= 3  # additions present


def test_deleted_file_patch():
    d = compute_file_diff("x\ny\n", None, "/ws/gone.go", workspace="/ws")

    assert d["status"] == "deleted"
    patch = d["patch"]
    assert "diff --git a/gone.go b/gone.go\n" in patch
    assert "deleted file mode 100644\n" in patch
    assert "--- a/gone.go\n" in patch
    assert "+++ /dev/null\n" in patch
    assert "-x\n" in patch and "-y\n" in patch


def test_unchanged_file_has_empty_patch():
    d = compute_file_diff("same\n", "same\n", "/ws/same.py", workspace="/ws")
    assert d["status"] == "unchanged"
    assert d["patch"] == ""
    assert d["hunks"] == []


def test_patch_filename_carries_extension_for_language_inference():
    # Language inference in the renderer relies on the extension surviving in
    # the patch header.
    d = compute_file_diff("import x\n", "import y\n", "/ws/a/b/Component.tsx", workspace="/ws")
    assert "b/a/b/Component.tsx\n" in d["patch"]


def test_build_patch_truncates_on_hunk_boundary():
    # Two well-separated hunks; budget admits only the first.
    orig = "".join(f"{c}\n" for c in "abcdefghij")
    curr = orig.replace("b\n", "B\n").replace("i\n", "I\n")
    raw = list(
        difflib.unified_diff(
            orig.splitlines(keepends=True),
            curr.splitlines(keepends=True),
            fromfile="a/x.py",
            tofile="b/x.py",
            n=1,
        )
    )
    patch, truncated = _build_patch(raw, "x.py", max_content_lines=3)

    assert truncated is True
    # first hunk (b -> B) kept, second hunk (i -> I) dropped
    assert "+B\n" in patch
    assert "+I\n" not in patch
    # still a valid patch: header + exactly one hunk
    assert patch.startswith("diff --git a/x.py b/x.py\n")
    assert patch.count("@@ -") == 1


def test_build_patch_keeps_first_hunk_even_if_over_budget():
    # A single oversized hunk must still be emitted whole (never an empty patch).
    orig = "".join(f"{i}\n" for i in range(20))
    curr = "".join(f"X{i}\n" for i in range(20))
    raw = list(
        difflib.unified_diff(
            orig.splitlines(keepends=True),
            curr.splitlines(keepends=True),
            fromfile="a/x.py",
            tofile="b/x.py",
            n=3,
        )
    )
    patch, _ = _build_patch(raw, "x.py", max_content_lines=1)
    assert "@@ -" in patch
    assert "+X0\n" in patch


def test_large_created_file_truncated_flag():
    content = "".join(f"line {i}\n" for i in range(MAX_DIFF_LINES + 50))
    d = compute_file_diff(None, content, "/ws/big.txt", workspace="/ws")
    assert d["truncated"] is True
    assert d["patch"]  # still produced
