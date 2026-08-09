"""The system prompt must never reach a command line, at any size.

Nerve assembles the operator's identity and memory files plus TOOLS.md —
an index of where the host's credentials live — into every session's
system prompt. argv is world-readable: a single unprivileged ``ps -ww``
reads it out of every running session at once. So the prompt travels out
of band, and the file it travels in is a secret's file: 0600, in a 0700
directory, written atomically so the mode is in place before the bytes
are.

These tests pin the transport itself (through the SDK's own argv builder,
not a shape assertion on our options object), the permissions, and the
upgrade path for installs that already spilled prompts at 0644.
"""

import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import ClaudeBackend
from nerve.config import NerveConfig

# A string that appears nowhere else, standing in for the real bundle.
MARKER = "SENTINEL-a-credential-index-and-a-home-address"


def _backend(tmp_path: Path) -> ClaudeBackend:
    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    return ClaudeBackend(SimpleNamespace(
        config=lambda: cfg,
        claude_plugins=lambda: [],
    ))


def _spec(tmp_path: Path, prompt: str, session_id: str = "sess-1234") -> SessionSpec:
    return SessionSpec(
        session_id=session_id,
        source="web",
        model="claude-opus-5",
        effort="high",
        system_prompt=prompt,
        cwd=str(tmp_path),
    )


def _build(backend: ClaudeBackend, spec: SessionSpec):
    with patch.object(backend, "_build_mcp_servers", return_value={}), \
         patch.object(backend, "_build_hooks", return_value={}):
        return backend._build_options(spec)


def _prompt_dir(tmp_path: Path) -> Path:
    return tmp_path / ".nerve" / "cache" / "system_prompts"


# --------------------------------------------------------------------- #
#  Transport                                                             #
# --------------------------------------------------------------------- #


@pytest.mark.parametrize("size", [0, 1, 500, 100_000, 150_000])
def test_prompt_never_inline_at_any_size(tmp_path, size):
    """No threshold, no inline branch — every size takes the file path.

    The old code inlined anything under 100 KB, which put the whole
    bundle in argv for most sessions and made which-leak-you-get a
    function of incidental prompt drift.
    """
    prompt = MARKER + "x" * size
    options = _build(_backend(tmp_path), _spec(tmp_path, prompt))

    assert isinstance(options.system_prompt, dict)
    assert options.system_prompt["type"] == "file"
    assert Path(options.system_prompt["path"]).read_text(encoding="utf-8") == prompt


def test_sdk_argv_carries_the_path_not_the_prompt(tmp_path):
    """End-to-end through the SDK's own argv builder.

    Asserting the shape of our options dict only proves we asked for the
    file transport. This proves the SDK honors it — that the bytes the
    kernel sees are a path, and that nothing else in the options assembly
    smuggles the prompt onto the command line by another route.
    """
    from claude_agent_sdk._internal.transport.subprocess_cli import (
        SubprocessCLITransport,
    )

    prompt = MARKER + "\n" + "line of private context\n" * 5000
    backend = _backend(tmp_path)
    options = _build(backend, _spec(tmp_path, prompt))
    options.cli_path = "/nonexistent/claude"  # skip the CLI discovery walk

    argv = SubprocessCLITransport(prompt="hi", options=options)._build_command()

    assert "--system-prompt-file" in argv
    assert "--system-prompt" not in argv
    # The prompt — or any fragment of it — must appear in no argument.
    joined = "\x00".join(argv)
    assert MARKER not in joined
    assert "line of private context" not in joined
    # And the whole command line stays small: a path, not a bundle.
    assert len(joined) < 4096 < len(prompt)


def test_codex_backend_keeps_the_prompt_out_of_argv(tmp_path):
    """The other backend satisfies the same invariant, differently.

    Codex sends the prompt as ``developerInstructions`` inside the
    app-server JSON payload. Pinned here so the two backends cannot
    silently drift apart on whether the prompt is a secret.
    """
    from nerve.agent.backends.codex.backend import CodexBackend

    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    backend = CodexBackend(SimpleNamespace(config=lambda: cfg))
    params = backend.thread_params(_spec(tmp_path, MARKER))

    assert MARKER in params["developerInstructions"]
    # Everything else in the payload is a scalar knob; none of it is argv,
    # and none of it repeats the prompt.
    assert not any(
        MARKER in str(v) for k, v in params.items() if k != "developerInstructions"
    )


# --------------------------------------------------------------------- #
#  Permissions                                                           #
# --------------------------------------------------------------------- #


def test_spill_file_and_dir_are_owner_only(tmp_path):
    backend = _backend(tmp_path)
    options = _build(backend, _spec(tmp_path, MARKER))

    path = Path(options.system_prompt["path"])
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def test_mode_survives_a_permissive_umask(tmp_path):
    """0600 by construction, not by inheriting a strict umask.

    ``open(path, "w")`` under ``umask 000`` yields 0666. If the mode came
    from the umask this test would fail on exactly the hosts where it
    matters most.
    """
    backend = _backend(tmp_path)
    old = os.umask(0o000)
    try:
        options = _build(backend, _spec(tmp_path, MARKER))
    finally:
        os.umask(old)

    assert Path(options.system_prompt["path"]).stat().st_mode & 0o777 == 0o600


def test_write_leaves_no_readable_debris(tmp_path):
    """The atomic write must not leave its temp file behind.

    A stray temp file is the same leak under a different name.
    """
    backend = _backend(tmp_path)
    _build(backend, _spec(tmp_path, MARKER))
    _build(backend, _spec(tmp_path, MARKER + " turn two"))

    files = list(_prompt_dir(tmp_path).iterdir())
    assert len(files) == 1
    assert files[0].suffix == ".md"
    assert all(f.stat().st_mode & 0o077 == 0 for f in files)


# --------------------------------------------------------------------- #
#  Upgrade path — what earlier versions left on disk                     #
# --------------------------------------------------------------------- #


def test_existing_world_readable_dir_is_clamped(tmp_path):
    """``mkdir(exist_ok=True)`` does not fix the mode of an existing dir.

    Every install that ran the pre-hardening code has this directory at
    0755. Re-creating it is a no-op; the chmod has to be unconditional.
    """
    d = _prompt_dir(tmp_path)
    d.mkdir(parents=True)
    os.chmod(d, 0o755)

    _build(_backend(tmp_path), _spec(tmp_path, MARKER))

    assert d.stat().st_mode & 0o777 == 0o700


def test_existing_0644_spills_are_clamped(tmp_path):
    """Old spills hold the full bundle and must not wait for GC to age out."""
    d = _prompt_dir(tmp_path)
    d.mkdir(parents=True)
    stale = d / "old-session.md"
    stale.write_text("previously world-readable bundle", encoding="utf-8")
    os.chmod(stale, 0o644)

    _build(_backend(tmp_path), _spec(tmp_path, MARKER))

    assert stale.stat().st_mode & 0o777 == 0o600
    # Untouched otherwise — this is a permission fix, not a purge.
    assert stale.read_text(encoding="utf-8") == "previously world-readable bundle"


def test_unclampable_file_does_not_block_a_session(tmp_path):
    """Best-effort: a file we cannot chmod must not stop the CLI starting."""
    d = _prompt_dir(tmp_path)
    d.mkdir(parents=True)
    (d / "foreign.md").write_text("not ours", encoding="utf-8")

    with patch("nerve.agent.backends.claude.os.chmod", side_effect=OSError("EPERM")):
        options = _build(_backend(tmp_path), _spec(tmp_path, MARKER))

    assert Path(options.system_prompt["path"]).exists()


# --------------------------------------------------------------------- #
#  Filename discipline and GC (unchanged behavior, still load-bearing)   #
# --------------------------------------------------------------------- #


def test_filename_is_deterministic_across_reconnects(tmp_path):
    """Resume re-uses the same path, and the content is refreshed."""
    backend = _backend(tmp_path)
    first = _build(backend, _spec(tmp_path, MARKER, "sess-resume"))
    second = _build(backend, _spec(tmp_path, MARKER + " v2", "sess-resume"))

    assert first.system_prompt["path"] == second.system_prompt["path"]
    assert Path(second.system_prompt["path"]).read_text(encoding="utf-8").endswith("v2")


def test_session_id_cannot_escape_the_directory(tmp_path):
    """A path-shaped session id is sanitized, not honored."""
    backend = _backend(tmp_path)
    options = _build(backend, _spec(tmp_path, MARKER, "../../../../tmp/evil"))

    path = Path(options.system_prompt["path"])
    assert path.parent == _prompt_dir(tmp_path)
    assert not Path("/tmp/evil.md").exists()


def test_sweep_does_not_follow_symlinks(tmp_path):
    """A symlink in the dir must not aim the chmod (or the unlink) elsewhere."""
    d = _prompt_dir(tmp_path)
    d.mkdir(parents=True)
    outside = tmp_path / "someone-elses-file"
    outside.write_text("not a system prompt", encoding="utf-8")
    os.chmod(outside, 0o644)
    link = d / "link.md"
    link.symlink_to(outside)
    os.utime(link, (time.time() - 8 * 24 * 3600,) * 2, follow_symlinks=False)

    _build(_backend(tmp_path), _spec(tmp_path, MARKER))

    assert outside.exists()
    assert outside.stat().st_mode & 0o777 == 0o644
    assert link.is_symlink()


def test_gc_prunes_stale_spills_and_keeps_fresh_ones(tmp_path):
    d = _prompt_dir(tmp_path)
    d.mkdir(parents=True)
    old, fresh = d / "old.md", d / "fresh.md"
    for f in (old, fresh):
        f.write_text("x", encoding="utf-8")
    eight_days = time.time() - 8 * 24 * 3600
    os.utime(old, (eight_days, eight_days))

    _build(_backend(tmp_path), _spec(tmp_path, MARKER))

    assert not old.exists()
    assert fresh.exists()
