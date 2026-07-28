"""Filesystem helpers shared by the writers that must not lose or leak data.

Kept dependency-free (stdlib only) so any module can import it.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def atomic_write_text(path: Path | str, content: str, *, mode: int | None = 0o600) -> None:
    """Replace ``path`` with ``content`` in one step, at ``mode``.

    ``mode=None`` means "whatever ``open(path, 'w')`` would have left behind":
    a file that already exists keeps the permissions it already has, and a new
    one is created at ``0o666`` minus the process umask. Use it for files that
    are meant to be shared; pass an explicit mode for anything holding a
    credential.

    Three properties the obvious ``open(path, "w")`` does not have:

    * **A crash never truncates the destination.** The bytes go to a temp file
      in the same directory, are flushed and fsynced, and only then does
      ``os.replace`` swap them in — an atomic rename on POSIX and on Windows.
      An interrupted write (power loss, ENOSPC, SIGKILL) leaves either the
      complete old file or the complete new one, never a half of either.
    * **The permissions are in place before the content is.** The temp file is
      created owner-only by ``mkstemp`` and chmod'd *before* the first write,
      so a secret is never briefly readable at the process umask. Chmod'ing
      after the write is a race, however short.
    * **The rename itself is durable.** The parent directory is fsynced too,
      so a crash right after the call cannot resurrect the old file.

    One thing it does *not* preserve: hard links. Swapping in a new file gives
    the destination a new inode, so any other name for the old one keeps the
    old content. Symlinks are followed (the link's target is rewritten, as a
    plain write would do), but a hard-linked destination cannot be updated in
    place and atomically at the same time.

    ``path``'s parent is created if missing. The temp file is removed if
    anything goes wrong, so a failed write leaves no debris.
    """
    # Follow symlinks to the real destination. Otherwise a config file the user
    # linked into a dotfiles repo would be *replaced* by a regular file and the
    # repo copy would silently keep the old content.
    path = Path(os.path.realpath(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    if mode is None:
        # Only a *new* file takes its mode from the umask; rewriting an existing
        # one through ``open(path, "w")`` leaves the permissions alone. This
        # helper writes a fresh inode and renames it into place, so the old mode
        # has to be carried across by hand or every write silently re-permissions
        # the file: a 0o600 config widened to 0o666 under a lax umask, or a
        # deliberately group-readable one clamped to 0o600 under a strict one.
        # The widening direction is the dangerous one — these are the files that
        # hold credentials.
        mode = _existing_file_mode(path)
        if mode is None:
            mode = _default_file_mode()
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            try:
                os.fchmod(f.fileno(), mode)
            except AttributeError:  # no fchmod on Windows
                os.chmod(tmp, mode)
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)


def _existing_file_mode(path: Path) -> int | None:
    """The permission bits already on ``path``, or ``None`` if there is no file.

    Only the low nine bits travel. Set-user-ID, set-group-ID and the sticky bit
    belong to the inode being discarded, and re-applying them to a file whose
    contents this process just wrote is how an ordinary config write turns into
    a privilege hand-out.
    """
    try:
        return os.stat(path).st_mode & 0o777
    except OSError:
        return None  # absent, or unstattable — fall back to what open() would do


def _default_file_mode() -> int:
    """The mode ``open()`` would create a file with, honoring the umask.

    There is no getter for the umask, only a setter that returns the previous
    value — so it has to be read by writing it. The value parked in the window
    is deliberately restrictive: if another thread creates a file in those few
    instructions, it lands too tight rather than too loose.
    """
    current = os.umask(0o077)
    os.umask(current)
    return 0o666 & ~current


def _fsync_dir(directory: Path) -> None:
    """Flush a directory entry, ignoring platforms that can't open one."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return  # Windows, and some network filesystems
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
