"""The bundle of a workspace path: the files that a preview shows.

A bundle is a set of regular files below one folder of the workspace (its
root), with one of them as the entry. :func:`describe_bundle` makes the
description of the bundle of a path by these rules:

- a folder: all regular files below it; the entry is its ``index.html``,
  which must exist;
- a file named ``index.html``: its folder, as above;
- another ``.html`` file: that one file;
- an image file (PNG, JPEG, GIF, WebP, AVIF, SVG): that one file.

Every other path is refused. A symbolic link or a special file (a socket, a
FIFO, a device) in the path or in the folder refuses the bundle. A folder or
file whose name starts with a dot is left out of a folder, and a path that
names one is refused. The workspace root is never a bundle. All paths stay
inside the workspace.

The description holds no file content. :func:`open_bundle_file` opens one
file of a description for reading, below the workspace, without following a
link. The description is read again for each use: a file that changed on
disk changes the fingerprint.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import BinaryIO

MIB = 1024 * 1024

# Fixed limits of one bundle. They are the limits of a published version.
MAX_FILE_BYTES = 50 * MIB
MAX_FILES = 1000
MAX_PATH_BYTES = 512
# The longest path that a request may name (the path itself is checked
# name by name; this bounds that work).
MAX_REQUEST_PATH_BYTES = 4096
# The folder entries that one description reads, left-out entries included.
MAX_SCANNED_ENTRIES = 10_000

ENTRY_FILE = "index.html"
PAGE_SUFFIX = ".html"
IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".svg"})

# The type of a file, from its extension. Every other file is
# ``DEFAULT_MEDIA_TYPE``. The type is never read from the content.
MEDIA_TYPES: dict[str, str] = {
    ".html": "text/html",
    ".htm": "text/html",
    ".css": "text/css",
    ".js": "text/javascript",
    ".mjs": "text/javascript",
    ".json": "application/json",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".csv": "text/csv",
    ".xml": "application/xml",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".avif": "image/avif",
    ".ico": "image/x-icon",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".ogg": "audio/ogg",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".pdf": "application/pdf",
    ".wasm": "application/wasm",
}
DEFAULT_MEDIA_TYPE = "application/octet-stream"


class BundleKind(StrEnum):
    """What the path of a bundle names."""

    FOLDER = "folder"
    PAGE = "page"
    IMAGE = "image"


class BundleRefusal(StrEnum):
    """Why a path has no bundle. The values are stable codes for an API."""

    INVALID_PATH = "invalid_path"
    OUTSIDE_WORKSPACE = "outside_workspace"
    WORKSPACE_ROOT = "workspace_root"
    NOT_FOUND = "not_found"
    HIDDEN = "hidden"
    LINK = "link"
    SPECIAL_FILE = "special_file"
    UNSUPPORTED = "unsupported"
    NO_ENTRY = "no_entry"
    FILE_TOO_LARGE = "file_too_large"
    TOO_MANY_FILES = "too_many_files"
    PATH_TOO_LONG = "path_too_long"
    # The file on disk is not the file of the description any more.
    CHANGED = "changed"


class BundleError(Exception):
    """A path that has no bundle, with a code and a sentence for a person."""

    def __init__(self, code: BundleRefusal, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class BundleFile:
    """One file of a bundle."""

    # Relative to the root of the bundle, with "/" between the names.
    path: str
    size: int
    media_type: str
    mtime_ns: int
    # The status change time and the identity of the file on disk. With
    # mtime they show a write that kept the size and set the old mtime again.
    ctime_ns: int = 0
    inode: int = 0
    device: int = 0


@dataclass(frozen=True)
class Bundle:
    """The description of the bundle of one workspace path."""

    # The workspace, with its links resolved.
    workspace: Path
    # The path that was asked for, relative to the workspace.
    path: str
    # The folder of the bundle, relative to the workspace ("" for the root).
    root: str
    kind: BundleKind
    # The path of the entry file, relative to the root.
    entry: str
    # Sorted by path.
    files: tuple[BundleFile, ...]

    @property
    def root_path(self) -> Path:
        """The folder of the bundle as an absolute path."""
        return self.workspace / self.root if self.root else self.workspace

    @property
    def size(self) -> int:
        return sum(f.size for f in self.files)

    @property
    def modified_ns(self) -> int:
        """The newest modification time of a file of the bundle."""
        return max((f.mtime_ns for f in self.files), default=0)

    @property
    def fingerprint(self) -> str:
        """A value that changes when a file of the bundle is added, removed,
        written or replaced, or when the entry changes (from the size, the
        modification and status change times, the inode and the device)."""
        state = [
            self.kind, self.root, self.entry,
            [[f.path, f.size, f.mtime_ns, f.ctime_ns, f.inode, f.device] for f in self.files],
        ]
        return hashlib.sha256(json.dumps(state, ensure_ascii=False).encode("utf-8")).hexdigest()

    def file(self, path: str) -> BundleFile | None:
        """The file ``path`` of the bundle (relative to its root), or None."""
        for f in self.files:
            if f.path == path:
                return f
        return None


def media_type(name: str) -> str:
    """The type of a file, from the extension of its name."""
    return MEDIA_TYPES.get(PurePosixPath(name).suffix.lower(), DEFAULT_MEDIA_TYPE)


def _check_name(name: str) -> None:
    """Refuse a name that cannot be part of a bundle path."""
    if "\\" in name or any(ord(c) < 0x20 or ord(c) == 0x7F for c in name):
        raise BundleError(BundleRefusal.INVALID_PATH, f"The name {name!r} has a character that a bundle path cannot have.")
    try:
        name.encode("utf-8")
    except UnicodeEncodeError:
        raise BundleError(BundleRefusal.INVALID_PATH, f"The name {name!r} is not valid UTF-8.") from None


def _relative_parts(workspace: Path, workspace_real: Path, path: str) -> tuple[str, ...]:
    """The names from the workspace down to ``path`` (relative to the
    workspace, or absolute below it). No ``..`` is left. Nothing on disk is
    read here."""
    if not isinstance(path, str) or not path.strip() or "\x00" in path:
        raise BundleError(BundleRefusal.INVALID_PATH, "The path is empty or not valid.")
    if len(path.encode("utf-8", "surrogatepass")) > MAX_REQUEST_PATH_BYTES:
        raise BundleError(BundleRefusal.PATH_TOO_LONG, "The path is too long.")
    if os.path.isabs(path):
        norm = Path(os.path.normpath(path))
        for base in (workspace, workspace_real):
            if norm == base or norm.is_relative_to(base):
                rel = norm.relative_to(base)
                break
        else:
            raise BundleError(BundleRefusal.OUTSIDE_WORKSPACE, "The path is outside the workspace.")
    else:
        rel = Path(os.path.normpath(path))
    parts = tuple(p for p in rel.parts if p != ".")
    if ".." in parts:
        raise BundleError(BundleRefusal.OUTSIDE_WORKSPACE, "The path is outside the workspace.")
    for part in parts:
        if part.startswith("."):
            raise BundleError(BundleRefusal.HIDDEN, "A path through a hidden file or folder cannot be a bundle.")
        _check_name(part)
    return parts


def _lstat_below(workspace_real: Path, parts: tuple[str, ...]) -> os.stat_result | None:
    """The status of the last name of ``parts`` below the workspace, without
    following a link anywhere on the way. None when the workspace itself is
    meant (no parts)."""
    current = workspace_real
    result: os.stat_result | None = None
    for index, part in enumerate(parts):
        current = current / part
        try:
            result = os.lstat(current)
        except FileNotFoundError:
            raise BundleError(BundleRefusal.NOT_FOUND, "The path does not exist.") from None
        except OSError as e:
            raise BundleError(BundleRefusal.NOT_FOUND, f"The path cannot be read ({e.strerror}).") from None
        if stat.S_ISLNK(result.st_mode):
            raise BundleError(BundleRefusal.LINK, "The path goes through a symbolic link.")
        if index < len(parts) - 1 and not stat.S_ISDIR(result.st_mode):
            raise BundleError(BundleRefusal.NOT_FOUND, "The path does not exist.")
    return result


def _bundle_path(parts: tuple[str, ...]) -> str:
    return "/".join(parts)


def _check_path_length(path: str) -> None:
    if len(path.encode("utf-8")) > MAX_PATH_BYTES:
        raise BundleError(
            BundleRefusal.PATH_TOO_LONG, f"The path {path!r} is longer than {MAX_PATH_BYTES} bytes.",
        )


def _check_file_size(path: str, size: int) -> None:
    if size > MAX_FILE_BYTES:
        raise BundleError(
            BundleRefusal.FILE_TOO_LARGE,
            f"The file {path!r} is larger than {MAX_FILE_BYTES // MIB} MiB.",
        )


def _one_file(path: str, st: os.stat_result) -> BundleFile:
    _check_path_length(path)
    _check_file_size(path, st.st_size)
    return BundleFile(
        path=path, size=st.st_size, media_type=media_type(path), mtime_ns=st.st_mtime_ns,
        ctime_ns=st.st_ctime_ns, inode=st.st_ino, device=st.st_dev,
    )


def _walk(folder: Path) -> tuple[BundleFile, ...]:
    """All regular files below ``folder``, by the rules of a folder bundle."""
    files: list[BundleFile] = []
    scanned = 0
    pending: list[tuple[Path, str]] = [(folder, "")]
    while pending:
        directory, prefix = pending.pop()
        # At most the entries that are left of the allowance, and one more
        # to see that it is passed: a large folder is not read in full.
        allowance = MAX_SCANNED_ENTRIES - scanned
        try:
            with os.scandir(directory) as it:
                taken = list(itertools.islice(it, allowance + 1))
        except OSError as e:
            raise BundleError(BundleRefusal.NOT_FOUND, f"A folder cannot be read ({e.strerror}).") from None
        if len(taken) > allowance:
            raise BundleError(
                BundleRefusal.TOO_MANY_FILES,
                f"The folder has more than {MAX_SCANNED_ENTRIES} entries.",
            )
        scanned += len(taken)
        for entry in sorted(taken, key=lambda e: e.name):
            if entry.name.startswith("."):
                continue
            _check_name(entry.name)
            rel = f"{prefix}{entry.name}"
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError as e:
                raise BundleError(BundleRefusal.NOT_FOUND, f"{rel!r} cannot be read ({e.strerror}).") from None
            if stat.S_ISLNK(st.st_mode):
                raise BundleError(BundleRefusal.LINK, f"{rel!r} is a symbolic link.")
            if stat.S_ISDIR(st.st_mode):
                pending.append((Path(entry.path), f"{rel}/"))
            elif stat.S_ISREG(st.st_mode):
                files.append(_one_file(rel, st))
                if len(files) > MAX_FILES:
                    raise BundleError(BundleRefusal.TOO_MANY_FILES, f"The folder has more than {MAX_FILES} files.")
            else:
                raise BundleError(BundleRefusal.SPECIAL_FILE, f"{rel!r} is not a regular file.")
    return tuple(sorted(files, key=lambda f: f.path))


def _folder_bundle(workspace_real: Path, request: str, parts: tuple[str, ...]) -> Bundle:
    if not parts:
        raise BundleError(BundleRefusal.WORKSPACE_ROOT, "The workspace root cannot be a bundle.")
    files = _walk(workspace_real.joinpath(*parts))
    entry = next((f for f in files if f.path == ENTRY_FILE), None)
    if entry is None:
        raise BundleError(BundleRefusal.NO_ENTRY, f"The folder has no {ENTRY_FILE}.")
    return Bundle(
        workspace=workspace_real, path=request, root=_bundle_path(parts),
        kind=BundleKind.FOLDER, entry=ENTRY_FILE, files=files,
    )


def describe_bundle(workspace: Path | str, path: str) -> Bundle:
    """The description of the bundle of ``path``, by the rules of this
    module. ``path`` is relative to ``workspace`` or an absolute path below
    it. Raises :class:`BundleError`."""
    workspace = Path(os.path.abspath(workspace))
    try:
        workspace_real = workspace.resolve(strict=True)
    except (OSError, RuntimeError):
        raise BundleError(BundleRefusal.NOT_FOUND, "The workspace does not exist.") from None
    parts = _relative_parts(workspace, workspace_real, path)
    request = _bundle_path(parts)
    st = _lstat_below(workspace_real, parts)
    if st is None or stat.S_ISDIR(st.st_mode):
        return _folder_bundle(workspace_real, request, parts)
    if not stat.S_ISREG(st.st_mode):
        raise BundleError(BundleRefusal.SPECIAL_FILE, "The path is not a regular file.")
    name = parts[-1]
    suffix = PurePosixPath(name).suffix.lower()
    if name == ENTRY_FILE:
        return _folder_bundle(workspace_real, request, parts[:-1])
    if suffix == PAGE_SUFFIX:
        kind = BundleKind.PAGE
    elif suffix in IMAGE_SUFFIXES:
        kind = BundleKind.IMAGE
    else:
        raise BundleError(
            BundleRefusal.UNSUPPORTED,
            "Only an HTML file, a folder with an index.html, or an image can be a bundle.",
        )
    return Bundle(
        workspace=workspace_real, path=request, root=_bundle_path(parts[:-1]),
        kind=kind, entry=name, files=(_one_file(name, st),),
    )


def open_bundle_file(bundle: Bundle, path: str) -> BinaryIO:
    """Open the file ``path`` of ``bundle`` for reading in binary mode.

    Each folder from the workspace down is opened without following a link,
    and so is the file, so a link that appeared after the description is not
    followed. Raises :class:`BundleError` for a path that is not in the
    bundle, and for a file that is not a regular file when it is opened or
    that is larger than the file limit.

    The opened file must be the file of the description: the same size,
    modification time, status change time, inode and device as its
    :class:`BundleFile`. Otherwise the code is ``BundleRefusal.CHANGED``,
    and the caller describes the bundle again.
    """
    described = bundle.file(path)
    if described is None:
        raise BundleError(BundleRefusal.NOT_FOUND, "The file is not part of the bundle.")
    names = [*(bundle.root.split("/") if bundle.root else []), *path.split("/")]
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(bundle.workspace, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0))
    except OSError as e:
        raise BundleError(BundleRefusal.NOT_FOUND, f"The workspace cannot be opened ({e.strerror}).") from None
    try:
        for name in names[:-1]:
            child = os.open(name, directory_flags, dir_fd=fd)
            os.close(fd)
            fd = child
        # O_NONBLOCK: a FIFO that took the place of the file does not hold the open.
        file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        file_fd = os.open(names[-1], file_flags, dir_fd=fd)
    except OSError as e:
        raise BundleError(BundleRefusal.NOT_FOUND, f"The file cannot be opened ({e.strerror}).") from None
    finally:
        os.close(fd)
    try:
        st = os.fstat(file_fd)
        if not stat.S_ISREG(st.st_mode):
            raise BundleError(BundleRefusal.SPECIAL_FILE, "The file is not a regular file.")
        _check_file_size(path, st.st_size)
        opened = (st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino, st.st_dev)
        if opened != (described.size, described.mtime_ns, described.ctime_ns, described.inode, described.device):
            raise BundleError(BundleRefusal.CHANGED, f"The file {path!r} changed after the description.")
        os.set_blocking(file_fd, True)
        return os.fdopen(file_fd, "rb")
    except BaseException:
        os.close(file_fd)
        raise


def read_bundle_file(bundle: Bundle, path: str, max_bytes: int = MAX_FILE_BYTES) -> bytes:
    """The bytes of the file ``path`` of ``bundle`` (see :func:`open_bundle_file`).
    Raises :class:`BundleError` when the file has more than ``max_bytes``."""
    with open_bundle_file(bundle, path) as handle:
        data = handle.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise BundleError(
            BundleRefusal.FILE_TOO_LARGE, f"The file {path!r} is larger than {max_bytes // MIB} MiB.",
        )
    return data
