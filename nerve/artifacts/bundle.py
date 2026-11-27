"""The bundle of a workspace path: the files that a preview shows.

A bundle is a set of regular files below one folder of the workspace (its
root), with one of them as the entry. :func:`describe_bundle` makes the
description of the bundle of a path by these rules:

- a folder: all regular files below it; the entry is its ``index.html``,
  which must exist;
- a file named ``index.html``: its folder, as above;
- another ``.html`` file: that one file;
- an image file (PNG, JPEG, GIF, WebP, AVIF, SVG): that one file;
- a Markdown file (``.md``) or a CSV file (``.csv``): a generated
  ``index.html`` that holds the source text as data, the kit files that
  draw it in the browser, and the source file under a fixed name
  (``source.md`` or ``data.csv``), for a download of the source.

The kit is a set of static files of the web build (``web/src/kit/``), in
the reserved folder ``nerve-kit/`` of a bundle. The entry page of a bundle
gets each kit file whose fixed address (``nerve-kit/<name>``) is in the
first ``KIT_SCAN_BYTES`` of the entry file, found as bytes: the runtime
parses no HTML. Only the fixed names of ``KIT_FILES`` are kit files, and a
workspace folder cannot have its own ``nerve-kit/`` folder in a bundle.

A workspace file comes from the agent on the same machine, so the rules
for it here are bounds on work, not isolation: a Markdown or CSV source is
decoded as UTF-8 within ``MAX_SOURCE_BYTES``, and the rows, columns and
cells of a CSV text are counted by a linear scan with limits.

Every other path is refused. A symbolic link or a special file (a socket, a
FIFO, a device) in the path or in the folder refuses the bundle. A folder or
file whose name starts with a dot is left out of a folder, and a path that
names one is refused. The workspace root is never a bundle. All paths stay
inside the workspace.

The description holds no file content. :func:`open_bundle_file` opens one
file of a description for reading: a workspace file below the workspace,
without following a link, and a generated page made again from its source
file. A file that changed on disk changes the fingerprint.
"""

from __future__ import annotations

import dataclasses
import hashlib
import html
import itertools
import io
import json
import os
import re
import stat
import threading
from collections import OrderedDict
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
MARKDOWN_SUFFIX = ".md"
CSV_SUFFIX = ".csv"

# The reserved folder of the kit in a bundle, and the kit files by name.
# ``web/src/kit/files.json`` lists the same names for the build.
KIT_FOLDER = "nerve-kit"
KIT_FILES = frozenset({"kit.css", "deck.css", "deck.js", "table.js", "markdown.js"})
# The kit files of the web build. ``npm run build`` writes them.
KIT_DIR = Path(__file__).resolve().parent.parent.parent / "web" / "dist" / KIT_FOLDER

# The fixed name of the source file in the bundle of a Markdown or CSV file.
MARKDOWN_SOURCE = "source.md"
CSV_SOURCE = "data.csv"
# The id of the data block of a generated page (``web/src/kit/source.ts``).
SOURCE_BLOCK_ID = "nerve-kit-source"
# The largest Markdown or CSV source of a generated page, and the most rows
# (without the header row) of a table page.
MAX_SOURCE_BYTES = 5 * MIB
MAX_TABLE_ROWS = 10_000
# The most columns and cells (all rows, the header row too, by the widest
# row) of a table page. ``web/src/kit/table.ts`` has the same limits.
MAX_TABLE_COLUMNS = 200
MAX_TABLE_CELLS = 200_000
# How much of the entry file is searched for the addresses of kit files.
KIT_SCAN_BYTES = 1 * MIB

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
    MARKDOWN = "markdown"
    CSV = "csv"


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
    RESERVED_PATH = "reserved_path"
    KIT_MISSING = "kit_missing"
    NOT_TEXT = "not_text"
    SOURCE_TOO_LARGE = "source_too_large"


class FileSource(StrEnum):
    """Where the bytes of a file of a bundle come from."""

    # A workspace file below the root of the bundle.
    WORKSPACE = "workspace"
    # A page that is made from a source file of the workspace (the page of a
    # Markdown or CSV file), again at each read. No bytes are kept.
    GENERATED = "generated"
    # A file of the kit of the web build.
    KIT = "kit"


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
    source: FileSource = FileSource.WORKSPACE
    # WORKSPACE: the path on disk relative to the root, when it is not
    # ``path``. GENERATED: the path of the file of the bundle that it is made
    # from. KIT: the name of the file in the kit folder.
    origin: str | None = None


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
    # The kit folder that the kit files of this bundle come from.
    kit_dir: Path = KIT_DIR

    @property
    def root_path(self) -> Path:
        """The folder of the bundle as an absolute path."""
        return self.workspace / self.root if self.root else self.workspace

    @property
    def size(self) -> int:
        return sum(f.size for f in self.files)

    @property
    def modified_ns(self) -> int:
        """The newest modification time of a file of the bundle. Kit files
        are left out: their time is the time of the build."""
        return max((f.mtime_ns for f in self.files if f.source is not FileSource.KIT), default=0)

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


def _folder_bundle(workspace_real: Path, request: str, parts: tuple[str, ...], kit_dir: Path) -> Bundle:
    if not parts:
        raise BundleError(BundleRefusal.WORKSPACE_ROOT, "The workspace root cannot be a bundle.")
    files = _walk(workspace_real.joinpath(*parts))
    if any(f.path.split("/", 1)[0] == KIT_FOLDER for f in files):
        raise BundleError(
            BundleRefusal.RESERVED_PATH,
            f"The folder has a {KIT_FOLDER}/ folder. That name is kept for the files of the Nerve kit.",
        )
    entry = next((f for f in files if f.path == ENTRY_FILE), None)
    if entry is None:
        raise BundleError(BundleRefusal.NO_ENTRY, f"The folder has no {ENTRY_FILE}.")
    return Bundle(
        workspace=workspace_real, path=request, root=_bundle_path(parts),
        kind=BundleKind.FOLDER, entry=ENTRY_FILE, files=files, kit_dir=kit_dir,
    )


class _Recent:
    """The last ``size`` values by key, for work that a poll would do again
    for a file that did not change. The key holds the size and the
    modification time of the file.

    The preview routes describe bundles in worker threads, so a get and a
    put can run at the same time; one lock makes each of them whole (a put
    cannot remove the key of a get between its read and its move)."""

    def __init__(self, size: int) -> None:
        self._size = size
        self._values: OrderedDict[tuple, object] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple) -> object | None:
        with self._lock:
            value = self._values.get(key)
            if value is not None:
                self._values.move_to_end(key)
            return value

    def put(self, key: tuple, value: object) -> None:
        with self._lock:
            self._values[key] = value
            self._values.move_to_end(key)
            while len(self._values) > self._size:
                self._values.popitem(last=False)


# The kit names of an entry file, and the size of the generated page of a
# source file or why it has none (a refusal code and its message). Each
# key holds the identity of the file. Only small values that never change
# are kept: no bytes, no exception.
_ENTRY_KIT = _Recent(256)
_PAGE_SIZES = _Recent(256)

def kit_names(head: bytes) -> frozenset[str]:
    """The kit files whose fixed address (``nerve-kit/<name>``) is in the
    bytes ``head`` of an entry page. A plain search: an address in a code
    sample of the page counts too, and only adds a kit file."""
    return frozenset(name for name in KIT_FILES if f"{KIT_FOLDER}/{name}".encode() in head)


def _open_below(workspace: Path, names: list[str], path: str, described: BundleFile | None = None) -> BinaryIO:
    """Open the file ``names`` below ``workspace`` for reading. Each folder
    and the file are opened without following a link. With ``described``,
    the opened file must be that file (see :func:`_checked_file`)."""
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0))
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
    return _checked_file(file_fd, path, described)


def _checked_file(file_fd: int, path: str, described: BundleFile | None) -> BinaryIO:
    """The open file ``file_fd`` as a binary file: a regular file within the
    file limit, and with ``described``, the same size, modification time,
    status change time, inode and device as that :class:`BundleFile`
    (otherwise ``BundleRefusal.CHANGED``)."""
    try:
        st = os.fstat(file_fd)
        if not stat.S_ISREG(st.st_mode):
            raise BundleError(BundleRefusal.SPECIAL_FILE, "The file is not a regular file.")
        _check_file_size(path, st.st_size)
        if described is not None:
            opened = (st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino, st.st_dev)
            if opened != (described.size, described.mtime_ns, described.ctime_ns, described.inode, described.device):
                raise BundleError(BundleRefusal.CHANGED, f"The file {path!r} changed after the description.")
        os.set_blocking(file_fd, True)
        return os.fdopen(file_fd, "rb")
    except BaseException:
        os.close(file_fd)
        raise


def _read_limited(handle: BinaryIO, path: str, max_bytes: int) -> bytes:
    with handle:
        data = handle.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise BundleError(
            BundleRefusal.FILE_TOO_LARGE, f"The file {path!r} is larger than {max_bytes // MIB} MiB.",
        )
    return data


def _entry_kit(workspace_real: Path, root: tuple[str, ...], kind: BundleKind, entry: BundleFile) -> frozenset[str]:
    """The kit files that the entry page of a bundle names."""
    if entry.source is FileSource.GENERATED:
        return _PAGE_KIT[kind]
    key = (str(workspace_real), root, entry.path, entry.size, entry.mtime_ns, entry.ctime_ns, entry.inode, entry.device)
    names = _ENTRY_KIT.get(key)
    if names is None:
        with _open_below(workspace_real, [*root, *entry.path.split("/")], entry.path, entry) as handle:
            names = kit_names(handle.read(KIT_SCAN_BYTES))
        _ENTRY_KIT.put(key, names)
    return names  # type: ignore[return-value]


def _with_kit(
    workspace_real: Path, root: tuple[str, ...], kind: BundleKind, entry: str, files: tuple[BundleFile, ...],
    kit_dir: Path,
) -> tuple[BundleFile, ...]:
    """``files`` and the kit files that the entry page names."""
    described = next(f for f in files if f.path == entry)
    wanted = _entry_kit(workspace_real, root, kind, described)
    kit: list[BundleFile] = []
    for name in sorted(wanted):
        try:
            st = os.stat(kit_dir / name, follow_symlinks=False)
        except OSError:
            st = None
        if st is None or not stat.S_ISREG(st.st_mode):
            raise BundleError(
                BundleRefusal.KIT_MISSING,
                f"The kit file {KIT_FOLDER}/{name} is not built. Build the web UI (cd web && npm run build).",
            )
        kit.append(BundleFile(
            path=f"{KIT_FOLDER}/{name}", size=st.st_size, media_type=media_type(name),
            mtime_ns=st.st_mtime_ns, ctime_ns=st.st_ctime_ns, inode=st.st_ino, device=st.st_dev,
            source=FileSource.KIT, origin=name,
        ))
    if len(files) + len(kit) > MAX_FILES:
        raise BundleError(BundleRefusal.TOO_MANY_FILES, f"The bundle has more than {MAX_FILES} files.")
    return tuple(sorted((*files, *kit), key=lambda f: f.path))


def source_data(name: str, text: str) -> str:
    """The data block of a generated page: JSON of the name and the text of
    the source.

    In a script element only two marks of the text matter to the HTML
    parser: ``</`` (it can end the element) and ``<!`` (``<!--`` changes how
    the parser finds the end). Both become JSON escapes (``<\\/`` and
    ``\\u003c!``), which ``JSON.parse`` reads back as the same text. The
    JSON syntax has no ``<`` outside a string, so no other place needs it.
    """
    data = json.dumps({"name": name, "text": text}, ensure_ascii=False)
    return data.replace("</", "<\\/").replace("<!", "\\u003c!")


_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="{folder}/kit.css">
</head>
<body>
<script type="application/json" id="{block}">{data}</script>
<main id="nerve-kit-page"></main>
<script src="{folder}/{script}"></script>
</body>
</html>
"""

_PAGE_SCRIPTS = {BundleKind.MARKDOWN: "markdown.js", BundleKind.CSV: "table.js"}
_SOURCE_NAMES = {BundleKind.MARKDOWN: MARKDOWN_SOURCE, BundleKind.CSV: CSV_SOURCE}
# The kit files that the generated page of a kind names (see kit_page).
_PAGE_KIT = {kind: frozenset({"kit.css", script}) for kind, script in _PAGE_SCRIPTS.items()}


def kit_page(kind: BundleKind, name: str, text: str) -> bytes:
    """The generated ``index.html`` of a Markdown or CSV source: the source
    text as data, drawn by the kit page script of ``kind``."""
    return _PAGE.format(
        title=html.escape(name), folder=KIT_FOLDER, block=SOURCE_BLOCK_ID,
        data=source_data(name, text), script=_PAGE_SCRIPTS[kind],
    ).encode("utf-8")


_CSV_SPECIAL = re.compile(r'[,"\r\n]')


def table_shape(text: str) -> tuple[int, int]:
    """The rows and the width (the most fields of a row) of a CSV text, by
    the rules of ``web/src/kit/csv.ts``: fields between commas; a double
    quote at the start of a field opens a quoted field that can hold
    commas, line ends and ``""``; a line end is ``\\n``, ``\\r\\n`` or
    ``\\r``; an empty line is no row; a byte order mark at the start is left
    out. Only counts: no value is kept. Stops with a refusal as soon as a
    row is wider than ``MAX_TABLE_COLUMNS``."""
    n = len(text)
    i = 1 if text.startswith("\ufeff") else 0
    rows = width = 0
    fields = 1
    started = False
    field_empty = True

    def end_row() -> None:
        nonlocal rows, width, fields, started, field_empty
        if started:
            rows += 1
            width = max(width, fields)
        fields, started, field_empty = 1, False, True

    while i < n:
        match = _CSV_SPECIAL.search(text, i)
        j = n if match is None else match.start()
        if j > i:
            started, field_empty = True, False
        if match is None:
            break
        c = text[j]
        if c == '"' and field_empty:
            started = True
            k = j + 1
            while True:
                q = text.find('"', k)
                if q == -1:
                    # A quote that is not closed takes the rest of the text.
                    i = n
                    break
                if q + 1 < n and text[q + 1] == '"':
                    k = q + 2
                    continue
                field_empty = q == j + 1
                i = q + 1
                break
            continue
        if c == '"':
            started, field_empty = True, False
        elif c == ",":
            fields += 1
            started, field_empty = True, True
            if fields > MAX_TABLE_COLUMNS:
                raise _table_error(f"A row of the CSV file has more than {MAX_TABLE_COLUMNS} columns.")
        else:
            if c == "\r" and j + 1 < n and text[j + 1] == "\n":
                j += 1
            end_row()
        i = j + 1
    end_row()
    return rows, width


def _table_error(message: str) -> BundleError:
    return BundleError(BundleRefusal.SOURCE_TOO_LARGE, f"{message} A table page shows at most "
                       f"{MAX_TABLE_ROWS} rows, {MAX_TABLE_COLUMNS} columns and {MAX_TABLE_CELLS} cells.")


def _check_table(text: str) -> None:
    """Refuse a CSV text that is larger than a table page shows."""
    rows, width = table_shape(text)
    if rows - 1 > MAX_TABLE_ROWS:
        raise _table_error(f"The CSV file has {rows - 1} rows.")
    if rows * width > MAX_TABLE_CELLS:
        raise _table_error(f"The CSV file has {rows * width} cells.")


def _page_of(raw: bytes, kind: BundleKind, name: str) -> bytes:
    """The generated page of the source bytes ``raw`` of the file ``name``.
    Refuses text that is not UTF-8 and a CSV text with too many rows."""
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise BundleError(BundleRefusal.NOT_TEXT, "The file is not UTF-8 text.") from None
    if kind is BundleKind.CSV:
        _check_table(text)
    return kit_page(kind, name, text)


def _page_size(workspace_real: Path, parts: tuple[str, ...], st: os.stat_result, kind: BundleKind) -> int:
    """The size of the generated page of the source file ``parts`` (status
    ``st``). The page is made to know it, and not kept."""
    name = parts[-1]
    if st.st_size > MAX_SOURCE_BYTES:
        raise BundleError(
            BundleRefusal.SOURCE_TOO_LARGE,
            f"The file is larger than {MAX_SOURCE_BYTES // MIB} MiB. A {kind.value} page shows at most that.",
        )
    key = (str(workspace_real), parts, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino, st.st_dev, kind)
    known = _PAGE_SIZES.get(key)
    if isinstance(known, tuple):
        code, message = known
        raise BundleError(code, message)
    if known is not None:
        return known  # type: ignore[return-value]
    source = BundleFile(
        path=name, size=st.st_size, media_type=media_type(name), mtime_ns=st.st_mtime_ns,
        ctime_ns=st.st_ctime_ns, inode=st.st_ino, device=st.st_dev,
    )
    raw = _read_limited(_open_below(workspace_real, list(parts), name, source), name, MAX_SOURCE_BYTES)
    try:
        size = len(_page_of(raw, kind, name))
    except BundleError as e:
        _PAGE_SIZES.put(key, (e.code, e.message))
        raise
    _PAGE_SIZES.put(key, size)
    return size


def _source_bundle(
    workspace_real: Path, request: str, parts: tuple[str, ...], st: os.stat_result, kind: BundleKind,
) -> tuple[str, tuple[BundleFile, ...]]:
    """The root and the files (without the kit) of the bundle of a Markdown or CSV file."""
    size = _page_size(workspace_real, parts, st, kind)
    source = _SOURCE_NAMES[kind]
    # The generated page changes only with its source: it takes the
    # identity of the source file for the fingerprint.
    identity = {"mtime_ns": st.st_mtime_ns, "ctime_ns": st.st_ctime_ns, "inode": st.st_ino, "device": st.st_dev}
    files = (
        BundleFile(
            path=ENTRY_FILE, size=size, media_type="text/html",
            source=FileSource.GENERATED, origin=source, **identity,
        ),
        BundleFile(path=source, size=st.st_size, media_type=media_type(source), origin=parts[-1], **identity),
    )
    return _bundle_path(parts[:-1]), files


def describe_bundle(workspace: Path | str, path: str, *, kit_dir: Path | None = None) -> Bundle:
    """The description of the bundle of ``path``, by the rules of this
    module. ``path`` is relative to ``workspace`` or an absolute path below
    it. ``kit_dir`` is the folder of the kit files (default ``KIT_DIR``).
    Raises :class:`BundleError`."""
    kit_dir = KIT_DIR if kit_dir is None else Path(kit_dir)
    workspace = Path(os.path.abspath(workspace))
    try:
        workspace_real = workspace.resolve(strict=True)
    except (OSError, RuntimeError):
        raise BundleError(BundleRefusal.NOT_FOUND, "The workspace does not exist.") from None
    parts = _relative_parts(workspace, workspace_real, path)
    request = _bundle_path(parts)
    st = _lstat_below(workspace_real, parts)
    if st is None or stat.S_ISDIR(st.st_mode):
        bundle = _folder_bundle(workspace_real, request, parts, kit_dir)
    elif not stat.S_ISREG(st.st_mode):
        raise BundleError(BundleRefusal.SPECIAL_FILE, "The path is not a regular file.")
    elif parts[-1] == ENTRY_FILE:
        bundle = _folder_bundle(workspace_real, request, parts[:-1], kit_dir)
    else:
        name = parts[-1]
        suffix = PurePosixPath(name).suffix.lower()
        if suffix in (MARKDOWN_SUFFIX, CSV_SUFFIX):
            kind = BundleKind.MARKDOWN if suffix == MARKDOWN_SUFFIX else BundleKind.CSV
            root, files = _source_bundle(workspace_real, request, parts, st, kind)
            entry = ENTRY_FILE
        elif suffix == PAGE_SUFFIX or suffix in IMAGE_SUFFIXES:
            kind = BundleKind.PAGE if suffix == PAGE_SUFFIX else BundleKind.IMAGE
            root, files, entry = _bundle_path(parts[:-1]), (_one_file(name, st),), name
        else:
            raise BundleError(
                BundleRefusal.UNSUPPORTED,
                "Only an HTML file, a folder with an index.html, an image, a Markdown file or a CSV file "
                "can be a bundle.",
            )
        bundle = Bundle(
            workspace=workspace_real, path=request, root=root,
            kind=kind, entry=entry, files=files, kit_dir=kit_dir,
        )
    if bundle.kind is BundleKind.IMAGE:
        return bundle
    root_parts = tuple(bundle.root.split("/")) if bundle.root else ()
    files = _with_kit(workspace_real, root_parts, bundle.kind, bundle.entry, bundle.files, kit_dir)
    return dataclasses.replace(bundle, files=files)


def open_bundle_file(bundle: Bundle, path: str) -> BinaryIO:
    """Open the file ``path`` of ``bundle`` for reading in binary mode.

    A workspace file: each folder from the workspace down is opened without
    following a link, and so is the file, so a link that appeared after the
    description is not followed. A kit file: from the kit folder, without
    following a link. Both must be the file of the description: the same
    size, modification time, status change time, inode and device as its
    :class:`BundleFile`; otherwise the code is ``BundleRefusal.CHANGED``,
    and the caller describes the bundle again.

    A generated page is made again from its source file, which is opened
    and checked the same way; a page of another size is ``CHANGED`` too.

    Raises :class:`BundleError` for a path that is not in the bundle, and
    for a file that is not a regular file when it is opened or that is
    larger than the file limit.
    """
    described = bundle.file(path)
    if described is None:
        raise BundleError(BundleRefusal.NOT_FOUND, "The file is not part of the bundle.")
    if described.source is FileSource.GENERATED:
        source = bundle.file(described.origin or "")
        if source is None or bundle.kind not in _PAGE_SCRIPTS:
            raise BundleError(BundleRefusal.NOT_FOUND, "The file is not part of the bundle.")
        raw = read_bundle_file(bundle, source.path, MAX_SOURCE_BYTES)
        try:
            page = _page_of(raw, bundle.kind, source.origin or source.path)
        except BundleError:
            raise BundleError(BundleRefusal.CHANGED, f"The file {path!r} changed after the description.") from None
        if len(page) != described.size:
            raise BundleError(BundleRefusal.CHANGED, f"The file {path!r} changed after the description.")
        return io.BytesIO(page)
    if described.source is FileSource.KIT:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        try:
            file_fd = os.open(bundle.kit_dir / (described.origin or ""), flags)
        except OSError as e:
            raise BundleError(BundleRefusal.KIT_MISSING, f"The kit file cannot be opened ({e.strerror}).") from None
        return _checked_file(file_fd, path, described)
    names = [*(bundle.root.split("/") if bundle.root else []), *(described.origin or path).split("/")]
    return _open_below(bundle.workspace, names, path, described)


def read_bundle_file(bundle: Bundle, path: str, max_bytes: int = MAX_FILE_BYTES) -> bytes:
    """The bytes of the file ``path`` of ``bundle`` (see :func:`open_bundle_file`).
    Raises :class:`BundleError` when the file has more than ``max_bytes``."""
    return _read_limited(open_bundle_file(bundle, path), path, max_bytes)
