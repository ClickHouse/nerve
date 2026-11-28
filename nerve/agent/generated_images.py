"""Materialize generated image bytes without trusting runtime filesystem paths."""

from __future__ import annotations

import base64
import binascii
import errno
import hashlib
import os
import stat
import subprocess
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Iterator

from nerve.agent.backends.images import IMAGE_MAGIC

MAX_IMAGE_URL_BYTES = 32 * 1024 * 1024
MAX_GENERATED_IMAGE_BYTES = 20 * 1024 * 1024
_IMAGE_EXTENSIONS = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _check_gitignore(directory: int, rule: bytes) -> None:
    descriptor = os.open(
        ".gitignore", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory,
    )
    with os.fdopen(descriptor, "rb") as existing:
        info = os.fstat(existing.fileno())
        if not stat.S_ISREG(info.st_mode) or existing.read(len(rule) + 1) != rule:
            raise ValueError("Generated image directory has an unexpected .gitignore")


def _install_gitignore(directory: int) -> None:
    # This rule ignores itself and all session directories even if the
    # workspace is only turned into a config repo after generation.
    rule = b"*\n"
    try:
        _check_gitignore(directory, rule)
    except FileNotFoundError:
        pass
    else:
        return

    name = ".gitignore-" + uuid.uuid4().hex
    descriptor = os.open(
        name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600, dir_fd=directory,
    )
    try:
        with os.fdopen(descriptor, "wb") as staged:
            staged.write(rule)
            staged.flush()
            os.fsync(staged.fileno())
        # Publish only a complete rule. Never replace a file that somebody
        # else created while we were writing the temporary one.
        try:
            os.link(name, ".gitignore", src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
        except FileExistsError:
            _check_gitignore(directory, rule)
    finally:
        os.unlink(name, dir_fd=directory)
    os.fsync(directory)


def _verify_git_exclusion(target: Path) -> None:
    # An ignore rule does not protect a path already in the Git index.
    # Ask every enclosing worktree with its own local metadata.
    for parent in target.parents:
        try:
            (parent / ".git").lstat()
        except FileNotFoundError:
            continue
        # Do not let an inherited alternate Git index or directory hide a
        # tracked path in this workspace's ordinary index.
        env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
        try:
            result = subprocess.run(
                ["git", "check-ignore", "--quiet", "--", str(target.relative_to(parent))],
                cwd=parent, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=5, check=False,
            )
        except subprocess.TimeoutExpired:
            raise ValueError("Could not verify Git exclusion for generated image") from None
        if result.returncode != 0:
            raise ValueError("Generated image path must be ignored and untracked by Git")


@contextmanager
def _output_directory(root: Path, session_key: str) -> Iterator[int]:
    """Open each fixed path component without following a replacement symlink."""
    with ExitStack() as stack:
        current = os.open(root, _DIRECTORY_FLAGS)
        stack.callback(os.close, current)
        for name in ("output", "generated-images", session_key):
            try:
                os.mkdir(name, mode=0o700, dir_fd=current)
            except FileExistsError:
                pass
            try:
                current = os.open(name, _DIRECTORY_FLAGS, dir_fd=current)
            except OSError as error:
                if error.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise ValueError("Generated image directory must not contain symlinks") from None
                raise
            stack.callback(os.close, current)
            if name == "generated-images":
                _install_gitignore(current)
        yield current


def _existing_image_matches(directory: int, filename: str, content: bytes) -> bool:
    """Inspect only a singly linked regular file reached through the safe directory."""
    try:
        descriptor = os.open(
            filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory,
        )
    except FileNotFoundError:
        return False
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise ValueError("Generated image destination must not be a symlink") from None
        raise
    with os.fdopen(descriptor, "rb") as existing:
        info = os.fstat(existing.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("Generated image destination must be a regular private file")
        if info.st_size != len(content):
            return False
        if existing.read(len(content) + 1) != content:
            return False
        os.fchmod(existing.fileno(), 0o600)
        return True


def store_generated_image(workspace: Path, session_id: str, image_url: str) -> tuple[Path, bytes]:
    """Save an inline image atomically and return its path and validated bytes.

    Only PNG, JPEG and WebP data URLs are accepted. The session's opaque ID
    and the bytes become hashes, so neither a native path nor an untrusted
    session name can select a destination. Repeated identical output reuses
    the same file. Refusals raise ``ValueError``; filesystem errors propagate.
    """
    if not isinstance(image_url, str) or len(image_url) > MAX_IMAGE_URL_BYTES:
        raise ValueError("Generated image data URL is missing or too large")
    header, separator, encoded = image_url.partition(",")
    if not separator or not header.startswith("data:") or not header.endswith(";base64"):
        raise ValueError("Generated image must use an inline base64 data URL")
    media_type = header[5:-7]
    extension = _IMAGE_EXTENSIONS.get(media_type)
    if extension is None:
        raise ValueError("Generated image format is not supported")
    if len(encoded) > 4 * ((MAX_GENERATED_IMAGE_BYTES + 2) // 3):
        raise ValueError("Generated image exceeds the decoded size limit")
    try:
        content = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("Generated image contains invalid base64") from None
    if not content or len(content) > MAX_GENERATED_IMAGE_BYTES:
        raise ValueError("Generated image is empty or exceeds the decoded size limit")
    signatures = IMAGE_MAGIC[extension]
    if not any(all(content[offset: offset + len(magic)] == magic
                   for magic, offset in signature) for signature in signatures):
        raise ValueError("Generated image bytes do not match the declared format")

    root = workspace.resolve(strict=True)
    session_key = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:16]
    filename = hashlib.sha256(content).hexdigest() + extension
    target = root / "output" / "generated-images" / session_key / filename
    with _output_directory(root, session_key) as directory:
        if not target.resolve().is_relative_to(root):
            raise ValueError("Generated image destination escapes the workspace")
        _verify_git_exclusion(target)
        if _existing_image_matches(directory, filename, content):
            return target, content
        temporary = ".generated-" + uuid.uuid4().hex
        created = False
        try:
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600, dir_fd=directory,
            )
            created = True
            with os.fdopen(descriptor, "wb") as staged:
                os.fchmod(staged.fileno(), 0o600)
                staged.write(content)
                staged.flush()
                os.fsync(staged.fileno())
            # A concurrent symlink is never followed by replace(), but refuse
            # it explicitly as well so a forged destination is not accepted.
            try:
                info = os.stat(filename, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError("Generated image destination must be a regular private file")
            if not target.resolve().is_relative_to(root):
                raise ValueError("Generated image destination escapes the workspace")
            os.replace(temporary, filename, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=directory)
                except FileNotFoundError:
                    pass
    return target, content
