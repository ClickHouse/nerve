"""One-time pairing codes for Telegram DM authorization.

The pairing flow connects a Telegram user to a Nerve instance without
hand-editing config files:

  1. A code is generated on the server — either automatically at channel
     startup (fresh install with no ``allowed_users``) or on demand via
     ``nerve pair``.
  2. The user sends ``/pair <code>`` to the bot.
  3. On a match the user's ID is appended to ``telegram.allowed_users`` in
     config.local.yaml and authorized immediately.

The code lives in ``~/.nerve/telegram_pairing`` (mode 0600) so the CLI and
the daemon — separate processes — share it. Codes are single-use, expire
after :data:`CODE_TTL_SECONDS`, and are invalidated after
:data:`MAX_ATTEMPTS` failed guesses.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time
from pathlib import Path

logger = logging.getLogger(__name__)

PAIRING_FILE = Path("~/.nerve/telegram_pairing")
CODE_TTL_SECONDS = 60 * 60  # 1 hour
MAX_ATTEMPTS = 5


def _pairing_path() -> Path:
    return PAIRING_FILE.expanduser()


def _read_state() -> dict | None:
    try:
        return json.loads(_pairing_path().read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _write_state(state: dict) -> None:
    path = _pairing_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def generate_pairing_code() -> str:
    """Generate, persist, and return a fresh 6-digit pairing code."""
    code = f"{secrets.randbelow(1_000_000):06d}"
    _write_state({
        "code": code,
        "created_at": time.time(),
        "expires_at": time.time() + CODE_TTL_SECONDS,
        "attempts": 0,
    })
    return code


def read_pairing_code() -> str | None:
    """Return the current valid pairing code, or None if absent/expired."""
    state = _read_state()
    if state is None:
        return None
    if time.time() > float(state.get("expires_at", 0)):
        return None
    if int(state.get("attempts", 0)) >= MAX_ATTEMPTS:
        return None
    return str(state.get("code") or "") or None


def get_or_create_pairing_code() -> str:
    """Return the current valid code, generating a new one if needed."""
    return read_pairing_code() or generate_pairing_code()


def verify_pairing_code(submitted: str) -> bool:
    """Check a submitted code. Single-use: the code is cleared on success.

    Failed attempts are counted; after MAX_ATTEMPTS the code is invalidated
    (a new one must be generated with ``nerve pair``).
    """
    state = _read_state()
    if state is None:
        return False
    if time.time() > float(state.get("expires_at", 0)):
        clear_pairing_code()
        return False
    if int(state.get("attempts", 0)) >= MAX_ATTEMPTS:
        return False

    expected = str(state.get("code") or "")
    ok = bool(expected) and secrets.compare_digest(
        submitted.strip(), expected,
    )
    if ok:
        clear_pairing_code()
        return True

    state["attempts"] = int(state.get("attempts", 0)) + 1
    if state["attempts"] >= MAX_ATTEMPTS:
        logger.warning(
            "Telegram pairing code invalidated after %d failed attempts",
            state["attempts"],
        )
    _write_state(state)
    return False


def clear_pairing_code() -> None:
    """Remove the pairing code file (e.g. after successful pairing)."""
    _pairing_path().unlink(missing_ok=True)
