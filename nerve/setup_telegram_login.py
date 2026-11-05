"""Sign the Telegram message sync into the person's own account — from the setup page.

The Telethon source reads the chats of the person's *own* Telegram account,
and for that it needs an authorised user session on disk
(``<NERVE_HOME>/telegram_sync.session``). API credentials alone do nothing.
Until now that session was created only by the terminal command
``nerve setup-telegram`` (``client.start()`` with interactive prompts); this
module runs the same sign-in — phone → code → optional two-step password —
as a small state machine the setup page drives over HTTP.

Rules:

* **One pending login per process**, kept in memory only (the phone, the
  ``phone_code_hash`` Telegram answered with, and the connected client). It
  expires after ten minutes; a new start cancels the previous one.
* **The status probe opens a short-lived client on the session file** (five
  seconds at most), never while a login is pending (the pending client holds
  the file). A session file locked by a running sync answers
  ``authorized: null, reason: "busy"`` rather than failing.
* **Nothing secret leaves**: the phone appears only masked, the code,
  password and hash never appear in logs, state or responses; exception
  texts from Telethon are never echoed.
* The session file is written by Telethon; after a sign-in it is made
  owner-only (``0600``), like every other credential file.

Everything Telethon-specific is behind ``client_factory`` so the whole flow
is testable with a fake client. :class:`LoginError` carries the HTTP shape
the route renders (``status_code``, ``detail``, ``field``, ``retry_after``).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import time
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from nerve import paths

logger = logging.getLogger(__name__)

SESSION_NAME = "telegram_sync"
#: The status probe: connect, is_user_authorized, get_me, disconnect.
CHECK_TIMEOUT = 5.0
#: Each sign-in call (connect, send the code, sign in, log out).
LOGIN_TIMEOUT = 20.0
#: A code Telegram sent is usable for this long.
PENDING_TTL = 600.0

REASON_BUSY = "busy"
REASON_NOT_CONFIGURED = "not_configured"
REASON_NO_SESSION = "no_session"
REASON_TELETHON_MISSING = "telethon_missing"
REASON_UNREACHABLE = "unreachable"

STAGE_CODE = "code"
STAGE_PASSWORD = "password"

_PHONE_RE = re.compile(r"^\+?\d{6,15}$")
_CODE_RE = re.compile(r"^\d{3,8}$")

_PHONE_DETAIL = (
    "Enter the phone number of the Telegram account in international format, "
    "with the country code (for example +44 20 7946 0000)."
)
_NOT_CONFIGURED_DETAIL = "Save the Telegram API id and API hash first, then sign in."
_BUSY_DETAIL = "The Telegram session file is in use by a running sync. Try again in a moment."
_UNREACHABLE_DETAIL = "The server could not reach Telegram; nothing was changed. Try again."
_NO_PENDING_DETAIL = "No sign-in is in progress. Send a code first."
_EXPIRED_DETAIL = "The code expired. Send a new one."
_BAD_CODE_DETAIL = "That code is not right. Check the message from Telegram and try again."
_BAD_PASSWORD_DETAIL = "That password is not right."
_PASSWORD_STAGE_DETAIL = "Telegram asked for the two-step password, not a code."
_CODE_STAGE_DETAIL = "Enter the code Telegram sent before the password."


class LoginError(Exception):
    """A refusal the route renders as ``{detail[, field][, retry_after]}``."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        *,
        field: str | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.field = field
        self.retry_after = retry_after


# --------------------------------------------------------------------------- #
#  Session file                                                                 #
# --------------------------------------------------------------------------- #


def session_base() -> Path:
    """The Telethon session name, minus ``.session`` — what the sync source uses."""
    return paths.nerve_path(SESSION_NAME)


def session_file() -> Path:
    return Path(str(session_base()) + ".session")


def session_present() -> bool:
    return session_file().is_file()


def _make_private(path: Path) -> None:
    """Owner-only, like every other credential file; a missing file is fine."""
    for candidate in (path, Path(str(path) + "-journal")):
        try:
            if candidate.exists():
                os.chmod(candidate, 0o600)
        except OSError as e:
            logger.warning("Could not make %s owner-only: %s", candidate, e)


def _remove_session_files() -> None:
    for candidate in (session_file(), Path(str(session_file()) + "-journal")):
        try:
            candidate.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("Could not remove %s: %s", candidate, e)


# --------------------------------------------------------------------------- #
#  Phone and code handling                                                      #
# --------------------------------------------------------------------------- #


def mask_phone(phone: str) -> str:
    """``+15550100123`` → ``+1••••••••23``: the country code's first digit and
    the last two digits; everything else hidden."""
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) <= 3:
        return "+" + "•" * len(digits)
    return "+" + digits[0] + "•" * (len(digits) - 3) + digits[-2:]


def normalise_phone(raw: str) -> str:
    """``+`` and digits only; anything else is a 400 on the ``phone`` field."""
    compact = re.sub(r"[\s().\-]", "", (raw or "").strip())
    if not _PHONE_RE.match(compact):
        raise LoginError(400, _PHONE_DETAIL, field="phone")
    return "+" + compact.lstrip("+")


def _clean_code(raw: str) -> str:
    compact = re.sub(r"[\s\-]", "", (raw or "").strip())
    if not _CODE_RE.match(compact):
        raise LoginError(400, "Enter the code Telegram sent you (digits only).", field="code")
    return compact


# --------------------------------------------------------------------------- #
#  Telethon adapter                                                             #
# --------------------------------------------------------------------------- #


def _default_client_factory(api_id: int, api_hash: str) -> Any:
    from telethon import TelegramClient  # imported here: optional at import time

    return TelegramClient(str(session_base()), int(api_id), api_hash)


def _telethon_errors() -> Any | None:
    try:
        from telethon import errors
    except ImportError:
        return None
    return errors


def classify(exc: BaseException) -> str:
    """``flood`` | ``password_needed`` | ``bad_code`` | ``expired_code`` |
    ``bad_password`` | ``bad_phone`` | ``busy`` | ``unreachable`` | ``rpc`` | ``other``."""
    if isinstance(exc, sqlite3.OperationalError):
        return "busy"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
        return "unreachable"
    errors = _telethon_errors()
    if errors is None:
        return "other"
    if isinstance(exc, errors.FloodWaitError):
        return "flood"
    if isinstance(exc, errors.SessionPasswordNeededError):
        return "password_needed"
    if isinstance(exc, (errors.PhoneCodeInvalidError, errors.PhoneCodeEmptyError)):
        return "bad_code"
    if isinstance(exc, errors.PhoneCodeExpiredError):
        return "expired_code"
    if isinstance(exc, errors.PasswordHashInvalidError):
        return "bad_password"
    if isinstance(exc, (
        errors.PhoneNumberInvalidError, errors.PhoneNumberBannedError,
        errors.PhoneNumberUnoccupiedError, errors.PhoneNumberFloodError,
    )):
        return "bad_phone"
    if isinstance(exc, errors.RPCError):
        return "rpc"
    return "other"


def _flood_seconds(exc: BaseException) -> int:
    seconds = getattr(exc, "seconds", None)
    try:
        return max(1, int(seconds))
    except (TypeError, ValueError):
        return 60


def _user_dict(me: Any) -> dict[str, Any] | None:
    if me is None or getattr(me, "id", None) is None:
        return None
    username = getattr(me, "username", None)
    return {
        "id": int(me.id),
        "username": str(username) if username else None,
        "first_name": str(getattr(me, "first_name", None) or ""),
    }


async def _disconnect(client: Any) -> None:
    try:
        result = client.disconnect()
        if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
            await asyncio.wait_for(result, CHECK_TIMEOUT)
    except Exception as e:  # noqa: BLE001 — closing is best effort
        logger.debug("Telegram sign-in: disconnect raised %s", type(e).__name__)


# --------------------------------------------------------------------------- #
#  The state machine                                                            #
# --------------------------------------------------------------------------- #


@dataclass
class PendingLogin:
    phone: str
    phone_code_hash: str
    client: Any
    started: float
    stage: str = STAGE_CODE

    def expired(self, now: float) -> bool:
        return now - self.started > PENDING_TTL


def _blank_status(configured: bool, present: bool) -> dict[str, Any]:
    return {
        "configured": configured,
        "session_present": present,
        "authorized": None,
        "user": None,
        "pending": None,
        "phone_masked": None,
        "reason": None,
    }


class TelegramLogin:
    """One sign-in state machine per process (see the module docstring)."""

    def __init__(
        self,
        client_factory: Callable[[int, str], Any] = _default_client_factory,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client_factory = client_factory
        self.clock = clock
        self.pending: PendingLogin | None = None
        self._locks: "weakref.WeakKeyDictionary[Any, asyncio.Lock]" = weakref.WeakKeyDictionary()

    # -- helpers ----------------------------------------------------------- #

    def _lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[loop] = lock
        return lock

    @staticmethod
    def _credentials(config: Any) -> tuple[int, str]:
        sync = getattr(config, "sync", None)
        telegram = getattr(sync, "telegram", None)
        try:
            api_id = int(getattr(telegram, "api_id", 0) or 0)
        except (TypeError, ValueError):
            api_id = 0
        api_hash = str(getattr(telegram, "api_hash", "") or "")
        return api_id, api_hash

    def _client(self, api_id: int, api_hash: str) -> Any:
        try:
            return self.client_factory(api_id, api_hash)
        except ImportError:
            raise LoginError(
                409, "Telethon is not installed on this server, so the Telegram sync cannot sign in.",
            )

    async def _drop_pending(self) -> None:
        pending, self.pending = self.pending, None
        if pending is not None:
            await _disconnect(pending.client)

    async def _expire(self) -> bool:
        """Drop a pending login whose code is too old. True when one was dropped."""
        if self.pending is not None and self.pending.expired(self.clock()):
            logger.info("Telegram sign-in: the pending code expired")
            await self._drop_pending()
            return True
        return False

    def _require_pending(self, stage: str) -> PendingLogin:
        pending = self.pending
        if pending is None:
            raise LoginError(409, _NO_PENDING_DETAIL)
        if pending.stage != stage:
            raise LoginError(
                409, _PASSWORD_STAGE_DETAIL if pending.stage == STAGE_PASSWORD else _CODE_STAGE_DETAIL,
            )
        return pending

    @staticmethod
    def _failure(exc: BaseException, *, field: str | None = None) -> LoginError:
        """The refusal for an exception from a sign-in call. Never echoes it."""
        kind = classify(exc)
        if kind == "flood":
            seconds = _flood_seconds(exc)
            return LoginError(
                429, f"Telegram asks you to wait {seconds} seconds before another attempt.",
                retry_after=seconds,
            )
        if kind == "busy":
            return LoginError(409, _BUSY_DETAIL)
        if kind == "unreachable":
            return LoginError(502, _UNREACHABLE_DETAIL)
        if kind == "bad_phone":
            return LoginError(400, "Telegram does not accept this phone number.", field="phone")
        if kind == "bad_code":
            return LoginError(400, _BAD_CODE_DETAIL, field="code")
        if kind == "expired_code":
            return LoginError(400, _EXPIRED_DETAIL, field="code")
        if kind == "bad_password":
            return LoginError(400, _BAD_PASSWORD_DETAIL, field="password")
        logger.warning("Telegram sign-in failed: %s", type(exc).__name__)
        return LoginError(
            502, f"Telegram answered with an error ({type(exc).__name__}); nothing was changed. Try again.",
            field=field,
        )

    # -- status ------------------------------------------------------------ #

    async def status(self, config: Any) -> dict[str, Any]:
        """The GET shape. Probes the session file only when nothing is pending."""
        api_id, api_hash = self._credentials(config)
        configured = bool(api_id and api_hash)
        await self._expire()
        present = session_present()
        result = _blank_status(configured, present)
        if self.pending is not None:
            result.update(
                authorized=False, pending=self.pending.stage, phone_masked=mask_phone(self.pending.phone),
            )
            return result
        if not configured:
            result.update(authorized=None if present else False, reason=REASON_NOT_CONFIGURED)
            return result
        if not present:
            result.update(authorized=False, reason=REASON_NO_SESSION)
            return result
        try:
            client = self._client(api_id, api_hash)
        except LoginError:
            result.update(reason=REASON_TELETHON_MISSING)
            return result
        try:
            await asyncio.wait_for(client.connect(), CHECK_TIMEOUT)
            authorized = bool(await asyncio.wait_for(client.is_user_authorized(), CHECK_TIMEOUT))
            me = await asyncio.wait_for(client.get_me(), CHECK_TIMEOUT) if authorized else None
        except Exception as exc:  # noqa: BLE001 — the probe reports, never raises
            kind = classify(exc)
            result.update(reason=REASON_BUSY if kind == "busy" else REASON_UNREACHABLE)
            logger.info("Telegram sync session check: %s (%s)", result["reason"], type(exc).__name__)
            return result
        finally:
            await _disconnect(client)
        user = _user_dict(me) if authorized else None
        result.update(authorized=authorized and user is not None, user=user)
        return result

    # -- the three steps --------------------------------------------------- #

    async def start(self, config: Any, phone_raw: str) -> dict[str, Any]:
        api_id, api_hash = self._credentials(config)
        if not (api_id and api_hash):
            raise LoginError(409, _NOT_CONFIGURED_DETAIL)
        phone = normalise_phone(phone_raw)
        async with self._lock():
            await self._drop_pending()  # a new start cancels the previous login
            client = self._client(api_id, api_hash)
            try:
                await asyncio.wait_for(client.connect(), LOGIN_TIMEOUT)
                sent = await asyncio.wait_for(client.send_code_request(phone), LOGIN_TIMEOUT)
            except Exception as exc:  # noqa: BLE001
                await _disconnect(client)
                raise self._failure(exc, field="phone") from None
            code_hash = str(getattr(sent, "phone_code_hash", "") or "")
            self.pending = PendingLogin(phone=phone, phone_code_hash=code_hash, client=client, started=self.clock())
            logger.info("Telegram sign-in: code requested")
            return {"pending": STAGE_CODE, "phone_masked": mask_phone(phone), "expires_in": int(PENDING_TTL)}

    async def code(self, config: Any, code_raw: str) -> dict[str, Any]:
        async with self._lock():
            if await self._expire():
                raise LoginError(409, _EXPIRED_DETAIL)
            pending = self._require_pending(STAGE_CODE)
            code = _clean_code(code_raw)
            try:
                me = await asyncio.wait_for(
                    pending.client.sign_in(pending.phone, code, phone_code_hash=pending.phone_code_hash),
                    LOGIN_TIMEOUT,
                )
            except Exception as exc:  # noqa: BLE001
                kind = classify(exc)
                if kind == "password_needed":
                    pending.stage = STAGE_PASSWORD
                    logger.info("Telegram sign-in: two-step password requested")
                    return await self.status(config)
                if kind == "expired_code":
                    await self._drop_pending()
                raise self._failure(exc, field="code") from None
            return await self._signed_in(config, pending, me)

    async def password(self, config: Any, password: str) -> dict[str, Any]:
        async with self._lock():
            if await self._expire():
                raise LoginError(409, _EXPIRED_DETAIL)
            pending = self._require_pending(STAGE_PASSWORD)
            if not (password or "").strip():
                raise LoginError(400, "Enter the two-step password.", field="password")
            try:
                me = await asyncio.wait_for(pending.client.sign_in(password=password), LOGIN_TIMEOUT)
            except Exception as exc:  # noqa: BLE001
                raise self._failure(exc, field="password") from None
            return await self._signed_in(config, pending, me)

    async def _signed_in(self, config: Any, pending: PendingLogin, me: Any) -> dict[str, Any]:
        user = _user_dict(me)
        if user is None:
            try:
                user = _user_dict(await asyncio.wait_for(pending.client.get_me(), CHECK_TIMEOUT))
            except Exception:  # noqa: BLE001
                user = None
        # Telethon saves the authorisation on sign-in and on disconnect; the
        # file then becomes owner-only like every other credential file.
        if self.pending is pending:
            self.pending = None
        await _disconnect(pending.client)
        _make_private(session_file())
        logger.info("Telegram sign-in: signed in")
        api_id, api_hash = self._credentials(config)
        result = _blank_status(bool(api_id and api_hash), session_present())
        result.update(authorized=True, user=user)
        return result

    # -- cancel / logout --------------------------------------------------- #

    async def cancel(self, config: Any) -> dict[str, Any]:
        async with self._lock():
            if self.pending is not None:
                logger.info("Telegram sign-in: cancelled")
            await self._drop_pending()
        return await self.status(config)

    async def logout(self, config: Any) -> dict[str, Any]:
        api_id, api_hash = self._credentials(config)
        async with self._lock():
            await self._drop_pending()
            if session_present() and api_id and api_hash:
                client = self._client(api_id, api_hash)
                try:
                    await asyncio.wait_for(client.connect(), LOGIN_TIMEOUT)
                    await asyncio.wait_for(client.log_out(), LOGIN_TIMEOUT)
                except Exception as exc:  # noqa: BLE001
                    kind = classify(exc)
                    if kind == "busy":
                        raise self._failure(exc) from None
                    # Unreachable or refused: the session cannot be revoked
                    # remotely right now, but it can be forgotten here.
                    logger.info("Telegram sign-out: %s; removing the session file", type(exc).__name__)
                finally:
                    await _disconnect(client)
            _remove_session_files()
            logger.info("Telegram sign-in: signed out")
        result = _blank_status(bool(api_id and api_hash), session_present())
        result.update(authorized=False, reason=REASON_NO_SESSION)
        return result


__all__ = [
    "CHECK_TIMEOUT",
    "LOGIN_TIMEOUT",
    "PENDING_TTL",
    "LoginError",
    "PendingLogin",
    "TelegramLogin",
    "classify",
    "mask_phone",
    "normalise_phone",
    "session_base",
    "session_file",
    "session_present",
]
