"""Agent backend seam — protocols and shared datatypes.

The engine talks to agent runtimes exclusively through
:class:`AgentBackend` / :class:`AgentClient`. Backend-specific behavior
the engine must act on is declared in :class:`BackendCapabilities` —
never discovered via ``isinstance``.

See docs/plans/codex-backend.md §3 for the full design rationale.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Protocol,
    runtime_checkable,
)

from nerve.agent.backends.events import AgentEvent

# async (session_id, file_path, content_or_none) -> None
SnapshotFn = Callable[[str, str, "str | None"], Awaitable[None]]
# async (session_id, tool_input) -> None
WakeupFn = Callable[[str, dict], Awaitable[Any]]


class BackendError(Exception):
    """Base class for backend failures the engine's retry path handles."""


class TransportDiedError(BackendError):
    """The backend subprocess/stream died mid-operation."""


class ResumeDroppedError(BackendError):
    """The stored native session id could not be resumed.

    Raised (or reported) by ``create_client`` after it has already
    recovered by starting a fresh native session — the engine must clear
    the persisted ``sdk_session_id``. Carries the live client so no work
    is lost.
    """

    def __init__(self, message: str, client: "AgentClient | None" = None):
        super().__init__(message)
        self.client = client


@dataclass
class TurnInput:
    """One user turn, engine-normalized.

    ``images``: list of dicts in one of two shapes —
    ``{"media_type": str, "data": <base64 str>}`` or ``{"path": str}``.
    ``documents``: PDF/document blocks (Claude-native; other backends
    surface an inline unsupported-input note instead of dropping them).
    """

    text: str
    images: list[dict[str, Any]] | None = None
    documents: list[dict[str, Any]] | None = None


@dataclass
class SessionSpec:
    """Everything a backend needs to build a client for one session."""

    session_id: str
    source: str
    model: str | None
    effort: str
    system_prompt: str
    cwd: str
    resume_native_id: str | None = None
    fork: bool = False
    interactive: Any = None          # InteractionHub (typed Any to avoid cycles)
    snapshot: SnapshotFn | None = None
    record_wakeup: WakeupFn | None = None
    cache_ttl: str = "5m"
    max_turns: int = 100
    idle_timeout: float = 900.0
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BackendCapabilities:
    """Engine-visible behavioral differences between backends."""

    cost_is_cumulative: bool = True
    supports_idle_stream: bool = True
    supports_cache_ttl: bool = True
    interactive_builtins: bool = True
    reports_context_window: bool = False


@runtime_checkable
class AgentClient(Protocol):
    """A live, per-session agent runtime (one subprocess under the hood)."""

    @property
    def native_session_id(self) -> str | None:
        """Backend-native session/thread id, once known.

        Codex knows it at ``thread/start``; Claude learns it from the
        first stream message. The engine's cancel-persistence path reads
        this property (never raw stream messages).
        """
        ...

    async def connect(self) -> None: ...

    async def start_turn(self, turn: TurnInput) -> None: ...

    def receive_turn(self) -> AsyncIterator[AgentEvent]:
        """Yield events until (and including) ``TurnCompleted``.

        Must terminate for interrupted and failed turns too — the /stop
        flow's graceful wait depends on it.
        """
        ...

    async def interrupt(self) -> None: ...

    async def disconnect(self) -> None:
        """Full teardown, owning every process/transport internal."""
        ...

    def is_alive(self) -> bool: ...

    # -- autonomous/idle stream (only when supports_idle_stream) ------- #

    def try_receive_idle_event(self) -> AgentEvent | None:
        """Non-parking probe of the between-turns stream (None = empty)."""
        ...

    async def receive_idle_event(self, timeout: float) -> AgentEvent | None:
        """Park up to ``timeout`` seconds for a between-turns event."""
        ...

    def buffer_used(self) -> int:
        """Bytes buffered in the native stream (0 when N/A)."""
        ...


class AgentBackend(Protocol):
    """Factory + policy for one agent runtime family."""

    name: str
    capabilities: BackendCapabilities

    def default_model(self, source: str) -> str:
        """Default model for a session source when no explicit override."""
        ...

    async def create_client(self, spec: SessionSpec) -> AgentClient:
        """Build and connect a client. May raise :class:`ResumeDroppedError`
        (carrying the recovered client) when a stale resume target had to
        be discarded."""
        ...

    def validate_resume_target(self, native_id: str, cwd: str) -> bool:
        """Cheap pre-check that a stored native id is still resumable.

        Backends without a cheap check return True and rely on
        ``create_client``'s resume-miss recovery instead.
        """
        ...

    def excluded_tools(self) -> set[str]:
        """Nerve-registry tool names NOT to expose for this backend."""
        ...
