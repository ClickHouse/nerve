"""Workflow runs — budget-capped, tracked, killable multi-agent jobs.

A *workflow run* wraps a dedicated agent session (Claude harness
Workflow tool, or Codex Ultracode) in a dollar budget enforced from
Nerve's own usage metering, with run-scoped lifecycle (kill affects
only this run's session/subprocess — never a pattern-matched pkill)
and a durable journal directory under ``workflows.runs_dir``.

The singleton service is constructed in the gateway lifespan and
reached from tool handlers / REST routes via :func:`get_workflow_run_service`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database
    from nerve.workflows.service import WorkflowRunService

_service: WorkflowRunService | None = None


def init_workflow_run_service(
    config: NerveConfig, db: Database, engine: AgentEngine,
) -> WorkflowRunService | None:
    """Initialise the singleton service. Returns None when disabled."""
    global _service
    if not config.workflows.enabled:
        _service = None
        return None
    from nerve.workflows.service import WorkflowRunService as _Cls
    _service = _Cls(config, db, engine)
    return _service


def get_workflow_run_service() -> WorkflowRunService | None:
    """Return the initialised service, or None when disabled/not started."""
    return _service


def reset_workflow_run_service() -> None:
    """Test hook: drop the singleton."""
    global _service
    _service = None


# --------------------------------------------------------------------- #
#  Review loops (implement→verify cycles over workflow runs)             #
# --------------------------------------------------------------------- #

_review_loop_service = None


def init_review_loop_service(
    config: Callable[[], NerveConfig], db: Database, engine: AgentEngine, runs,
):
    """Initialise the review-loop singleton. Requires a live
    WorkflowRunService (legs are workflow runs). Returns None when the
    feature (or workflow runs) is disabled.

    ``config`` is a callable, not the object: the service reads it per use so
    its budgets and caps follow a reload (see
    :attr:`~nerve.workflows.review_loop.ReviewLoopService.rl`). The two flags
    below are read once here, which is exactly why they stay restart-only —
    with the feature off there is no service for a later reload to reach."""
    global _review_loop_service
    settings = config()
    if runs is None or not settings.workflows.enabled \
            or not settings.workflows.review_loop.enabled:
        _review_loop_service = None
        return None
    from nerve.workflows.review_loop import ReviewLoopService as _RL
    _review_loop_service = _RL(config, db, engine, runs)
    return _review_loop_service


def get_review_loop_service():
    """Return the initialised review-loop service, or None when disabled."""
    return _review_loop_service


def reset_review_loop_service() -> None:
    """Test hook: drop the singleton."""
    global _review_loop_service
    _review_loop_service = None
