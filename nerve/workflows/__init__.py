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

from typing import TYPE_CHECKING

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
    config: NerveConfig, db: Database, engine: AgentEngine, runs,
):
    """Initialise the review-loop singleton. Requires a live
    WorkflowRunService (legs are workflow runs). Returns None when the
    feature (or workflow runs) is disabled."""
    global _review_loop_service
    if runs is None or not config.workflows.enabled \
            or not config.workflows.review_loop.enabled:
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
