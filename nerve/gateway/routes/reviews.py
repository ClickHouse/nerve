"""Code-review panel routes.

Feature-gated by ``config.code_review.enabled``. Serves git working-tree diffs
and file contents for configured repo worktrees, and manages line-anchored
review threads whose comments route into a Nerve session (and back). Every
route requires the standard web-UI auth; file access is confined to configured
repo roots by :func:`nerve.gateway.gitreview.resolve_within_repos`.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway import gitreview
from nerve.gateway.auth import require_auth
from nerve.gateway.diff import compute_file_diff
from nerve.gateway.gitreview import RepoAccessError
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter()


def _cfg():
    cfg = get_config().code_review
    if not cfg.enabled:
        raise HTTPException(status_code=404, detail="Code review is not enabled")
    return cfg


def _mint_session_id() -> str:
    return str(uuid.uuid4())[:8]


# --- Comment → session injection -------------------------------------------
#
# A human comment is delivered to the review's target session by running a
# turn in that session — exactly what POST /api/chat does, but detached so the
# HTTP response returns immediately (engine.run blocks for the whole turn). A
# per-session lock serializes our injects so two comments can't start
# overlapping turns on the same session.

_inject_locks: dict[str, asyncio.Lock] = {}


def _lock_for(session_id: str) -> asyncio.Lock:
    lock = _inject_locks.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        _inject_locks[session_id] = lock
    return lock


async def _run_inject(session_id: str, message: str) -> None:
    deps = get_deps()
    async with _lock_for(session_id):
        try:
            await deps.engine.run(
                session_id=session_id,
                user_message=message,
                source="web",
                channel="web",
            )
        except Exception:
            logger.exception("code-review inject into session %s failed", session_id)


def _format_review_submission(review: dict, summary: str, items: list[dict]) -> str:
    """One message delivering a whole review — an optional overall summary plus
    every staged inline comment — so the agent acts on it in a single turn."""
    header = (
        f'Review {review["id"]} "{review.get("title") or "(untitled)"}" — '
        f'{review["worktree"]}'
    )
    if review.get("branch"):
        header += f' (branch {review["branch"]})'
    n = len(items)
    lines = [
        f"[code review · {n} inline comment{'s' if n != 1 else ''} from the reviewer "
        "— automated delivery, not a chat message]",
        header,
    ]
    if summary and summary.strip():
        lines += ["", "Overall:", summary.strip()]
    lines.append("")
    for i, it in enumerate(items, 1):
        loc = it["file_path"]
        ls, le = it.get("line_start"), it.get("line_end")
        if ls and le and le != ls:
            loc += f":{ls}-{le}"
        elif ls:
            loc += f":{ls}"
        lines.append(f'{i}. {loc} ({it.get("side", "new")} side)  [thread {it["thread_id"]}]')
        if it.get("anchor_snippet"):
            lines.append(f'   > {it["anchor_snippet"]}')
        for bl in ((it.get("body") or "").splitlines() or [""]):
            lines.append(f'   {bl}')
        lines.append("")
    lines.append(
        "To respond: address the points and reply per thread with your code-review "
        f'reply tool (review {review["id"]}, the thread ids above), or discuss here.'
    )
    return "\n".join(lines)


async def _ensure_target(review: dict) -> str:
    """Return the review's target session id, minting + persisting one if unset."""
    target = review.get("target_session_id")
    if not target:
        target = _mint_session_id()
        await get_deps().db.update_review(review["id"], target_session_id=target)
        review["target_session_id"] = target
    return target


# --- Request models ---------------------------------------------------------

class ReviewCreate(BaseModel):
    worktree: str
    branch: str | None = None
    base_ref: str = "HEAD"
    target_session_id: str | None = None
    created_by: str = "human"
    title: str = ""


class ReviewPatch(BaseModel):
    title: str | None = None
    status: str | None = None
    target_session_id: str | None = None


class ThreadCreate(BaseModel):
    file_path: str
    side: str = "new"
    line_start: int | None = None
    line_end: int | None = None
    anchor_snippet: str | None = None
    body: str = ""
    author: str = "human"


class CommentCreate(BaseModel):
    body: str
    author: str = "human"


class SubmitReview(BaseModel):
    summary: str = ""


# --- Git-backed read endpoints ---------------------------------------------

@router.get("/api/review/repos")
async def review_repos(user: dict = Depends(require_auth)):
    cfg = _cfg()
    repos = []
    for r in cfg.repos:
        root = Path(r).expanduser()
        try:
            worktrees = await asyncio.to_thread(gitreview.list_worktrees_sync, root.resolve())
        except (RepoAccessError, OSError):
            continue
        repos.append({"root": str(root), "resolved": str(root.resolve()), "worktrees": worktrees})
    return {"repos": repos}


@router.get("/api/review/changed")
async def review_changed(worktree: str, base: str = "HEAD", user: dict = Depends(require_auth)):
    cfg = _cfg()
    try:
        wt, root, _ = await asyncio.to_thread(gitreview.resolve_within_repos, worktree, cfg.repos, None)
        files = await asyncio.to_thread(gitreview.changed_files_sync, wt, base)
    except RepoAccessError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"worktree": str(wt), "repo_root": str(root), "base": base, "files": files}


@router.get("/api/review/diff")
async def review_diff(
    worktree: str,
    path: str,
    base: str = "HEAD",
    context: int = 4,
    user: dict = Depends(require_auth),
):
    cfg = _cfg()
    try:
        wt, _root, target = await asyncio.to_thread(
            gitreview.resolve_within_repos, worktree, cfg.repos, path,
        )
        original = await asyncio.to_thread(gitreview.file_at_ref_sync, wt, base, path)
        current = await asyncio.to_thread(gitreview.read_working_file_sync, target, cfg.max_file_bytes)
    except RepoAccessError as e:
        raise HTTPException(status_code=400, detail=str(e))
    diff = await asyncio.to_thread(compute_file_diff, original, current, path, context, None)
    return diff


@router.get("/api/review/file")
async def review_file(worktree: str, path: str, user: dict = Depends(require_auth)):
    cfg = _cfg()
    try:
        _wt, _root, target = await asyncio.to_thread(
            gitreview.resolve_within_repos, worktree, cfg.repos, path,
        )
    except RepoAccessError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        content = await asyncio.to_thread(gitreview.read_working_file_sync, target, cfg.max_file_bytes)
        return {"path": path, "content": content, "binary": False, "too_large": False}
    except RepoAccessError as e:
        msg = str(e)
        return {
            "path": path,
            "content": None,
            "binary": "binary" in msg,
            "too_large": "max_file_bytes" in msg,
        }


# --- Review / thread / comment endpoints -----------------------------------

@router.get("/api/reviews")
async def list_reviews(
    status: str | None = None,
    session: str | None = None,
    user: dict = Depends(require_auth),
):
    _cfg()
    reviews = await get_deps().db.list_reviews(status=status, target_session_id=session)
    return {"reviews": reviews}


@router.post("/api/reviews")
async def create_review(req: ReviewCreate, user: dict = Depends(require_auth)):
    cfg = _cfg()
    try:
        wt, root, _ = await asyncio.to_thread(gitreview.resolve_within_repos, req.worktree, cfg.repos, None)
    except RepoAccessError as e:
        raise HTTPException(status_code=400, detail=str(e))

    branch = req.branch
    if not branch:
        try:
            for w in await asyncio.to_thread(gitreview.list_worktrees_sync, root):
                if Path(w["path"]).resolve() == wt:
                    branch = w.get("branch")
                    break
        except RepoAccessError:
            branch = None

    review = await get_deps().db.create_review(
        repo_root=str(root),
        worktree=str(wt),
        branch=branch,
        base_ref=req.base_ref,
        target_session_id=req.target_session_id,
        created_by=req.created_by,
        title=req.title,
    )
    return review


@router.get("/api/reviews/{review_id}")
async def get_review(review_id: str, user: dict = Depends(require_auth)):
    _cfg()
    review = await get_deps().db.get_review_full(review_id)
    if review is None:
        raise HTTPException(status_code=404, detail="Review not found")
    return review


@router.patch("/api/reviews/{review_id}")
async def patch_review(review_id: str, req: ReviewPatch, user: dict = Depends(require_auth)):
    _cfg()
    db = get_deps().db
    if await db.get_review(review_id) is None:
        raise HTTPException(status_code=404, detail="Review not found")
    fields = {k: v for k, v in req.model_dump().items() if v is not None}
    await db.update_review(review_id, **fields)
    return await db.get_review_full(review_id)


@router.delete("/api/reviews/{review_id}")
async def delete_review(review_id: str, user: dict = Depends(require_auth)):
    _cfg()
    await get_deps().db.delete_review(review_id)
    return {"deleted": True}


@router.post("/api/reviews/{review_id}/threads")
async def create_thread(review_id: str, req: ThreadCreate, user: dict = Depends(require_auth)):
    _cfg()
    db = get_deps().db
    review = await db.get_review(review_id)
    if review is None:
        raise HTTPException(status_code=404, detail="Review not found")

    thread = await db.add_thread(
        review_id=review_id,
        file_path=req.file_path,
        side=req.side,
        line_start=req.line_start,
        line_end=req.line_end,
        anchor_snippet=req.anchor_snippet,
    )
    if req.body:
        # Human comments are staged (pending) until the review is submitted;
        # agent-authored threads are published immediately.
        await db.add_comment(
            thread_id=thread["id"], author=req.author, body=req.body,
            pending=(req.author == "human"),
        )

    thread["comments"] = await db.list_comments(thread["id"])
    return {"thread": thread, "target_session_id": review.get("target_session_id")}


@router.post("/api/reviews/{review_id}/threads/{thread_id}/comments")
async def add_comment(review_id: str, thread_id: str, req: CommentCreate, user: dict = Depends(require_auth)):
    _cfg()
    db = get_deps().db
    review = await db.get_review(review_id)
    thread = await db.get_thread(thread_id)
    if review is None or thread is None or thread["review_id"] != review_id:
        raise HTTPException(status_code=404, detail="Review or thread not found")

    thread_status = "answered" if req.author == "agent" else None
    comment = await db.add_comment(
        thread_id=thread_id, author=req.author, body=req.body,
        thread_status=thread_status, pending=(req.author == "human"),
    )
    return {"comment": comment, "target_session_id": review.get("target_session_id")}


@router.post("/api/reviews/{review_id}/threads/{thread_id}/resolve")
async def resolve_thread(review_id: str, thread_id: str, user: dict = Depends(require_auth)):
    _cfg()
    db = get_deps().db
    thread = await db.get_thread(thread_id)
    if thread is None or thread["review_id"] != review_id:
        raise HTTPException(status_code=404, detail="Thread not found")
    await db.set_thread_status(thread_id, "resolved")
    return {"resolved": True}


@router.post("/api/reviews/{review_id}/submit")
async def submit_review(review_id: str, req: SubmitReview, user: dict = Depends(require_auth)):
    """Deliver the whole review — optional summary + all staged (pending) inline
    comments — to the target session as ONE turn, then clear the pending flags.

    This is the batched alternative to per-comment delivery: the reviewer stages
    N line comments (each pending), then submits once and the agent receives the
    entire review in a single turn.
    """
    _cfg()
    db = get_deps().db
    review = await db.get_review(review_id)
    if review is None:
        raise HTTPException(status_code=404, detail="Review not found")

    items = await db.list_pending_comments(review_id)
    if not items and not (req.summary or "").strip():
        return {"submitted": 0, "target_session_id": review.get("target_session_id")}

    target = await _ensure_target(review)
    message = _format_review_submission(review, req.summary, items)
    asyncio.create_task(_run_inject(target, message))
    await db.mark_review_submitted(review_id)
    return {"submitted": len(items), "target_session_id": target}
