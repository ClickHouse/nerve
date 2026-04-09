"""memU integration — index workspace files and conversations for semantic recall.

memU is a search index over the canonical .md memory files and past conversations.
The .md files remain the source of truth; memU makes them semantically searchable.
"""

from __future__ import annotations

import asyncio
import ctypes
import gc
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

# Semantic dedup threshold — set from config at init time.
_SEMANTIC_DEDUP_THRESHOLD = 0.85


class _BedrockLLMClient:
    """Drop-in replacement for memU's OpenAISDKClient that uses AsyncAnthropicBedrock.

    memU's MemoryService expects LLM clients with .chat(), .summarize(), and
    attribute-level .chat_model / .embed_model.  The default OpenAISDKClient
    uses the OpenAI SDK, which requires a base_url and api_key — both empty
    when the provider is Bedrock (Bedrock uses IAM auth, not API keys).

    This adapter wraps AsyncAnthropicBedrock and translates between memU's
    OpenAI-style interface and the Anthropic Messages API format.
    """

    def __init__(self, *, chat_model: str, aws_region: str = "", aws_profile: str = "",
                 aws_access_key_id: str = "", aws_secret_access_key: str = "",
                 timeout: float = 120.0):
        from anthropic import AsyncAnthropicBedrock

        kwargs: dict[str, Any] = {"timeout": timeout}
        if aws_region:
            kwargs["aws_region"] = aws_region
        if aws_profile:
            kwargs["aws_profile"] = aws_profile
        if aws_access_key_id:
            kwargs["aws_access_key"] = aws_access_key_id
            kwargs["aws_secret_key"] = aws_secret_access_key

        self._bedrock = AsyncAnthropicBedrock(**kwargs)
        self.chat_model = chat_model
        self.embed_model = ""  # Bedrock doesn't do embeddings via this client

    async def chat(
        self,
        prompt: str,
        *,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
        temperature: float = 0.2,
    ) -> tuple[str, Any]:
        kwargs: dict[str, Any] = {
            "model": self.chat_model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens or 4096,
            "temperature": temperature,
        }
        if system_prompt:
            kwargs["system"] = system_prompt
        response = await self._bedrock.messages.create(**kwargs)
        text = response.content[0].text if response.content else ""
        return text, response

    async def summarize(
        self,
        text: str,
        *,
        max_tokens: int | None = None,
        system_prompt: str | None = None,
    ) -> tuple[str, Any]:
        prompt = system_prompt or "Summarize the text in one short paragraph."
        return await self.chat(text, max_tokens=max_tokens, system_prompt=prompt)

    async def embed(self, inputs: list[str]) -> tuple[list[list[float]], None]:
        raise NotImplementedError("Bedrock LLM client does not support embeddings — use the OpenAI embedding profile")

    async def close(self) -> None:
        """Close the underlying httpx transport."""
        try:
            await self._bedrock.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Custom event extraction prompt blocks
# ---------------------------------------------------------------------------
# Override the "rules" and "examples" blocks for the event memory type so the
# extraction LLM always includes temporal context in event content and correctly
# frames future events as "on [conversation date], user planned X for [future date]".

_EVENT_CUSTOM_RULES = """
# Rules
## General requirements (must satisfy all)
- Use "user" to refer to the user consistently.
- Each memory item must be complete and self-contained, written as a declarative descriptive sentence.
- Each memory item must express one single complete piece of information and be understandable without context.
- Similar/redundant items must be merged into one, and assigned to only one category.
- Each memory item must be < 50 words worth of length (keep it concise but include relevant details).
- Focus on specific events that happened at a particular time or period.
- Include relevant details such as time, location, and participants where available.
Important: Extract only events directly stated or confirmed by the user. No guesses, no suggestions, and no content introduced only by the assistant.
Important: Accurately reflect whether the subject is the user or someone around the user.

## Special rules for Event Information
- Behavioral patterns, habits, preferences, or factual knowledge are forbidden in Event Information.
- Focus on concrete happenings, activities, and experiences.
- Do not extract content that was obtained only through the model's follow-up questions unless the user shows strong proactive intent.

## Date and time requirements (CRITICAL)
- EVERY event memory item MUST include a date or time period.
- Conversation messages include timestamps (e.g., [0] 2026-02-27T10:30:00 [user]: ...). Use them to resolve relative dates to absolute ones.
- Convert relative references: "next weekend" -> actual dates, "yesterday" -> actual date, "last month" -> actual month/year.
- For FUTURE events (things that haven't happened yet), frame them as planning/scheduling that happened on the conversation date:
  BAD:  "The user has a dentist appointment on March 15, 2026"
  GOOD: "On February 10, 2026, the user scheduled a dentist appointment for March 15, 2026"
- For PAST events, include the actual date when the event happened:
  GOOD: "The user went hiking at a nature park on February 5, 2026"
- For past events without a clear date, include the best approximation ("in early 2026", "in 2022").
- An event without any date or time reference is INCOMPLETE. Always include temporal context.

## Forbidden content
- Knowledge Q&A without a clear user event.
- Trivial daily activities unless significant (e.g., routine meals, commuting).
- Temporary, ephemeral situations that lack meaningful significance.
- Turns where the user did not respond and only the assistant spoke.
- Illegal / harmful sensitive topics (violence, politics, drugs, etc.).
- Private financial accounts, IDs, addresses, military/defense/government job details, precise street addresses—unless explicitly requested by the user (still avoid if not necessary).
- Any content mentioned only by the assistant and not explicitly confirmed by the user.

## Review & validation rules
- Merge similar items: keep only one and assign a single category.
- Resolve conflicts: keep the latest / most certain item.
- Final check: every item must have a date reference and comply with all rules.
"""

_EVENT_CUSTOM_EXAMPLES = """
# Examples (Input / Output / Explanation)
Example 1: Event Information Extraction
## Input
[0] 2026-02-20T14:00:00 [user]: Hi, are you busy? I just got off work and I'm going to the supermarket to buy some groceries.
[1] 2026-02-20T14:01:00 [assistant]: Not busy. Are you cooking for yourself?
[2] 2026-02-20T14:02:00 [user]: Yes. It's healthier. I work as a product manager in an internet company. I'm 30 this year. After work I like experimenting with cooking, I often figure out dishes by myself.
[3] 2026-02-20T14:03:00 [assistant]: Being a PM is tough. You're so disciplined to cook at 30!
[4] 2026-02-20T14:04:00 [user]: It's fine. Cooking relaxes me. It's better than takeout. Also I'm traveling next weekend. And last Saturday I went to a great concert downtown.
[5] 2026-02-20T14:05:00 [assistant]: Nice! How was the concert?
[6] 2026-02-20T14:06:00 [user]: Amazing. It was a jazz band at the Blue Note. I haven't started packing for the trip yet though.
## Output
<item>
    <memory>
        <content>On February 20, 2026, the user mentioned planning a trip on February 28 - March 1, 2026 and hasn't started packing yet</content>
        <categories>
            <category>Travel</category>
        </categories>
    </memory>
    <memory>
        <content>The user attended a jazz concert at the Blue Note downtown on February 15, 2026</content>
        <categories>
            <category>Conversations</category>
        </categories>
    </memory>
</item>
## Explanation
- The travel plan is a FUTURE event: framed as "On [conversation date], the user mentioned planning..." with the resolved date (next weekend = Feb 28).
- The concert is a PAST event: "last Saturday" resolved to Feb 15 using conversation timestamp (Feb 20).
- "next weekend" and "last Saturday" are resolved to absolute dates using the conversation timestamp.
- User's job, age, and cooking habits are stable traits, not events — excluded.
- Only events explicitly stated by the user are extracted.
"""


# ---------------------------------------------------------------------------
# Custom knowledge extraction prompt blocks
# ---------------------------------------------------------------------------
# Override extraction rules to prevent storing generic programming/CS knowledge
# that any LLM already knows. Only project-specific, environment-specific,
# or non-obvious gotcha knowledge should be persisted.

_KNOWLEDGE_CUSTOM_RULES = """
# Rules
## General requirements (must satisfy all)
- Each memory item must be complete and self-contained, written as a declarative descriptive sentence.
- Each memory item must express one single complete piece of information and be understandable without context.
- Similar/redundant items must be merged into one, and assigned to only one category.
- Each memory item must be < 50 words worth of length (keep it concise but include relevant details).
- Focus on factual knowledge, concepts, definitions, and explanations.
Important: Extract only knowledge directly stated or discussed in the conversation. No guesses or unsupported extensions.

## Special rules for Knowledge Information
- Personal opinions, subjective preferences, or personal experiences are forbidden in Knowledge Information.
- User-specific traits, events, or behaviors are not knowledge items.

## CRITICAL: Relevance filter
The goal is to filter out TEXTBOOK knowledge — things you'd find in documentation, tutorials, or Stack Overflow. We want to KEEP anything tied to the user's specific work, projects, or environment.

MUST NOT extract (textbook/generic knowledge):
- Programming language features, syntax, or standard library behavior (e.g., how decorators work, json.dumps behavior, async/await)
- Common CS concepts (hashing, caching, serialization, data structures, algorithms)
- Well-known framework/library behavior (React hooks, FastAPI routing, SQLAlchemy sessions, Pydantic validation)
- Standard DevOps/infrastructure facts (Docker networking, Linux permissions, nginx config, OOM killer behavior)
- Common error patterns and their standard fixes (numpy truthiness checks, GIL limitations)
- Widely documented API behavior of popular libraries

SHOULD extract (project-specific or user-specific knowledge):
- Architecture decisions or conventions specific to the user's projects
- Non-obvious gotchas discovered in the user's environment or toolchain
- Custom tool behavior, internal API quirks, or undocumented behavior the user discovered
- How two systems interact in the user's specific setup
- Configuration or deployment details unique to the user's infrastructure
- Work-related findings: CI/CD issues, bug reports, test failures, monitoring data tied to the user's projects
- Domain-specific data the user tracks or works with (issue trackers, project status, team findings)
- Summaries of project-specific technical investigations or root cause analyses

The test: "Could you find this in public documentation or a textbook?" If YES → skip. If NO (it's specific to this user's work) → extract.

## Forbidden content
- Textbook programming/CS/DevOps knowledge found in standard documentation
- Opinions or subjective statements without factual basis
- Personal experiences or events (these belong to event type)
- User preferences or behavioral patterns (these belong to profile/behavior type)
- Illegal / harmful sensitive topics

## Review & validation rules
- Merge similar items: keep only one and assign a single category.
- Resolve conflicts: keep the latest / most certain item.
- Final check: every item must pass the relevance filter above.
- If no items pass the filter, return an empty result.
"""

_KNOWLEDGE_CUSTOM_EXAMPLES = """
# Examples (Input / Output / Explanation)
Example 1: Filtering generic vs. project-specific knowledge
## Input
[0] 2026-03-01T10:00:00 [user]: I need to hash passwords in my Flask app. Should I use bcrypt?
[1] 2026-03-01T10:01:00 [assistant]: Yes, bcrypt is the standard choice for password hashing.
[2] 2026-03-01T10:02:00 [user]: Got it. Also, I found that our staging deployment has a weird issue where WebSocket connections drop after exactly 60 seconds. Turns out the load balancer has an idle timeout we need to override.
[3] 2026-03-01T10:03:00 [assistant]: That's a common issue with cloud LBs. You can work around it by sending periodic ping frames.
[4] 2026-03-01T10:04:00 [user]: Yeah, we already do that in ws_manager.py's keep_alive() method. Also, json.dumps doesn't handle numpy arrays, so we have to call .tolist() first.
## Output
<item>
    <memory>
        <content>Staging WebSocket connections drop after 60 seconds due to load balancer idle timeout; requires periodic ping frames to keep connections alive</content>
        <categories>
            <category>Infrastructure</category>
        </categories>
    </memory>
    <memory>
        <content>ws_manager.py's keep_alive() method sends periodic WebSocket pings to prevent load balancer idle timeout disconnections</content>
        <categories>
            <category>Infrastructure</category>
        </categories>
    </memory>
</item>
## Explanation
- "bcrypt is for password hashing" — general CS knowledge, any developer knows this. NOT extracted.
- "json.dumps doesn't handle numpy" — standard Python knowledge, well documented. NOT extracted.
- WebSocket drop on staging — non-obvious, environment-specific gotcha. EXTRACTED.
- ws_manager.py keep_alive workaround — project-specific architecture knowledge. EXTRACTED.

Example 2: Work-specific data SHOULD be extracted
## Input
[0] 2026-03-12T09:00:00 [assistant]: CI Monitor found 3 new issues: #1058 Connection pool exhaustion (Mar 11), #1057 Flaky pagination test (Mar 11), #1042 Missing index on users table (Mar 10). Also 2 integration tests consistently failing on NightlyBuilds.
## Output
<item>
    <memory>
        <content>CI Monitor tracked issues as of March 12, 2026: #1058 Connection pool exhaustion, #1057 Flaky pagination test, #1042 Missing index on users table, plus 2 consistently failing integration tests on NightlyBuilds</content>
        <categories>
            <category>Work</category>
        </categories>
    </memory>
</item>
## Explanation
These are specific CI findings from the user's project — issue numbers, error descriptions, and test status. This is NOT generic knowledge; it's project-specific monitoring data. EXTRACTED.

Example 3: Empty result when only generic knowledge discussed
## Input
[0] 2026-03-05T14:00:00 [user]: Can you explain how Python's GIL works?
[1] 2026-03-05T14:01:00 [assistant]: The GIL is a mutex that allows only one thread to execute Python bytecodes at a time...
[2] 2026-03-05T14:03:00 [user]: And asyncio vs threading?
[3] 2026-03-05T14:04:00 [assistant]: asyncio uses cooperative multitasking via coroutines on a single thread...
## Output
<item>
</item>
## Explanation
Python GIL and asyncio vs threading are textbook CS knowledge. Nothing project-specific was discussed. Empty result is correct here.
"""


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class _InFlightOp:
    op_id: int
    operation: str
    description: str
    started_at: float  # time.monotonic()


@dataclass
class MemUOpStats:
    call_count: int = 0
    error_count: int = 0
    total_duration_s: float = 0.0
    last_duration_s: float = 0.0
    last_error: str = ""
    last_success_at: str | None = None


@dataclass
class MemUMetrics:
    initialized_at: str = ""
    service_available: bool = False

    ops: dict[str, MemUOpStats] = field(default_factory=lambda: {
        "memorize_conversation": MemUOpStats(),
        "memorize_file": MemUOpStats(),
        "recall": MemUOpStats(),
        "reindex_file": MemUOpStats(),
    })

    _op_counter: int = 0
    in_flight: dict[int, _InFlightOp] = field(default_factory=dict)

    def begin_op(self, operation: str, description: str) -> int:
        self._op_counter += 1
        self.in_flight[self._op_counter] = _InFlightOp(
            op_id=self._op_counter,
            operation=operation,
            description=description,
            started_at=time.monotonic(),
        )
        return self._op_counter

    def end_op(self, op_id: int, *, success: bool, error: str = "") -> None:
        inflight = self.in_flight.pop(op_id, None)
        if inflight is None:
            return
        duration = time.monotonic() - inflight.started_at
        stats = self.ops.get(inflight.operation)
        if stats is None:
            return
        stats.call_count += 1
        stats.total_duration_s += duration
        stats.last_duration_s = duration
        if success:
            stats.last_success_at = datetime.now(timezone.utc).isoformat()
        else:
            stats.error_count += 1
            stats.last_error = error[:200]

    def to_dict(self) -> dict:
        now_mono = time.monotonic()
        return {
            "initialized_at": self.initialized_at,
            "service_available": self.service_available,
            "operations": {
                name: {
                    "call_count": s.call_count,
                    "error_count": s.error_count,
                    "avg_duration_s": round(s.total_duration_s / s.call_count, 2) if s.call_count else 0,
                    "last_duration_s": round(s.last_duration_s, 2),
                    "last_error": s.last_error,
                    "last_success_at": s.last_success_at,
                }
                for name, s in self.ops.items()
            },
            "in_flight": [
                {
                    "operation": op.operation,
                    "description": op.description,
                    "elapsed_s": round(now_mono - op.started_at, 1),
                }
                for op in self.in_flight.values()
            ],
        }


class MemUBridge:
    """Bridge to memU memory service with SQLite persistence."""

    # Dedicated thread pool for blocking operations (SQLite + sync API calls)
    # so they can never starve the default asyncio thread pool and freeze
    # the event loop.
    _blocking_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="memu-blocking")

    def __init__(self, config: NerveConfig, audit_db: Any = None):
        self.config = config
        self._service = None
        self._available = False
        self._metrics = MemUMetrics()
        self._audit_db = audit_db
        # Debounce tracking for file re-indexing (path -> asyncio.Task)
        self._reindex_tasks: dict[str, asyncio.Task] = {}
        self._anthropic_client: Any | None = None  # Lazy sync Anthropic

    async def _audit(self, action: str, target_type: str, target_id: str | None = None,
                     source: str = "bridge", details: dict | None = None) -> None:
        if self._audit_db:
            try:
                await self._audit_db.log_audit(action, target_type, target_id, source, details)
            except Exception as e:
                logger.warning("Audit log failed: %s", e)

    # memU version these patches were written for. If the installed version
    # differs, the internal APIs we monkey-patch may have changed.
    _MEMU_PATCHED_VERSION = "1.4.0"

    @classmethod
    def _check_memu_version(cls) -> bool:
        """Verify installed memU version matches what the patches target.

        Returns True if version matches, False otherwise.  Logs a loud
        warning on mismatch so operators notice before things break.
        """
        try:
            from importlib.metadata import version as pkg_version
            installed = pkg_version("memu-py")
            if installed != cls._MEMU_PATCHED_VERSION:
                logger.warning(
                    "memU version mismatch: installed %s, patches target %s. "
                    "Monkey-patches in _patch_sqlite_bugs() may silently break. "
                    "Verify patched internals still exist before upgrading.",
                    installed, cls._MEMU_PATCHED_VERSION,
                )
                return False
            return True
        except Exception:
            logger.warning("Could not determine memU version — patches may be stale")
            return False

    @staticmethod
    def _patch_sqlite_bugs():
        """Monkey-patch memU's SQLite backend to fix bugs in memu-py 1.4.0.

        WARNING: These patches reach into memU internals and are tightly
        coupled to memu-py==1.4.0.  If you upgrade memU, verify that all
        patched symbols still exist and behave the same way.  See
        _MEMU_PATCHED_VERSION and the pin in pyproject.toml.

        Patches applied:
        1. MRO conflict: _merge_models creates type(name, (BaseModel, SQLiteXxxModel))
           but SQLiteXxxModel already inherits from BaseModel. Fix: skip merge.
        2. list[float] type error: base models have `embedding: list[float] | None`
           which leaks into SQLModel table creation. The SQLite models define
           `embedding_json` + property but the original field persists in model_fields.
           Fix: remove `embedding` from model_fields directly on the model classes.
        3. vector_search_items re-reads all embeddings from SQLite on every query.
           Fix: use in-memory cache when available.
        4. _rank_categories_by_summary re-embeds all summaries via API on every recall.
           Fix: use stored category embeddings.
        5. list_items/list_categories bypass the vector cache.
           Fix: return cache when populated and unfiltered.
        """
        try:
            from pydantic import BaseModel
            from memu.database.sqlite.models import (
                SQLiteResourceModel,
                SQLiteMemoryItemModel,
                SQLiteMemoryCategoryModel,
                _merge_models,
            )
            import memu.database.sqlite.models as sqlite_models

            # Fix 2: Remove 'embedding' field from all model classes in the
            # inheritance chain so SQLModel doesn't create a column for list[float].
            # The SQLite models use embedding_json + property instead.
            # Must remove from both model_fields AND __annotations__ because
            # Pydantic rebuilds model_fields from annotations on class creation.
            from memu.database.models import Resource, MemoryItem, MemoryCategory
            for model_cls in [
                Resource, MemoryItem, MemoryCategory,
                SQLiteResourceModel, SQLiteMemoryItemModel, SQLiteMemoryCategoryModel,
            ]:
                model_cls.model_fields.pop('embedding', None)
                if 'embedding' in getattr(model_cls, '__annotations__', {}):
                    del model_cls.__annotations__['embedding']

            # Fix 1: Patch _merge_models for MRO conflict (patch in both modules)
            def patched_merge(user_model, core_model, *, name_suffix, base_attrs):
                if user_model is BaseModel or not user_model.model_fields:
                    return type(
                        f"{core_model.__name__}{name_suffix}",
                        (core_model,),
                        base_attrs,
                    )
                return _merge_models(user_model, core_model, name_suffix=name_suffix, base_attrs=base_attrs)

            sqlite_models._merge_models = patched_merge

            # Fix 6: memu-py uses table names prefixed with "sqlite_" (e.g.
            # "sqlite_resources") which SQLite reserves for internal use.
            # Patch get_sqlite_sqlalchemy_models to rename them to "memu_*".
            # Also skip the global SQLModel.metadata.create_all() call in
            # _create_tables which can include stale registrations.
            import memu.database.sqlite.schema as schema_mod
            _original_get_models = schema_mod.get_sqlite_sqlalchemy_models

            def _patched_get_models(*, scope_model=None):
                # Clear the model cache to force rebuild with new names
                schema_mod._MODEL_CACHE.clear()

                from memu.database.sqlite.models import (
                    SQLiteResourceModel as _Res,
                    SQLiteMemoryCategoryModel as _Cat,
                    SQLiteMemoryItemModel as _Item,
                    SQLiteCategoryItemModel as _Rel,
                    build_sqlite_table_model,
                )
                from sqlalchemy import MetaData

                metadata_obj = MetaData()
                scope = scope_model or BaseModel

                resource_model = build_sqlite_table_model(scope, _Res, tablename="memu_resources", metadata=metadata_obj)
                category_model = build_sqlite_table_model(scope, _Cat, tablename="memu_memory_categories", metadata=metadata_obj)
                item_model = build_sqlite_table_model(scope, _Item, tablename="memu_memory_items", metadata=metadata_obj)
                rel_model = build_sqlite_table_model(scope, _Rel, tablename="memu_category_items", metadata=metadata_obj)

                from sqlmodel import SQLModel as _SM

                class SQLiteBase(_SM):
                    __abstract__ = True
                    metadata = metadata_obj

                from memu.database.sqlite.schema import SQLiteSQLAModels
                models = SQLiteSQLAModels(
                    Base=SQLiteBase,
                    Resource=resource_model,
                    MemoryCategory=category_model,
                    MemoryItem=item_model,
                    CategoryItem=rel_model,
                )
                schema_mod._MODEL_CACHE[scope] = models
                return models

            schema_mod.get_sqlite_sqlalchemy_models = _patched_get_models

            # Also patch the reference in sqlite.py which imported it at module level
            import memu.database.sqlite.sqlite as sqlite_store_mod
            sqlite_store_mod.get_sqlite_sqlalchemy_models = _patched_get_models

            from memu.database.sqlite.sqlite import SQLiteStore

            def _safe_create_tables(self):
                self._sqla_models.Base.metadata.create_all(self._sessions.engine)
                logger.debug("SQLite tables created/verified (scoped metadata only)")

            SQLiteStore._create_tables = _safe_create_tables

            # Fix 3: vector_search_items calls list_items() on every query,
            # re-reading and JSON-parsing all embeddings from SQLite (~2s for 3K items).
            # The items cache (self.items) is already kept in sync by create/update/delete,
            # so use it directly when available.
            from memu.database.sqlite.repositories.memory_item_repo import SQLiteMemoryItemRepo
            from memu.database.inmemory.vector import cosine_topk, cosine_topk_salience

            _original_vector_search = SQLiteMemoryItemRepo.vector_search_items

            def _fast_vector_search(self, query_vec, top_k, where=None, *, ranking="similarity", recency_decay_days=30.0):
                # Use in-memory cache when it's populated and no filters applied
                if self.items and not where:
                    pool = self.items
                else:
                    pool = self.list_items(where)

                if ranking == "salience":
                    corpus = [
                        (
                            i.id, i.embedding,
                            (i.extra or {}).get("reinforcement_count", 1),
                            self._parse_datetime((i.extra or {}).get("last_reinforced_at")),
                        )
                        for i in pool.values()
                    ]
                    return cosine_topk_salience(query_vec, corpus, k=top_k, recency_decay_days=recency_decay_days)
                return cosine_topk(query_vec, [(i.id, i.embedding) for i in pool.values()], k=top_k)

            SQLiteMemoryItemRepo.vector_search_items = _fast_vector_search

            # Same fix for category repo
            from memu.database.sqlite.repositories.memory_category_repo import SQLiteMemoryCategoryRepo

            _original_cat_vector_search = getattr(SQLiteMemoryCategoryRepo, 'vector_search_categories', None)
            if _original_cat_vector_search:
                def _fast_cat_vector_search(self, query_vec, top_k, where=None):
                    if self.categories and not where:
                        pool = self.categories
                    else:
                        pool = self.list_categories(where)
                    return cosine_topk(query_vec, [(c.id, c.embedding) for c in pool.values()], k=top_k)

                SQLiteMemoryCategoryRepo.vector_search_categories = _fast_cat_vector_search

            # Fix 4: _rank_categories_by_summary re-embeds all category summaries
            # via OpenAI API on every retrieve (~3.8s for 15 summaries). Use the
            # stored category embeddings instead — cosine on 24 vectors is instant.
            from memu.app.retrieve import RetrieveMixin

            async def _fast_rank_categories(
                self, query_vec, top_k, ctx, store,
                embed_client=None, categories=None,
            ):
                cat_pool = categories if categories is not None else store.memory_category_repo.categories
                corpus = [(cid, cat.embedding) for cid, cat in cat_pool.items() if cat.embedding is not None]
                hits = cosine_topk(query_vec, corpus, k=top_k)
                summary_lookup = {cid: cat.summary for cid, cat in cat_pool.items() if cat.summary}
                return hits, summary_lookup

            RetrieveMixin._rank_categories_by_summary = _fast_rank_categories

            # Fix 5: The retrieve pipeline calls list_items() via the pipeline
            # state initialization, bypassing our vector_search cache. Patch
            # list_items to return the cache when it's populated and unfiltered.
            _original_list_items = SQLiteMemoryItemRepo.list_items

            def _cached_list_items(self, where=None):
                if self.items and not where:
                    return self.items
                return _original_list_items(self, where)

            SQLiteMemoryItemRepo.list_items = _cached_list_items

            _original_list_cats = SQLiteMemoryCategoryRepo.list_categories

            def _cached_list_cats(self, where=None):
                if self.categories and not where:
                    return self.categories
                return _original_list_cats(self, where)

            SQLiteMemoryCategoryRepo.list_categories = _cached_list_cats

            # Fix 6: Store embeddings as numpy float32 arrays instead of
            # Python list[float].  Each 1536-dim embedding drops from ~48 KB
            # (list overhead + 1536 Python float objects) to ~6 KB (contiguous
            # float32 buffer).  For 4K+ items this saves ~170 MB of RSS on the
            # Pi.  cosine_topk already converts to numpy internally, so having
            # numpy input is a no-op (faster, not slower).
            from memu.database.sqlite.repositories.base import SQLiteRepoBase

            _original_normalize = SQLiteRepoBase._normalize_embedding

            def _numpy_normalize_embedding(self, embedding):
                if embedding is None:
                    return None
                if isinstance(embedding, np.ndarray):
                    return embedding.astype(np.float32, copy=False)
                if isinstance(embedding, str):
                    try:
                        return np.array(json.loads(embedding), dtype=np.float32)
                    except (json.JSONDecodeError, TypeError):
                        return None
                try:
                    return np.array(list(embedding), dtype=np.float32)
                except (ValueError, TypeError, OverflowError):
                    return None

            SQLiteRepoBase._normalize_embedding = _numpy_normalize_embedding

            # Patch _prepare_embedding to serialize numpy arrays to JSON.
            def _numpy_prepare_embedding(self, embedding):
                if embedding is None:
                    return None
                if isinstance(embedding, np.ndarray):
                    return json.dumps(embedding.tolist())
                return json.dumps(embedding)

            SQLiteRepoBase._prepare_embedding = _numpy_prepare_embedding

            # Patch SQLite model embedding property getters/setters to handle
            # numpy.  The getters return numpy directly; the setters accept both
            # numpy and list[float] for serialization to JSON.
            for _model_cls in (SQLiteResourceModel, SQLiteMemoryItemModel, SQLiteMemoryCategoryModel):
                def _make_embedding_property(cls):
                    def _get_embedding(self):
                        if self.embedding_json is None:
                            return None
                        try:
                            return np.array(json.loads(self.embedding_json), dtype=np.float32)
                        except (json.JSONDecodeError, TypeError):
                            return None

                    def _set_embedding(self, value):
                        if value is None:
                            self.embedding_json = None
                        elif isinstance(value, np.ndarray):
                            self.embedding_json = json.dumps(value.tolist())
                        else:
                            self.embedding_json = json.dumps(value)

                    cls.embedding = property(_get_embedding, _set_embedding)
                _make_embedding_property(_model_cls)

            logger.info("Patched embeddings to use numpy float32 (saves ~170 MB on 4K items)")

            # Fix 7: Semantic deduplication in create_item_reinforce.
            # The default dedup is content-hash only (exact text after normalization).
            # This adds cosine similarity check against the in-memory item cache so
            # items saying the same thing differently get reinforced instead of duplicated.
            from memu.database.inmemory.repositories.memory_item_repo import InMemoryMemoryItemRepository

            _original_sqlite_reinforce = SQLiteMemoryItemRepo.create_item_reinforce
            _original_inmemory_reinforce = InMemoryMemoryItemRepository.create_item_reinforce

            def _semantic_sqlite_reinforce(
                self, *, resource_id, memory_type, summary, embedding, user_data,
            ):
                threshold = _SEMANTIC_DEDUP_THRESHOLD
                if threshold > 0 and embedding is not None and self.items:
                    corpus = [
                        (iid, item.embedding)
                        for iid, item in self.items.items()
                        if item.memory_type == memory_type and item.embedding is not None
                    ]
                    if corpus:
                        hits = cosine_topk(embedding, corpus, k=1)
                        if hits and hits[0][1] >= threshold:
                            match_id, score = hits[0]
                            matched = self.items[match_id]
                            logger.info(
                                "Semantic dedup: reinforcing %s item %s (%.3f) instead of creating '%s'",
                                memory_type, match_id, score, summary[:80],
                            )
                            # Update DB row
                            from sqlmodel import select as _sel
                            now = self._now()
                            extra = dict(matched.extra or {})
                            extra["reinforcement_count"] = extra.get("reinforcement_count", 1) + 1
                            extra["last_reinforced_at"] = now.isoformat()
                            with self._sessions.session() as session:
                                row = session.exec(
                                    _sel(self._memory_item_model).where(
                                        self._memory_item_model.id == match_id
                                    )
                                ).first()
                                if row:
                                    row.extra = extra
                                    row.updated_at = now
                                    session.add(row)
                                    session.commit()
                            # Update in-memory cache
                            matched.extra = extra
                            matched.updated_at = now
                            return matched

                return _original_sqlite_reinforce(
                    self,
                    resource_id=resource_id,
                    memory_type=memory_type,
                    summary=summary,
                    embedding=embedding,
                    user_data=user_data,
                )

            SQLiteMemoryItemRepo.create_item_reinforce = _semantic_sqlite_reinforce

            def _semantic_inmemory_reinforce(
                self, *, resource_id, memory_type, summary, embedding, user_data, reinforce=False,
            ):
                threshold = _SEMANTIC_DEDUP_THRESHOLD
                if threshold > 0 and embedding is not None and self.items:
                    corpus = [
                        (iid, item.embedding)
                        for iid, item in self.items.items()
                        if item.memory_type == memory_type and item.embedding is not None
                    ]
                    if corpus:
                        hits = cosine_topk(embedding, corpus, k=1)
                        if hits and hits[0][1] >= threshold:
                            match_id, score = hits[0]
                            matched = self.items[match_id]
                            logger.info(
                                "Semantic dedup: reinforcing %s item %s (%.3f) instead of creating '%s'",
                                memory_type, match_id, score, summary[:80],
                            )
                            import pendulum as _pend
                            extra = dict(matched.extra or {})
                            extra["reinforcement_count"] = extra.get("reinforcement_count", 1) + 1
                            extra["last_reinforced_at"] = _pend.now("UTC").isoformat()
                            matched.extra = extra
                            matched.updated_at = _pend.now("UTC")
                            return matched

                return _original_inmemory_reinforce(
                    self,
                    resource_id=resource_id,
                    memory_type=memory_type,
                    summary=summary,
                    embedding=embedding,
                    user_data=user_data,
                    reinforce=reinforce,
                )

            InMemoryMemoryItemRepository.create_item_reinforce = _semantic_inmemory_reinforce

            logger.info(
                "Patched create_item_reinforce with semantic dedup (threshold=%.2f)",
                _SEMANTIC_DEDUP_THRESHOLD,
            )

        except Exception as e:
            logger.error(
                "memU monkey-patching failed (expected memu-py==%s): %s. "
                "Memory recall may be slow or broken.",
                MemUBridge._MEMU_PATCHED_VERSION, e,
            )

    @staticmethod
    def _sanitize_memu_datetimes(sqlite_dsn: str) -> None:
        """Fix non-string values in datetime columns of the memU database.

        The LLM date resolver can produce bare integers (e.g. ``2025`` for
        "tax year 2025") or other non-text values for the ``happened_at``
        column.  ``datetime.fromisoformat()`` requires a string, so any
        non-text value crashes ``list_items()`` and prevents the item cache
        from loading — effectively breaking all recall.

        Run this once at startup, before memu-py opens the database.
        """
        import sqlite3 as _sqlite3

        db_path = sqlite_dsn.replace("sqlite:///", "")
        try:
            conn = _sqlite3.connect(db_path)
            # Fix integer/real values → 'YYYY-01-01 00:00:00'
            fixed_int = conn.execute(
                "UPDATE memu_memory_items "
                "SET happened_at = CAST(happened_at AS TEXT) || '-01-01 00:00:00' "
                "WHERE typeof(happened_at) IN ('integer', 'real')"
            ).rowcount
            # Null out any remaining non-text values (blobs, etc.)
            fixed_other = conn.execute(
                "UPDATE memu_memory_items "
                "SET happened_at = NULL "
                "WHERE happened_at IS NOT NULL AND typeof(happened_at) != 'text'"
            ).rowcount
            if fixed_int or fixed_other:
                conn.commit()
                logger.warning(
                    "Sanitized %d memU datetime values (%d int→date, %d other→NULL)",
                    fixed_int + fixed_other, fixed_int, fixed_other,
                )
            conn.close()
        except Exception as exc:
            logger.warning("memU datetime sanitization skipped: %s", exc)

    async def initialize(self) -> bool:
        """Initialize the memU service with SQLite persistence."""
        try:
            from memu.app.service import MemoryService

            self._check_memu_version()

            # Set semantic dedup threshold from config before patches run.
            global _SEMANTIC_DEDUP_THRESHOLD
            _SEMANTIC_DEDUP_THRESHOLD = self.config.memory.semantic_dedup_threshold

            self._patch_sqlite_bugs()

            sqlite_dsn = self.config.memory.sqlite_dsn

            # ── Fix non-string datetime values in memU database ──
            # The LLM date resolver can emit bare integers (e.g. 2025 for
            # "tax year 2025") into the happened_at column, which is typed
            # DateTime.  SQLAlchemy's datetime.fromisoformat() crashes on
            # non-string values, breaking list_items() and the entire item
            # cache.  Sanitize on startup and add a defensive type adapter.
            self._sanitize_memu_datetimes(sqlite_dsn)

            # ── Bedrock detection ──
            # When provider is Bedrock, anthropic_api_base_url and
            # effective_api_key are both empty (Bedrock uses IAM auth).
            # memU's OpenAISDKClient can't talk to Bedrock, so we:
            # 1. Pass a placeholder base_url so LLMConfig validation passes
            # 2. After MemoryService init, inject _BedrockLLMClient instances
            is_bedrock = self.config.provider.is_bedrock

            # For Bedrock we still need a base_url that passes validation
            # inside memU's LLMConfig.  It will never be used because we
            # replace the clients immediately after init.
            chat_base_url = self.config.anthropic_api_base_url or "https://placeholder.invalid/v1"
            chat_api_key = self.config.effective_api_key or "placeholder"

            llm_profiles: dict[str, Any] = {
                "default": {
                    "base_url": chat_base_url,
                    "api_key": chat_api_key,
                    "chat_model": self.config.memory.recall_model,
                    "client_backend": "sdk",
                },
            }

            if self.config.openai_api_key:
                llm_profiles["embedding"] = {
                    "base_url": "https://api.openai.com/v1",
                    "api_key": self.config.openai_api_key,
                    "embed_model": self.config.memory.embed_model,
                    "client_backend": "sdk",
                }

            resources_dir = Path("~/.nerve/memu-resources").expanduser()
            resources_dir.mkdir(parents=True, exist_ok=True)

            # Fast model for category summaries and date resolution (Haiku).
            fast_profile = "default"
            if self.config.memory.fast_model:
                llm_profiles["fast"] = {
                    "base_url": chat_base_url,
                    "api_key": chat_api_key,
                    "chat_model": self.config.memory.fast_model,
                    "client_backend": "sdk",
                }
                fast_profile = "fast"

            # Memorize model for extraction & preprocessing (Sonnet).
            memorize_profile = "default"
            if self.config.memory.memorize_model:
                llm_profiles["memorize"] = {
                    "base_url": chat_base_url,
                    "api_key": chat_api_key,
                    "chat_model": self.config.memory.memorize_model,
                    "client_backend": "sdk",
                }
                memorize_profile = "memorize"

            self._service = MemoryService(
                llm_profiles=llm_profiles,
                blob_config={
                    "resources_dir": str(resources_dir),
                },
                database_config={
                    "metadata_store": {
                        "provider": "sqlite",
                        "dsn": sqlite_dsn,
                    },
                },
                memorize_config={
                    "enable_item_reinforcement": True,
                    "memory_types": ["profile", "event", "knowledge", "behavior"],
                    # Use fast (Haiku) for preprocessing — it's a mechanical task
                    # (conversation segmentation + summarization) where Haiku is
                    # faster and more reliable.  Sonnet can misinterpret technical
                    # conversations as instructions, generating long off-topic
                    # responses that exceed the per-call timeout.
                    "preprocess_llm_profile": fast_profile,
                    # When no embedding provider is configured, all memory
                    # work goes through Anthropic — use Haiku for extraction
                    # too to avoid saturating the rate-limit budget.
                    "memory_extract_llm_profile": (
                        fast_profile if not self.config.openai_api_key
                        else memorize_profile
                    ),
                    "category_update_llm_profile": fast_profile,
                    # Pass Nerve's configured categories to memU so the LLM
                    # prompt only shows categories that actually exist in the
                    # name→ID mapping.  Without this, memU's 10 hard-coded
                    # defaults leak into the prompt while the mapping only
                    # contains Nerve's categories — causing lookup mismatches.
                    **(
                        {"memory_categories": [
                            {"name": c.name, "description": c.description}
                            for c in self.config.memory.categories
                        ]}
                        if self.config.memory.categories
                        else {}  # no Nerve categories → use memU defaults
                    ),
                    "memory_type_prompts": {
                        "event": {
                            "rules": {"ordinal": 30, "prompt": _EVENT_CUSTOM_RULES},
                            "examples": {"ordinal": 60, "prompt": _EVENT_CUSTOM_EXAMPLES},
                        },
                        # Knowledge prompt override disabled — needs tuning to avoid
                        # rejecting project-specific assistant-reported knowledge.
                        # See _KNOWLEDGE_CUSTOM_RULES / _KNOWLEDGE_CUSTOM_EXAMPLES.
                    },
                },
                retrieve_config={
                    "method": "llm" if not self.config.openai_api_key else "rag",
                    "route_intention": False,
                    "sufficiency_check": False,
                    "resource": {"enabled": False},
                    # Use Haiku for LLM-based ranking — cheaper and avoids
                    # sharing Sonnet's rate-limit budget with the main agent.
                    **({"llm_ranking_llm_profile": fast_profile}
                       if not self.config.openai_api_key else {}),
                },
            )
            self._available = True
            self._metrics.service_available = True
            self._metrics.initialized_at = datetime.now(timezone.utc).isoformat()
            logger.info("memU service initialized with SQLite at %s", sqlite_dsn)

            # ── Bedrock client injection ──
            # Replace the placeholder OpenAISDKClient instances with real
            # Bedrock-backed clients.  Must happen before any LLM call
            # (warmup, category sync, etc.).
            if is_bedrock:
                self._inject_bedrock_clients()

            await self._ensure_categories()

            # Mark categories as ready so memU's _initialize_categories
            # doesn't re-embed all default categories on every memorize().
            self._service._context.categories_ready = True

            # Populate category name→ID mapping from DB.
            # _initialize_categories (which normally does this) is skipped
            # because we set categories_ready=True above.  On restart,
            # _ensure_categories also skips categories that already exist,
            # so the mapping would stay empty without this block.
            ctx = self._service._get_context()
            ctx.category_ids = []
            ctx.category_name_to_id = {}
            for cat_id, cat in self._service.database.memory_category_repo.categories.items():
                ctx.category_ids.append(cat.id)
                ctx.category_name_to_id[cat.name.lower()] = cat.id
            if ctx.category_name_to_id:
                logger.info(
                    "Populated category mapping: %d categories",
                    len(ctx.category_name_to_id),
                )

            # When no embedding provider is configured, replace the
            # memorize pipeline's "categorize_items" step with one that
            # stores items and resources with embedding=None.  This
            # avoids KeyError on the missing "embedding" LLM profile.
            if not self.config.openai_api_key:
                from memu.workflow.step import WorkflowStep as _WfStep

                _svc = self._service

                async def _categorize_no_embed(
                    state: dict, step_context: Any,
                ) -> dict:
                    svc_ctx = state["ctx"]
                    store = state["store"]
                    modality = state["modality"]
                    local_path = state["local_path"]
                    resources: list = []
                    items: list = []
                    relations: list = []
                    category_updates: dict[str, list[tuple[str, str]]] = {}
                    user_scope = state.get("user", {})

                    for plan in state.get("resource_plans", []):
                        caption_text = (plan.get("caption") or "").strip() or None
                        res = store.resource_repo.create_resource(
                            url=plan["resource_url"],
                            modality=modality,
                            local_path=local_path,
                            caption=caption_text,
                            embedding=None,
                            user_data=dict(user_scope or {}),
                        )
                        resources.append(res)

                        entries = plan.get("entries") or []
                        if not entries:
                            continue

                        reinforce = _svc.memorize_config.enable_item_reinforcement
                        for memory_type, summary_text, cat_names in entries:
                            item = store.memory_item_repo.create_item(
                                resource_id=res.id,
                                memory_type=memory_type,
                                summary=summary_text,
                                embedding=None,
                                user_data=dict(user_scope or {}),
                                reinforce=reinforce,
                            )
                            items.append(item)
                            if reinforce and item.extra.get(
                                "reinforcement_count", 1,
                            ) > 1:
                                continue
                            mapped = _svc._map_category_names_to_ids(
                                cat_names, svc_ctx,
                            )
                            for cid in mapped:
                                relations.append(
                                    store.category_item_repo.link_item_category(
                                        item.id, cid,
                                        user_data=dict(user_scope or {}),
                                    )
                                )
                                category_updates.setdefault(cid, []).append(
                                    (item.id, summary_text),
                                )

                    state.update({
                        "resources": resources,
                        "items": items,
                        "relations": relations,
                        "category_updates": category_updates,
                    })
                    return state

                self._service.replace_step(
                    target_step_id="categorize_items",
                    new_step=_WfStep(
                        step_id="categorize_items",
                        role="categorize",
                        handler=_categorize_no_embed,
                        requires={
                            "resource_plans", "ctx", "store",
                            "local_path", "modality", "user",
                        },
                        produces={
                            "resources", "items",
                            "relations", "category_updates",
                        },
                        capabilities={"db"},
                    ),
                    pipeline="memorize",
                )
                logger.info(
                    "No embedding provider — replaced memorize categorize_items "
                    "step (embeddings disabled, using LLM-based recall)"
                )

            # Warm up the LLM clients.  The first HTTP request on a fresh
            # connection can hang (HTTP/2 negotiation issue with Cloudflare,
            # or cold Bedrock endpoint).  A cheap throwaway call here forces
            # the connection open so real memorize calls don't stall.
            for profile in ("memorize", "fast", "default"):
                try:
                    client = self._service._get_llm_base_client(profile)
                    if isinstance(client, _BedrockLLMClient):
                        await asyncio.wait_for(
                            client.chat("ping", max_tokens=1),
                            timeout=15,
                        )
                        logger.debug("Warmed up Bedrock LLM client: %s", profile)
                    elif hasattr(client, "client"):  # OpenAISDKClient
                        await asyncio.wait_for(
                            client.client.chat.completions.create(
                                model=client.chat_model,
                                messages=[{"role": "user", "content": "ping"}],
                                max_tokens=1,
                            ),
                            timeout=15,
                        )
                        logger.debug("Warmed up LLM client: %s", profile)
                except Exception as e:
                    logger.debug("LLM client warmup for %s: %s", profile, e)

            # --- LLM call logging via interceptor registry ---
            # Uses memU's built-in LLMClientWrapper interceptor hooks which
            # correctly wrap ALL LLM calls (including those routed through
            # _get_step_llm_client).  Interceptors handle logging; the
            # monkey-patching in _instrument_llm_timeouts() handles per-call
            # timeouts (it works because LLMClientWrapper.chat() delegates
            # to self._client.chat() which resolves the instance attribute).

            def _on_llm_before(ctx, request_view):
                logger.info(
                    "memU LLM call [%s/%s]: %s, prompt=%d chars",
                    ctx.profile, ctx.step_id or "?",
                    request_view.kind, request_view.input_chars or 0,
                )

            def _on_llm_after(ctx, request_view, response_view, usage):
                logger.info(
                    "memU LLM done [%s/%s]: %.0fms, response=%d chars",
                    ctx.profile, ctx.step_id or "?",
                    usage.latency_ms or 0, response_view.output_chars or 0,
                )

            def _on_llm_error(ctx, request_view, error, usage):
                logger.error(
                    "memU LLM error [%s/%s]: %s after %.0fms (prompt=%d chars)",
                    ctx.profile, ctx.step_id or "?",
                    error, usage.latency_ms or 0, request_view.input_chars or 0,
                )

            self._service.intercept_before_llm_call(_on_llm_before, name="nerve_llm_log")
            self._service.intercept_after_llm_call(_on_llm_after, name="nerve_llm_log_after")
            self._service.intercept_on_error_llm_call(_on_llm_error, name="nerve_llm_log_error")

            # --- Per-call timeout on base LLM clients ---
            # LLMClientWrapper._invoke() has no built-in call timeout, so a
            # stale HTTP/2 connection can hang forever.  We wrap each base
            # client's .chat() with asyncio.wait_for() so individual calls
            # fail fast instead of blocking the entire pipeline timeout.
            self._instrument_llm_timeouts()

            # Log each workflow step with elapsed time for debugging hangs.
            # Stored on self so memorize_file can report which step is stuck on timeout.
            self._step_timers: dict[str, float] = {}

            def _log_before_step(step_ctx, state):
                step_id = getattr(step_ctx, "step_id", "?")
                self._step_timers[step_id] = time.monotonic()
                resource = getattr(state, "resource_url", None) or getattr(state, "url", None) or ""
                if resource:
                    resource = Path(resource).name
                logger.info("memU step starting: %s%s", step_id, f" [{resource}]" if resource else "")

            def _log_after_step(step_ctx, state):
                step_id = getattr(step_ctx, "step_id", "?")
                t0 = self._step_timers.pop(step_id, None)
                elapsed = f" ({time.monotonic() - t0:.1f}s)" if t0 is not None else ""
                logger.info("memU step finished: %s%s", step_id, elapsed)

            self._service.intercept_before_workflow_step(_log_before_step, name="nerve_step_log")
            self._service.intercept_after_workflow_step(_log_after_step, name="nerve_step_log_after")

            # Preload item/category embeddings into memory so the first
            # recall doesn't pay the 2s JSON-parse cost.
            try:
                self._service.database.memory_item_repo.list_items()
                self._service.database.memory_category_repo.list_categories()

                # Convert cached embeddings from list[float] to numpy float32.
                # Pydantic coerces numpy → list during model construction, so we
                # bypass it with __dict__ assignment after the models are built.
                # This drops ~170 MB of RSS (48 KB → 6 KB per 1536-dim embedding).
                converted = 0
                for item in self._service.database.memory_item_repo.items.values():
                    if item.embedding is not None and not isinstance(item.embedding, np.ndarray):
                        item.__dict__["embedding"] = np.array(item.embedding, dtype=np.float32)
                        converted += 1
                for cat in self._service.database.memory_category_repo.categories.values():
                    if cat.embedding is not None and not isinstance(cat.embedding, np.ndarray):
                        cat.__dict__["embedding"] = np.array(cat.embedding, dtype=np.float32)
                        converted += 1

                # Release the list[float] objects immediately
                self._release_memory()

                logger.info(
                    "Preloaded %d items and %d categories into vector cache "
                    "(%d embeddings converted to numpy float32)",
                    len(self._service.database.memory_item_repo.items),
                    len(self._service.database.memory_category_repo.categories),
                    converted,
                )
            except Exception as e:
                logger.warning("Failed to preload vector cache: %s", e)

            # Monkey-patch create_item to keep new embeddings as numpy too.
            from memu.database.sqlite.repositories.memory_item_repo import SQLiteMemoryItemRepo
            _original_create_item = SQLiteMemoryItemRepo.create_item

            def _numpy_create_item(self, *args, **kwargs):
                result = _original_create_item(self, *args, **kwargs)
                # Convert the newly cached item's embedding to numpy
                if result and hasattr(result, "id"):
                    cached = self.items.get(result.id)
                    if cached and cached.embedding is not None and not isinstance(cached.embedding, np.ndarray):
                        cached.__dict__["embedding"] = np.array(cached.embedding, dtype=np.float32)
                return result

            SQLiteMemoryItemRepo.create_item = _numpy_create_item

            return True

        except ImportError:
            logger.warning("memU not installed — memory recall will be unavailable")
            return False
        except Exception as e:
            logger.error("Failed to initialize memU: %s", e, exc_info=True)
            return False

    async def _ensure_categories(self) -> None:
        """Create seed categories from config that don't already exist in the DB."""
        if not self._service or not self.config.memory.categories:
            return

        existing: set[str] = set()
        try:
            cats = self._service.database.memory_category_repo.list_categories()
            for _, cat in cats.items():
                existing.add(getattr(cat, "name", ""))
        except Exception:
            pass

        for cat_cfg in self.config.memory.categories:
            if cat_cfg.name in existing:
                continue
            try:
                await self.create_category(cat_cfg.name, cat_cfg.description)
            except Exception as e:
                logger.warning("Failed to seed category %s: %s", cat_cfg.name, e)

    # Maximum time (seconds) for a single memorize operation before cancellation.
    # Try to load malloc_trim for returning freed arenas to the OS.
    # On glibc (Linux), Python's arena allocator holds freed pages;
    # malloc_trim(0) forces them back, preventing RSS ratcheting.
    _libc = None
    try:
        _libc = ctypes.CDLL("libc.so.6")
    except OSError:
        pass

    @staticmethod
    def _release_memory() -> None:
        """Force Python GC and return freed pages to the OS."""
        gc.collect()
        if MemUBridge._libc is not None:
            try:
                MemUBridge._libc.malloc_trim(0)
            except Exception:
                pass

    _MEMORIZE_TIMEOUT = 300
    # Number of retry attempts after a timeout.
    _MEMORIZE_MAX_RETRIES = 2
    # Base delay (seconds) before retrying after a timeout.
    # Each retry doubles: 15s, 30s.
    _MEMORIZE_RETRY_DELAY = 15
    # Per-call timeout for individual LLM requests (chat, embed, etc.).
    _LLM_CALL_TIMEOUT = 120

    def _make_bedrock_client(self, chat_model: str) -> _BedrockLLMClient:
        """Create a _BedrockLLMClient using Nerve's provider config."""
        return _BedrockLLMClient(
            chat_model=chat_model,
            aws_region=self.config.provider.aws_region,
            aws_profile=self.config.provider.aws_profile,
            aws_access_key_id=self.config.provider.aws_access_key_id,
            aws_secret_access_key=self.config.provider.aws_secret_access_key,
        )

    def _inject_bedrock_clients(self) -> None:
        """Replace placeholder OpenAISDKClient instances with Bedrock clients.

        Called once after MemoryService init when provider is Bedrock.
        The placeholder clients were necessary for memU's config validation
        but can't actually make API calls.
        """
        profiles_to_replace = {}
        for name, cfg in self._service.llm_profiles.profiles.items():
            # Skip the embedding profile — it uses OpenAI directly
            if name == "embedding":
                continue
            profiles_to_replace[name] = cfg.chat_model

        for name, model in profiles_to_replace.items():
            self._service._llm_clients[name] = self._make_bedrock_client(model)
            logger.info("Injected Bedrock LLM client for profile '%s' (model=%s)", name, model)

    def _instrument_llm_timeouts(self) -> None:
        """Configure per-call timeouts on LLM clients (two layers).

        Layer 1: httpx-level timeout on the AsyncOpenAI transport.  This
        catches unresponsive API calls at the socket level and raises
        openai.APITimeoutError with a descriptive message.

        Layer 2: asyncio.wait_for() wrapper on the base client's .chat()
        method.  Safety net in case the httpx timeout doesn't fire
        (e.g. the coroutine is stuck in Python code, not I/O).
        It works because LLMClientWrapper.chat() delegates to
        self._client.chat() which resolves the instance attribute we set.
        """
        import httpx as _httpx

        for profile in ("memorize", "fast", "default"):
            try:
                client = self._service._get_llm_base_client(profile)

                # --- Layer 1: httpx timeout + disable SDK retries ---
                # (Bedrock clients use their own timeout; skip Layer 1 for them)
                if not isinstance(client, _BedrockLLMClient):
                    inner = getattr(client, "client", None)  # OpenAISDKClient.client = AsyncOpenAI
                    if inner is not None:
                        inner.timeout = _httpx.Timeout(
                            self._LLM_CALL_TIMEOUT,
                            connect=10.0,
                        )
                        # The OpenAI SDK defaults to max_retries=2 and 600s timeout.
                        # With our 120s asyncio.wait_for wrapper, SDK retries just
                        # waste time inside a doomed coroutine.  Disable them so the
                        # httpx timeout fires cleanly and propagates immediately.
                        inner.max_retries = 0

                # --- Layer 2: asyncio.wait_for wrapper ---
                if not callable(getattr(client, "chat", None)):
                    continue
                # Skip if already wrapped (e.g. after a retry reset)
                if getattr(client.chat, "_nerve_timeout_wrapped", False):
                    continue
                original_chat = client.chat

                async def _timeout_chat(
                    prompt, *, max_tokens=None, system_prompt=None, temperature=0.2,
                    _orig=original_chat, _prof=profile,
                ):
                    # Anthropic API requires max_tokens >= 1; memU sometimes
                    # omits it.  Default to 4096 to prevent 400 errors.
                    if max_tokens is None:
                        max_tokens = 4096
                    t0 = time.monotonic()
                    try:
                        return await asyncio.wait_for(
                            _orig(
                                prompt,
                                max_tokens=max_tokens,
                                system_prompt=system_prompt,
                                temperature=temperature,
                            ),
                            timeout=self._LLM_CALL_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        elapsed = time.monotonic() - t0
                        in_flight = len(self._metrics.in_flight)
                        # Dump httpx connection pool state for diagnosis
                        pool_info = "unknown"
                        try:
                            base = self._service._llm_clients.get(_prof)
                            sdk = getattr(base, "client", None)
                            transport = getattr(sdk, "_client", None)
                            pool = getattr(transport, "_pool", None) or getattr(transport, "_transport", None)
                            if pool:
                                pool_info = repr(pool)
                        except Exception:
                            pass
                        logger.error(
                            "memU LLM HUNG [%s]: no response after %.0fs "
                            "(prompt=%d chars, in_flight=%d, pool=%s)",
                            _prof, elapsed, len(prompt),
                            in_flight, pool_info,
                        )
                        raise

                _timeout_chat._nerve_timeout_wrapped = True  # type: ignore[attr-defined]
                client.chat = _timeout_chat  # type: ignore[method-assign]
                logger.info("Configured %ds timeout on LLM client: %s", self._LLM_CALL_TIMEOUT, profile)
            except Exception as e:
                logger.warning("Could not configure LLM client %s: %s", profile, e)

    @staticmethod
    def _is_llm_timeout(exc: Exception) -> bool:
        """Check if exception is an LLM-level timeout (not a logic error).

        httpx-level timeouts raise openai.APITimeoutError (or httpx.TimeoutException)
        which should trigger retry, not immediate failure.
        """
        try:
            import httpx as _httpx
            if isinstance(exc, _httpx.TimeoutException):
                return True
        except ImportError:
            pass
        try:
            from openai import APITimeoutError
            if isinstance(exc, APITimeoutError):
                return True
        except ImportError:
            pass
        return False

    async def _probe_api_health(self, profile: str = "fast") -> str:
        """Quick health check against the API after a timeout.

        Sends a tiny request on the 'fast' profile (Haiku) to distinguish
        between API-wide outage vs. model-specific throttling.  Returns a
        short diagnostic string for the log.
        """
        try:
            client = self._service._get_llm_base_client(profile)
            t0 = time.monotonic()
            if isinstance(client, _BedrockLLMClient):
                await asyncio.wait_for(
                    client.chat("ping", max_tokens=1),
                    timeout=15,
                )
            elif hasattr(client, "client"):
                await asyncio.wait_for(
                    client.client.chat.completions.create(
                        model=client.chat_model,
                        messages=[{"role": "user", "content": "ping"}],
                        max_tokens=1,
                    ),
                    timeout=15,
                )
            else:
                return "unknown (no SDK client)"
            elapsed = time.monotonic() - t0
            return f"ok ({profile}/{client.chat_model} responded in {elapsed:.1f}s)"
        except Exception as e:
            return f"FAIL ({profile}: {type(e).__name__}: {e})"

    async def _reset_llm_clients(self) -> None:
        """Evict cached LLM clients and force fresh HTTP connections.

        After a timeout the API may be rate-limiting, overloaded, or the
        connection may be stale.  We close the transport, evict the cached
        client so _get_llm_base_client() creates a new one, then re-apply
        per-call timeouts.
        """
        is_bedrock = self.config.provider.is_bedrock

        # Probe API health before resetting — helps diagnose whether the
        # issue is model-specific throttling vs. API-wide outage.
        health = await self._probe_api_health("fast")
        logger.info("API health probe before reset: %s", health)

        for profile in ("memorize", "fast", "default"):
            try:
                client = self._service._llm_clients.get(profile)
                if client is None:
                    continue
                # Close the underlying transport
                if isinstance(client, _BedrockLLMClient):
                    await client.close()
                else:
                    inner = getattr(client, "client", None)  # OpenAISDKClient
                    http = getattr(inner, "_client", None) or getattr(inner, "http_client", None)
                    if http:
                        if hasattr(http, "aclose"):
                            try:
                                await http.aclose()
                            except Exception:
                                pass  # Best-effort async close
                        elif hasattr(http, "close"):
                            http.close()
                # Evict so next access creates a fresh client
                del self._service._llm_clients[profile]
                logger.info("Evicted stale LLM client for profile '%s'", profile)
            except Exception as e:
                logger.warning("Could not reset LLM client for '%s': %s", profile, e)

        # For Bedrock, re-inject fresh Bedrock clients (memU's default
        # _get_llm_base_client would recreate broken OpenAISDK ones).
        if is_bedrock:
            self._inject_bedrock_clients()

        # Re-apply per-call timeouts on the fresh clients
        self._instrument_llm_timeouts()

        # Warm up the new clients — the first HTTP/2 request on a fresh
        # AsyncOpenAI→httpx connection can stall.  A cheap throwaway call
        # forces the connection open before the real memorize call.
        for profile in ("memorize", "fast"):
            try:
                client = self._service._get_llm_base_client(profile)
                if isinstance(client, _BedrockLLMClient):
                    # Bedrock warmup — use the adapter's chat method directly
                    await asyncio.wait_for(
                        client.chat("ping", max_tokens=1),
                        timeout=15,
                    )
                    logger.info("Warmed up Bedrock LLM client after reset: %s", profile)
                elif hasattr(client, "client"):
                    await asyncio.wait_for(
                        client.client.chat.completions.create(
                            model=client.chat_model,
                            messages=[{"role": "user", "content": "ping"}],
                            max_tokens=1,
                        ),
                        timeout=15,
                    )
                    logger.info("Warmed up LLM client after reset: %s", profile)
            except Exception as e:
                logger.warning("LLM warmup after reset for %s: %s", profile, e)

    async def memorize_file(self, file_path: str, modality: str = "document", source: str = "bridge") -> bool:
        """Memorize a local file by its absolute path."""
        if not self._available or not self._service:
            return False

        file_size = -1
        try:
            file_size = Path(file_path).stat().st_size
        except OSError:
            pass

        attempt = 0
        last_error = ""

        while attempt <= self._MEMORIZE_MAX_RETRIES:
            op_id = self._metrics.begin_op("memorize_file", file_path)
            label = f"(attempt {attempt + 1}/{self._MEMORIZE_MAX_RETRIES + 1})" if attempt > 0 else ""

            try:
                logger.info(
                    "memU memorize_file starting %sfor %s (%d bytes, modality=%s)",
                    label + " " if label else "", file_path, file_size, modality,
                )
                t0 = time.monotonic()

                response = await asyncio.wait_for(
                    self._service.memorize(
                        resource_url=file_path,
                        modality=modality,
                    ),
                    timeout=self._MEMORIZE_TIMEOUT,
                )

                elapsed = time.monotonic() - t0
                logger.info("Indexed file: %s (%.1fs)", file_path, elapsed)
                self._metrics.end_op(op_id, success=True)
                await self._audit("file_indexed", "resource", file_path, source)

                # Fire-and-forget: filter out generic knowledge items (opt-in)
                if self.config.memory.knowledge_filter and response:
                    knowledge_items = [
                        item for item in response.get("items", [])
                        if item.get("memory_type") == "knowledge"
                    ]
                    if knowledge_items:
                        asyncio.create_task(
                            self._filter_knowledge_items(knowledge_items),
                            name=f"filter-knowledge-{Path(file_path).name}",
                        )

                self._release_memory()
                return True

            except asyncio.TimeoutError:
                elapsed = time.monotonic() - t0
                # Report which memU pipeline step was still running when we timed out
                now_mono = time.monotonic()
                stuck_steps = [
                    f"{step_id} (running {now_mono - t:.0f}s)"
                    for step_id, t in getattr(self, "_step_timers", {}).items()
                ]
                stuck_info = "; stuck on: " + ", ".join(stuck_steps) if stuck_steps else ""
                # Use actual elapsed time — the TimeoutError may come from the
                # inner per-call timeout (120s) rather than the outer pipeline
                # timeout (300s).
                last_error = f"timeout after {elapsed:.0f}s{stuck_info}"
                # Clear stale step timers from the cancelled pipeline
                if hasattr(self, "_step_timers"):
                    self._step_timers.clear()

                logger.error(
                    "memU memorize_file timed out after %.0fs %sfor %s (%d bytes)%s",
                    elapsed, label + " " if label else "",
                    file_path, file_size, stuck_info,
                )
                self._metrics.end_op(op_id, success=False, error=last_error)

                if attempt < self._MEMORIZE_MAX_RETRIES:
                    # Exponential backoff: base_delay * 2^attempt
                    delay = self._MEMORIZE_RETRY_DELAY * (2 ** attempt)
                    logger.info(
                        "Resetting LLM clients and retrying in %ds...", delay,
                    )
                    await self._reset_llm_clients()
                    await asyncio.sleep(delay)

            except Exception as e:
                # Check if this is an LLM timeout that propagated as a
                # non-asyncio exception (e.g. openai.APITimeoutError from
                # the httpx-level timeout).
                if self._is_llm_timeout(e):
                    elapsed = time.monotonic() - t0
                    now_mono = time.monotonic()
                    stuck_steps = [
                        f"{step_id} (running {now_mono - t:.0f}s)"
                        for step_id, t in getattr(self, "_step_timers", {}).items()
                    ]
                    stuck_info = "; stuck on: " + ", ".join(stuck_steps) if stuck_steps else ""
                    last_error = f"LLM timeout after {elapsed:.0f}s: {e}{stuck_info}"
                    if hasattr(self, "_step_timers"):
                        self._step_timers.clear()
                    logger.error(
                        "memU memorize_file LLM timeout after %.0fs %sfor %s (%d bytes): %s%s",
                        elapsed, label + " " if label else "",
                        file_path, file_size, e, stuck_info,
                    )
                    self._metrics.end_op(op_id, success=False, error=last_error)

                    if attempt < self._MEMORIZE_MAX_RETRIES:
                        delay = self._MEMORIZE_RETRY_DELAY * (2 ** attempt)
                        logger.info("Resetting LLM clients and retrying in %ds...", delay)
                        await self._reset_llm_clients()
                        await asyncio.sleep(delay)
                else:
                    last_error = str(e)
                    logger.error("memU memorize_file failed %sfor %s: %s", label + " " if label else "", file_path, e)
                    self._metrics.end_op(op_id, success=False, error=last_error)
                    return False

            attempt += 1

        logger.error("memU memorize_file gave up after %d attempts for %s", attempt, file_path)
        self._release_memory()
        return False

    async def memorize_conversation(self, session_id: str, messages: list[dict]) -> bool:
        """Write conversation as JSON then memorize it into memU.

        memU expects conversation files to be a JSON array of
        ``{"role": ..., "content": ..., "created_at": ...}`` dicts so its
        ``format_conversation_for_preprocess`` can segment and index them properly.
        """
        if not messages or not self._available:
            return False

        entries: list[dict[str, str]] = []
        earliest_ts: str | None = None
        for msg in messages:
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if not content:
                continue
            entry: dict[str, str] = {"role": role, "content": content}
            created_at = msg.get("created_at")
            if created_at:
                ts_str = str(created_at)
                entry["created_at"] = ts_str
                if earliest_ts is None or ts_str < earliest_ts:
                    earliest_ts = ts_str
            entries.append(entry)
        if not entries:
            return False

        op_id = self._metrics.begin_op("memorize_conversation", f"session {session_id}")

        now = int(time.time())
        conv_dir = Path("~/.nerve/memu-conversations").expanduser()
        conv_dir.mkdir(parents=True, exist_ok=True)
        conv_path = conv_dir / f"session-{session_id}-{now}.json"
        conv_path.write_text(json.dumps(entries, ensure_ascii=False), encoding="utf-8")

        try:
            logger.info(
                "Memorizing conversation: session=%s, messages=%d, file=%s (%d bytes)",
                session_id, len(entries), conv_path.name, conv_path.stat().st_size,
            )
            ok = await self.memorize_file(str(conv_path), modality="conversation")

            # memU doesn't set happened_at from conversation timestamps.
            # Resolve dates: events get LLM-resolved dates, non-events stay NULL.
            # Always attempt this — even on timeout, memU may have partially
            # persisted items before the pipeline was cancelled.
            if earliest_ts:
                try:
                    await self._resolve_event_dates(earliest_ts)
                except Exception as e:
                    logger.warning("Event date resolution failed: %s", e)

            if ok:
                await self._audit("conversation_indexed", "resource", session_id, "bridge", {
                    "message_count": len(entries),
                })
            else:
                # Propagate the underlying memorize_file error for diagnostics
                file_stats = self._metrics.ops.get("memorize_file")
                underlying = file_stats.last_error if file_stats and file_stats.last_error else "unknown"
                error_msg = f"memorize_file failed: {underlying}"
                logger.warning("memorize_conversation failed for session %s: %s", session_id, error_msg)
                self._metrics.end_op(op_id, success=False, error=error_msg)
                return False

            self._metrics.end_op(op_id, success=True)
            return True
        except Exception as e:
            self._metrics.end_op(op_id, success=False, error=str(e))
            raise

    async def _resolve_event_dates(self, conversation_ts: str) -> None:
        """Resolve happened_at for newly indexed items using LLM for events.

        - Event items: LLM resolves actual date from content (future events
          get conversation_date since the "event" is the planning/scheduling).
        - Non-event items: happened_at stays NULL (timeless facts).
        - All items: extra.mentioned_at = conversation date.

        Runs synchronous work (sqlite3 + Anthropic API) in a dedicated thread
        pool so it cannot starve the default asyncio executor.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._blocking_pool, self._resolve_event_dates_sync, conversation_ts,
        )

    def _resolve_event_dates_sync(self, conversation_ts: str) -> None:
        """Synchronous implementation of event date resolution."""
        import sqlite3

        db_path = self.config.memory.sqlite_dsn.replace("sqlite:///", "")
        try:
            db = sqlite3.connect(db_path, timeout=10)
            db.row_factory = sqlite3.Row

            rows = db.execute(
                "SELECT id, memory_type, summary, extra "
                "FROM memu_memory_items WHERE happened_at IS NULL"
            ).fetchall()

            if not rows:
                db.close()
                return

            # Convert UTC timestamp to user's local date
            try:
                tz = ZoneInfo(self.config.timezone)
                utc_dt = datetime.fromisoformat(conversation_ts)
                conv_date = utc_dt.astimezone(tz).strftime("%Y-%m-%d")
            except Exception:
                conv_date = conversation_ts[:10]

            # Split into event and non-event items
            event_items = [(r["id"], r["summary"]) for r in rows if r["memory_type"] == "event"]
            non_event_ids = [r["id"] for r in rows if r["memory_type"] != "event"]

            # Resolve event dates via LLM
            resolved_dates: dict[str, str | None] = {}
            if event_items:
                try:
                    resolved_dates = self._resolve_dates_via_llm(event_items, conv_date)
                except Exception as e:
                    logger.warning("LLM date resolution failed, falling back to conversation date: %s", e)

            # Update event items
            for item_id, summary in event_items:
                happened_at = resolved_dates.get(item_id) or conv_date
                db.execute(
                    "UPDATE memu_memory_items SET happened_at = ? WHERE id = ?",
                    (happened_at, item_id),
                )

            # Set mentioned_at on ALL items (events + non-events)
            for row in rows:
                item_id = row["id"]
                extra = json.loads(row["extra"]) if row["extra"] else {}
                extra["mentioned_at"] = conv_date
                db.execute(
                    "UPDATE memu_memory_items SET extra = ? WHERE id = ?",
                    (json.dumps(extra, ensure_ascii=False), item_id),
                )

            db.commit()
            db.close()
            logger.debug(
                "Resolved dates for %d items (%d events via LLM, %d non-events left NULL)",
                len(rows), len(event_items), len(non_event_ids),
            )
        except Exception as e:
            logger.warning("Event date resolution failed: %s", e)

    def _get_anthropic_client(self) -> Any:
        """Get or create the shared sync Anthropic client.

        Uses the config factory method which returns AnthropicBedrock
        when provider is "bedrock", or standard Anthropic otherwise.
        """
        if self._anthropic_client is not None:
            return self._anthropic_client
        self._anthropic_client = self.config.create_anthropic_client(timeout=60.0)
        return self._anthropic_client

    def _resolve_dates_via_llm(
        self, items: list[tuple[str, str]], conversation_date: str,
    ) -> dict[str, str | None]:
        """Call Anthropic API to resolve actual happened_at dates for event items.

        Uses the fast_model (Haiku) for this structured extraction task.
        Returns a dict mapping item_id -> resolved ISO date string or None.
        """
        model = self.config.memory.fast_model or self.config.memory.recall_model
        client = self._get_anthropic_client()

        items_text = "\n".join(
            f"{i}. {summary}" for i, (_, summary) in enumerate(items)
        )

        prompt = (
            f"Given these event memory items extracted from a conversation on {conversation_date}, "
            f"determine when each event actually happened or was first mentioned.\n\n"
            f"Rules:\n"
            f"- Past events with specific dates (e.g., 'went hiking on February 5') → return that date\n"
            f"- Future events or plans (e.g., 'scheduled appointment for March 15') → return {conversation_date} "
            f"(the planning/scheduling happened on the conversation date)\n"
            f"- Events on the conversation date → return {conversation_date}\n"
            f"- Undeterminable date → return null\n\n"
            f"Items:\n{items_text}\n\n"
            f"Return ONLY a valid JSON array with one object per item, in the same order:\n"
            f'[{{"happened_at": "YYYY-MM-DD"}}, {{"happened_at": null}}, ...]'
        )

        response = client.messages.create(
            model=model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        result: dict[str, str | None] = {}
        try:
            import re
            text = response.content[0].text
            json_match = re.search(r"\[.*\]", text, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                for idx, entry in enumerate(parsed):
                    if 0 <= idx < len(items):
                        item_id = items[idx][0]
                        raw = entry.get("happened_at")
                        result[item_id] = self._validate_date_value(raw)
        except Exception as e:
            logger.warning("Failed to parse LLM date resolution response: %s", e)

        return result

    @staticmethod
    def _validate_date_value(raw: Any) -> str | None:
        """Validate and normalize a date value from LLM output.

        The LLM may return bare integers (e.g. 2025), partial dates, or
        other non-standard values.  Only well-formed ISO date strings are
        accepted; everything else is coerced or rejected.
        """
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            # Bare year like 2025 → "2025-01-01"
            year = int(raw)
            if 1900 <= year <= 2100:
                return f"{year}-01-01"
            return None
        if not isinstance(raw, str):
            return None
        raw = raw.strip()
        if not raw:
            return None
        # Validate it's a parseable date
        try:
            datetime.fromisoformat(raw)
            return raw
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Post-extraction knowledge relevance filter
    # ------------------------------------------------------------------

    async def _filter_knowledge_items(self, items: list[dict[str, Any]]) -> None:
        """Post-extraction filter: delete generic knowledge items using Haiku.

        Runs as fire-and-forget after memorize() completes.  Items that any
        experienced software engineer would know without project context are
        deleted from the database.
        """
        if not items or not self._available or not self._service:
            return

        try:
            model = self.config.memory.fast_model or self.config.memory.recall_model

            items_text = "\n".join(
                f"{i}. {item.get('summary', '')}"
                for i, item in enumerate(items)
            )

            prompt = (
                "You are a memory quality filter. Review these knowledge items extracted "
                "from a conversation and stored in a personal memory system.\n\n"
                "Identify items that are GENERIC knowledge — facts any experienced software "
                "engineer would know without access to this user's specific projects or environment.\n\n"
                "GENERIC (delete):\n"
                "- Standard programming language features, syntax, standard library behavior\n"
                "- Common CS concepts (hashing, caching, data structures, algorithms)\n"
                "- Well-known framework/library behavior\n"
                "- Standard DevOps facts (Docker, Linux, nginx basics)\n"
                "- Widely documented API behavior of popular libraries\n\n"
                "KEEP:\n"
                "- Project-specific architecture decisions or conventions\n"
                "- Non-obvious gotchas specific to this user's environment\n"
                "- Custom tool behavior, internal API quirks\n"
                "- Integration-specific knowledge unique to the user's setup\n\n"
                f"Items:\n{items_text}\n\n"
                "Return ONLY a JSON array of 0-based indices of GENERIC items to delete.\n"
                "Example: [0, 2, 5]\n"
                "If all items are worth keeping, return: []\n"
                "Return ONLY the JSON array, nothing else."
            )

            loop = asyncio.get_running_loop()
            indices = await loop.run_in_executor(
                self._blocking_pool,
                self._call_knowledge_filter_sync,
                model,
                prompt,
            )

            deleted = 0
            for idx in indices:
                if 0 <= idx < len(items):
                    item_id = items[idx].get("id", "")
                    if item_id:
                        try:
                            await self.delete_item(item_id, source="knowledge_filter")
                            deleted += 1
                            logger.debug(
                                "Knowledge filter: deleted generic item %s: %s",
                                item_id, items[idx].get("summary", "")[:80],
                            )
                        except Exception as e:
                            logger.warning("Knowledge filter: failed to delete %s: %s", item_id, e)

            if deleted:
                logger.info(
                    "Knowledge filter: deleted %d/%d generic items", deleted, len(items),
                )

        except Exception as e:
            logger.warning("Knowledge filter failed (non-fatal): %s", e)

    def _call_knowledge_filter_sync(self, model: str, prompt: str) -> list[int]:
        """Synchronous Haiku call for knowledge filtering (runs in thread pool)."""
        import re as _re

        client = self._get_anthropic_client()
        response = client.messages.create(
            model=model,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )

        try:
            text = response.content[0].text
            json_match = _re.search(r"\[.*?\]", text, _re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                if isinstance(parsed, list):
                    return [int(x) for x in parsed if isinstance(x, (int, float))]
        except Exception as e:
            logger.warning("Failed to parse knowledge filter response: %s", e)

        return []

    async def recall(self, query: str, limit: int = 10) -> list[dict[str, str]]:
        """Recall relevant memories via semantic search.

        Returns list of dicts with keys: id, type, summary.
        Category results use id="cat:<id>".
        """
        if not self._available or not self._service:
            return []
        op_id = self._metrics.begin_op("recall", query[:80])
        try:
            result = await self._service.retrieve(
                queries=[{"role": "user", "content": query}],
            )

            memories: list[dict[str, str]] = []

            # Extract from items (individual memory entries)
            for item in result.get("items", []):
                if isinstance(item, dict):
                    text = item.get("summary", item.get("content", ""))
                    item_id = item.get("id", "")
                    mtype = item.get("memory_type", "")
                elif hasattr(item, "summary"):
                    text = item.summary
                    item_id = getattr(item, "id", "")
                    mtype = getattr(item, "memory_type", "")
                else:
                    text = str(item)
                    item_id = ""
                    mtype = ""
                if text:
                    memories.append({"id": item_id, "type": mtype, "summary": text})

            # Extract from categories (higher-level summaries)
            for cat in result.get("categories", []):
                if isinstance(cat, dict):
                    summary = cat.get("summary", "")
                    name = cat.get("name", "")
                    cat_id = cat.get("id", "")
                elif hasattr(cat, "summary"):
                    summary = cat.summary
                    name = getattr(cat, "name", "")
                    cat_id = getattr(cat, "id", "")
                else:
                    continue
                if summary:
                    memories.append({
                        "id": f"cat:{cat_id}",
                        "type": "category",
                        "summary": f"[{name}] {summary}" if name else summary,
                    })

            self._metrics.end_op(op_id, success=True)
            return memories[:limit]

        except Exception as e:
            logger.error("memU recall failed: %s", e)
            self._metrics.end_op(op_id, success=False, error=str(e))
            return []

    async def list_items(self) -> list[dict[str, Any]]:
        """List all memory items."""
        if not self._available or not self._service:
            return []
        try:
            result = await self._service.list_memory_items(where=None)
            items = []
            for item in result.get("items", []):
                items.append({
                    "id": item.id,
                    "memory_type": item.memory_type,
                    "summary": item.summary,
                    "resource_id": getattr(item, "resource_id", None),
                    "created_at": str(item.created_at) if item.created_at else None,
                    "updated_at": str(item.updated_at) if item.updated_at else None,
                })
            return items
        except Exception as e:
            logger.error("list_items failed: %s", e)
            return []

    async def list_categories(self) -> list[dict[str, Any]]:
        """List all memory categories."""
        if not self._available or not self._service:
            return []
        try:
            result = await self._service.list_memory_categories(where=None)
            cats = []
            for cat in result.get("categories", []):
                cats.append({
                    "id": cat.id,
                    "name": cat.name,
                    "description": cat.description,
                    "summary": getattr(cat, "summary", None),
                })
            return cats
        except Exception as e:
            logger.error("list_categories failed: %s", e)
            return []

    async def update_item(
        self,
        memory_id: str,
        content: str | None = None,
        memory_type: str | None = None,
        categories: list[str] | None = None,
        source: str = "bridge",
    ) -> bool:
        """Update an existing memory item's content, type, or categories."""
        if not self._available or not self._service:
            return False
        try:
            kwargs: dict[str, Any] = {"memory_id": memory_id}
            if content is not None:
                kwargs["memory_content"] = content
            if memory_type is not None:
                kwargs["memory_type"] = memory_type
            if categories is not None:
                kwargs["memory_categories"] = categories
            await self._service.update_memory_item(**kwargs)
            logger.info("Updated memory item: %s", memory_id)
            await self._audit("item_updated", "item", memory_id, source, {
                "content_changed": content is not None,
                "type_changed": memory_type is not None,
                "categories_changed": categories is not None,
            })
            return True
        except Exception as e:
            logger.error("update_item failed for %s: %s", memory_id, e)
            return False

    async def delete_item(self, memory_id: str, source: str = "bridge") -> bool:
        """Delete a memory item by ID."""
        if not self._available or not self._service:
            return False
        try:
            await self._service.delete_memory_item(memory_id=memory_id)
            logger.info("Deleted memory item: %s", memory_id)
            await self._audit("item_deleted", "item", memory_id, source)
            return True
        except Exception as e:
            logger.error("delete_item failed for %s: %s", memory_id, e)
            return False

    async def create_category(self, name: str, description: str, source: str = "bridge") -> bool:
        """Create a new memory category at runtime."""
        if not self._available or not self._service:
            return False
        try:
            # Generate embedding for the category (requires OpenAI key)
            embedding = None
            if self._has_embeddings:
                try:
                    embed_text = f"{name}: {description}" if description else name
                    vecs = await self._service._get_llm_client("embedding").embed([embed_text])
                    embedding = vecs[0]
                except Exception as e:
                    logger.warning("Could not embed category %s: %s", name, e)

            # Create in the DB repo
            cat = self._service.database.memory_category_repo.get_or_create_category(
                name=name, description=description, embedding=embedding, user_data={},
            )

            # Update in-memory config so new memorizations can assign to this category
            from memu.app.service import CategoryConfig
            cfg = CategoryConfig(name=name, description=description)
            self._service.category_configs.append(cfg)
            self._service.category_config_map[name] = cfg
            self._service._category_prompt_str = self._service._format_categories_for_prompt(
                self._service.category_configs
            )

            # Update context category mappings
            ctx = self._service._get_context()
            if hasattr(ctx, 'category_ids') and ctx.category_ids is not None:
                ctx.category_ids.append(cat.id)
            if hasattr(ctx, 'category_name_to_id') and ctx.category_name_to_id is not None:
                ctx.category_name_to_id[name.lower()] = cat.id

            logger.info("Created category: %s", name)
            await self._audit("category_created", "category", name, source, {"description": description})
            return True
        except Exception as e:
            logger.error("Failed to create category %s: %s", name, e)
            return False

    async def update_category(
        self,
        category_id: str,
        summary: str | None = None,
        description: str | None = None,
        source: str = "bridge",
    ) -> bool:
        """Update a category's summary and/or description, then re-embed."""
        if not self._available or not self._service:
            return False
        try:
            repo = self._service.database.memory_category_repo
            cat = repo.categories.get(category_id)
            if not cat:
                return False

            new_name = cat.name
            new_desc = description if description is not None else cat.description
            new_summary = summary if summary is not None else cat.summary

            # Re-embed from the updated text (requires OpenAI key)
            embedding = None
            if self._has_embeddings:
                try:
                    embed_text = f"{new_name}: {new_desc}"
                    if new_summary:
                        embed_text += f"\n{new_summary}"
                    vecs = await self._service._get_llm_client("embedding").embed([embed_text])
                    embedding = vecs[0]
                except Exception as e:
                    logger.warning("Could not re-embed category %s: %s", category_id, e)

            repo.update_category(
                category_id=category_id,
                description=new_desc if description is not None else None,
                summary=new_summary if summary is not None else None,
                embedding=embedding,
            )
            logger.info("Updated category: %s", category_id)
            await self._audit("category_updated", "category", category_id, source, {
                "summary_changed": summary is not None,
                "description_changed": description is not None,
            })
            return True
        except Exception as e:
            logger.error("update_category failed for %s: %s", category_id, e)
            return False

    async def index_workspace_files(self, workspace: Path) -> int:
        """Index all .md memory files into memU. Skips already-indexed files.

        Returns the count of newly indexed files.
        """
        if not self._available or not self._service:
            return 0

        # Get already-indexed resource URLs
        existing_urls: set[str] = set()
        try:
            resources = self._service.database.resource_repo.list_resources()
            for res_id, res in resources.items():
                url = getattr(res, "url", "")
                if url:
                    existing_urls.add(url)
        except Exception as e:
            logger.warning("Could not load existing resources: %s", e)

        md_files = self._collect_md_files(workspace)
        indexed_count = 0

        for file_path, file_type in md_files:
            path_str = str(file_path)
            if path_str in existing_urls:
                continue
            modality = "conversation" if file_type == "daily" else "document"
            success = await self.memorize_file(path_str, modality=modality)
            if success:
                indexed_count += 1

        logger.info("Indexed %d new files into memU (%d already indexed)", indexed_count, len(existing_urls))
        return indexed_count

    async def reindex_file(self, file_path: str) -> bool:
        """Re-index a single file after edit, with 5s debounce."""
        # Cancel any pending reindex for this file
        existing_task = self._reindex_tasks.pop(file_path, None)
        if existing_task and not existing_task.done():
            existing_task.cancel()

        async def _delayed_reindex():
            await asyncio.sleep(5)
            await self._do_reindex(file_path)
            self._reindex_tasks.pop(file_path, None)

        task = asyncio.create_task(_delayed_reindex())
        self._reindex_tasks[file_path] = task
        return True

    async def _do_reindex(self, file_path: str) -> bool:
        """Actually re-index a file: remove old entries then re-memorize."""
        if not self._available or not self._service:
            return False

        op_id = self._metrics.begin_op("reindex_file", file_path)
        try:
            # Find and remove old resource entries for this URL
            resources = self._service.database.resource_repo.list_resources()
            for res_id, res in resources.items():
                if getattr(res, "url", "") == file_path:
                    # Find items linked to this resource and delete them
                    items = self._service.database.memory_item_repo.list_items()
                    for item_id, item in items.items():
                        if getattr(item, "resource_id", None) == res_id:
                            await self._service.delete_memory_item(memory_id=item_id)
            logger.debug("Cleared old entries for %s", file_path)
        except Exception as e:
            logger.warning("Failed to clear old entries for %s: %s", file_path, e)

        # Re-memorize
        p = Path(file_path)
        if not p.exists() or p.stat().st_size == 0:
            self._metrics.end_op(op_id, success=False, error="file missing or empty")
            return False
        modality = "conversation" if p.stem.count("-") == 2 else "document"
        result = await self.memorize_file(file_path, modality=modality)
        self._metrics.end_op(op_id, success=result)
        return result

    def _collect_md_files(self, workspace: Path) -> list[tuple[Path, str]]:
        """Collect all .md files that should be indexed."""
        files: list[tuple[Path, str]] = []

        # Core identity/memory files
        for name in ["SOUL.md", "IDENTITY.md", "USER.md", "MEMORY.md"]:
            p = workspace / name
            if p.exists() and p.stat().st_size > 0:
                files.append((p, "identity"))

        # Memory directory files
        memory_dir = workspace / "memory"
        if memory_dir.exists():
            for p in sorted(memory_dir.rglob("*.md")):
                if p.stat().st_size > 0:
                    # Daily logs have pattern YYYY-MM-DD (2 hyphens in stem)
                    file_type = "daily" if p.stem.count("-") == 2 else "reference"
                    files.append((p, file_type))

        return files

    async def get_db_stats(self) -> dict:
        """Query memU SQLite for aggregate statistics.

        Runs synchronous sqlite3 queries in a thread to avoid blocking
        the event loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._blocking_pool, self._get_db_stats_sync,
        )

    def _get_db_stats_sync(self) -> dict:
        """Synchronous implementation of DB stats query."""
        import sqlite3 as _sqlite3

        db_path = self.config.memory.sqlite_dsn.replace("sqlite:///", "")
        stats: dict[str, Any] = {
            "total_items": 0,
            "total_categories": 0,
            "total_resources": 0,
            "type_distribution": {},
            "events_missing_happened_at": 0,
            "db_size_mb": 0,
        }

        try:
            p = Path(db_path)
            if p.exists():
                stats["db_size_mb"] = round(p.stat().st_size / (1024 * 1024), 2)
        except Exception:
            pass

        try:
            db = _sqlite3.connect(db_path, timeout=10)
            db.row_factory = _sqlite3.Row
            stats["total_items"] = db.execute("SELECT COUNT(*) FROM memu_memory_items").fetchone()[0]
            stats["total_categories"] = db.execute("SELECT COUNT(*) FROM memu_memory_categories").fetchone()[0]
            stats["total_resources"] = db.execute("SELECT COUNT(*) FROM memu_resources").fetchone()[0]
            for row in db.execute("SELECT memory_type, COUNT(*) as cnt FROM memu_memory_items GROUP BY memory_type"):
                stats["type_distribution"][row["memory_type"]] = row["cnt"]
            stats["events_missing_happened_at"] = db.execute(
                "SELECT COUNT(*) FROM memu_memory_items WHERE happened_at IS NULL AND memory_type = 'event'"
            ).fetchone()[0]
            db.close()
        except Exception as e:
            logger.warning("Failed to query memU DB stats: %s", e)

        return stats

    async def get_health(self) -> dict:
        """Complete health snapshot for the diagnostics endpoint."""
        result = self._metrics.to_dict()
        result["database"] = await self.get_db_stats()
        return result

    @property
    def metrics(self) -> MemUMetrics:
        return self._metrics

    @property
    def available(self) -> bool:
        return self._available

    @property
    def _has_embeddings(self) -> bool:
        """Whether an embedding provider (e.g. OpenAI) is configured."""
        return bool(self.config.openai_api_key)
