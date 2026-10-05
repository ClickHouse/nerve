"""memU repositories on the pre-provisioned PostgreSQL schema."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from memu.app.service import (
    MemoryService as MemoryService,
)  # Initialize memU before its repository imports.
from pydantic import BaseModel

from memu.database.postgres.postgres import PostgresStore
from memu.database.postgres.repositories.resource_repo import PostgresResourceRepo
from memu.database.postgres.repositories.memory_item_repo import PostgresMemoryItemRepo
from memu.database.postgres.schema import get_sqlalchemy_models
from memu.database.state import DatabaseState
from sqlalchemy import create_engine, text
from sqlmodel import Session


class DurableResourceRepo(PostgresResourceRepo):
    """Commit source bytes and their resource row in one transaction."""

    def create_resource(
        self, *, url, modality, local_path, caption, embedding, user_data
    ):
        content = Path(local_path).read_bytes()
        resource = self._resource_model(
            url=url,
            modality=modality,
            local_path=local_path,
            caption=caption,
            embedding=self._prepare_embedding(embedding),
            **user_data,
            created_at=self._now(),
            updated_at=self._now(),
        )
        with self._sessions.session() as session:
            session.add(resource)
            session.flush()
            session.exec(
                text(
                    "INSERT INTO resource_blobs(resource_id,content) VALUES (:id,:content)"
                ),
                params={"id": resource.id, "content": content},
            )
            session.commit()
            session.refresh(resource)
        return self._cache_resource(resource)


class SemanticItemRepo(PostgresMemoryItemRepo):
    threshold = 0.0

    def create_item_reinforce(
        self, *, resource_id=None, memory_type, summary, embedding, user_data
    ):
        if self.threshold > 0 and embedding is not None:
            query = "[" + ",".join(str(float(value)) for value in embedding) + "]"
            with self._sessions.session() as session:
                match = session.exec(
                    text(
                        "SELECT id,1-(embedding <=> CAST(:query AS vector)) AS similarity "
                        "FROM memory_items WHERE memory_type=:kind AND embedding IS NOT NULL "
                        "AND vector_dims(embedding)=:dimension "
                        "ORDER BY embedding <=> CAST(:query AS vector) LIMIT 1"
                    ),
                    params={
                        "query": query,
                        "kind": str(memory_type),
                        "dimension": len(embedding),
                    },
                ).first()
            if match and match[1] >= self.threshold:
                item = self.items[match[0]]
                extra = dict(item.extra or {})
                extra["reinforcement_count"] = extra.get("reinforcement_count", 1) + 1
                extra["last_reinforced_at"] = self._now().isoformat()
                return self.update_item(item_id=item.id, extra=extra)
        return super().create_item_reinforce(
            resource_id=resource_id,
            memory_type=memory_type,
            summary=summary,
            embedding=embedding,
            user_data=user_data,
        )


class MemoryScope(BaseModel):
    pass


class Sessions:
    def __init__(self, config):
        import psycopg

        scope = json.dumps(
            [config.tenant_id, config.workflow_id], separators=(",", ":")
        )

        def connect():
            conn = psycopg.connect(config.postgresql_dsn, connect_timeout=5)
            try:
                role = conn.execute(
                    "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user"
                ).fetchone()
                if role[0]:
                    raise PermissionError("Memory role must not bypass RLS")
                conn.execute("SET search_path = nerve_pg, public")
                conn.execute("SELECT set_config('nerve.scope', %s, false)", (scope,))
                conn.commit()
                return conn
            except BaseException:
                conn.close()
                raise

        self.engine = create_engine(
            "postgresql+psycopg://", creator=connect, pool_pre_ping=True
        )

    def session(self):
        return Session(self.engine, expire_on_commit=False)

    def close(self):
        self.engine.dispose()


class MemoryStore(PostgresStore):
    """Use memU's PostgreSQL repositories without its automatic Alembic/DDL."""

    def __init__(self, config):
        from memu.database.postgres.repositories.memory_category_repo import (
            PostgresMemoryCategoryRepo,
        )
        from memu.database.postgres.repositories.category_item_repo import (
            PostgresCategoryItemRepo,
        )

        self._scope = json.dumps(
            [config.tenant_id, config.workflow_id], separators=(",", ":")
        )
        self._guard_lock = asyncio.Lock()
        self._state = DatabaseState()
        self._sessions = Sessions(config)
        self._sqla_models = get_sqlalchemy_models(scope_model=MemoryScope)
        self._scope_fields = []
        self._use_vector_type = True
        common = dict(
            state=self._state,
            sessions=self._sessions,
            sqla_models=self._sqla_models,
            scope_fields=[],
        )
        self.resource_repo = DurableResourceRepo(
            resource_model=self._sqla_models.Resource, **common
        )
        self.memory_item_repo = SemanticItemRepo(
            memory_item_model=self._sqla_models.MemoryItem, use_vector=True, **common
        )
        self.memory_item_repo.threshold = config.memory.semantic_dedup_threshold
        self.memory_category_repo = PostgresMemoryCategoryRepo(
            memory_category_model=self._sqla_models.MemoryCategory, **common
        )
        self.category_item_repo = PostgresCategoryItemRepo(
            category_item_model=self._sqla_models.CategoryItem, **common
        )
        self.resources = self._state.resources
        self.items = self._state.items
        self.categories = self._state.categories
        self.relations = self._state.relations
        with self._sessions.session() as session:
            if session.exec(text("SELECT version FROM storage_version")).scalar() != 1:
                raise RuntimeError("Unsupported PostgreSQL memory schema")
        self.refresh()

    def refresh(self):
        self.resources.clear()
        self.items.clear()
        self.categories.clear()
        self.relations.clear()
        self._load_existing()

    def restore_resources(self, directory):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        with self._sessions.session() as session:
            for resource_id, content in session.exec(
                text("SELECT resource_id,content FROM resource_blobs")
            ):
                path = directory / resource_id
                if path.parent != directory or path.name != resource_id:
                    raise ValueError("Invalid memory resource identifier")
                path.write_bytes(content)
                session.exec(
                    text("UPDATE resources SET local_path=:path WHERE id=:id"),
                    params={"id": resource_id, "path": str(path)},
                )
            session.commit()
        self.refresh()

    @asynccontextmanager
    async def guard(self):
        async with self._guard_lock:
            with self._sessions.engine.connect() as conn:
                acquired = False
                try:
                    while not acquired:
                        acquired = conn.execute(
                            text(
                                "SELECT pg_try_advisory_lock(hashtextextended(:scope,1))"
                            ),
                            {"scope": self._scope},
                        ).scalar()
                        if not acquired:
                            await asyncio.sleep(0.1)
                    yield
                finally:
                    if acquired:
                        conn.execute(
                            text(
                                "SELECT pg_advisory_unlock(hashtextextended(:scope,1))"
                            ),
                            {"scope": self._scope},
                        )
