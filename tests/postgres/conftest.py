import os
import uuid

import pytest
import pytest_asyncio

from nerve.db.postgres import PostgresDatabase


@pytest_asyncio.fixture
async def db():
    dsn = os.environ.get("NERVE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("NERVE_TEST_POSTGRES_DSN is required")
    database = PostgresDatabase(dsn, workflow=uuid.uuid4().hex)
    await database.connect()
    try:
        yield database
    finally:
        await database.close()


@pytest.fixture
def db_scope():
    if not os.environ.get("NERVE_TEST_POSTGRES_DSN"):
        pytest.skip("NERVE_TEST_POSTGRES_DSN is required")
    return uuid.uuid4().hex
