"""Shared test fixtures for Nerve tests."""

import asyncio
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from nerve.db import Database


@pytest.fixture(scope="session")
def event_loop():
    """Use a single event loop for all tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def _isolate_nerve_state_files(tmp_path, monkeypatch):
    """Keep tests away from the real ~/.nerve state files.

    The config-dir pointer, wizard init-state, and Telegram pairing files
    all live under ~/.nerve on a real install. Tests must never read or
    mutate them (running the suite on a live box would otherwise repoint
    the daemon's config discovery or leak pairing codes).
    """
    import nerve.bootstrap
    import nerve.config
    import nerve.pairing

    state_dir = tmp_path / "_nerve_state"
    monkeypatch.setattr(
        nerve.config, "CONFIG_POINTER_FILE", state_dir / "config_dir",
    )
    monkeypatch.setattr(
        nerve.bootstrap, "INIT_STATE_FILE", state_dir / "init-state.json",
    )
    monkeypatch.setattr(
        nerve.pairing, "PAIRING_FILE", state_dir / "telegram_pairing",
    )


@pytest_asyncio.fixture
async def db(tmp_path):
    """Create a fresh in-memory-like database for each test."""
    db_path = tmp_path / "test.db"
    database = Database(db_path)
    await database.connect()
    yield database
    await database.close()
