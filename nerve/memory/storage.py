"""Synchronous reads for memory tools and the gateway."""

import sqlite3


def connect_memory(path, config=None, *, timeout=10):
    if config is not None and config.use_postgresql:
        from nerve.db.postgres.reader import SyncConnection

        return SyncConnection(config, memory=True)
    return sqlite3.connect(path, timeout=timeout)
