"""Tests for nerve.cli.setup_logging — structured logging via structlog.

Verifies the classic console format is preserved by default and that JSON
mode emits parseable records. Saves/restores root logger handlers so the
global logging config isn't left mutated for other tests.
"""

from __future__ import annotations

import json
import logging
import re

import pytest

from nerve import cli


@pytest.fixture(autouse=True)
def _restore_root_logging():
    root = logging.getLogger()
    saved = list(root.handlers)
    saved_level = root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved:
        root.addHandler(h)
    root.setLevel(saved_level)


def _format_one(record: logging.LogRecord) -> str:
    handler = logging.getLogger().handlers[0]
    return handler.format(record)


def _record(level=logging.INFO, name="nerve.agent.engine", msg="session started"):
    return logging.LogRecord(name, level, "f.py", 1, msg, None, None)


def test_console_format_preserved():
    cli.setup_logging(log_format="console")
    out = _format_one(_record())
    # HH:MM:SS [LEVEL] name: message
    assert re.match(
        r"^\d\d:\d\d:\d\d \[INFO\] nerve\.agent\.engine: session started",
        out,
    ), out


def test_json_format():
    cli.setup_logging(log_format="json")
    out = _format_one(_record(level=logging.WARNING, msg="heads up"))
    data = json.loads(out)
    assert data["event"] == "heads up"
    assert data["level"] == "warning"
    assert data["logger"] == "nerve.agent.engine"


def test_env_var_selects_json(monkeypatch):
    monkeypatch.setenv("NERVE_LOG_FORMAT", "json")
    cli.setup_logging()  # no explicit log_format → reads env
    out = _format_one(_record(msg="from env"))
    assert json.loads(out)["event"] == "from env"


def test_idempotent_single_handler():
    cli.setup_logging(log_format="console")
    cli.setup_logging(log_format="console")
    # Re-running replaces handlers rather than stacking them.
    assert len(logging.getLogger().handlers) == 1


def test_console_percent_style_args():
    # The bridge must apply %-style args (record.getMessage()), not print the
    # raw format string — the dominant logging idiom across the codebase.
    cli.setup_logging(log_format="console")
    rec = logging.LogRecord(
        "nerve.test", logging.INFO, "f.py", 1, "value is %s and %d", ("hi", 7), None,
    )
    out = _format_one(rec)
    assert "value is hi and 7" in out


def test_console_includes_exception_traceback():
    cli.setup_logging(log_format="console")
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        rec = logging.LogRecord(
            "nerve.test", logging.ERROR, "f.py", 1, "it failed", None, sys.exc_info(),
        )
    out = _format_one(rec)
    assert "it failed" in out
    assert "ValueError" in out and "boom" in out


def test_json_includes_args_and_exception():
    cli.setup_logging(log_format="json")
    try:
        raise RuntimeError("kaboom")
    except RuntimeError:
        import sys
        rec = logging.LogRecord(
            "nerve.test", logging.ERROR, "f.py", 1, "n=%d", (3,), sys.exc_info(),
        )
    data = json.loads(_format_one(rec))
    assert data["event"] == "n=3"
    assert "kaboom" in data["exception"]
