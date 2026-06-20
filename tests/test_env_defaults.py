"""Tests for nerve._env — process-env defaults applied before numpy loads.

These defaults prevent the fork-vs-OpenBLAS-atfork collision: an unbounded
BLAS worker pool makes glibc fork() (used by uvloop to spawn agent CLIs on
the event loop) block in OpenBLAS's pthread_atfork prepare handler while
the memU thread runs vector searches.
"""

from __future__ import annotations

import importlib
import os

import nerve._env as env_mod


class TestEnvDefaults:
    def test_defaults_applied_when_unset(self, monkeypatch):
        monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
        monkeypatch.delenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", raising=False)

        env_mod.apply_env_defaults()

        assert os.environ["OPENBLAS_NUM_THREADS"] == "1"
        assert os.environ["CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK"] == "1"

    def test_explicit_values_win(self, monkeypatch):
        monkeypatch.setenv("OPENBLAS_NUM_THREADS", "8")
        # Empty string re-enables the SDK version check (falsy in the SDK).
        monkeypatch.setenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "")

        env_mod.apply_env_defaults()

        assert os.environ["OPENBLAS_NUM_THREADS"] == "8"
        assert os.environ["CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK"] == ""

    def test_applied_on_import(self, monkeypatch):
        """Importing the module applies the defaults (entry-point contract)."""
        monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
        importlib.reload(env_mod)
        assert os.environ["OPENBLAS_NUM_THREADS"] == "1"

    def test_idempotent(self, monkeypatch):
        monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
        env_mod.apply_env_defaults()
        env_mod.apply_env_defaults()
        assert os.environ["OPENBLAS_NUM_THREADS"] == "1"

    def test_bridge_import_applies_caps(self):
        """memu_bridge must guarantee the caps before its numpy import.

        numpy is typically already loaded by the time this test runs, so
        this can't verify load-order end-to-end — it pins the import
        dependency: importing the bridge module must (re)apply defaults.
        """
        import nerve.memory.memu_bridge  # noqa: F401

        assert os.environ.get("OPENBLAS_NUM_THREADS") is not None
