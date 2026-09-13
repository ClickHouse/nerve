"""The wizard's scratch pad: it may be wrong, it may not take the page down.

``setup-state.json`` is written by the wizard and read by nobody else, so
anything malformed in it is a partially written file or a version that did not
exist yet. Neither is a reason for the checklist to answer 500 until somebody
deletes a file they have never heard of.
"""

from __future__ import annotations

import json

import pytest

from nerve import setup_state
from nerve.config import NerveConfig


@pytest.fixture
def state_path(tmp_path, monkeypatch):
    monkeypatch.setattr("nerve.paths.nerve_home", lambda: tmp_path)
    path = setup_state.state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class TestLoadNeverRaises:
    @pytest.mark.parametrize("body", [
        "",
        "not json at all",
        "[]",
        '"a string"',
        "null",
        '{"applied": []}',            # valid JSON, valid object, wrong type
        '{"version": 1, "applied": "nope"}',
        '{"version": 1, "skipped": {"provider": true}}',
        '{"version": 1, "done": 7}',
        '{"version": 1, "applied_at": []}',
    ])
    def test_a_malformed_file_reads_as_an_empty_state(self, state_path, body):
        state_path.write_text(body, encoding="utf-8")
        state = setup_state.load_state()
        assert state.skipped == set()
        assert state.applied == {}

    def test_a_missing_file_is_an_empty_state(self, state_path):
        assert not state_path.exists()
        assert setup_state.load_state().done == set()

    def test_a_newer_version_is_not_guessed_at(self, state_path):
        """A downgrade must not read a newer file's fields as its own — that
        is how it corrupts them on the next save."""
        state_path.write_text(json.dumps({
            "version": 99, "skipped": ["channels"], "applied": {"timezone": "UTC"},
        }), encoding="utf-8")
        state = setup_state.load_state()
        assert state.skipped == set()
        assert state.applied == {}

    def test_the_usable_parts_of_a_partly_wrong_file_survive(self, state_path):
        state_path.write_text(json.dumps({
            "version": 1,
            "skipped": ["channels", 7, None],
            "done": ["provider"],
            "applied": {"timezone": "Europe/Berlin"},
        }), encoding="utf-8")
        state = setup_state.load_state()
        # The two entries that are not strings are dropped rather than
        # stringified: a skipped step named "7" is not a step.
        assert state.skipped == {"channels"}
        assert state.done == {"provider"}
        assert state.applied == {"timezone": "Europe/Berlin"}

    def test_a_round_trip_keeps_everything(self, state_path):
        state = setup_state.SetupState()
        state.skipped.add("channels")
        state.done.add("provider")
        setup_state.record_applied(state, {"timezone": "UTC", "telegram.bot_token": "x"})
        assert setup_state.save_state(state) is True

        reloaded = setup_state.load_state()
        assert reloaded.skipped == {"channels"}
        assert reloaded.done == {"provider"}
        assert reloaded.applied["timezone"] == "UTC"
        # A secret is recorded as "present", never as itself: this file is
        # bookkeeping, not a credential store.
        assert reloaded.applied["telegram.bot_token"] is True
        assert "x" not in state_path.read_text(encoding="utf-8")

    def test_account_decisions_are_isolated_but_restart_debt_is_shared(
        self, state_path,
    ):
        alice = setup_state.SetupState(account_id="account-alice")
        alice.skipped.add("channels")
        alice.done.add("profile")
        setup_state.record_applied(alice, {"timezone": "Europe/Berlin"})
        assert setup_state.save_state(alice) is True

        bob = setup_state.load_state("account-bob")
        assert bob.skipped == set()
        assert bob.done == set()
        assert bob.applied == {"timezone": "Europe/Berlin"}
        bob.skipped.add("provider")
        assert setup_state.save_state(bob) is True

        alice = setup_state.load_state("account-alice")
        assert alice.skipped == {"channels"}
        assert alice.done == {"profile"}
        assert "provider" not in alice.skipped
        assert setup_state.load_state("account-bob").skipped == {"provider"}

    def test_a_legacy_file_is_adopted_by_one_account_and_rewritten(self, state_path):
        state_path.write_text(json.dumps({
            "version": 2,
            "skipped": ["channels"],
            "done": ["profile"],
            "answered": ["profile"],
            "applied": {},
            "debts": [],
            "boot": "",
        }), encoding="utf-8")

        alice = setup_state.load_state("account-alice")
        assert alice.skipped == {"channels"}
        assert alice.done == {"profile"}
        assert alice.retired is True
        assert setup_state.save_state(alice) is True

        raw = json.loads(state_path.read_text(encoding="utf-8"))
        assert raw["version"] == 3
        assert "skipped" not in raw
        assert raw["accounts"]["account-alice"]["skipped"] == ["channels"]
        assert setup_state.load_state("account-bob").skipped == set()

    def test_ambiguous_legacy_decisions_are_dropped_but_restart_facts_survive(
        self, state_path,
    ):
        state_path.write_text(json.dumps({
            "version": 2,
            "skipped": ["channels"],
            "done": ["profile"],
            "answered": ["profile"],
            "applied": {"timezone": "Europe/Berlin"},
            "debts": ["scheduler"],
            "boot": "old-boot",
        }), encoding="utf-8")

        bob = setup_state.load_state(
            "account-bob", adopt_legacy_decisions=False,
        )
        assert bob.skipped == set()
        assert bob.done == set()
        assert bob.answered == set()
        assert bob.applied == {"timezone": "Europe/Berlin"}
        assert bob.debts == {"scheduler"}
        assert bob.retired is True

    def test_pending_paths_tolerates_junk_it_did_not_write(self, state_path):
        state_path.write_text(json.dumps({
            "version": 1,
            "applied": {"timezone": "Europe/Berlin", "nothing.like.this": 1},
        }), encoding="utf-8")
        pending = setup_state.pending_paths(setup_state.load_state(), NerveConfig())
        # The unknown path names nothing on the config object and is skipped
        # rather than reported as a restart nobody can ever satisfy.
        assert pending == ["timezone"]
