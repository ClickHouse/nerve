"""The wizard's scratch pad: it may be wrong, it may not take the page down.

``setup-state.json`` is written by the wizard and read by nobody else (and
by ``nerve doctor``), so anything malformed in it is a partially written file
or a version that did not exist yet. Neither is a reason for the checklist to
answer 500 until somebody deletes a file they have never heard of. And it
holds decisions only: never a configuration value, never a secret.
"""

from __future__ import annotations

import json

import pytest

from nerve import boot, setup_state


@pytest.fixture
def state_path(tmp_path, monkeypatch):
    monkeypatch.setattr("nerve.paths.nerve_home", lambda: tmp_path)
    path = setup_state.state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class TestTelegramSyncUser:
    """Who the message sync is signed in as: a decision-like note, validated on load."""

    @pytest.mark.parametrize("raw, expected", [
        ({"id": 7, "username": "alice_example", "first_name": "Alice"}, {"id": 7, "username": "alice_example", "first_name": "Alice"}),
        ({"id": 7}, {"id": 7, "username": None, "first_name": ""}),
        ({"id": 7, "username": "", "first_name": None}, {"id": 7, "username": None, "first_name": ""}),
        ({"id": "7"}, None),
        ({"id": 0}, None),
        ({"id": True}, None),
        ({"username": "alice_example"}, None),
        ("alice_example", None),
        (["alice_example"], None),
        (None, None),
    ])
    def test_only_the_right_shape_is_kept(self, state_path, raw, expected):
        state_path.write_text(json.dumps({"version": 3, "telegram_sync_user": raw}), encoding="utf-8")
        assert setup_state.load_state().telegram_sync_user == expected

    def test_absent_in_an_older_file_reads_as_none(self, state_path):
        state_path.write_text(json.dumps({"version": 3, "skipped": ["memory"]}), encoding="utf-8")
        state = setup_state.load_state()
        assert state.telegram_sync_user is None and state.skipped == {"memory"}

    def test_clearing_survives_a_round_trip(self, state_path):
        state = setup_state.SetupState(telegram_sync_user={"id": 7, "username": None, "first_name": "A"})
        assert setup_state.save_state(state)
        state = setup_state.load_state()
        state.telegram_sync_user = None
        assert setup_state.save_state(state)
        assert json.loads(state_path.read_text(encoding="utf-8"))["telegram_sync_user"] is None
        assert setup_state.load_state().telegram_sync_user is None


class TestLoadNeverRaises:
    @pytest.mark.parametrize("body", [
        "",
        "not json at all",
        "[]",
        '"a string"',
        "null",
        '{"debts": []}',
        '{"version": 1, "applied": "nope"}',
        '{"version": 1, "skipped": {"provider": true}}',
        '{"version": 2, "done": 7, "debts": 3}',
        '{"version": 3, "boot_errors": ["x"], "telegram_bot_username": 5}',
    ])
    def test_a_malformed_file_reads_as_an_empty_state(self, state_path, body):
        state_path.write_text(body, encoding="utf-8")
        state = setup_state.load_state()
        assert state.skipped == set()
        assert state.answered == set()
        assert state.boot_errors == {}
        assert state.telegram_bot_username is None

    def test_a_missing_file_is_an_empty_state(self, state_path):
        assert not state_path.exists()
        assert setup_state.load_state().debts == {}

    def test_a_newer_version_is_not_guessed_at(self, state_path):
        state_path.write_text(json.dumps({
            "version": 99, "skipped": ["channels"], "answered": ["profile"],
        }), encoding="utf-8")
        state = setup_state.load_state()
        assert state.skipped == set()
        assert state.answered == set()

    def test_a_v2_file_keeps_its_decisions_and_drops_the_rest(self, state_path):
        """The September build stored ``done`` and ``applied``; a v3 loader
        keeps the two decisions and forgets what was in flight."""
        state_path.write_text(json.dumps({
            "version": 2,
            "skipped": ["channels", 7, None],
            "done": ["provider"],
            "answered": ["profile"],
            "applied": {"timezone": "Europe/Berlin", "telegram.bot_token": True},
            "debts": ["the scheduler is still running the cron settings it started with"],
            "boot": "deadbeefdeadbeef",
        }), encoding="utf-8")
        state = setup_state.load_state()
        assert state.skipped == {"channels"}
        assert state.answered == {"profile"}
        assert not hasattr(state, "applied")
        # A v2 debt list is kept under itself until the next boot retires it.
        assert list(state.debts.values()) == [
            "the scheduler is still running the cron settings it started with",
        ]
        assert state.boot == "deadbeefdeadbeef"


class TestRoundTrip:
    def test_round_trip_has_only_decisions(self, state_path):
        """No configured secret substring ever lands in the file — including
        inside a boot error that an upstream library phrased around the token."""
        bot_token = "123456:REPLACEabcdefghijklmnopqrstuvwxyz0123"
        state = setup_state.SetupState()
        state.skipped.add("channels")
        state.answered.add("profile")
        state.debts["openai_api_key"] = "embeddings start after a restart"
        state.boot_errors["telegram"] = "InvalidToken: The token <token> was rejected by the server."
        state.telegram_bot_username = "example_assistant_bot"
        state.telegram_sync_user = {"id": 424242, "username": "alice_example", "first_name": "Alice"}
        assert setup_state.save_state(state) is True

        text = state_path.read_text(encoding="utf-8")
        for secret in (bot_token, "sk-ant-REPLACE", "sk-REPLACE"):
            assert secret not in text
        raw = json.loads(text)
        assert set(raw) == {
            "version", "skipped", "answered", "debts", "boot_errors",
            "telegram_bot_username", "telegram_sync_user", "boot", "updated_at",
        }
        assert raw["telegram_sync_user"] == {"id": 424242, "username": "alice_example", "first_name": "Alice"}
        assert setup_state.load_state().telegram_sync_user == state.telegram_sync_user
        assert raw["version"] == 3
        reloaded = setup_state.load_state()
        assert reloaded.skipped == {"channels"}
        assert reloaded.answered == {"profile"}
        assert reloaded.debts == {"openai_api_key": "embeddings start after a restart"}
        assert reloaded.boot_errors["telegram"].startswith("InvalidToken")
        assert reloaded.telegram_bot_username == "example_assistant_bot"
        assert reloaded.boot == boot.boot_id()

    def test_the_file_is_owner_only(self, state_path):
        import stat

        setup_state.save_state(setup_state.SetupState())
        assert stat.S_IMODE(state_path.stat().st_mode) == 0o600

    def test_telegram_bot_username_round_trips_and_clears(self, state_path):
        state = setup_state.SetupState(telegram_bot_username="example_assistant_bot")
        setup_state.save_state(state)
        assert setup_state.load_state().telegram_bot_username == "example_assistant_bot"
        state.telegram_bot_username = None
        setup_state.save_state(state)
        assert setup_state.load_state().telegram_bot_username is None


class TestBootScoping:
    def test_debts_retire_on_new_boot(self, state_path, monkeypatch):
        state = setup_state.SetupState()
        state.debts["cron"] = "x"
        setup_state.save_state(state)
        # Same process: nothing goes.
        same = setup_state.load_state()
        assert setup_state.retire_on_boot(same) is False
        assert same.debts == {"cron": "x"}
        # A later process: the debt described a process that no longer runs.
        monkeypatch.setattr(boot, "BOOT_ID", "0000000000000001")
        later = setup_state.load_state()
        assert setup_state.retire_on_boot(later) is True
        assert later.debts == {}
        assert later.retired is True
        assert later.boot == "0000000000000001"

    def test_boot_errors_retire_on_new_boot(self, state_path, monkeypatch):
        assert setup_state.record_boot_error("telegram", "InvalidToken: <token>") is True
        assert setup_state.boot_errors_for_current_boot() == {"telegram": "InvalidToken: <token>"}
        monkeypatch.setattr(boot, "BOOT_ID", "0000000000000002")
        assert setup_state.boot_errors_for_current_boot() == {}
        later = setup_state.load_state()
        setup_state.retire_on_boot(later)
        assert later.boot_errors == {}

    def test_answered_and_skipped_survive_boot(self, state_path, monkeypatch):
        state = setup_state.SetupState(skipped={"memory"}, answered={"automation"})
        state.debts["cron"] = "x"
        setup_state.save_state(state)
        monkeypatch.setattr(boot, "BOOT_ID", "0000000000000003")
        later = setup_state.load_state()
        setup_state.retire_on_boot(later)
        assert later.skipped == {"memory"}
        assert later.answered == {"automation"}
        assert later.debts == {}

    def test_save_failure_is_reported_not_raised(self, state_path, monkeypatch):
        def _explode(*_a, **_k):
            raise OSError("read-only file system")

        monkeypatch.setattr("nerve.paths.write_private_text", _explode)
        assert setup_state.save_state(setup_state.SetupState()) is False
