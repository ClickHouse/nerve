"""Tests for the Telegram pairing-code store."""

from __future__ import annotations

import json

import nerve.pairing as pairing
from nerve.pairing import (
    MAX_ATTEMPTS,
    clear_pairing_code,
    generate_pairing_code,
    get_or_create_pairing_code,
    read_pairing_code,
    verify_pairing_code,
)


class TestGenerate:
    def test_six_digits(self):
        code = generate_pairing_code()
        assert len(code) == 6
        assert code.isdigit()

    def test_persisted_and_readable(self):
        code = generate_pairing_code()
        assert read_pairing_code() == code

    def test_get_or_create_reuses_valid_code(self):
        code = generate_pairing_code()
        assert get_or_create_pairing_code() == code

    def test_get_or_create_generates_when_missing(self):
        clear_pairing_code()
        code = get_or_create_pairing_code()
        assert read_pairing_code() == code


class TestVerify:
    def test_correct_code_single_use(self):
        code = generate_pairing_code()
        assert verify_pairing_code(code) is True
        # Single use: cleared after success
        assert read_pairing_code() is None
        assert verify_pairing_code(code) is False

    def test_wrong_code_rejected_then_correct_succeeds(self):
        code = generate_pairing_code()
        wrong = "000000" if code != "000000" else "111111"
        assert verify_pairing_code(wrong) is False
        # A few failed attempts don't burn the code (until MAX_ATTEMPTS)
        assert verify_pairing_code(code) is True

    def test_whitespace_tolerated(self):
        code = generate_pairing_code()
        assert verify_pairing_code(f"  {code} ") is True

    def test_missing_state_rejects(self):
        clear_pairing_code()
        assert verify_pairing_code("123456") is False

    def test_attempt_limit_invalidates(self):
        code = generate_pairing_code()
        wrong = "000000" if code != "000000" else "111111"
        for _ in range(MAX_ATTEMPTS):
            assert verify_pairing_code(wrong) is False
        # Code is now burned — even the correct one fails
        assert verify_pairing_code(code) is False
        assert read_pairing_code() is None


class TestExpiry:
    def _force_expire(self):
        path = pairing._pairing_path()
        state = json.loads(path.read_text())
        state["expires_at"] = 1.0  # long past
        path.write_text(json.dumps(state))

    def test_expired_code_unreadable(self):
        generate_pairing_code()
        self._force_expire()
        assert read_pairing_code() is None

    def test_expired_code_rejected(self):
        code = generate_pairing_code()
        self._force_expire()
        assert verify_pairing_code(code) is False

    def test_get_or_create_replaces_expired(self):
        old = generate_pairing_code()
        self._force_expire()
        new = get_or_create_pairing_code()
        assert read_pairing_code() == new
