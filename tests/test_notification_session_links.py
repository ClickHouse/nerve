"""The session line of a Telegram or Slack notification.

With ``gateway.public_url`` the line links to the session's page in the web
UI; without it the line names the session by its ID, as before. Covers the
setting and its environment default, the address of a session, and the line as
each channel renders it.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve import config as cfg
from nerve.channels.slack_presentation import _md_to_slack, build_notification_blocks
from nerve.channels.telegram import _md_to_tg_html
from nerve.config import GatewayConfig, NerveConfig, NotificationsConfig
from nerve.config_validate import validate_config_bundle
from nerve.links import link_stays_whole, markdown_link, session_url, web_url
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_system_principal

BASE = "https://nerve.example.com"
CRON_SESSION = "cron:nightly-report:20260101-050000-0a1b2c3d"
CRON_SESSION_URL = f"{BASE}/chat/cron%3Anightly-report%3A20260101-050000-0a1b2c3d"


@pytest.fixture(autouse=True)
def _no_address_from_the_environment(monkeypatch: pytest.MonkeyPatch):
    """The machine that runs the tests may set the variable."""
    monkeypatch.delenv(cfg.PUBLIC_URL_ENV, raising=False)
    cfg._refused_public_urls.clear()


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 — the conftest database, with an identity
    await ensure_system_principal(db)
    return db


def _config(public_url: str = BASE) -> NerveConfig:
    config = NerveConfig()
    config.gateway = GatewayConfig(public_url=public_url)
    config.notifications = NotificationsConfig(
        channels=["web"], telegram_chat_id=1001, priority_prefixes={},
    )
    return config


def _engine(channel: object | None = None) -> MagicMock:
    engine = MagicMock()
    engine.router.get_channel.return_value = channel
    return engine


class TestPublicUrlSetting:
    def test_it_is_empty_by_default(self):
        assert GatewayConfig.from_dict({}).public_url == ""
        assert NerveConfig.from_dict({}).gateway.public_url == ""

    @pytest.mark.parametrize("value, stored", [
        ("https://nerve.example.com", "https://nerve.example.com"),
        ("https://nerve.example.com/", "https://nerve.example.com"),
        ("  http://192.0.2.10:8900  ", "http://192.0.2.10:8900"),
        ("http://[2001:db8::1]:8900/", "http://[2001:db8::1]:8900"),
        ("https://nerve-2.example.com:443", "https://nerve-2.example.com:443"),
        ("http://localhost:8900", "http://localhost:8900"),
        ("http://my_nerve:8900", "http://my_nerve:8900"),
    ])
    def test_an_address_is_kept_without_its_last_slash(self, value, stored):
        assert GatewayConfig.from_dict({"public_url": value}).public_url == stored

    @pytest.mark.parametrize("value", [
        "nerve.example.com",
        "ftp://nerve.example.com",
        "https://",
        "https://user:secret@nerve.example.com",
        "https://nerve.example.com/?tab=1",
        "https://nerve.example.com/#chat",
        "https://nerve example.com",
        # The web UI is served at the root of its address.
        "https://example.com/nerve",
        "https://example.com/nerve/",
        # Nothing that would need an escape in a message's markup.
        'https://nerve.example.com"',
        "https://nerve.example.com)",
        "https://nerve.example.com:0",
        "https://nerve.example.com:65536",
        "https://nerve.example.com:port",
        "https://-nerve.example.com",
    ])
    def test_a_value_that_is_not_an_address_gives_no_links_and_one_warning(self, value, caplog):
        with caplog.at_level(logging.WARNING, logger="nerve.config"):
            first = GatewayConfig.from_dict({"public_url": value})
            again = GatewayConfig.from_dict({"public_url": value})
        assert first.public_url == "" and again.public_url == ""
        warnings = [r.getMessage() for r in caplog.records if "gateway.public_url" in r.getMessage()]
        assert len(warnings) == 1
        assert "no links" in warnings[0]

    def test_an_unset_reference_gives_no_links_and_no_warning(self, caplog):
        """The validator builds a config with the references it could not resolve."""
        with caplog.at_level(logging.WARNING, logger="nerve.config"):
            assert GatewayConfig.from_dict({"public_url": "${NERVE_TEST_UNSET_ADDRESS}"}).public_url == ""
        assert not [r for r in caplog.records if "gateway.public_url" in r.getMessage()]

    def test_the_environment_gives_the_address_when_no_layer_names_it(self, monkeypatch):
        monkeypatch.setenv(cfg.PUBLIC_URL_ENV, "https://agent.example.net/")
        assert GatewayConfig.from_dict({}).public_url == "https://agent.example.net"
        assert GatewayConfig.from_dict({"public_url": "  "}).public_url == "https://agent.example.net"
        assert GatewayConfig.from_dict({"public_url": None}).public_url == "https://agent.example.net"

    def test_a_named_address_wins_over_the_environment(self, monkeypatch):
        monkeypatch.setenv(cfg.PUBLIC_URL_ENV, "https://agent.example.net")
        assert GatewayConfig.from_dict({"public_url": BASE}).public_url == BASE

    def test_a_refused_named_value_does_not_fall_back_to_the_environment(self, monkeypatch):
        monkeypatch.setenv(cfg.PUBLIC_URL_ENV, "https://agent.example.net")
        assert GatewayConfig.from_dict({"public_url": "nerve.example.com"}).public_url == ""

    def test_a_refused_environment_value_gives_no_links_and_names_the_variable(self, monkeypatch, caplog):
        monkeypatch.setenv(cfg.PUBLIC_URL_ENV, "agent.example.net")
        with caplog.at_level(logging.WARNING, logger="nerve.config"):
            assert GatewayConfig.from_dict({}).public_url == ""
        assert any(cfg.PUBLIC_URL_ENV in r.getMessage() for r in caplog.records)

    def test_the_key_is_a_known_key(self):
        assert cfg.validate_config_keys({"gateway": {"public_url": BASE}}) == []

    @pytest.mark.parametrize("line, warned", [
        (f"  public_url: {BASE}\n", False),
        ("  public_url: nerve.example.com\n", True),
        ("  public_url: https://example.com/nerve\n", True),
        ("  public_url: ''\n", False),
        ("  public_url: 0\n", False),
        ("  public_url: ${NERVE_TEST_UNSET_ADDRESS}\n", False),
        ("  port: 8900\n", False),
    ])
    def test_the_validator_names_a_refused_address(self, tmp_path, line, warned):
        workspace = tmp_path / "ws"
        workspace.mkdir()
        config_dir = tmp_path / "cfg"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text(f"workspace: {workspace}\ngateway:\n{line}", encoding="utf-8")

        result = validate_config_bundle(config_dir)

        assert result.ok
        assert any("gateway.public_url" in warning for warning in result.warnings) is warned


class TestAddresses:
    def test_there_is_no_address_without_the_setting(self):
        assert web_url(_config(""), "/chat/s1") is None
        assert session_url(_config(""), "s1") is None

    def test_a_session_id_is_one_encoded_path_segment(self):
        assert session_url(_config(), "0a1b2c3d") == f"{BASE}/chat/0a1b2c3d"
        assert session_url(_config(), CRON_SESSION) == CRON_SESSION_URL
        assert session_url(_config(), "a/b c)d") == f"{BASE}/chat/a%2Fb%20c%29d"

    def test_an_empty_id_has_no_address(self):
        assert session_url(_config(), "") is None

    def test_a_label_cannot_end_the_link_early(self):
        link = markdown_link("Fix [draft] of `parse()`\nsecond line", "https://example.com/a(b)c")
        assert link == "[Fix (draft) of 'parse()' second line](https://example.com/a%28b%29c)"

    @pytest.mark.parametrize("before", [
        "Report ready\n\nSession: ",
        "See [the docs](https://example.com/docs) first.\n\nSession: ",
        "Checked [done] and [skipped] items.\n\nSession: ",
        "Run `items[0` and\n```\nrows = [1, 2\n```\nthen wait.\n\nSession: ",
        "",
    ])
    def test_a_link_after_ordinary_text_stays_whole(self, before):
        assert link_stays_whole(before, markdown_link("Nightly report", f"{BASE}/chat/s1")) is True

    @pytest.mark.parametrize("before", [
        # An open bracket would take the link's end as its own.
        "items: [1, 2, 3\n\nSession: ",
        # So would an address without its end.
        "see [docs](https://example.com/do\n\nSession: ",
        "see [docs](\n\nSession: ",
    ])
    def test_a_link_after_an_open_bracket_or_an_unfinished_link_does_not(self, before):
        link = markdown_link("Nightly report", f"{BASE}/chat/s1")
        assert link_stays_whole(before, link) is False
        # What the check guards: the conversions make something else of it.
        assert f'<a href="{BASE}/chat/s1">Nightly report</a>' not in _md_to_tg_html(before + link)
        assert f"<{BASE}/chat/s1|Nightly report>" not in _md_to_slack(before + link)

    def test_what_telegram_takes_out_first_cannot_hide_an_open_bracket(self):
        """Telegram takes an expandable blockquote out before it looks for
        links, and with it the bracket that closed an earlier one."""
        link = markdown_link("Nightly report", f"{BASE}/chat/s1")
        quoted = "<blockquote expandable>[stderr] boom</blockquote>\n\nSession: "

        assert link_stays_whole("Tests passed\n" + quoted, link) is True
        assert _md_to_tg_html("Tests passed\n" + quoted + link).endswith(
            f'Session: <a href="{BASE}/chat/s1">Nightly report</a>',
        )
        assert link_stays_whole("Tests failed [3/10\n" + quoted, link) is False

    def test_a_link_without_a_label_is_no_link(self):
        assert link_stays_whole("Done\n\nSession: ", f"[]({BASE}/chat/s1)") is False

    def test_a_long_label_is_cut_to_one_line(self):
        link = markdown_link("word " * 40, f"{BASE}/chat/s1")
        label = link[1:link.index("](")]
        assert len(label) == 80 and label.endswith("…") and "\n" not in label


@pytest.mark.asyncio
class TestSessionLine:
    async def test_without_the_setting_the_line_names_the_session_by_its_id(self, db):
        await db.create_session("s1", title="Nightly report", actor=None)
        service = NotificationService(_config(""), db, _engine())

        text = await service._build_notification_text("s1", "Done", "All green.", "normal")

        assert text == "Done\n\nAll green.\n\nSession: s1"

    async def test_with_the_setting_the_line_links_to_the_session_under_its_title(self, db):
        await db.create_session(CRON_SESSION, title="Cron: nightly-report", source="cron", actor=None)
        service = NotificationService(_config(), db, _engine())

        text = await service._build_notification_text(CRON_SESSION, "Done", "All green.", "normal")

        assert text == f"Done\n\nAll green.\n\nSession: [Cron: nightly-report]({CRON_SESSION_URL})"
        assert CRON_SESSION not in text

    async def test_a_session_without_a_title_links_under_its_id(self, db):
        await db.create_session("0a1b2c3d", actor=None)
        service = NotificationService(_config(), db, _engine())

        text = await service._build_notification_text("0a1b2c3d", "Done", "", "normal")

        assert text == f"Done\n\nSession: [0a1b2c3d]({BASE}/chat/0a1b2c3d)"

    async def test_an_id_that_names_no_session_gets_no_link(self, db):
        service = NotificationService(_config(), db, _engine())

        text = await service._build_notification_text("system", "Backup OK", "", "normal")

        assert text == "Backup OK\n\nSession: system"

    async def test_a_hidden_label_stays_hidden(self, db):
        await db.create_session("cron:quiet:20260101-050000-0a1b2c3d", title="Cron: quiet", actor=None)
        service = NotificationService(_config(), db, _engine())
        service.hide_session_label_for("cron:quiet")

        text = await service._build_notification_text(
            "cron:quiet:20260101-050000-0a1b2c3d", "Done", "", "normal",
        )

        assert text == "Done"

    @pytest.mark.parametrize("body", [
        "items: [1, 2, 3",
        "see [docs](https://example.com/do",
    ])
    async def test_a_body_that_would_break_the_link_gets_the_id(self, db, body):
        await db.create_session("s1", title="Nightly report", actor=None)
        service = NotificationService(_config(), db, _engine())

        text = await service._build_notification_text("s1", "Done", body, "normal")

        assert text == f"Done\n\n{body}\n\nSession: s1"
        # The body reaches each channel as it did without the setting.
        without = await NotificationService(_config(""), db, _engine())._build_notification_text(
            "s1", "Done", body, "normal",
        )
        assert text == without

    async def test_a_bracket_inside_code_does_not_cost_the_link(self, db):
        await db.create_session("s1", title="Nightly report", actor=None)
        service = NotificationService(_config(), db, _engine())

        text = await service._build_notification_text("s1", "Done", "Ran `rows[0` twice.", "normal")

        assert text.endswith(f"Session: [Nightly report]({BASE}/chat/s1)")
        assert _md_to_tg_html(text).endswith(f'Session: <a href="{BASE}/chat/s1">Nightly report</a>')

    async def test_a_failed_lookup_keeps_the_notification(self, db, monkeypatch, caplog):
        service = NotificationService(_config(), db, _engine())
        monkeypatch.setattr(db, "get_session", AsyncMock(side_effect=RuntimeError("database is locked")))

        with caplog.at_level(logging.WARNING, logger="nerve.notifications.service"):
            text = await service._build_notification_text("s1", "Done", "", "normal")

        assert text == "Done\n\nSession: s1"
        assert any("Session lookup" in r.getMessage() for r in caplog.records)

    async def test_a_reload_that_sets_the_address_applies_to_the_next_message(self, db):
        await db.create_session("s1", title="Nightly report", actor=None)
        service = NotificationService(_config(""), db, _engine())
        assert (await service._build_notification_text("s1", "Done", "", "normal")).endswith("Session: s1")

        service.config = _config()  # what a config reload does

        text = await service._build_notification_text("s1", "Done", "", "normal")
        assert text.endswith(f"Session: [Nightly report]({BASE}/chat/s1)")


@pytest.mark.asyncio
class TestTheLineInEachChannel:
    async def test_telegram_gets_an_anchor(self, db):
        await db.create_session(CRON_SESSION, title="Cron: nightly-report", source="cron", actor=None)
        await db.create_notification("n1", CRON_SESSION, "notify", "Done")
        bot = MagicMock()
        bot.send_message = AsyncMock(return_value=MagicMock(message_id=42))
        channel = MagicMock()
        channel._app.bot = bot
        channel._record_reply_route = AsyncMock()
        service = NotificationService(_config(), db, _engine(channel))

        message_id = await service._deliver_telegram(
            "n1", CRON_SESSION, "notify", "Done", "All green.", "normal", None,
        )

        assert message_id == "42"
        sent = bot.send_message.await_args.kwargs
        assert sent["text"].endswith(f'Session: <a href="{CRON_SESSION_URL}">Cron: nightly-report</a>')
        # The session link is the only address, so Telegram shows no preview of it.
        assert sent["link_preview_options"].is_disabled is True
        # A reply to the message still reaches the session that raised it.
        channel._record_reply_route.assert_awaited_once_with(42, 1001, CRON_SESSION)

    @pytest.mark.parametrize("body", [
        # Telegram makes no link of an address inside code, of one with
        # another scheme, or of a host without a dot.
        "Request failed:\n```\nGET https://example.com/api returned 500\n```",
        "Ran `curl https://example.com/health`.",
        "Copied s3://bucket/report.csv.",
        "Open http://localhost:3000/ to see it.",
        "All green.",
        # A page of the web UI itself would give the same card.
        f"See {BASE}/tasks/42 and then https://example.com/changes/101.",
        f"Read [the plan]({BASE}/plans/7).",
    ])
    async def test_telegram_shows_no_preview_where_the_session_link_would_be_first(self, db, body):
        await db.create_session("s1", title="Nightly report", actor=None)
        await db.create_notification("n1", "s1", "notify", "Done")
        bot = MagicMock()
        bot.send_message = AsyncMock(return_value=MagicMock(message_id=42))
        channel = MagicMock()
        channel._app.bot = bot
        channel._record_reply_route = AsyncMock()
        service = NotificationService(_config(), db, _engine(channel))

        await service._deliver_telegram("n1", "s1", "notify", "Done", body, "normal", None)

        sent = bot.send_message.await_args.kwargs
        assert f'<a href="{BASE}/chat/s1">Nightly report</a>' in sent["text"]
        assert sent["link_preview_options"].is_disabled is True

    @pytest.mark.parametrize("public_url, body", [
        # An address of the notification's own keeps Telegram's default preview.
        (BASE, "Merged https://example.com/changes/101 today."),
        (BASE, "Dashboard: http://192.0.2.10:3000/d/1"),
        # Another site first, and another host or port that only begins like the web UI's.
        (BASE, f"Merged https://example.com/changes/101, see {BASE}/tasks/42."),
        (BASE, f"See {BASE}.example.org/x"),
        (BASE, f"See {BASE}:8443/x"),
        # Without the setting nothing about the message changes.
        ("", "All green."),
    ])
    async def test_telegram_keeps_its_default_preview_otherwise(self, db, public_url, body):
        await db.create_session("s1", title="Nightly report", actor=None)
        await db.create_notification("n1", "s1", "question", "Deploy?")
        bot = MagicMock()
        bot.send_message = AsyncMock(return_value=MagicMock(message_id=42))
        channel = MagicMock()
        channel._app.bot = bot
        channel._record_reply_route = AsyncMock()
        service = NotificationService(_config(public_url), db, _engine(channel))

        await service._deliver_telegram("n1", "s1", "question", "Deploy?", body, "normal", ["Yes", "No"])

        assert "link_preview_options" not in bot.send_message.await_args.kwargs

    async def test_slack_gets_a_link(self, db):
        await db.create_session(CRON_SESSION, title="Cron: nightly-report", source="cron", actor=None)
        await db.create_notification("n1", CRON_SESSION, "notify", "Done")
        channel = MagicMock(is_available=True)
        channel.post_notification = AsyncMock(return_value=("C0123ABCD", "1.0"))
        service = NotificationService(_config(), db, _engine(channel))

        await service._deliver_slack("n1", CRON_SESSION, "notify", "Done", "All green.", "normal", None)

        text = channel.post_notification.await_args.args[1]
        blocks = build_notification_blocks(text, "n1")
        assert blocks[-1]["text"]["text"].endswith(f"Session: <{CRON_SESSION_URL}|Cron: nightly-report>")

    async def test_the_expiry_edit_keeps_the_link(self, db):
        await db.create_session("s1", title="Nightly report", actor=None)
        bot = MagicMock()
        bot.edit_message_text = AsyncMock()
        channel = MagicMock()
        channel._app.bot = bot
        service = NotificationService(_config(), db, _engine(channel))
        notif = {
            "id": "n1", "session_id": "s1", "title": "Deploy?", "body": "", "priority": "normal",
            "telegram_message_id": "42", "telegram_chat_id": "1001",
        }

        await service._edit_telegram_closed(notif, "⏰ Expired unanswered")

        edited = bot.edit_message_text.await_args.kwargs
        assert f'Session: <a href="{BASE}/chat/s1">Nightly report</a>\n\n⏰ Expired unanswered' in edited["text"]
        # An edit sets the preview again: it stays off, as at the delivery.
        assert edited["link_preview_options"].is_disabled is True

    async def test_a_title_with_markup_characters_stays_one_link(self, db):
        await db.create_session("s1", title="Fix a -> b [draft] & `parse()` *now*", actor=None)
        service = NotificationService(_config(), db, _engine())

        text = await service._build_notification_text("s1", "Done", "", "normal")

        url = f"{BASE}/chat/s1"
        assert _md_to_tg_html(text).endswith(
            f'Session: <a href="{url}">Fix a -&gt; b (draft) &amp; &#x27;parse()&#x27; *now*</a>',
        )
        assert _md_to_slack(text).endswith(
            f"Session: <{url}|Fix a -&gt; b (draft) &amp; 'parse()' *now*>",
        )


class TestSlackLinkConversion:
    def test_angle_brackets_in_a_label_do_not_end_the_link(self):
        assert _md_to_slack("[a > b < c](https://example.com/x)") == "<https://example.com/x|a &gt; b &lt; c>"

    def test_reserved_characters_in_an_address_are_encoded(self):
        assert _md_to_slack("[x](https://example.com/a|b<c>d?e=1&f=2)") == (
            "<https://example.com/a%7Cb%3Cc%3Ed?e=1&amp;f=2|x>"
        )
