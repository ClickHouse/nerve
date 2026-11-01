"""The hosted GitHub source: settings, subscriptions, the record consumer, the
drain into the inbox, and the jobs that a delivery runs.

The end-to-end tests run the fake gateway against the real stream endpoint, as
the hosted channel tests do, with a real database, source runner, and cron
service; only the agent turn is a stand-in.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
import yaml

import nerve.config as config_module
from nerve import config_reload
from nerve.agent.sessions import SessionManager
from nerve.channels.hosted.contract import (
    SourceFilters,
    SourceSubscription,
    Subscriptions,
)
from nerve.channels.hosted.contract.model import MAX_SUBSCRIPTION_PATTERNS
from nerve.channels.hosted.contract.wire import dumps
from nerve.channels.hosted.runtime import (
    HostedChannelRuntime,
    hosted_channel_providers,
    hosted_providers,
)
from nerve.channels.hosted.sources import (
    BUFFER_MAX_ROWS,
    HostedSourceConsumer,
    buffered_record,
    github_repos_subscription,
    github_subscription,
    hosted_sources,
    subscriptions_for,
)
from nerve.channels.router import ChannelRouter
from nerve.cli import doctor_report
from nerve.config import (
    GitHubReposSyncConfig,
    GitHubSyncConfig,
    NerveConfig,
    SlackConfig,
    validate_config_keys,
)
from nerve.cron.gates import GateContext, GitHubPrActivityGate
from nerve.cron.service import CronService
from nerve.gateway import server as gateway_server
from nerve.sources.github import GitHubSource, notification_record
from nerve.sources.github_hosted import HostedGitHubSource, record_from_buffer
from nerve.sources.registry import build_source_runners
from tests.fake_channel_gateway import (
    EVENT_TIMEOUT,
    FakeChannelGateway,
    FakeStream,
    NerveServer,
    gateway_negotiation,
    message_event,
    source_record_event,
)
from tests.test_hosted_channel import FAST_READER, FAST_STREAM
from tests.test_hosted_channel import hosted_config as slack_hosted_config
from tests.test_hosted_channel_config import HOSTED

GITHUB_DENY = {"deny_repos": ["owner/noisy"], "deny_reasons": ["subscribed"]}


def hosted_github(**settings: Any) -> GitHubSyncConfig:
    return GitHubSyncConfig(mode="hosted", **settings)


# The metadata of a github_repos record: every key, empty ones included.
REPO_ACTIVITY_METADATA = {
    "repo_name": "acme/widgets", "number": "12", "event": "opened", "subject_type": "PullRequest",
    "author": "alice", "author_kind": "human", "head_sha": "a" * 40,
    "subject_url": "https://github.com/acme/widgets/pull/12", "repo_url": "https://github.com/acme/widgets",
}


def repo_activity_event(*, record_id: str = "acme/widgets#12:opened:open", **metadata: str) -> dict[str, Any]:
    """A record of the repository watch as the gateway delivers it, without a delivery."""
    event = source_record_event(
        record_id=record_id, record_type="github_repo_activity", repository="acme/widgets",
    )
    event["provider"] = "github_repos"
    event["event_id"] = f"github_repos:{record_id}"
    event["admission"]["observe"]["provenance"]["provider"] = "github_repos"
    event["record"]["summary"] = "[acme/widgets] New PR #12: Add a widget cache"
    event["record"]["content"] = (
        "Repository: acme/widgets\nType: PullRequest\nEvent: opened\nPR: #12 Add a widget cache"
    )
    event["record"]["metadata"] = [
        {"key": key, "value": value} if value else {"key": key}
        for key, value in {**REPO_ACTIVITY_METADATA, **metadata}.items()
    ]
    return event


def repo_activity_payload() -> dict[str, Any]:
    """The buffer row of :func:`repo_activity_event`."""
    return buffered_record(decoded(repo_activity_event()))


async def eventually(predicate, timeout: float = EVENT_TIMEOUT) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out waiting for the condition")
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------- #
#  Settings                                                               #
# ---------------------------------------------------------------------- #


class TestSettings:
    def test_poll_is_the_default_and_hosted_is_read(self):
        assert NerveConfig().sync.github.mode == "poll"
        config = NerveConfig.from_dict({"sync": {"github": {"mode": " Hosted ", "pr_watch": "false"}}})

        assert (config.sync.github.mode, config.sync.github.pr_watch) == ("hosted", False)
        assert NerveConfig().sync.github.pr_watch is True

    def test_an_unknown_mode_falls_back_to_poll(self, caplog):
        with caplog.at_level(logging.WARNING):
            config = NerveConfig.from_dict({"sync": {"github": {"mode": "webhook"}}})

        assert config.sync.github.mode == "poll"
        assert "sync.github.mode" in caplog.text

    def test_the_inbox_is_participating_by_default_and_all_is_read(self):
        assert NerveConfig().sync.github.inbox == "participating"
        assert GitHubSyncConfig.from_dict({}).inbox == "participating"
        config = NerveConfig.from_dict({"sync": {"github": {"mode": "hosted", "inbox": " All "}}})

        assert config.sync.github.inbox == "all"
        assert validate_config_keys({"sync": {"github": {"inbox": "all"}}}) == []

    @pytest.mark.parametrize("blank", ["", "   ", None])
    def test_a_blank_inbox_means_unset(self, blank):
        assert GitHubSyncConfig.from_dict({"inbox": blank}).inbox == "participating"

    @pytest.mark.parametrize(("value", "shown"), [
        ("everything", "'everything'"), ("subscribed", "'subscribed'"), (True, "'true'"),
    ])
    def test_an_unknown_inbox_is_refused_with_the_two_accepted_values(self, value, shown):
        with pytest.raises(ValueError) as refused:
            NerveConfig.from_dict({"sync": {"github": {"inbox": value}}})

        assert str(refused.value) == (
            f"sync.github.inbox must be one of ('participating', 'all'), got {shown}"
        )

    def test_an_unknown_inbox_fails_the_load_and_the_validation(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        (tmp_path / "config.yaml").write_text(
            "sync:\n  github:\n    inbox: everything\n", encoding="utf-8",
        )

        with pytest.raises(ValueError, match=r"sync\.github\.inbox must be one of"):
            config_module.load_config(tmp_path)
        errors = validate_config_bundle(tmp_path).errors
        assert any("sync.github.inbox must be one of ('participating', 'all')" in e for e in errors)

    def test_the_inbox_follows_a_reload(self):
        after = NerveConfig()
        after.sync.github = GitHubSyncConfig(inbox="all")

        assert config_reload.restart_required(NerveConfig(), after) == []

    def test_the_repository_watch_defaults(self):
        watch = NerveConfig().sync.github_repos

        assert (
            watch.enabled, watch.repos, watch.types, watch.include_bots, watch.include_drafts,
            watch.updates, watch.deny_authors,
        ) == (False, [], ["pull_request", "issue"], False, True, False, [])
        assert GitHubReposSyncConfig.from_dict({}) == watch

    def test_the_repository_watch_settings_are_read(self):
        watch = NerveConfig.from_dict({"sync": {"github_repos": {
            "enabled": "true", "repos": [" acme/widgets ", None, "", "Other-Org/other.repo_2"],
            "types": ["Release", "pull_request", "release"], "include_bots": "yes",
            "include_drafts": "false", "updates": "on", "deny_authors": [" noisy-* ", ""],
        }}}).sync.github_repos

        assert watch.repos == ["acme/widgets", "Other-Org/other.repo_2"]
        assert watch.types == ["pull_request", "release"]
        assert (watch.include_bots, watch.include_drafts, watch.updates) == (True, False, True)
        assert watch.deny_authors == ["noisy-*"]
        assert validate_config_keys({"sync": {"github_repos": {
            "types": ["issue"], "include_bots": True, "include_drafts": False, "updates": True,
            "deny_authors": [],
        }}}) == []

    @pytest.mark.parametrize("blank", [None, "", "  "])
    def test_blank_types_mean_the_default(self, blank):
        assert GitHubReposSyncConfig.from_dict({"types": blank}).types == ["pull_request", "issue"]

    def test_one_repository_written_as_a_string_is_a_list_of_one(self):
        """A ${VAR} reference on a list setting reads as one string."""
        watch = GitHubReposSyncConfig.from_dict({"repos": "acme/widgets", "types": "release"})

        assert (watch.repos, watch.types) == (["acme/widgets"], ["release"])

    @pytest.mark.parametrize(("settings", "message"), [
        ({"types": ["pull_request", "commits"]},
         "sync.github_repos.types must be one or more of ('pull_request', 'issue', 'release'), got 'commits'"),
        ({"types": []},
         "sync.github_repos.types must be one or more of ('pull_request', 'issue', 'release'), got none"),
        ({"enabled": True, "repos": [f"acme/repo-{n}" for n in range(51)]},
         "sync.github_repos.repos must name at most 50 repositories, got 51"),
        ({"enabled": True, "repos": ["acme"]},
         "sync.github_repos.repos entries must be owner/repo names as GitHub writes them, got 'acme'"),
        ({"enabled": True, "repos": ["acme/widgets/extra"]},
         "sync.github_repos.repos entries must be owner/repo names as GitHub writes them, got 'acme/widgets/extra'"),
        ({"enabled": True, "repos": ["-acme/widgets"]},
         "sync.github_repos.repos entries must be owner/repo names as GitHub writes them, got '-acme/widgets'"),
        ({"enabled": "true", "repos": ["acme/wid gets"]},
         "sync.github_repos.repos entries must be owner/repo names as GitHub writes them, got 'acme/wid gets'"),
        ({"enabled": True, "repos": ["acme/.."]},
         "sync.github_repos.repos entries must be owner/repo names as GitHub writes them, got 'acme/..'"),
        ({"enabled": True, "repos": ["acme/widgets", "ACME/Widgets"]},
         "sync.github_repos.repos names 'ACME/Widgets' twice"),
        ({"enabled": True, "repos": {"acme": "widgets"}}, (
            "sync.github_repos.repos entries must be owner/repo names as GitHub writes them, "
            "got \"{'acme': 'widgets'}\""
        )),
        ({"types": {"issue": True}}, (
            "sync.github_repos.types must be one or more of ('pull_request', 'issue', 'release'), "
            "got \"{'issue': true}\""
        )),
    ])
    def test_a_repository_watch_the_gateway_would_refuse_refuses_to_load(self, settings, message):
        with pytest.raises(ValueError) as refused:
            NerveConfig.from_dict({"sync": {"github_repos": settings}})

        assert str(refused.value) == message

    def test_fifty_repositories_load(self):
        repos = [f"acme/repo-{n}" for n in range(50)]

        assert GitHubReposSyncConfig.from_dict({"enabled": True, "repos": repos}).repos == repos

    def test_repos_of_the_earlier_source_load_with_a_warning_while_the_watch_is_off(self, caplog):
        """The earlier source took any value. A watch that is off must not stop
        such a config from loading; switching it on refuses it until it is
        fixed. types and the other new keys have no earlier values and stay
        strict."""
        legacy = [
            "https://github.com/acme/widgets", "acme/widgets/", "acme/widgets/extra",
            "Acme/Widgets", "acme/widgets", *(f"acme/repo-{n}" for n in range(46)),
        ]

        with caplog.at_level(logging.WARNING, logger="nerve.config"):
            watch = NerveConfig.from_dict({"sync": {"github_repos": {"repos": legacy}}}).sync.github_repos

        assert (watch.enabled, watch.repos) == (False, legacy)
        [warning] = [r.getMessage() for r in caplog.records if r.name == "nerve.config"]
        assert warning == (
            "sync.github_repos is switched off, and its repos would refuse to load once it is "
            "switched on: not owner/repo names: 'https://github.com/acme/widgets', 'acme/widgets/', "
            "'acme/widgets/extra'; named twice: 'acme/widgets'; more than 50 repositories (51)"
        )
        with pytest.raises(ValueError, match="got 'https://github.com/acme/widgets'"):
            NerveConfig.from_dict({"sync": {"github_repos": {"enabled": True, "repos": legacy}}})
        with pytest.raises(ValueError, match="sync.github_repos.types must be one or more of"):
            NerveConfig.from_dict({"sync": {"github_repos": {"types": ["commits"]}}})

    def test_a_watch_that_is_off_with_good_repos_logs_nothing(self, caplog):
        with caplog.at_level(logging.WARNING, logger="nerve.config"):
            GitHubReposSyncConfig.from_dict({"repos": ["acme/widgets", "other-org/other-repo"]})

        assert [r for r in caplog.records if r.name == "nerve.config"] == []

    def test_a_bad_repository_watch_fails_the_load_and_the_validation(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        (tmp_path / "config.yaml").write_text(
            "sync:\n  github_repos:\n    types: [pull_request, commits]\n", encoding="utf-8",
        )

        with pytest.raises(ValueError, match=r"sync\.github_repos\.types must be one or more of"):
            config_module.load_config(tmp_path)
        errors = validate_config_bundle(tmp_path).errors
        assert any("sync.github_repos.types must be one or more of" in e for e in errors)

    def test_the_repository_watch_follows_a_reload(self):
        after = NerveConfig()
        after.sync.github_repos = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets"], types=["release"], include_bots=True,
            include_drafts=False, updates=True, deny_authors=["noisy-*"],
        )

        assert config_reload.restart_required(NerveConfig(), after) == []

    @pytest.mark.parametrize(("value", "expected"), [
        (None, 60), (0, 0), (15, 15), ("120", 120), (900, 900), (-5, 0), ("soon", 60),
    ])
    def test_the_delivery_debounce_is_read_as_a_whole_number_of_seconds(self, value, expected):
        cron = {} if value is None else {"delivery_debounce_seconds": value}

        assert NerveConfig.from_dict({"cron": cron}).cron.delivery_debounce_seconds == expected

    def test_config_validate_warns_about_a_debounce_above_the_maximum(self, tmp_path):
        from nerve.config_validate import ValidationResult, _validate_delivery_debounce

        result = ValidationResult()
        _validate_delivery_debounce(
            NerveConfig.from_dict({"cron": {"delivery_debounce_seconds": 900}}), result,
        )
        quiet = ValidationResult()
        _validate_delivery_debounce(NerveConfig(), quiet)

        assert result.warnings == [(
            "cron.delivery_debounce_seconds: 900 is above the maximum of 600, so the jobs "
            "a hosted delivery starts wait 600s"
        )]
        assert quiet.warnings == []

    def test_the_new_keys_are_known(self):
        merged = {
            "sync": {"github": {"mode": "hosted", "pr_watch": True}},
            "cron": {"delivery_debounce_seconds": 30},
        }

        assert validate_config_keys(merged) == []

    def test_the_mode_needs_a_restart_and_the_filters_do_not(self):
        before = NerveConfig()
        after = NerveConfig()
        after.sync.github = hosted_github(deny_repos=["owner/noisy"], pr_watch=False)

        changed = config_reload.restart_required(before, after)

        assert changed == ["sync.github.mode: 'poll' → 'hosted'"]

    def test_hosted_github_is_a_hosted_provider_without_hosted_slack(self):
        """GitHub is hosted as a whole: the repository watch comes with it,
        switched on or not."""
        config = NerveConfig()
        config.sync.github = hosted_github()

        assert hosted_providers(config) == ["github", "github_repos"]
        assert hosted_channel_providers(config) == []
        config.slack = SlackConfig(mode="hosted")
        assert hosted_providers(config) == ["slack", "github", "github_repos"]
        assert hosted_providers(NerveConfig()) == []


# ---------------------------------------------------------------------- #
#  Subscriptions                                                          #
# ---------------------------------------------------------------------- #


class TestSubscriptionsBuilder:
    def test_the_guardrail_patterns_and_the_pull_request_watch_are_published(self):
        config = NerveConfig()
        config.sync.github = hosted_github(
            allow_repos=["owner/*", " "], deny_repos=[" owner/secret "], allow_actors=["alice"],
            deny_actors=["codecov"], allow_reasons=["mention"], deny_reasons=["subscribed"],
            deny_ci_branches=["main"], pr_watch=False,
        )

        assert subscriptions_for(config, ["github"]) == Subscriptions(version="1", sources=(
            SourceSubscription(
                provider="github",
                filters=SourceFilters(
                    allow_repos=("owner/*",), deny_repos=("owner/secret",), allow_actors=("alice",),
                    deny_actors=("codecov",), allow_reasons=("mention",), deny_reasons=("subscribed",),
                    deny_ci_branches=("main",),
                ),
                pr_watch=False,
            ),
        ))

    def test_a_switched_off_source_subscribes_to_nothing(self):
        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)

        assert subscriptions_for(config, ["github"]) == Subscriptions(version="1")

    def test_the_inbox_is_published_only_when_it_is_all(self):
        def frame(**settings: Any) -> str:
            config = NerveConfig()
            config.sync.github = hosted_github(**settings)
            return dumps(subscriptions_for(config, ["github"]))

        assert frame() == frame(inbox="participating") == (
            '{"version":"1","sources":[{"provider":"github","filters":{},"pr_watch":true}]}'
        )
        assert frame(inbox="all") == (
            '{"version":"1","sources":[{"provider":"github","filters":{},"pr_watch":true,"inbox":"all"}]}'
        )

    def test_an_allow_list_the_gateway_cannot_take_is_published_empty(self, caplog):
        patterns = [f"owner/repo-{n}" for n in range(MAX_SUBSCRIPTION_PATTERNS + 1)]

        with caplog.at_level(logging.WARNING):
            subscription = github_subscription(hosted_github(allow_repos=patterns))

        assert subscription.filters.allow_repos == ()
        assert "sync.github.allow_repos" in caplog.text

    @pytest.mark.parametrize("bad", ["x" * 257, "owner/\x07bell", "line\nbreak"])
    def test_an_unusable_allow_pattern_widens_the_whole_list(self, bad):
        subscription = github_subscription(hosted_github(allow_actors=["alice", bad]))

        assert subscription.filters.allow_actors == ()

    def test_a_deny_list_keeps_the_patterns_that_fit(self):
        patterns = ["x" * 257, "owner/noisy", "tab\there"] + [f"owner/r{n}" for n in range(300)]

        subscription = github_subscription(hosted_github(deny_repos=patterns))

        assert subscription.filters.deny_repos[0] == "owner/noisy"
        assert len(subscription.filters.deny_repos) == MAX_SUBSCRIPTION_PATTERNS
        Subscriptions(version="1", sources=(subscription,)).validate()

    def test_the_frame_is_unchanged_unless_the_repository_watch_is_on_and_lists_a_repository(self):
        """An older gateway refuses the repos member and closes the stream,
        Slack included: every agent that does not watch a repository sends
        the very bytes it sent before."""
        def frame(**watch: Any) -> str:
            config = NerveConfig()
            config.sync.github = hosted_github()
            config.sync.github_repos = GitHubReposSyncConfig(**watch)
            subscriptions = subscriptions_for(config, hosted_sources(config))
            subscriptions.validate()
            return dumps(subscriptions)

        unchanged = '{"version":"1","sources":[{"provider":"github","filters":{},"pr_watch":true}]}'
        assert frame() == unchanged
        assert frame(repos=["acme/widgets"], updates=True) == unchanged
        assert frame(enabled=True) == unchanged
        assert frame(
            enabled=True, repos=["acme/widgets", "other-org/other-repo"], include_drafts=False,
            deny_authors=["noisy-*"],
        ) == (
            '{"version":"1","sources":[{"provider":"github","filters":{},"pr_watch":true},'
            '{"provider":"github_repos","filters":{},"repos":['
            '{"name":"acme/widgets","types":["pull_request","issue"],"include_bots":false,'
            '"include_drafts":false,"updates":false,"deny_authors":["noisy-*"]},'
            '{"name":"other-org/other-repo","types":["pull_request","issue"],"include_bots":false,'
            '"include_drafts":false,"updates":false,"deny_authors":["noisy-*"]}]}]}'
        )

    def test_the_repository_watch_goes_without_the_notifications_and_never_in_poll_mode(self):
        watch = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets"], types=["release"], include_bots=True, updates=True,
        )
        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)
        config.sync.github_repos = watch

        assert dumps(subscriptions_for(config, hosted_sources(config))) == (
            '{"version":"1","sources":[{"provider":"github_repos","filters":{},"repos":['
            '{"name":"acme/widgets","types":["release"],"include_bots":true,"include_drafts":true,'
            '"updates":true,"deny_authors":[]}]}]}'
        )
        polling = NerveConfig()
        polling.sync.github_repos = watch
        assert hosted_sources(polling) == []

    def test_deny_authors_the_gateway_cannot_take_keep_the_patterns_that_fit(self, caplog):
        patterns = ["x" * 257, "noisy-*", "tab\there"] + [f"bot-{n}" for n in range(300)]

        with caplog.at_level(logging.WARNING):
            watch = github_repos_subscription(GitHubReposSyncConfig(
                enabled=True, repos=["acme/widgets"], deny_authors=patterns,
            ))

        assert watch.repos[0].deny_authors[0] == "noisy-*"
        assert len(watch.repos[0].deny_authors) == MAX_SUBSCRIPTION_PATTERNS
        assert "sync.github_repos.deny_authors" in caplog.text
        Subscriptions(version="1", sources=(watch,)).validate()

    def test_names_the_gateway_would_refuse_are_not_published(self, caplog):
        """The config refuses to load them; a list built in code loses them,
        so that the gateway never closes the stream over one."""
        settings = GitHubReposSyncConfig(enabled=True, repos=[
            "acme/widgets", "not a repo", "ACME/Widgets", "acme/..", *(f"acme/r{n}" for n in range(60)),
        ])

        with caplog.at_level(logging.WARNING):
            watch = github_repos_subscription(settings)

        assert [repo.name for repo in watch.repos][:2] == ["acme/widgets", "acme/r0"]
        assert len(watch.repos) == 50
        assert "'not a repo' is not published" in caplog.text
        assert "the gateway watches the first 50" in caplog.text
        Subscriptions(version="1", sources=(watch,)).validate()
        assert github_repos_subscription(GitHubReposSyncConfig(enabled=True, repos=["bad name"])) is None
        assert github_repos_subscription(GitHubReposSyncConfig(enabled=True, repos=["a/b"], types=[])) is None


# ---------------------------------------------------------------------- #
#  Consumer and buffer                                                    #
# ---------------------------------------------------------------------- #


def decoded(event: dict[str, Any]):
    """*event* as the reader hands it to a consumer: decoded and checked."""
    from nerve.channels.hosted.contract.model import Event
    from nerve.channels.hosted.contract.wire import decode_record

    row = {**event, "delivery": {"inbox_id": "1001"}}
    value = decode_record(Event, row)
    value.validate()
    return value


@pytest.mark.asyncio
class TestConsumer:
    @pytest.fixture
    def config(self) -> NerveConfig:
        config = NerveConfig()
        config.sync.github = hosted_github()
        return config

    async def test_a_record_is_accepted_once_its_buffer_row_is_stored(self, db, config):
        heard: list[str] = []
        consumer = HostedSourceConsumer("github", db, lambda: config)
        consumer.set_delivery_listener(heard.append)

        disposition = await consumer.deliver(decoded(source_record_event()))

        assert disposition.outcome == "accepted"
        [(_, payload)] = await db.read_channel_observations("github")
        assert payload["conversation_id"] == "owner/repo"
        assert payload["record"]["id"] == "9000000001"
        assert payload["record"]["timestamp"] == "2026-09-16T10:00:00Z"
        assert {"key": "actors", "value": "", "values": ["alice", "bob"]} in payload["record"]["metadata"]
        assert heard == ["github"]

    async def test_it_never_refuses_capacity(self, db, config):
        config.sync.github.enabled = False

        assert HostedSourceConsumer("github", db, lambda: config).can_accept()

    async def test_an_async_listener_runs_on_its_own(self, db, config):
        started = asyncio.Event()
        release = asyncio.Event()

        async def listener(provider: str) -> None:
            started.set()
            await release.wait()

        consumer = HostedSourceConsumer("github", db, lambda: config)
        consumer.set_delivery_listener(listener)

        assert (await consumer.deliver(decoded(source_record_event()))).outcome == "accepted"
        await asyncio.wait_for(started.wait(), EVENT_TIMEOUT)
        await consumer.stop()

    @pytest.mark.parametrize("event", [
        message_event(purpose="observe"),
        source_record_event(purpose="invoke"),
    ])
    async def test_anything_else_is_rejected_with_a_reason_the_gateway_takes(self, db, config, event):
        disposition = await HostedSourceConsumer("github", db, lambda: config).deliver(decoded(event))

        assert (disposition.outcome, disposition.reason_code) == ("rejected", "admission_rejected")
        assert await db.read_channel_observations("github") == []

    async def test_a_failed_write_defers_the_row(self, db, config, monkeypatch):
        async def locked(*args, **kwargs):
            raise RuntimeError("database is locked")

        monkeypatch.setattr(db, "insert_channel_observation", locked)
        heard: list[str] = []
        consumer = HostedSourceConsumer("github", db, lambda: config)
        consumer.set_delivery_listener(heard.append)

        assert (await consumer.deliver(decoded(source_record_event()))).outcome == "deferred"
        assert heard == []

    async def test_the_buffer_has_its_own_row_target(self, db, config, monkeypatch):
        seen: dict[str, Any] = {}
        original = db.insert_channel_observation

        async def spy(**kwargs):
            seen.update(kwargs)
            return await original(**kwargs)

        monkeypatch.setattr(db, "insert_channel_observation", spy)
        await HostedSourceConsumer("github", db, lambda: config).deliver(decoded(source_record_event()))

        assert (seen["channel"], seen["max_rows"]) == ("github", BUFFER_MAX_ROWS)


class TestBufferedRecords:
    def payload(self, **record: Any) -> dict[str, Any]:
        base = {
            "id": "9000000001", "record_type": "github_notification",
            "summary": "[owner/repo] Fix it (mention)", "content": "Repository: owner/repo",
            "timestamp": "2026-09-16T10:00:00Z",
            "metadata": [
                {"key": "reason", "value": "mention", "values": []},
                {"key": "unread", "value": "false", "values": []},
                {"key": "actors", "value": "", "values": ["alice"]},
                {"key": "changes", "value": "", "values": []},
                {"key": "ci_branch", "value": "", "values": []},
                {"key": "", "value": "ignored", "values": []},
            ],
        }
        base.update(record)
        return {"record": base}

    def test_a_payload_maps_to_the_polling_sources_record_shape(self):
        record = record_from_buffer(self.payload())

        assert (record.id, record.source, record.record_type) == ("9000000001", "github", "github_notification")
        assert record.timestamp == "2026-09-16T10:00:00Z"
        assert record.metadata == {
            "reason": "mention", "unread": False, "actors": ["alice"], "changes": [], "ci_branch": "",
        }

    @pytest.mark.parametrize("change", [{"id": ""}, {"summary": None}, {"record_type": 7}])
    def test_an_unusable_payload_gives_no_record(self, change):
        assert record_from_buffer(self.payload(**change)) is None
        assert record_from_buffer({"record": "text"}) is None

    @pytest.mark.asyncio
    async def test_the_drain_reads_past_a_polling_cursor_and_skips_unreadable_rows(self, db):
        await db.insert_channel_observation("github", "github:owner/repo", self.payload())
        await db.insert_channel_observation("github", "github:owner/repo", {"record": {}})
        await db.insert_channel_observation("slack", "slack:C1", {"text": "not ours"})

        result = await HostedGitHubSource(db).fetch("2026-09-16T09:00:00Z", limit=10)

        assert [r.id for r in result.records] == ["9000000001"]
        assert (result.next_cursor, result.has_more) == ("2", False)
        again = await HostedGitHubSource(db).fetch(result.next_cursor, limit=10)
        assert (again.records, again.next_cursor) == ([], "2")


@pytest.mark.asyncio
class TestRegistry:
    def runner(self, config, db):
        runners = [r for r in build_source_runners(config, db) if r.job_id == "source:github"]
        return runners[0] if runners else None

    async def test_hosted_mode_drains_the_buffer_with_the_github_guardrail(self, db):
        config = NerveConfig()
        config.sync.github = hosted_github(**GITHUB_DENY, drop_hook=["/bin/true"])

        runner = self.runner(config, db)

        assert isinstance(runner.source, HostedGitHubSource)
        assert runner.source.source_name == "github"
        assert [rule.field for rule in runner.inbox_filter.rules if rule.active] == ["repo_name", "reason"]
        assert runner.drop_hook == ["/bin/true"]

    async def test_poll_mode_is_unchanged(self, db):
        runner = self.runner(NerveConfig(), db)

        assert isinstance(runner.source, GitHubSource)

    @pytest.mark.parametrize(("inbox", "participating"), [("participating", True), ("all", False)])
    async def test_poll_mode_lists_the_inbox_that_the_setting_names(self, db, monkeypatch, inbox, participating):
        seen = TestPollingCursor.listings(monkeypatch)
        config = NerveConfig.from_dict({"sync": {"github": {"inbox": inbox}}})

        await self.runner(config, db).source.fetch(None)

        assert ("&participating=true" in seen[0]) is participating
        assert seen[0].endswith("&all=true")

    async def test_a_source_reload_hands_a_changed_inbox_to_the_polling_source(self, db, monkeypatch):
        seen = TestPollingCursor.listings(monkeypatch)
        service = CronService(NerveConfig(), AsyncMock(), db)
        service.scheduler.start(paused=True)
        try:
            service._register_source_runners()
            service.config = NerveConfig.from_dict({"sync": {"github": {"inbox": "all"}}})
            await service.reload_sources()
            (runner,) = [r for r in service._source_runners if r.job_id == "source:github"]
            await runner.source.fetch(None)
        finally:
            service.scheduler.shutdown(wait=False)

        assert len(seen) == 1 and "participating" not in seen[0]

    async def test_a_switched_off_source_has_no_runner(self, db):
        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)

        assert self.runner(config, db) is None

    async def test_hosted_mode_drains_the_repository_watch_from_its_own_buffer(self, db):
        """GitHub is hosted as a whole: the repository watch runs as the
        push-fed drain of the github_repos buffer, with its guardrail."""
        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)
        config.sync.github_repos = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets"], deny_authors=["noisy-*"],
        )
        await db.insert_channel_observation("github", "github:owner/repo", TestBufferedRecords().payload())
        await db.insert_channel_observation("github_repos", "github_repos:acme/widgets", repo_activity_payload())

        (runner,) = [r for r in build_source_runners(config, db) if r.job_id == "source:github_repos"]
        result = await runner.source.fetch(None)

        assert isinstance(runner.source, HostedGitHubSource)
        assert (runner.source.source_name, runner.source.push_fed) == ("github_repos", True)
        assert [rule.field for rule in runner.inbox_filter.rules if rule.active] == [
            "repo_name", "author", "subject_type", "author_kind",
        ]
        assert [(r.id, r.source) for r in result.records] == [("acme/widgets#12:opened:open", "github_repos")]
        assert result.records[0].metadata == REPO_ACTIVITY_METADATA
        assert await runner.source.has_undrained(None) is True
        assert await runner.source.has_undrained(result.next_cursor) is False

    async def test_no_hosted_source_for_another_provider(self, db):
        with pytest.raises(ValueError, match="no hosted GitHub source for provider 'gitlab'"):
            HostedGitHubSource(db, "gitlab")


@pytest.mark.asyncio
class TestPollingCursor:
    @staticmethod
    def listings(monkeypatch) -> list[str]:
        """The notification listings that the polling source asks gh for."""
        seen: list[str] = []

        class _Proc:
            returncode = 0

            async def communicate(self):
                return b"", b""

        async def fake_exec(*argv, **kwargs):
            seen.append(argv[2])
            return _Proc()

        monkeypatch.setattr("nerve.sources.github.asyncio.create_subprocess_exec", fake_exec)
        return seen

    async def test_a_cursor_that_is_no_timestamp_looks_back_a_day(self, monkeypatch):
        seen = self.listings(monkeypatch)

        result = await GitHubSource().fetch("57")

        assert result.next_cursor == "57"
        assert seen and "since=57" not in seen[0] and "since=20" in seen[0]

    @pytest.mark.parametrize("failure", [RuntimeError("gh failed"), asyncio.CancelledError()])
    async def test_a_failed_or_cancelled_enrichment_gives_the_notification_alone(self, failure):
        notification = {
            "id": "9000000001", "reason": "mention", "unread": True, "updated_at": "2026-09-16T10:00:00Z",
            "subject": {"title": "Fix the flaky retry test", "type": "PullRequest"},
            "repository": {"full_name": "owner/repo"},
        }

        record = notification_record(notification, failure)

        assert (record.id, record.summary) == ("9000000001", "[owner/repo] Fix the flaky retry test (mention)")
        assert record.metadata["repo_name"] == "owner/repo" and record.metadata["actors"] == []

    async def test_digits_are_no_timestamp_even_when_they_read_as_a_date(self, monkeypatch):
        seen = self.listings(monkeypatch)
        a_day_ago = datetime.now(UTC) - timedelta(hours=24, minutes=5)

        await GitHubSource().fetch("20000101")

        since = seen[0].split("since=", 1)[1].split("&", 1)[0]
        assert datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC) > a_day_ago


# ---------------------------------------------------------------------- #
#  End to end over real streams                                           #
# ---------------------------------------------------------------------- #


@dataclass
class Harness:
    config: NerveConfig
    db: Any
    router: ChannelRouter
    gateway: FakeChannelGateway
    runtime: HostedChannelRuntime
    server: NerveServer

    async def stream(self, **kwargs: Any) -> FakeStream:
        return await self.gateway.open_stream(self.server.stream_url, **kwargs)

    async def acknowledged(self, *inbox_ids: str) -> dict[str, str]:
        await self.gateway.wait_for(lambda: all(i in self.gateway.acknowledged for i in inbox_ids))
        return {i: self.gateway.acknowledged[i] for i in inbox_ids}

    async def subscriptions(self, count: int = 1) -> list[dict[str, Any]]:
        await self.gateway.wait_for(lambda: len(self.gateway.subscriptions) >= count)
        return self.gateway.subscriptions

    def github_runner(self):
        (runner,) = [r for r in build_source_runners(self.config, self.db) if r.job_id == "source:github"]
        return runner


async def start(
    tmp_path, db, monkeypatch, *, slack: bool = False, github: bool = True, email: bool = False,
    config_getter: Callable[[], NerveConfig] | None = None, **settings: Any,
) -> Harness:
    """A hosted runtime on real streams. Its config getter returns this config
    object unless *config_getter* is given (``get_config`` follows a reload).
    *email* switches the hosted email source on."""
    config = slack_hosted_config(tmp_path)
    if not slack:
        config.slack = SlackConfig()
    if github:
        config.sync.github = hosted_github(**settings)
    config.sync.email.enabled = email
    monkeypatch.setattr(config_module, "_config", config)
    engine = MagicMock()
    engine.db = db
    engine.sessions = SessionManager(db)
    router = ChannelRouter(engine)
    engine.router = router
    gateway = FakeChannelGateway(jwks_file=config.channels.hosted.gateway_jwks_file)
    runtime = HostedChannelRuntime(
        config, router, config_getter or (lambda: config),
        stream_timing=FAST_STREAM, reader_settings=FAST_READER, db=db,
    )
    await runtime.start()
    monkeypatch.setattr(gateway_server, "_hosted_channels", runtime)
    server = NerveServer(gateway_server.create_app())
    await server.__aenter__()
    return Harness(config, db, router, gateway, runtime, server)


async def stop(harness: Harness) -> None:
    await harness.runtime.stop()
    await harness.gateway.close_all()
    await harness.server.__aexit__(None, None, None)


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start(tmp_path, db, monkeypatch, **GITHUB_DENY)
    try:
        yield harness
    finally:
        await stop(harness)


@contextlib.asynccontextmanager
async def stalled_through_negotiation(harness: Harness) -> AsyncIterator[FakeStream]:
    """A stream that negotiates while Nerve's side of it cannot send, as when
    another frame is stuck on the socket. The stall clears on leaving, and the
    gateway then advertises its capabilities."""
    stream = await harness.stream(negotiate=False)
    await asyncio.wait_for(stream.nerve_negotiation.wait(), EVENT_TIMEOUT)
    [nerve_side] = harness.runtime.streams.streams
    await nerve_side._send_lock.acquire()
    try:
        await stream.send("negotiation", gateway_negotiation())
        await eventually(lambda: nerve_side.ready)
        yield stream
    finally:
        nerve_side._send_lock.release()
    await stream.advertise()


@pytest.mark.asyncio
class TestStreams:
    async def test_a_stream_publishes_the_subscriptions_right_after_negotiation(self, hosted):
        stream = await hosted.stream()

        [subscriptions] = await hosted.subscriptions()

        assert subscriptions == {"version": "1", "sources": [{
            "provider": "github",
            "filters": {"deny_repos": ["owner/noisy"], "deny_reasons": ["subscribed"]},
            "pr_watch": True,
        }]}
        await hosted.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))
        kinds = [frame["kind"] for frame in stream.frames if frame["kind"] != "heartbeat"]
        assert kinds[:3] == ["negotiation", "subscriptions", "inbox_read"]
        frame = stream.sent("subscriptions")[0]
        assert frame["request_id"].startswith("o-") and "connection_id" not in frame

    async def test_a_record_is_buffered_then_drained_into_the_inbox(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(source_record_event())

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert len(await hosted.db.read_channel_observations("github")) == 1
        assert hosted.router.get_channel("github") is None
        result = await hosted.github_runner().run()
        assert result.records_ingested == 1
        stored = await hosted.db.get_source_message("github", "9000000001")
        assert stored["summary"] == "[owner/repo] Fix the flaky retry test (mention)"
        assert stored["content"].endswith("@example-agent could you take a look?")
        assert stored["metadata"]["actors"] == ["alice", "bob"]
        assert stored["metadata"]["unread"] is True
        assert stored["metadata"]["ci_branch"] == ""

    async def test_a_changed_record_resurfaces_in_place_of_the_stored_one(self, hosted):
        stream = await hosted.stream()
        runner = hosted.github_runner()
        first = hosted.gateway.store(source_record_event())
        await stream.nudge("observe")
        assert (await hosted.acknowledged(first))[first] == "accepted"
        assert (await runner.run()).records_ingested == 1
        before = await hosted.db.get_source_max_rowid("github")

        second = hosted.gateway.store(source_record_event(
            title="Fix the flaky retry test, take two", updated_at="2026-09-16T11:00:00Z", version=1789556400,
        ))
        await stream.nudge("observe")

        assert (await hosted.acknowledged(second))[second] == "accepted"
        assert (await runner.run()).records_ingested == 1
        stored = await hosted.db.get_source_message("github", "9000000001")
        assert stored["summary"] == "[owner/repo] Fix the flaky retry test, take two (mention)"
        assert await hosted.db.get_source_max_rowid("github") > before

    async def test_the_inbox_filter_drops_what_the_published_filters_deny(self, hosted):
        stream = await hosted.stream()
        noisy = hosted.gateway.store(source_record_event(record_id="9000000002", repository="owner/noisy"))
        kept = hosted.gateway.store(source_record_event(record_id="9000000003"))

        await stream.nudge("observe")

        assert await hosted.acknowledged(noisy, kept) == {noisy: "accepted", kept: "accepted"}
        result = await hosted.github_runner().run()
        assert (result.records_ingested, result.records_dropped) == (1, 1)
        assert await hosted.db.get_source_message("github", "9000000002") is None

    async def test_a_pull_request_record_meets_the_repo_rule_only(self, tmp_path, db, monkeypatch):
        harness = await start(
            tmp_path, db, monkeypatch, allow_actors=["someone-else"], allow_reasons=["mention"],
        )
        try:
            stream = await harness.stream()
            pull_request = harness.gateway.store(source_record_event(
                record_id="pr:owner/repo#12", record_type="github_pr_activity", reason="author",
            ))
            notification = harness.gateway.store(source_record_event(record_id="9000000004"))

            await stream.nudge("observe")

            await harness.acknowledged(pull_request, notification)
            result = await harness.github_runner().run()
            assert (result.records_ingested, result.records_dropped) == (1, 1)
            stored = await db.get_source_message("github", "pr:owner/repo#12")
            assert stored["record_type"] == "github_pr_activity"
            assert stored["metadata"]["reason"] == "author"
            assert await db.get_source_message("github", "9000000004") is None
        finally:
            await stop(harness)

    async def test_other_github_events_are_rejected_and_settled(self, hosted):
        stream = await hosted.stream()
        invoke = hosted.gateway.store(source_record_event(purpose="invoke"))
        message = message_event(purpose="observe")
        message["provider"] = "github"
        chat = hosted.gateway.store(message)

        await stream.nudge("observe")

        assert await hosted.acknowledged(invoke, chat) == {invoke: "rejected", chat: "rejected"}
        assert {item["reason_code"] for ack in hosted.gateway.acks for item in ack} == {"admission_rejected"}

    async def test_a_failed_buffer_write_leaves_the_row_for_a_later_read(self, hosted, monkeypatch):
        calls = 0
        original = hosted.db.insert_channel_observation

        async def failing_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("database is locked")
            return await original(*args, **kwargs)

        monkeypatch.setattr(hosted.db, "insert_channel_observation", failing_once)
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(source_record_event())

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert calls == 2

    async def test_a_reload_publishes_the_changed_settings_on_every_stream(self, hosted):
        await hosted.stream()
        await hosted.stream()
        await hosted.subscriptions(2)
        hosted.config.sync.github.deny_reasons = ["subscribed", "comment"]
        hosted.config.sync.github.pr_watch = False
        hosted.config.sync.github.inbox = "all"
        engine = SimpleNamespace(get_channel_runtime={"hosted": hosted.runtime}.get)

        outcome = await config_reload._republish_hosted_subscriptions(engine)

        assert outcome == "published on 2 stream(s)"
        latest = (await hosted.subscriptions(4))[-1]
        assert latest["sources"][0]["filters"]["deny_reasons"] == ["subscribed", "comment"]
        assert "pr_watch" not in latest["sources"][0]
        assert latest["sources"][0]["inbox"] == "all"

    async def test_a_config_reload_publishes_the_reloaded_settings(self, tmp_path, db, monkeypatch):
        harness = await start(
            tmp_path, db, monkeypatch, config_getter=config_module.get_config, **GITHUB_DENY,
        )
        try:
            await harness.stream()
            await harness.subscriptions(1)
            reloaded = copy.deepcopy(harness.config)
            reloaded.sync.github.deny_reasons = ["subscribed", "comment"]
            reloaded.sync.github.pr_watch = False
            reloaded.sync.github.inbox = "all"
            monkeypatch.setattr(config_module, "load_config", lambda config_dir=None: reloaded)
            engine = SimpleNamespace(
                get_channel_runtime={"hosted": harness.runtime}.get,
                reload_mcp_config=AsyncMock(return_value=[]),
            )

            summary = await config_reload.reload_all(engine, None, tmp_path)

            assert summary["subscriptions"] == "published on 1 stream(s)"
            assert config_reload.reload_failures(summary) == {}
            latest = (await harness.subscriptions(2))[-1]
            assert latest["sources"][0]["filters"]["deny_reasons"] == ["subscribed", "comment"]
            assert "pr_watch" not in latest["sources"][0]
            assert latest["sources"][0]["inbox"] == "all"
        finally:
            await stop(harness)

    async def test_a_stalled_stream_does_not_hold_up_a_republish(self, hosted, monkeypatch, caplog):
        monkeypatch.setattr("nerve.channels.hosted.stream._SUBSCRIPTIONS_SEND_SECONDS", 0.2)
        await hosted.stream()
        await hosted.stream()
        await hosted.subscriptions(2)
        stalled = hosted.runtime.streams.streams[0]
        caplog.set_level(logging.WARNING, logger="nerve.channels.hosted.stream")
        await stalled._send_lock.acquire()
        try:
            outcome = await asyncio.wait_for(hosted.runtime.republish_subscriptions(), timeout=3)
        finally:
            stalled._send_lock.release()

        assert outcome == "published on 1 stream(s)"
        assert (
            f"Channel stream {stalled.id}: source subscriptions not sent: the stream could not "
            "send them within 0.2s"
        ) in caplog.text

    async def test_a_stall_right_after_negotiation_delays_the_subscriptions_but_never_drops_them(
        self, hosted, monkeypatch, caplog,
    ):
        """Nothing waits for the publish that negotiation starts, and nothing
        sends the frame again before a reload, so a stall longer than a
        republish's bound still ends with the frame sent, once."""
        monkeypatch.setattr("nerve.channels.hosted.stream._SUBSCRIPTIONS_SEND_SECONDS", 0.2)
        caplog.set_level(logging.WARNING, logger="nerve.channels.hosted.stream")

        async with stalled_through_negotiation(hosted) as stream:
            await asyncio.sleep(0.6)
            assert stream.sent("subscriptions") == []

        await hosted.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))
        await asyncio.sleep(0.3)
        kinds = [frame["kind"] for frame in stream.frames if frame["kind"] != "heartbeat"]
        assert kinds[:3] == ["negotiation", "subscriptions", "inbox_read"]
        assert len(stream.sent("subscriptions")) == 1
        assert hosted.gateway.subscriptions[0]["sources"][0]["filters"] == GITHUB_DENY
        assert stream.close_code is None
        assert "source subscriptions not sent" not in caplog.text

    async def test_subscriptions_that_waited_through_a_stall_carry_the_settings_of_when_they_left(
        self, hosted, monkeypatch,
    ):
        """A reload during the stall gives up on the stream after its bound, so
        the frame that waited is the one the gateway gets: it is built when it
        leaves, from the reloaded settings."""
        monkeypatch.setattr("nerve.channels.hosted.stream._SUBSCRIPTIONS_SEND_SECONDS", 0.2)

        async with stalled_through_negotiation(hosted) as stream:
            hosted.config.sync.github.deny_reasons = ["subscribed", "comment"]
            outcome = await asyncio.wait_for(hosted.runtime.republish_subscriptions(), timeout=3)
            assert outcome == "published on 0 stream(s)"

        await hosted.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))
        [frame] = stream.sent("subscriptions")
        filters = frame["payload"]["subscriptions"]["sources"][0]["filters"]
        assert filters["deny_reasons"] == ["subscribed", "comment"]

    async def test_a_stream_that_fails_to_publish_is_logged_and_the_others_still_send(
        self, hosted, monkeypatch, caplog,
    ):
        await hosted.stream()
        await hosted.stream()
        await hosted.subscriptions(2)
        failing = hosted.runtime.streams.streams[0]

        async def broken() -> bool:
            raise RuntimeError("socket gone")

        monkeypatch.setattr(failing, "publish_subscriptions", broken)
        caplog.set_level(logging.ERROR, logger="nerve.channels.hosted.manager")

        outcome = await hosted.runtime.republish_subscriptions()

        assert outcome == "published on 1 stream(s)"
        assert f"Channel stream {failing.id}: publishing the source subscriptions failed" in caplog.text

    async def test_a_publish_after_negotiation_that_fails_is_logged_at_once(self, hosted, monkeypatch, caplog):
        class Unchecked:
            def validate(self) -> None:
                raise RuntimeError("cannot check")

        monkeypatch.setattr(
            "nerve.channels.hosted.runtime.subscriptions_for", lambda config, providers: Unchecked(),
        )
        caplog.set_level(logging.ERROR, logger="nerve.channels.hosted.stream")

        stream = await hosted.stream()

        await eventually(lambda: "publishing the source subscriptions failed" in caplog.text)
        assert stream.sent("subscriptions") == []
        assert stream.close_code is None

    async def test_subscriptions_above_the_gateways_frame_limit_go_without_patterns(self, hosted):
        hosted.config.sync.github.allow_repos = [f"owner/{'r' * 200}-{n}" for n in range(100)]
        stream = await hosted.stream(negotiate=False)
        await stream.negotiate(frame_bytes=16384)

        [subscriptions] = await hosted.subscriptions()

        assert subscriptions["sources"] == [{"provider": "github", "filters": {}, "pr_watch": True}]
        assert stream.close_code is None

    async def test_subscriptions_refused_even_without_patterns_are_not_sent(self, hosted, monkeypatch, caplog):
        refused = Subscriptions(version="1", sources=(
            SourceSubscription(provider="github", filters=SourceFilters(allow_repos=("",))),
            SourceSubscription(provider="github"),
        ))
        monkeypatch.setattr(
            "nerve.channels.hosted.runtime.subscriptions_for", lambda config, providers: refused,
        )
        caplog.set_level(logging.WARNING, logger="nerve.channels.hosted.stream")

        stream = await hosted.stream()
        await hosted.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))

        assert stream.sent("subscriptions") == []
        assert stream.close_code is None
        assert "source subscriptions not sent: the gateway would refuse them" in caplog.text

    async def test_the_repository_watch_is_published_and_its_records_reach_the_inbox(self, hosted):
        hosted.config.sync.github_repos = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets"], types=["pull_request"], updates=True,
        )
        stream = await hosted.stream()
        [subscriptions] = await hosted.subscriptions()
        inbox_id = hosted.gateway.store(repo_activity_event())

        await stream.nudge("observe")

        assert subscriptions["sources"][1] == {"provider": "github_repos", "filters": {}, "repos": [{
            "name": "acme/widgets", "types": ["pull_request"], "include_bots": False,
            "include_drafts": True, "updates": True, "deny_authors": [],
        }]}
        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert len(await hosted.db.read_channel_observations("github_repos")) == 1
        assert await hosted.db.read_channel_observations("github") == []
        (runner,) = [
            r for r in build_source_runners(hosted.config, hosted.db) if r.job_id == "source:github_repos"
        ]
        assert (await runner.run()).records_ingested == 1
        stored = await hosted.db.get_source_message("github_repos", "acme/widgets#12:opened:open")
        assert stored["record_type"] == "github_repo_activity"
        assert stored["summary"] == "[acme/widgets] New PR #12: Add a widget cache"
        assert stored["metadata"] == REPO_ACTIVITY_METADATA

    async def test_the_repository_watch_guardrail_drops_what_the_settings_drop(self, hosted):
        """Records the gateway took before a reload changed the settings. The
        bot rule is for pull requests and issues: a bot's release is kept."""
        hosted.config.sync.github_repos = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets"], types=["pull_request", "release"],
            deny_authors=["noisy-*"],
        )
        stream = await hosted.stream()
        kept = hosted.gateway.store(repo_activity_event())
        denied = hosted.gateway.store(repo_activity_event(record_id="acme/widgets#13:opened:open", author="noisy-ci"))
        bot = hosted.gateway.store(repo_activity_event(
            record_id="acme/widgets#14:opened:open", author="updater[bot]", author_kind="bot",
        ))
        bot_release = hosted.gateway.store(repo_activity_event(
            record_id="acme/widgets#:released:55", number="", event="released", subject_type="Release",
            author="github-actions[bot]", author_kind="bot", head_sha="",
            subject_url="https://github.com/acme/widgets/releases/tag/v1.2.0",
        ))

        await stream.nudge("observe")

        await hosted.acknowledged(kept, denied, bot, bot_release)
        (runner,) = [
            r for r in build_source_runners(hosted.config, hosted.db) if r.job_id == "source:github_repos"
        ]
        result = await runner.run()
        assert (result.records_ingested, result.records_dropped) == (2, 2)
        stored = await hosted.db.get_source_message("github_repos", "acme/widgets#:released:55")
        assert stored["metadata"]["author_kind"] == "bot"
        assert stored["metadata"]["subject_url"] == "https://github.com/acme/widgets/releases/tag/v1.2.0"

    async def test_switching_the_repository_watch_on_and_off_follows_a_reload(self, hosted, db):
        """No new UI: the switch is sync.github_repos.enabled, and a reload
        rebuilds the runner and publishes the subscriptions again."""
        cron = CronService(hosted.config, AsyncMock(), db)
        cron.scheduler.start(paused=True)
        engine = SimpleNamespace(get_channel_runtime={"hosted": hosted.runtime}.get)
        try:
            cron._register_source_runners()
            await hosted.stream()
            await hosted.subscriptions(1)

            async def reload(**watch: Any) -> tuple[dict, list[str]]:
                hosted.config.sync.github_repos = GitHubReposSyncConfig(**watch)
                sources = await cron.reload_sources()
                assert await config_reload._republish_hosted_subscriptions(engine) == "published on 1 stream(s)"
                latest = hosted.gateway.subscriptions[-1]
                return sources, [source["provider"] for source in latest["sources"]]

            sources, providers = await reload(enabled=True, repos=["acme/widgets"])
            await hosted.subscriptions(2)
            assert "source:github_repos" in sources["sources"]
            assert providers == ["github", "github_repos"]
            (runner,) = [r for r in cron._source_runners if r.job_id == "source:github_repos"]
            assert isinstance(runner.source, HostedGitHubSource) and runner.source.push_fed is True

            sources, providers = await reload(enabled=False, repos=["acme/widgets"])
            await hosted.subscriptions(3)
            assert sources["removed"] == ["source:github_repos"]
            assert providers == ["github"]
        finally:
            cron.scheduler.shutdown(wait=False)

    async def test_deny_authors_go_with_the_filter_patterns_when_the_frame_is_too_large(self, hosted):
        hosted.config.sync.github.allow_repos = [f"owner/{'r' * 200}-{n}" for n in range(100)]
        hosted.config.sync.github_repos = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets"], deny_authors=["noisy-*"],
        )
        stream = await hosted.stream(negotiate=False)
        await stream.negotiate(frame_bytes=16384)

        [subscriptions] = await hosted.subscriptions()

        assert subscriptions["sources"][0] == {"provider": "github", "filters": {}, "pr_watch": True}
        assert subscriptions["sources"][1]["repos"][0]["deny_authors"] == []
        assert subscriptions["sources"][1]["repos"][0]["name"] == "acme/widgets"
        assert stream.close_code is None

    async def test_a_switched_off_source_subscribes_to_nothing_and_keeps_what_arrives(self, hosted):
        hosted.config.sync.github.enabled = False
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(source_record_event())

        [subscriptions] = await hosted.subscriptions()
        await stream.nudge("observe")

        assert subscriptions == {"version": "1"}
        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert len(await hosted.db.read_channel_observations("github")) == 1


@pytest.mark.asyncio
class TestSharedStreams:
    async def test_slack_and_github_share_the_streams_and_the_reader(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, slack=True)
        try:
            assert set(harness.runtime.channels) == {"slack"}
            assert set(harness.runtime.sources) == {"github", "github_repos"}
            assert harness.router.get_channel("github") is None
            stream = await harness.stream()
            observed = harness.gateway.store(message_event(
                purpose="observe", conversation="C_FIXTURE_OBSERVATION", message_id="1700002000.000100",
            ))
            record = harness.gateway.store(source_record_event())

            await stream.nudge("observe")

            assert await harness.acknowledged(observed, record) == {observed: "accepted", record: "accepted"}
            assert len(await db.read_channel_observations("slack")) == 1
            assert len(await db.read_channel_observations("github")) == 1
            assert len(await harness.subscriptions()) == 1
        finally:
            await stop(harness)

    async def test_a_switched_off_hosted_slack_holds_the_github_records_back(self, tmp_path, db, monkeypatch):
        """One ordered inbox for every provider, which Nerve stops reading
        while a channel cannot take events: GitHub records wait while hosted
        Slack is off and arrive once it is on again."""
        harness = await start(tmp_path, db, monkeypatch, slack=True)
        try:
            harness.config.slack.enabled = False
            stream = await harness.stream()
            record = harness.gateway.store(source_record_event())

            await stream.nudge("observe")
            await asyncio.sleep(0.5)

            assert record not in harness.gateway.acknowledged
            assert await db.read_channel_observations("github") == []
            harness.config.slack.enabled = True
            assert (await harness.acknowledged(record))[record] == "accepted"
            assert len(await db.read_channel_observations("github")) == 1
        finally:
            await stop(harness)

    async def test_without_hosted_sources_no_subscriptions_are_sent(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, slack=True, github=False)
        try:
            stream = await harness.stream()
            await harness.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))

            assert harness.runtime.subscriptions() is None
            assert stream.sent("subscriptions") == []
            assert await harness.runtime.republish_subscriptions() is None
        finally:
            await stop(harness)

    async def test_a_runtime_without_any_hosted_provider_is_refused(self, tmp_path, db):
        config = slack_hosted_config(tmp_path)
        config.slack = SlackConfig()

        with pytest.raises(ValueError, match="no provider is in hosted mode"):
            HostedChannelRuntime(config, ChannelRouter(MagicMock()), lambda: config, db=db)


# ---------------------------------------------------------------------- #
#  Jobs that a delivery runs                                              #
# ---------------------------------------------------------------------- #


def write_jobs(path: Path, jobs: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")


@pytest.mark.asyncio
class TestRunsOnDelivery:
    async def test_a_delivery_runs_the_gated_job_once_and_not_the_others(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch)
        cron_dir = tmp_path / "cron"
        harness.config.cron.jobs_file = cron_dir / "jobs.yaml"
        harness.config.cron.system_file = cron_dir / "system.yaml"
        harness.config.cron.gate_plugins_dir = cron_dir / "gates"
        harness.config.cron.delivery_debounce_seconds = 1
        never = "0 0 1 1 *"
        write_jobs(harness.config.cron.jobs_file, [
            {"id": "pr-monitor", "schedule": never, "prompt": "Look at the PRs",
             "run_if": [{"type": "messages", "sources": ["github"]}]},
            {"id": "mail-triage", "schedule": never, "prompt": "Look at the mail",
             "run_if": [{"type": "messages", "sources": ["gmail"]}]},
            {"id": "daily-briefing", "schedule": never, "prompt": "Brief me"},
        ])
        turns: list[str] = []

        async def run_cron(**kwargs: Any):
            turns.append(kwargs["job_id"])
            return SimpleNamespace(text="done", outcome=SimpleNamespace(ok=True, describe=lambda: ""))

        engine = MagicMock()
        engine.run_cron = run_cron
        cron = CronService(harness.config, engine, db)
        await cron.start()
        harness.runtime.set_delivery_listener(cron.on_source_delivered)
        try:
            # The consumer cursor exists before the records arrive, as it does for a
            # job that has polled before.
            await db.get_consumer_cursor("inbox", "github")
            stream = await harness.stream()
            inbox_ids = [
                harness.gateway.store(source_record_event(record_id=str(9000000010 + n)))
                for n in range(3)
            ]

            await stream.nudge("observe")

            await harness.acknowledged(*inbox_ids)
            await eventually(lambda: turns == ["pr-monitor"])
            assert await db.get_source_message("github", "9000000012") is not None
            await asyncio.sleep(1.5)
            assert turns == ["pr-monitor"]
            logs = await db.get_cron_logs(job_id="source:github", limit=10)
            assert logs and all(log["status"] == "success" for log in logs)
        finally:
            await cron.stop()
            await stop(harness)


@pytest.mark.asyncio
class TestSchedule:
    async def forecast(self, config: NerveConfig, db) -> dict[str, Any]:
        """The schedule report's entry for the repository watch's runner."""
        service = CronService(config, AsyncMock(), db)
        service.scheduler.start(paused=True)
        try:
            service._register_source_runners()
            report = await service.schedule_report()
        finally:
            service.scheduler.shutdown(wait=False)
        return {job["id"]: job for job in report["jobs"]}["source:github_repos"]

    async def test_a_scheduled_drain_of_the_hosted_repository_watch_is_no_wake(self, db):
        """The gateway delivers the records and the delivery runs the drain;
        with nothing waiting, its scheduled run is not counted as the next
        task, so a parked machine is not woken for it."""
        from nerve.cron import service as cron_service

        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True, repos=["acme/widgets"])

        idle = await self.forecast(config, db)
        await db.insert_channel_observation("github_repos", "github_repos:acme/widgets", repo_activity_payload())
        waiting = await self.forecast(config, db)

        assert (idle["forecast"], idle["reason"]) == ("skip", cron_service._PUSH_FED_REASON)
        assert waiting["forecast"] == "run"

    async def test_the_polling_repository_watch_counts(self, db):
        config = NerveConfig()
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True, repos=["acme/widgets"])

        assert (await self.forecast(config, db))["forecast"] == "run"


# ---------------------------------------------------------------------- #
#  The pull request gate, doctor, and the setup page                      #
# ---------------------------------------------------------------------- #


def recorded_gh(monkeypatch) -> list[tuple[str, ...]]:
    """The processes the gate starts. Each fails, so the gate fails open, and
    only the record tells whether gh ran."""
    calls: list[tuple[str, ...]] = []

    async def failing(*args, **kwargs):
        calls.append(args)
        raise FileNotFoundError("gh")

    monkeypatch.setattr("nerve.cron.gates.asyncio.create_subprocess_exec", failing)
    return calls


def github_config(mode: str) -> NerveConfig:
    config = NerveConfig()
    config.sync.github = GitHubSyncConfig(mode=mode)
    return config


@pytest.mark.asyncio
class TestPullRequestGate:
    async def test_in_hosted_mode_it_passes_without_gh(self, db, monkeypatch):
        monkeypatch.setattr(config_module, "_config", github_config("hosted"))
        calls = recorded_gh(monkeypatch)
        gate = GitHubPrActivityGate(author="example-agent")

        assert await gate.is_satisfied(GateContext(job_id="pr-monitor", db=db)) is True
        assert calls == []

    async def test_in_poll_mode_it_still_runs_gh(self, db, monkeypatch):
        monkeypatch.setattr(config_module, "_config", NerveConfig())
        calls = recorded_gh(monkeypatch)

        assert await GitHubPrActivityGate(author="example-agent").is_satisfied(
            GateContext(job_id="pr-monitor", db=db),
        ) is True
        assert calls and calls[0][0] == "gh"

    @pytest.mark.parametrize(("started", "reloaded", "runs_gh"), [
        ("poll", "hosted", True),
        ("hosted", "poll", False),
    ])
    async def test_it_keeps_the_mode_the_process_started_with(
        self, db, monkeypatch, started, reloaded, runs_gh,
    ):
        """The mode is restart-only: after a reload that changes it, the
        source still runs in the old mode until the restart, and so does the
        gate."""
        config_reload.remember_startup_config(github_config(started))
        monkeypatch.setattr(config_module, "_config", github_config(reloaded))
        calls = recorded_gh(monkeypatch)

        assert await GitHubPrActivityGate(author="example-agent").is_satisfied(
            GateContext(job_id="pr-monitor", db=db),
        ) is True
        assert bool(calls) is runs_gh


def doctor_config(tmp_path: Path, **github: Any) -> NerveConfig:
    gateway = FakeChannelGateway(jwks_file=tmp_path / "gateway-jwks.json")
    return NerveConfig.from_dict({
        "sync": {"github": {"mode": "hosted", **github}},
        "channels": {"hosted": {**HOSTED, "gateway_jwks_file": str(gateway.jwks_file)}},
    })


class TestDoctor:
    def test_hosted_github_is_reported(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, pr_watch=False))

        assert (
            "[OK] GitHub notifications come from the channel gateway: filters published "
            "from sync.github, PR watch off, inbox participating\n"
        ) in report

    def test_the_inbox_is_reported(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, inbox="all"))

        assert "from sync.github, PR watch on, inbox all\n" in report

    def test_unusable_hosted_settings_are_errors(self, tmp_path):
        config = doctor_config(tmp_path)
        config.channels.hosted.gateway_jwks_file = tmp_path / "missing.json"

        report = doctor_report(config)

        assert "[ERR] GitHub is hosted but channels.hosted.gateway_jwks_file: cannot read" in report

    def test_a_switched_off_hosted_source_says_so(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, enabled=False))

        assert "[--] GitHub is hosted but switched off (sync.github.enabled is false)" in report

    @pytest.mark.parametrize(("slack", "github", "warned"), [
        ({"mode": "hosted", "enabled": False}, {}, True),
        ({"mode": "hosted"}, {}, False),
        ({"mode": "socket", "enabled": False}, {}, False),
        ({"mode": "hosted", "enabled": False}, {"enabled": False}, False),
        ({"mode": "hosted", "enabled": False}, {"mode": "poll"}, False),
    ])
    def test_a_switched_off_hosted_slack_is_said_to_hold_github_back(self, tmp_path, slack, github, warned):
        config = doctor_config(tmp_path, **github)
        config.slack = SlackConfig.from_dict(slack)

        report = doctor_report(config)

        assert ("[WARN] Slack is hosted but switched off (slack.enabled is false)" in report) is warned

    def test_jobs_on_the_pull_request_gate_are_named(self, tmp_path):
        config = doctor_config(tmp_path)
        config.cron.jobs_file = tmp_path / "cron" / "jobs.yaml"
        config.cron.system_file = tmp_path / "cron" / "system.yaml"
        write_jobs(config.cron.jobs_file, [
            {"id": "pr-monitor", "schedule": "30m", "prompt": "PRs",
             "run_if": [{"type": "github_pr_activity", "author": "example-agent"}]},
            {"id": "inbox", "schedule": "30m", "prompt": "Inbox",
             "run_if": [{"type": "messages", "sources": ["github"]}]},
        ])

        report = doctor_report(config)

        assert "[WARN] Cron job(s) pr-monitor use the github_pr_activity gate" in report
        assert "inbox use the" not in report

    @pytest.mark.parametrize(("mode", "expected"), [
        ("hosted", "[--] gh CLI not found (not needed: GitHub comes from the channel gateway)"),
        ("poll", "[WARN] gh CLI not found (needed for sync)"),
    ])
    def test_gh_is_asked_for_only_when_something_runs_it(self, tmp_path, monkeypatch, mode, expected):
        """The repository watch is hosted with the rest of GitHub."""
        import shutil

        monkeypatch.setattr(shutil, "which", lambda name: None)
        config = doctor_config(tmp_path)
        config.sync.github.mode = mode
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True, repos=["acme/widgets"])

        assert expected in doctor_report(config)

    @pytest.mark.parametrize(("mode", "line"), [
        ("hosted", (
            "[OK] GitHub repository watch (mode hosted, the channel gateway polls): 2 repositories; "
            "types pull_request, release; updates on\n"
        )),
        ("poll", (
            "[OK] GitHub repository watch (mode poll, gh on this machine): 2 repositories; "
            "types pull_request, release; updates on\n"
        )),
    ])
    def test_the_repository_watch_is_reported(self, tmp_path, mode, line):
        config = doctor_config(tmp_path)
        config.sync.github.mode = mode
        config.sync.github_repos = GitHubReposSyncConfig(
            enabled=True, repos=["acme/widgets", "other-org/other-repo"], types=["pull_request", "release"],
            updates=True,
        )

        assert line in doctor_report(config)

    def test_a_repository_watch_that_is_off_or_lists_nothing_says_so(self, tmp_path):
        config = doctor_config(tmp_path)

        assert "[--] GitHub repository watch off (sync.github_repos.enabled is false)\n" in doctor_report(config)
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True)
        assert (
            "[WARN] GitHub repository watch is on but sync.github_repos.repos is empty, so it watches nothing"
        ) in doctor_report(config)
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True, repos=["acme/widgets"])
        assert "1 repository; types pull_request, issue; updates off\n" in doctor_report(config)

    def test_notifications_off_with_a_repository_watch_on_still_subscribe_to_the_watch(self, tmp_path):
        config = doctor_config(tmp_path, enabled=False)
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True, repos=["acme/widgets"])
        config.slack = SlackConfig.from_dict({"mode": "hosted", "enabled": False})

        report = doctor_report(config)

        assert (
            "[--] GitHub is hosted but switched off (sync.github.enabled is false); the gateway is "
            "told that this agent takes no notifications\n"
        ) in report
        assert "[WARN] Slack is hosted but switched off (slack.enabled is false)" in report


@pytest.mark.asyncio
class TestSetupPage:
    async def test_the_gateway_status_is_reported_when_only_github_is_hosted(self, monkeypatch):
        from nerve.gateway.routes.setup import _hosted_gateway_status

        config = NerveConfig()
        assert _hosted_gateway_status(config) is None
        config.sync.github = hosted_github()
        monkeypatch.setattr(gateway_server, "_hosted_channels", None)

        assert _hosted_gateway_status(config) == {"streams": 0, "connected": False, "last_event_at": None}

    async def test_the_sources_preflight_does_not_run_gh_in_hosted_mode(self, monkeypatch):
        import subprocess

        from nerve import setup_checks

        setup_checks.reset_caches()

        def no_gh(argv, **kwargs):
            raise AssertionError(f"ran {argv}")

        monkeypatch.setattr(subprocess, "run", no_gh)
        monkeypatch.setattr(setup_checks.shutil, "which", lambda name: None)
        config = NerveConfig()
        config.sync.github = hosted_github()
        try:
            result = await setup_checks.sources_preflight(config)
        finally:
            setup_checks.reset_caches()

        assert result["github"] == {
            "cli": False, "authenticated": False, "login": None,
            "hint": setup_checks.GH_HOSTED_HINT, "hosted": True,
        }
        # The gateway polls for the agent only while a credential is connected to it.
        assert "once a GitHub credential is connected to it" in result["github"]["hint"]

    async def test_the_environment_check_does_not_ask_for_gh_in_hosted_mode(self, monkeypatch):
        from nerve import setup_checks

        setup_checks.reset_caches()
        monkeypatch.setattr(setup_checks, "_version_of", lambda argv: None)
        config = NerveConfig()
        config.sync.github = hosted_github()
        config.sync.github_repos = GitHubReposSyncConfig(enabled=True, repos=["acme/widgets"])
        try:
            checks = {check["id"]: check for check in await setup_checks.environment(config)}
            config.sync.github.mode = "poll"
            setup_checks.reset_caches()
            polling = {check["id"]: check for check in await setup_checks.environment(config)}
        finally:
            setup_checks.reset_caches()

        assert (checks["gh"]["status"], checks["gh"]["detail"]) == ("ok", "not needed")
        assert polling["gh"]["status"] == "missing"
