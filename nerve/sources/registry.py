"""Source registry — builds SourceRunner instances from config.

Called by CronService.start() to get the list of runners to register
as APScheduler jobs. Centralizes all source construction and config extraction.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nerve.sources.runner import SourceRunner

if TYPE_CHECKING:
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)


def build_source_runners(
    config: NerveConfig,
    db: Database,
) -> list[SourceRunner]:
    """Build SourceRunner instances for all enabled sources.

    Runners are pure ingestors — they fetch, preprocess, condense, and persist.
    No agent processing. Consumption is handled separately by consumer tools.

    Returns:
        List of SourceRunner objects ready to be registered as cron jobs.
    """
    runners: list[SourceRunner] = []
    ttl_days = config.sync.message_ttl_days

    # Build condense config from API credentials
    condense_cfg: dict[str, Any] | None = None
    if config.effective_api_key and config.memory.fast_model:
        condense_cfg = {
            "api_key": config.effective_api_key,
            "model": config.memory.fast_model,
            "base_url": config.anthropic_api_base_url,
            "use_proxy": config.proxy.enabled,
        }

    # Telegram
    tg = config.sync.telegram
    if tg.enabled and tg.api_id:
        from nerve.sources.telegram import TelegramSource

        source = TelegramSource(config={
            "api_id": tg.api_id,
            "api_hash": tg.api_hash,
            "monitored_folders": tg.monitored_folders,
            "exclude_chats": getattr(tg, "exclude_chats", []),
        })
        runners.append(SourceRunner(
            source=source,
            db=db,
            batch_size=tg.batch_size,
            condense=tg.condense,
            condense_config=condense_cfg,
            ttl_days=ttl_days,
        ))
        logger.info("Registered source: telegram (batch=%d)", tg.batch_size)

    # Gmail — one source per account, each with independent cursor
    gmail = config.sync.gmail
    if gmail.enabled and gmail.accounts:
        from nerve.sources.gmail import GmailSource

        for account in gmail.accounts:
            source = GmailSource(account=account, config={
                "keyring_password": gmail.keyring_password,
            })
            gmail_condense_cfg = condense_cfg
            if condense_cfg and gmail.condense_prompt:
                gmail_condense_cfg = {**condense_cfg, "prompt": gmail.condense_prompt}
            runners.append(SourceRunner(
                source=source,
                db=db,
                batch_size=gmail.batch_size,
                condense=gmail.condense,
                condense_config=gmail_condense_cfg,
                ttl_days=ttl_days,
            ))
            logger.info("Registered source: %s (batch=%d)", source.source_name, gmail.batch_size)

    # GitHub (notifications)
    gh = config.sync.github
    if gh.enabled:
        from nerve.sources.github import GitHubSource

        source = GitHubSource()
        runners.append(SourceRunner(
            source=source,
            db=db,
            batch_size=gh.batch_size,
            condense=gh.condense,
            condense_config=condense_cfg,
            ttl_days=ttl_days,
        ))
        logger.info("Registered source: github (batch=%d)", gh.batch_size)

    # GitHub Events (user's own activity)
    gh_events = config.sync.github_events
    if gh_events.enabled:
        from nerve.sources.github_events import GitHubEventsSource

        source = GitHubEventsSource(config={
            "repos": gh_events.repos,
            "username": gh_events.username,
        })
        runners.append(SourceRunner(
            source=source,
            db=db,
            batch_size=gh_events.batch_size,
            condense=gh_events.condense,
            condense_config=condense_cfg,
            ttl_days=ttl_days,
        ))
        logger.info("Registered source: github_events (batch=%d, repos=%s)", gh_events.batch_size, gh_events.repos or "all")

    return runners
