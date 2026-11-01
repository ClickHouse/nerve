"""The hosted email source: mail from the members of the agent.

With ``sync.email.enabled`` in hosted mode (``channels.hosted``), the channel
gateway delivers records of provider ``email`` and record type
``email_message``: the mail that a member of the agent sent to the agent's
address. The gateway sends only mail from members of the agent; the platform
decides that, and Nerve says in its subscriptions only whether it takes
email. The hosted source consumer writes every record to the
``channel_observations`` buffer under ``email`` (see
:mod:`nerve.channels.hosted.sources`), and this source drains that buffer
into the inbox (:mod:`nerve.sources.hosted`). It makes no calls to a mail
server, and there is no polling mode.

The source runs as ``source:email``, with condensing, health checks,
consumer cursors and ``messages`` gates. It has no guardrail and no drop
hook, because the platform admits only member mail. Right before the
runner stores mail, a consumer without a cursor on the source, or with an
expired one, gets a cursor at the newest message (``starts_consumer_cursors``),
so the first mail, and the first mail after a long park, is unread for it.

A record names the sender by principal ID and display name, never by an
email address. Its content is the member's text as written, rendered as
plain text with a short header (sender, subject, date, the number of
attachments). Attachments are listed in the metadata and are never
delivered. The content is untrusted data.

Metadata values arrive as strings. ``attachments`` is a list (one entry per
attachment: ``"<name>; <type>; <bytes> bytes"``) and ``truncated`` becomes a
boolean. The other keys are ``sender_principal_id``, ``sender_name``,
``subject``, ``message_id`` (the ``Message-ID`` header, with angle
brackets), ``in_reply_to`` and ``thread_id`` (the first ``References`` ID,
else ``In-Reply-To``, else ``message_id``). A key whose value does not fit
the contract's limits is left out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nerve.sources.hosted import HostedRecordSource

if TYPE_CHECKING:
    from nerve.db import Database

# The provider, the buffer channel, and the source name.
SOURCE_NAME = "email"
# Metadata keys whose value is a list of strings, and those that are flags.
_LIST_KEYS = frozenset({"attachments"})
_FLAG_KEYS = frozenset({"truncated"})


class HostedEmailSource(HostedRecordSource):
    """The email source over the hosted records buffer of provider ``email``."""

    source_name = SOURCE_NAME
    list_keys = _LIST_KEYS
    flag_keys = _FLAG_KEYS
    # A member who sends a mail expects the agent to read it, also the first
    # one and one that arrives after the machine was parked for days.
    starts_consumer_cursors = True

    def __init__(self, db: Database):
        super().__init__(db, SOURCE_NAME)


__all__ = ["SOURCE_NAME", "HostedEmailSource"]
