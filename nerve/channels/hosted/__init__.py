"""Hosted channels: provider traffic that a shared gateway holds for Nerve.

In hosted mode Nerve has no provider tokens. Gateway replicas open
WebSocket streams to ``/_internal/channel/v1/stream``, authenticated with a
token that the gateway signs, and Nerve pulls admitted events from the
gateway's inbox over them.

Hosted sources use the same streams: the gateway polls a source (GitHub, with
``sync.github.mode: hosted``) under the filters that Nerve publishes, and
Nerve reads the records like any other event (see :mod:`.sources`).
"""
