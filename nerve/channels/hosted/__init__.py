"""Hosted channels: provider traffic that a shared gateway holds for Nerve.

In hosted mode Nerve has no provider tokens. Gateway replicas open
WebSocket streams to ``/_internal/channel/v1/stream``, authenticated with a
control plane workload identity token, and Nerve pulls admitted events from
the gateway's inbox over them.
"""
