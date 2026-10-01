"""``agent.model_effort`` on the real client-build path.

The engine resolves a turn's effort when it builds the session's client:
an explicit per-run override wins, cron/hook turns use ``cron_effort``,
and every other turn uses ``agent.effort`` unless ``agent.model_effort``
has an entry matching the session's model.
"""

from __future__ import annotations

import pytest

from tests.test_static_system_prompt import _engine

AGENT = {"effort": "max", "cron_effort": "medium", "model_effort": {"opus-5-5": "high"}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source, model, override, expected",
    [
        ("web",      "claude-opus-5-5", None,  "high"),
        ("telegram", "claude-opus-5-5", None,  "high"),
        ("web",      "claude-opus-4-8", None,  "max"),
        ("cron",     "claude-opus-5-5", None,  "medium"),
        ("web",      "claude-opus-5-5", "max", "max"),
    ],
)
async def test_spec_effort(tmp_path, db, source, model, override, expected):
    engine, backend = _engine(tmp_path, db, **AGENT)
    await engine.run(
        "s-effort", "hello", source=source, channel=source, model=model,
        effort_override=override, actor=None,
    )
    (spec,) = backend.specs
    assert spec.model == model
    assert spec.effort == expected
