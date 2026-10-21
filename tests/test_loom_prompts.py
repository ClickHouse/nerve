"""What agents are told when memory is Loom, and that memU's wording stays.

Every place that names Nerve's memory tools has a Loom branch
(``nerve/memory/loom_prompts.py``): the system prompt, the worker onboarding
step, the Codex backend note, the bundles written for external agents, the
workspace templates and bundled skills, and the built-in cron jobs. Under
memU each of them renders as before (the system prompt is checked against a
frozen copy of main's rendering); the worker guide the onboarding prompt
carries, and the ``nerve-dev`` skill's source map, now mention Loom.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

from nerve.agent.backends.base import config_excluded_tools
from nerve.agent.backends.codex.backend import _BACKEND_NOTES, _backend_notes
from nerve.agent.prompts import (
    _ONBOARDING_STEPS,
    build_system_prompt,
    onboarding_prompt,
    worker_onboarding_prompt,
)
from nerve.config import MemoryConfig, NerveConfig
from nerve.external_agents.registry import FileTarget
from nerve.external_agents.renderers import ClaudeCodeRenderer, CodexGlobalRenderer
from nerve.memory import loom_prompts
from nerve.setup_writer import SetupChoices, apply_mode, build_cron_jobs
from nerve.workspace import initialize_workspace, install_bundled_skills, read_manifest

TEMPLATES = Path(__file__).resolve().parents[1] / "nerve" / "templates"
# Nerve's memory tools, as a workspace file would name them.
MEMU_TOOLS = re.compile(
    r"`(memorize|memory_recall|memory_expand_category|session_context|conversation_history"
    r"|memory_records_by_date|memory_update|memory_delete|category_update)`"
    r"|\b(memorize|memory_recall|memory_records_by_date|memory_update|conversation_history)\("
)


def _loom_config() -> NerveConfig:
    config = NerveConfig()
    config.memory = MemoryConfig(backend="loom")
    return config


# --------------------------------------------------------------------------- #
# The system prompt and the onboarding steps
# --------------------------------------------------------------------------- #


# main's memU system prompt for a workspace holding only `SOUL.md` ("Be kind.")
# and one tool offered, frozen before the Loom backend: the date, the
# workspace path and the tool's description are normalized away.
MAIN_STATIC_PROMPT = (
    "Be kind.\n\n---\n\n# Session Context\n- **Source:** web\n- **Current date:** <date>\n"
    "- **Workspace:** <workspace>\n\nPer-session details — this session's id and the memories "
    "recalled at session start — are in the `<session-context>` block at the top of this "
    "conversation's first user message. `mcp__nerve__session_context` returns them live if "
    "that message is no longer in context.\n\nYou have access to the following custom tools:\n"
    "- <tool>"
)
MAIN_LEGACY_PROMPT = (
    "Be kind.\n\n---\n\n# Session Context\n- **Session ID:** s1\n- **Source:** web\n"
    "- **Current date:** <date>\n- **Workspace:** <workspace>\n\nYou have access to the "
    "following custom tools:\n- <tool>\n\n---\n\n# Recalled Memories\n\n- a fact"
)


def _normalized(prompt: str, workspace: Path) -> str:
    prompt = re.sub(r"\*\*Current date:\*\* .*", "**Current date:** <date>", prompt)
    prompt = re.sub(r"(?m)^- `mcp__nerve__\w+` — .*$", "- <tool>", prompt)
    return prompt.replace(str(workspace), "<workspace>")


def test_the_memu_prompt_is_mains(tmp_path: Path) -> None:
    """The memU prompt, in both shapes, is the one main rendered."""
    from nerve.agent.prompts import _get_prompt_tool_registry

    (tmp_path / "SOUL.md").write_text("Be kind.\n", encoding="utf-8")
    others = {spec.name for spec in _get_prompt_tool_registry().list(include_hoa=True)}
    others.discard("session_context")
    for static, expected in ((True, MAIN_STATIC_PROMPT), (False, MAIN_LEGACY_PROMPT)):
        prompt = build_system_prompt(
            tmp_path, "s1", "web", ["a fact"], excluded_tools=others, static=static,
        )
        assert _normalized(prompt, tmp_path) == expected
        assert prompt == build_system_prompt(
            tmp_path, "s1", "web", ["a fact"], excluded_tools=others, static=static,
            memory_backend="memu", memory_tools=False,
        )


def test_the_loom_prompt_names_loom_tools_and_offers_no_memu_tool(tmp_path: Path) -> None:
    excluded = config_excluded_tools(_loom_config())
    prompt = build_system_prompt(tmp_path, excluded_tools=excluded, memory_backend="loom")
    assert loom_prompts.SYSTEM_PROMPT_SECTION in prompt
    tool_list = prompt.split("You have access to the following custom tools:", 1)[1].split("# Memory: Loom")[0]
    assert "mcp__nerve__memorize" not in tool_list and "mcp__nerve__memory_recall" not in tool_list
    assert "mcp__nerve__session_context" not in prompt
    # The section is static per configuration: the prompt cache keeps working.
    assert prompt == build_system_prompt(tmp_path, excluded_tools=excluded, memory_backend="loom")


def test_the_loom_section_covers_levels_proposals_and_verdicts() -> None:
    section = loom_prompts.SYSTEM_PROMPT_SECTION
    for words in (
        "home namespace", "Team and", "read-only for you", "propose instead",
        "`proposed: true`", "conflict_verdict", "PINNED_CONFLICT", "`abstain: true`",
        "mcp__loom__<tool>", "session id: find them with `loom_search`",
    ):
        assert words in section, words


def test_without_loom_tools_the_prompt_says_memory_cannot_be_used(tmp_path: Path) -> None:
    """Loom is the backend but the session gets no `loom` server (local
    mode, or not provisioned yet): no tool guidance for tools it lacks."""
    excluded = config_excluded_tools(_loom_config())
    prompt = build_system_prompt(
        tmp_path, excluded_tools=excluded, memory_backend="loom", memory_tools=False,
    )
    assert loom_prompts.NO_TOOLS_SECTION in prompt
    assert loom_prompts.SYSTEM_PROMPT_SECTION not in prompt and "loom_search" not in prompt


def test_onboarding_saves_to_loom_under_loom() -> None:
    memu = worker_onboarding_prompt("Watch the build.", "guide")
    assert memu == worker_onboarding_prompt("Watch the build.", "guide", "memu")
    assert _ONBOARDING_STEPS in memu
    loom = worker_onboarding_prompt("Watch the build.", "guide", "loom")
    assert "`loom_memory_set`" in loom and "- what you saved to Loom." in loom
    assert "`memorize`" not in loom and "what you memorized" not in loom


def test_the_onboarding_conversation_saves_to_loom_under_loom() -> None:
    for mode in ("personal", "worker"):
        memu = onboarding_prompt(mode, "Watch the build.", "guide")
        assert memu == onboarding_prompt(mode, "Watch the build.", "guide", memory_backend="memu")
        loom = onboarding_prompt(mode, "Watch the build.", "guide", memory_backend="loom")
        for old, new in loom_prompts.CONVERSATION_EDITS:
            assert old in memu and new in loom, (mode, old[:60])
        assert not MEMU_TOOLS.search(loom), (mode, MEMU_TOOLS.search(loom))


def test_the_codex_note_names_loom_tools_under_loom() -> None:
    assert _backend_notes("memu") == _BACKEND_NOTES
    loom = _backend_notes("loom")
    assert "memorize" not in loom and "memory_recall" not in loom
    assert "`loom` MCP server" in loom
    without = _backend_notes("loom", memory_tools=False)
    assert "memorize" not in without and "loom" not in without
    assert "Nerve's tools (task_*" in without


def test_the_codex_note_follows_the_catalog(tmp_path: Path) -> None:
    """Loom's tools in the note only for a client that gets the `loom`
    server from the gateway's catalog (external mode)."""
    from types import SimpleNamespace

    from nerve.agent.backends.base import SessionSpec
    from nerve.agent.backends.codex.backend import CodexBackend
    from nerve.config import McpServerConfig

    config = NerveConfig.from_dict({"workspace": str(tmp_path), "memory": {"backend": "loom"}})
    servers: list = []
    deps = SimpleNamespace(config=lambda: config, managed_mcp=True, external_mcp_servers=lambda: servers)
    spec = SessionSpec(
        session_id="s1", source="web", model=None, effort="", system_prompt="PROMPT", cwd=str(tmp_path),
    )
    backend = CodexBackend(deps)
    assert "`loom` MCP server" not in backend.thread_params(spec)["developerInstructions"]
    servers.append(McpServerConfig(name="loom", type="http", url="http://192.0.2.1:3130/s/loom/mcp"))
    assert "`loom` MCP server" in backend.thread_params(spec)["developerInstructions"]
    deps.managed_mcp = False  # local mode: no catalog reaches the client
    assert "`loom` MCP server" not in backend.thread_params(spec)["developerInstructions"]


# --------------------------------------------------------------------------- #
# External agents
# --------------------------------------------------------------------------- #


def test_external_agents_are_not_sent_to_memu_tools_under_loom(tmp_path: Path) -> None:
    (tmp_path / "SOUL.md").write_text("Be kind.\n", encoding="utf-8")
    target = FileTarget(output=tmp_path / "out.md", includes=["SOUL.md"], style="codex-global")
    for renderer in (CodexGlobalRenderer(), ClaudeCodeRenderer()):
        memu = renderer.render(target, workspace=tmp_path)
        assert memu == renderer.render(target, workspace=tmp_path, memory_backend="memu")
        assert "session_context(" in memu
        loom = renderer.render(target, workspace=tmp_path, memory_backend="loom")
        assert loom_prompts.EXTERNAL_AGENT_MEMORY in loom and "Be kind." in loom
        # Beyond the note that says they are not served, none is named.
        rest = loom.replace(loom_prompts.EXTERNAL_AGENT_MEMORY, "")
        assert not MEMU_TOOLS.search(rest), MEMU_TOOLS.search(rest)


# --------------------------------------------------------------------------- #
# Workspace templates and bundled skills
# --------------------------------------------------------------------------- #


def test_each_memu_wording_occurs_once_in_its_template() -> None:
    for relative, edits in loom_prompts.TEMPLATE_EDITS.items():
        text = (TEMPLATES / relative).read_text(encoding="utf-8")
        for old, _new in edits:
            assert text.count(old) == 1, (relative, old[:60])


def test_a_memu_workspace_gets_the_templates_unchanged(tmp_path: Path) -> None:
    for mode in ("personal", "worker"):
        ws = tmp_path / mode
        initialize_workspace(ws, mode, skip=("TASK.md",))
        for filename, _ in read_manifest(mode):
            if filename != "TASK.md":
                assert (ws / filename).read_bytes() == (TEMPLATES / mode / filename).read_bytes()


def test_a_loom_workspace_names_no_memu_tool(tmp_path: Path) -> None:
    for mode in ("personal", "worker"):
        ws = tmp_path / mode
        created = initialize_workspace(ws, mode, skip=("TASK.md",), memory_backend="loom")
        assert {"AGENTS.md", "TOOLS.md", "MEMORY.md"} <= set(created)
        for filename in ("AGENTS.md", "TOOLS.md", "MEMORY.md"):
            text = (ws / filename).read_text(encoding="utf-8")
            assert not MEMU_TOOLS.search(text), (mode, filename, MEMU_TOOLS.search(text))
            assert "loom_" in text, (mode, filename)
        # Files that name no memory tool are copied as they are.
        assert (ws / "SOUL.md").read_bytes() == (TEMPLATES / mode / "SOUL.md").read_bytes()


def test_bundled_skills_name_loom_tools_under_loom(tmp_path: Path) -> None:
    install_bundled_skills(tmp_path / "memu")
    install_bundled_skills(tmp_path / "loom", memory_backend="loom")
    memu = (tmp_path / "memu" / "skills" / "nerve-dev" / "SKILL.md").read_text(encoding="utf-8")
    loom = (tmp_path / "loom" / "skills" / "nerve-dev" / "SKILL.md").read_text(encoding="utf-8")
    assert memu == (TEMPLATES / "skills" / "nerve-dev" / "SKILL.md").read_text(encoding="utf-8")
    assert "`memory_recall`" in memu and "`memory_recall`" not in loom and "`loom_search`" in loom


def test_a_wording_that_moved_is_left_and_logged(caplog) -> None:
    edits = (("absent words", "new words"), ("present", "replaced"))
    with caplog.at_level(logging.WARNING, logger="nerve.memory.loom_prompts"):
        text = loom_prompts.apply_edits("present text", edits, name="a template")
    assert text == "replaced text"
    assert "a template" in caplog.text


# --------------------------------------------------------------------------- #
# Built-in cron jobs and the setup Mode step
# --------------------------------------------------------------------------- #


def _jobs(memory_backend: str) -> dict[str, dict]:
    choices = SetupChoices(
        mode="personal", enabled_crons=["inbox-processor"], memory_backend=memory_backend,
    )
    return {job["id"]: job for job in build_cron_jobs(choices)}


def test_the_built_in_jobs_name_loom_tools_under_loom() -> None:
    memu, loom = _jobs("memu"), _jobs("loom")
    assert "memory_records_by_date" in memu["memory-maintenance"]["prompt"]
    assert loom["memory-maintenance"]["prompt"] == loom_prompts.MAINTENANCE_PROMPT
    assert loom["memory-maintenance"]["description"] == loom_prompts.MAINTENANCE_DESCRIPTION
    assert "via memorize" in memu["inbox-processor"]["prompt"]
    assert "via memorize" not in loom["inbox-processor"]["prompt"]
    assert "loom_memory_set" in loom["inbox-processor"]["prompt"]
    for job_id in memu:
        if job_id not in ("memory-maintenance", "inbox-processor"):
            assert memu[job_id] == loom[job_id]


def test_the_mode_step_writes_loom_wording_under_loom(tmp_path: Path) -> None:
    apply_mode(tmp_path / "ws", "personal", memory_backend="loom")
    ws = tmp_path / "ws"
    assert "`loom_memory_set`" in (ws / "AGENTS.md").read_text(encoding="utf-8")
    assert "`loom_search`" in (ws / "skills" / "nerve-dev" / "SKILL.md").read_text(encoding="utf-8")
    jobs = yaml.safe_load((ws / "config" / "cron" / "system.yaml").read_text(encoding="utf-8"))["jobs"]
    maintenance = next(job for job in jobs if job["id"] == "memory-maintenance")
    assert maintenance["prompt"] == loom_prompts.MAINTENANCE_PROMPT


def test_a_system_yaml_from_before_the_switch_runs_in_loom_wording() -> None:
    """memory-maintenance written for memU would prune memories through
    Loom's tools: under Loom, the shipped memU prompt runs as its Loom
    wording; an edited one is left alone."""
    from nerve.cron.jobs import CronJob
    from nerve.cron.service import _reword_for_loom
    from nerve.setup_writer import loom_wording_of_shipped_job

    memu = _jobs("memu")
    jobs = [
        CronJob(id=job_id, schedule=job["schedule"], prompt=job["prompt"], description=job["description"])
        for job_id, job in memu.items()
    ]
    jobs.append(CronJob(id="memory-maintenance-edited", schedule="0 5 * * *", prompt="Tidy up."))
    edited = CronJob(id="memory-maintenance", schedule="0 5 * * *", prompt="My own tidy-up.")
    _reword_for_loom([*jobs, edited])
    by_id = {job.id: job for job in jobs}
    assert by_id["memory-maintenance"].prompt == loom_prompts.MAINTENANCE_PROMPT
    assert by_id["memory-maintenance"].description == loom_prompts.MAINTENANCE_DESCRIPTION
    assert "loom_memory_set" in by_id["inbox-processor"].prompt
    assert by_id["task-planner"].prompt == memu["task-planner"]["prompt"]
    assert edited.prompt == "My own tidy-up."
    assert loom_wording_of_shipped_job("memory-maintenance", "Tidy up.", "") is None


def test_an_install_with_every_job_in_jobs_yaml_is_reworded_too(tmp_path: Path) -> None:
    """An old install keeps its built-in jobs in jobs.yaml, with no
    system.yaml: those are reworded for Loom as well."""
    from types import SimpleNamespace

    from nerve.cron.service import CronService

    jobs = [job for job in _jobs("memu").values() if job["id"] == "memory-maintenance"]
    (tmp_path / "jobs.yaml").write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")
    service = SimpleNamespace(
        config=SimpleNamespace(
            cron=SimpleNamespace(system_file=tmp_path / "system.yaml", jobs_file=tmp_path / "jobs.yaml"),
            memory=SimpleNamespace(backend="loom"),
        ),
        _rejected_job_ids=[],
    )
    (loaded,) = CronService._load_merged_jobs(service)
    assert loaded.prompt == loom_prompts.MAINTENANCE_PROMPT


def test_nerve_init_words_the_workspace_for_the_configured_backend(tmp_path: Path) -> None:
    from nerve.setup_writer import existing_memory_backend

    config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
    assert existing_memory_backend(config_dir, workspace) == "memu"
    (workspace / "config").mkdir(parents=True)
    (workspace / "config" / "settings.yaml").write_text("memory:\n  backend: Loom\n", encoding="utf-8")
    assert existing_memory_backend(config_dir, workspace) == "loom"  # written before nerve init
    config_dir.mkdir()
    (config_dir / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
    assert existing_memory_backend(config_dir, None) == "loom"  # through the configuration
    (workspace / "config" / "settings.yaml").write_text("memory:\n  backend: memu\n", encoding="utf-8")
    assert existing_memory_backend(config_dir, workspace) == "memu"
