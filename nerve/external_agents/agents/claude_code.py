"""Claude Code external agent — writes ``~/.claude/settings.json`` + ``CLAUDE.md``.

Claude Code stores user-level config in JSON. We deep-merge the Nerve
MCP block into the existing settings so the user's model preference,
plugin allowlist, etc. are preserved. The memory bundle in
``CLAUDE.md`` is kept fresh by the periodic sync service.
"""

from __future__ import annotations

from pathlib import Path

from nerve.external_agents.registry import (
    AgentSetupResult,
    ExternalAgent,
    FileTarget,
)
from nerve.external_agents.renderers.claude_code import ClaudeCodeRenderer
from nerve.external_agents.writer import ConfigWriter


class ClaudeCodeAgent(ExternalAgent):
    """Anthropic Claude Code (``claude`` CLI)."""

    name = "claude-code"
    display_name = "Claude Code"
    cli_command = "claude"

    def default_config_paths(self) -> list[Path]:
        return [
            Path("~/.claude/settings.json").expanduser(),
            Path("~/.claude/CLAUDE.md").expanduser(),
        ]

    def default_file_targets(self, workspace: Path) -> list[FileTarget]:
        return [
            FileTarget(
                output=Path("~/.claude/CLAUDE.md").expanduser(),
                includes=[
                    "SOUL.md",
                    "IDENTITY.md",
                    "USER.md",
                    "AGENTS.md",
                    "TOOLS.md",
                    "MEMORY.md",
                ],
                style="claude-code",
            ),
        ]

    async def write_config(
        self,
        *,
        nerve_url: str,
        mcp_token: str,
        workspace: Path,
        writer: ConfigWriter,
    ) -> AgentSetupResult:
        settings_path = Path("~/.claude/settings.json").expanduser()
        backups: list[Path] = []
        warnings: list[str] = []

        nerve_block = {
            "mcpServers": {
                "nerve": {
                    "type": "http",
                    "url": nerve_url,
                    "headers": {"Authorization": f"Bearer {mcp_token}"},
                }
            },
            # Match Nerve's trust model: external agents run with full
            # tool access. Users can tighten this later by editing
            # settings.json directly — we re-merge but never overwrite
            # user keys when the wizard runs in ``merge`` policy.
            "permissions": {"defaultMode": "bypassPermissions"},
        }

        b = writer.merge_json(settings_path, nerve_block)
        if b is not None:
            backups.append(b)

        # Initial CLAUDE.md render
        claude_md_path = Path("~/.claude/CLAUDE.md").expanduser()
        target = self.default_file_targets(workspace)[0]
        bundle = ClaudeCodeRenderer().render(target, workspace=workspace)
        b = writer.write(claude_md_path, bundle)
        if b is not None:
            backups.append(b)

        if not self.smoke_check():
            warnings.append(
                "`claude` CLI not found on PATH — install Claude Code "
                "to actually use this config."
            )

        return AgentSetupResult(
            agent=self.name,
            config_files_written=[settings_path, claude_md_path],
            backups_created=backups,
            token=mcp_token,
            warnings=warnings,
        )
