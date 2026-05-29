"""JSON Schemas for every registered tool — module-level constants.

Hoisting these out of decorator arguments has two benefits:
  1. They aren't reallocated on every import / module reload.
  2. Adapters can introspect them without instantiating handlers.

Schemas use the explicit JSON Schema form (``{"type": "object", ...}``) so
the Claude Agent SDK passes them through unchanged. The shorthand form
(bare ``{field: {type: ...}}``) is converted to the explicit form by
:func:`nerve.agent.tools.claude_sdk_adapter._shim_schema` at registration
time — see that function for the historical rationale.
"""

from __future__ import annotations


# ----- Task tools -----

TASK_SEARCH_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": "Search keyword(s), partial words, or task ID/slug to match against title, content, tags, and task ID",
        },
        "status": {
            "type": "string",
            "description": "Filter: 'all' (include done), specific status, or empty (open tasks only)",
            "default": "",
        },
        "tag": {
            "type": "string",
            "description": "Filter by tag name (exact match)",
            "default": "",
        },
    },
    "required": ["query"],
}

TASK_CREATE_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string", "description": "Task title"},
        "content": {"type": "string", "description": "Task details and context"},
        "source": {
            "type": "string",
            "description": "Where this task came from (telegram, github, gmail, manual)",
            "default": "manual",
        },
        "source_url": {
            "type": "string",
            "description": "URL to the source (PR, email, etc.)",
            "default": "",
        },
        "deadline": {
            "type": "string",
            "description": "Deadline in YYYY-MM-DD format",
            "default": "",
        },
        "status": {
            "type": "string",
            "description": "Initial status. Must be one of the configured task statuses (see task_status_list). Defaults to 'pending' when omitted.",
            "default": "",
        },
        "tags": {
            "type": "string",
            "description": "Comma-separated tags (e.g. 'urgent,backend,bug')",
            "default": "",
        },
        "confirm_duplicate": {
            "type": "boolean",
            "description": "Set to true to force creation even when duplicates exist",
            "default": False,
        },
    },
    "required": ["title", "content"],
}

TASK_LIST_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "description": "Filter by a configured status name (see task_status_list), 'open'/'' for all non-done, or 'all' for everything. Default (empty) = all non-done.",
            "default": "",
        },
        "tag": {
            "type": "string",
            "description": "Filter by tag name (exact match)",
            "default": "",
        },
        "limit": {
            "type": "number",
            "description": "Max results (default 100)",
            "default": 100,
        },
    },
    "required": [],
}

TASK_UPDATE_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string", "description": "Task ID"},
        "status": {
            "type": "string",
            "description": "New status. Must be one of the configured task statuses (see task_status_list). Invalid values are rejected with the list of valid options.",
            "default": "",
        },
        "note": {
            "type": "string",
            "description": "Update note to append to the task file",
            "default": "",
        },
        "deadline": {
            "type": "string",
            "description": "New deadline in YYYY-MM-DD format",
            "default": "",
        },
        "tags": {
            "type": "string",
            "description": "Replace tags (comma-separated). Use '+tag' to add, '-tag' to remove, or 'tag1,tag2' to set.",
            "default": "",
        },
        "title": {
            "type": "string",
            "description": "New task title. Updates the H1 heading in the markdown file and the SQLite index.",
            "default": "",
        },
    },
    "required": ["task_id"],
}

TASK_READ_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string", "description": "Task ID"},
    },
    "required": ["task_id"],
}

TASK_WRITE_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string", "description": "Task ID"},
        "content": {
            "type": "string",
            "description": "Full markdown content to write to the task file",
        },
    },
    "required": ["task_id", "content"],
}

TASK_DONE_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string", "description": "Task ID"},
        "note": {
            "type": "string",
            "description": "Completion note",
            "default": "",
        },
    },
    "required": ["task_id"],
}

TASK_STATUS_LIST_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

TASK_STATUS_CREATE_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": "Status identifier — lowercase letters, digits, and underscores (e.g. 'blocked', 'in_review').",
        },
        "label": {
            "type": "string",
            "description": "Human-readable label shown in the UI (e.g. 'In Review'). Defaults to a title-cased version of name.",
            "default": "",
        },
        "color": {
            "type": "string",
            "description": "Hex color like '#3b82f6'. A random color is chosen if omitted.",
            "default": "",
        },
        "description": {
            "type": "string",
            "description": "Optional explanation of what this status means.",
            "default": "",
        },
    },
    "required": ["name"],
}

# ----- Memory tools -----

MEMORY_RECALL_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string", "description": "What to search for in memory"},
        "limit": {"type": "number", "description": "Max results", "default": 10},
    },
    "required": ["query"],
}

SESSION_CONTEXT_SCHEMA = {
    "type": "object",
    "properties": {
        "topic": {
            "type": "string",
            "description": (
                "Short description of what you're about to work on. "
                "Used to bias the memU recall query so the priors you "
                "get back are actually relevant to the task."
            ),
        },
        "include_skills": {
            "type": "boolean",
            "description": "Include a summary of currently active skills.",
            "default": True,
        },
        "memory_limit": {
            "type": "number",
            "description": "Max number of recalled memories to return.",
            "default": 15,
        },
    },
    "required": ["topic"],
}

CONVERSATION_HISTORY_SCHEMA = {
    "type": "object",
    "properties": {
        "date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
        "end_date": {
            "type": "string",
            "description": "Optional end date for range (YYYY-MM-DD)",
            "default": "",
        },
        "limit": {"type": "number", "description": "Max results", "default": 30},
    },
    "required": ["date"],
}

MEMORY_RECORDS_BY_DATE_SCHEMA = {
    "type": "object",
    "properties": {
        "date": {
            "type": "string",
            "description": "Date in YYYY-MM-DD format. Returns records created/updated on this date.",
        },
        "end_date": {
            "type": "string",
            "description": "Optional end date for range (YYYY-MM-DD). Defaults to same as date.",
            "default": "",
        },
        "limit": {"type": "number", "description": "Max results (default 100)", "default": 100},
        "updated": {
            "type": "boolean",
            "description": "If true, also include records updated (not just created) in the date range. Default: false.",
            "default": False,
        },
    },
    "required": ["date"],
}

MEMORIZE_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {
            "type": "string",
            "description": "The fact or information to remember",
        },
        "memory_type": {
            "type": "string",
            "description": "profile (stable personal facts), event (specific occurrences with a date), knowledge (objective factual info), behavior (recurring patterns/routines). Default: knowledge",
            "default": "knowledge",
        },
    },
    "required": ["content"],
}

MEMORY_UPDATE_SCHEMA = {
    "type": "object",
    "properties": {
        "memory_id": {
            "type": "string",
            "description": "ID of the memory item to update",
        },
        "content": {
            "type": "string",
            "description": "New content for the memory",
            "default": "",
        },
        "memory_type": {
            "type": "string",
            "description": "profile (stable personal facts), event (specific occurrences with a date), knowledge (objective factual info), behavior (recurring patterns/routines)",
            "default": "",
        },
        "categories": {
            "type": "string",
            "description": "Comma-separated category names to reassign to (e.g. 'work,personal')",
            "default": "",
        },
    },
    "required": ["memory_id"],
}

MEMORY_DELETE_SCHEMA = {
    "type": "object",
    "properties": {
        "memory_id": {
            "type": "string",
            "description": "ID of the memory item to delete",
        },
    },
    "required": ["memory_id"],
}

CATEGORY_UPDATE_SCHEMA = {
    "type": "object",
    "properties": {
        "category_id": {
            "type": "string",
            "description": "ID of the category (without 'cat:' prefix)",
        },
        "summary": {
            "type": "string",
            "description": "New summary text for the category",
            "default": "",
        },
        "description": {
            "type": "string",
            "description": "New description for the category",
            "default": "",
        },
    },
    "required": ["category_id"],
}

# ----- Source / sync tools -----

SYNC_STATUS_SCHEMA = {
    "type": "object",
    "properties": {
        "source": {
            "type": "string",
            "description": "Specific source to check, or 'all'",
            "default": "all",
        },
    },
    "required": [],
}

LIST_SOURCES_SCHEMA = {
    "type": "object",
    "properties": {
        "consumer": {
            "type": "string",
            "description": "Show unread counts for this consumer name",
            "default": "",
        },
    },
    "required": [],
}

POLL_SOURCE_SCHEMA = {
    "type": "object",
    "properties": {
        "source": {
            "type": "string",
            "description": "Source name (e.g., 'github', 'gmail:user@example.com')",
        },
        "consumer": {
            "type": "string",
            "description": "Consumer name for persistent cursor (e.g., 'inbox')",
        },
        "limit": {
            "type": "number",
            "description": "Max messages to return",
            "default": 50,
        },
    },
    "required": ["source", "consumer"],
}

POLL_ALL_SOURCES_SCHEMA = {
    "type": "object",
    "properties": {
        "consumer": {
            "type": "string",
            "description": "Consumer name for persistent cursor (e.g., 'inbox')",
        },
        "limit": {
            "type": "number",
            "description": "Max messages per source",
            "default": 50,
        },
    },
    "required": ["consumer"],
}

READ_SOURCE_SCHEMA = {
    "type": "object",
    "properties": {
        "source": {
            "type": "string",
            "description": "Source name (e.g., 'github', 'gmail:user@example.com')",
        },
        "limit": {
            "type": "number",
            "description": "Max messages to return",
            "default": 20,
        },
        "before_seq": {
            "type": "number",
            "description": "Return messages before this seq (paginate backwards)",
            "default": 0,
        },
        "after_seq": {
            "type": "number",
            "description": "Return messages after this seq (paginate forwards)",
            "default": 0,
        },
    },
    "required": ["source"],
}

# ----- Plan tools -----

PLAN_PROPOSE_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string", "description": "The task ID to propose a plan for"},
        "content": {"type": "string", "description": "The plan content in markdown format"},
        "plan_type": {
            "type": "string",
            "description": "Plan type: 'generic' (default), 'skill-create', 'skill-update'. Auto-detected from task source if omitted.",
            "default": "",
        },
    },
    "required": ["task_id", "content"],
}

PLAN_UPDATE_SCHEMA = {
    "type": "object",
    "properties": {
        "plan_id": {"type": "string", "description": "The pending plan ID to update"},
        "content": {"type": "string", "description": "The full revised plan content in markdown"},
        "feedback": {
            "type": "string",
            "description": "Optional reason for the revision — stored on the superseded plan",
            "default": "",
        },
    },
    "required": ["plan_id", "content"],
}

PLAN_LIST_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "description": "Filter by status: 'pending', 'approved', 'declined', 'implementing', 'superseded', or empty for pending+implementing",
            "default": "",
        },
    },
    "required": [],
}

PLAN_READ_SCHEMA = {
    "type": "object",
    "properties": {
        "plan_id": {
            "type": "string",
            "description": "The plan ID to read (e.g. plan-abc12345)",
        },
    },
    "required": ["plan_id"],
}

PLAN_APPROVE_SCHEMA = {
    "type": "object",
    "properties": {
        "plan_id": {
            "type": "string",
            "description": "The plan ID to approve (e.g. plan-abc12345)",
        },
    },
    "required": ["plan_id"],
}

PLAN_DECLINE_SCHEMA = {
    "type": "object",
    "properties": {
        "plan_id": {"type": "string", "description": "The plan ID to decline"},
        "feedback": {
            "type": "string",
            "description": "Optional reason for declining",
            "default": "",
        },
    },
    "required": ["plan_id"],
}

PLAN_REVISE_SCHEMA = {
    "type": "object",
    "properties": {
        "plan_id": {
            "type": "string",
            "description": "The plan ID to request revision for",
        },
        "feedback": {
            "type": "string",
            "description": "What should be changed in the plan",
        },
    },
    "required": ["plan_id", "feedback"],
}

# ----- Skill tools -----

SKILL_LIST_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

SKILL_GET_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "description": "Skill ID (directory name)"},
    },
    "required": ["name"],
}

SKILL_READ_REFERENCE_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "description": "Skill ID (directory name)"},
        "path": {
            "type": "string",
            "description": "Relative path within the skill's references/ directory",
        },
    },
    "required": ["name", "path"],
}

SKILL_RUN_SCRIPT_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "description": "Skill ID (directory name)"},
        "path": {
            "type": "string",
            "description": "Relative path within the skill's scripts/ directory",
        },
        "args": {
            "type": "string",
            "description": "Arguments to pass to the script",
            "default": "",
        },
    },
    "required": ["name", "path"],
}

SKILL_CREATE_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": "Human-readable skill name (e.g. 'code-review', 'deploy-app')",
        },
        "description": {
            "type": "string",
            "description": (
                "Third-person description with trigger phrases. Example: "
                "'This skill should be used when the user asks to \"deploy the app\", "
                "\"push to staging\", or \"release a new version\".'"
            ),
        },
        "content": {
            "type": "string",
            "description": "Markdown instructions for the skill body. Write in imperative form. Include steps, commands, gotchas, and examples.",
            "default": "",
        },
    },
    "required": ["name", "description"],
}

SKILL_UPDATE_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "description": "Skill ID (directory name) to update"},
        "content": {
            "type": "string",
            "description": "Full SKILL.md content (frontmatter + body)",
        },
    },
    "required": ["name", "content"],
}

# ----- Notification tools -----

NOTIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {
            "type": "string",
            "description": "Optional short heading. Omit or leave empty for regular notifications.",
            "default": "",
        },
        "body": {
            "type": "string",
            "description": "Notification body with details (markdown supported)",
        },
        "priority": {
            "type": "string",
            "description": "Priority level: 'low', 'normal', 'high', 'urgent'. Default: 'normal'",
            "default": "normal",
        },
    },
    "required": ["body"],
}

ASK_USER_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string", "description": "The question to ask"},
        "body": {
            "type": "string",
            "description": "Additional context for the question (markdown supported)",
            "default": "",
        },
        "options": {
            "type": "string",
            "description": "Predefined answer options (shown as buttons). Comma-separated string or JSON array. Optional — user can always type free text.",
            "default": "",
        },
        "wait": {
            "type": "string",
            "description": "If 'true', block agent execution until user answers. Default: 'false' (async).",
            "default": "false",
        },
        "priority": {
            "type": "string",
            "description": "Priority: 'low', 'normal', 'high', 'urgent'. Default: 'normal'",
            "default": "normal",
        },
    },
    "required": ["title"],
}

PROPOSE_ACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "target_kind": {
            "type": "string",
            "description": (
                "Dispatcher key the user's answer routes through. "
                "Currently supported: 'mechanical-action'."
            ),
        },
        "target_id": {
            "type": "string",
            "description": (
                "Dispatcher-specific identifier the chosen decision acts "
                "on (e.g. a queued mechanical-action proposal id like "
                "'20260519T143906Z-d2e62e')."
            ),
        },
        "title": {
            "type": "string",
            "description": "Short headline shown on the notification card.",
        },
        "body": {
            "type": "string",
            "description": (
                "Markdown body with the justification and any details "
                "the user needs to decide."
            ),
            "default": "",
        },
        "options": {
            "type": "string",
            "description": (
                "JSON array of {label, value} dicts overriding the "
                "dispatcher's default options. Leave empty for the "
                "canonical Approve / Decline / Snooze 24h triplet."
            ),
            "default": "",
        },
        "priority": {
            "type": "string",
            "description": "'low', 'normal', 'high', 'urgent'. Default: 'high'.",
            "default": "high",
        },
        "expires_at": {
            "type": "string",
            "description": (
                "ISO-8601 UTC timestamp the row expires at. Omit to use "
                "the configured default expiry window."
            ),
            "default": "",
        },
    },
    "required": ["target_kind", "target_id", "title"],
}

REACT_SCHEMA = {
    "type": "object",
    "properties": {
        "emoji": {
            "type": "string",
            "description": "Emoji to react with (e.g., '👍', '❤', '🔥', '😂')",
        },
    },
    "required": ["emoji"],
}

SEND_STICKER_SCHEMA = {
    "type": "object",
    "properties": {
        "sticker": {
            "type": "string",
            "description": "Telegram sticker file_id. Included in [Sticker: ..., file_id: ...] when users send stickers.",
        },
    },
    "required": ["sticker"],
}

SEND_FILE_SCHEMA = {
    "type": "object",
    "properties": {
        "file_path": {
            "type": "string",
            "description": "Absolute path to the file to send to the user",
        },
    },
    "required": ["file_path"],
}

# ----- MCP admin tools -----

NERVE_API_SCHEMA = {
    "type": "object",
    "properties": {
        "endpoint": {
            "type": "string",
            "description": "API endpoint path, e.g. 'sessions', 'mcp-servers/nerve', 'plans?status=pending'",
        },
    },
    "required": ["endpoint"],
}

MCP_RELOAD_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

# ----- HouseOfAgents tools -----

HOA_STATUS_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

HOA_LIST_PIPELINES_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

HOA_EXECUTE_SCHEMA = {
    "type": "object",
    "properties": {
        "prompt": {
            "type": "string",
            "description": "The task/prompt for the multi-agent team",
        },
        "mode": {
            "type": "string",
            "description": "Execution mode: 'relay' (sequential handoff), 'swarm' (parallel rounds), or 'pipeline' (DAG)",
            "default": "relay",
        },
        "agents": {
            "type": "string",
            "description": "Comma-separated agent names as configured in houseofagents (e.g. 'Claude,OpenAI'). Leave empty for defaults.",
            "default": "",
        },
        "iterations": {
            "type": "integer",
            "description": "Number of iterations for relay/swarm modes",
            "default": 3,
        },
        "pipeline_id": {
            "type": "string",
            "description": "Pipeline ID to use (for pipeline mode). Use hoa_list_pipelines to see available pipelines.",
            "default": "",
        },
    },
    "required": ["prompt"],
}
