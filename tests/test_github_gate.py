"""GitHub write gate: shell commands that write to GitHub wait for the person.

Covers the classifier, the "push"/"post" message rule, the approvals the
git/gh wrappers check, the wrappers themselves, the Claude permission paths
(the PreToolUse grant hook and can_use_tool), the hub's pause with its
Telegram relay, the Telegram buttons, and the engine's relay choice.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from nerve.agent import github_gate as gg
from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import ClaudeBackend, ClaudeToolPermissions, github_gate_check
from nerve.agent.interactive import (
    GITHUB_WRITE_KIND,
    file_github_approval,
    GITHUB_WRITE_TOOL,
    UNREACHABLE_MESSAGE,
    InteractiveToolHandler,
    answer_github_approval,
    register_handler,
    unregister_handler,
)


@pytest.fixture(autouse=True)
def _gate_dir(tmp_path, monkeypatch):
    """Approvals go to a temporary directory."""
    monkeypatch.setenv(gg.GATE_DIR_ENV, str(tmp_path / "gate"))
    return tmp_path / "gate"


def kinds(command: str) -> list[str]:
    return [w.kind for w in gg.classify_command(command)]


# --------------------------------------------------------------------- #
#  Classifier                                                            #
# --------------------------------------------------------------------- #

@pytest.mark.parametrize("command, expected", [
    # The push of 2026-11-10 that this gate exists for.
    ("timeout 120 git push fork fork/x:refs/heads/fork/x 2>&1 | tail -4", ["push"]),
    ("cd /x && git -C repo push origin HEAD", ["push"]),
    ("git -c user.name=x push origin main", ["push"]),
    ("cd repo\ngit push origin x", ["push"]),
    ("for b in a b; do git push origin $b; done", ["push"]),
    ("echo $(git push origin x)", ["push"]),
    ("/usr/bin/git push origin x", ["push"]),
    ("bash -c 'git push origin x'", ["push"]),
    ("git push --force origin main", ["destructive"]),
    ("git push --force-with-lease origin main", ["destructive"]),
    ("git push -fu origin x", ["destructive"]),
    ("git push --delete origin old", ["destructive"]),
    ("git push origin :old", ["destructive"]),
    ("git push origin +x:y", ["destructive"]),
    ("git push --dry-run origin x", []),
    ("git status && git log --oneline -3", []),
    ("git log --grep push", []),
    ("git -c user.name=x commit -m 'git push later'", []),
    ("git commit -F - <<'EOF'\nsubject\n\nthen git push and gh pr create\nEOF\ngit log -1", []),
    ("echo 'git push' | grep push", []),
    ("ls /usr/bin/git", []),
    ("gh pr view 123 --json state", []),
    ("gh pr create --draft --title t --body b", ["post"]),
    ("gh pr comment 9 -b hi", ["post"]),
    ("gh -R a/b pr comment 9 -b hi", ["post"]),
    ("sh -lc \"gh pr comment 1 -b x\"", ["post"]),
    ("gh issue edit 9033 --add-label waiting-on-support -R a/b", ["post"]),
    ("gh pr merge 9 --squash", ["destructive"]),
    ("gh issue close 9", ["destructive"]),
    ("gh repo delete a/b --yes", ["destructive"]),
    ("gh repo view a/b", []),
    ("gh repo clone a/b", []),
    ("gh search code foo", []),
    ("gh api repos/a/b/pulls/1", []),
    ("gh api --paginate repos/a/b/pulls/1/comments --jq '.[].body'", []),
    ("gh api -X GET search/issues -f q=x", []),
    ("gh api repos/a/b/issues/1/comments -f body=hi", ["post"]),
    ("gh api -XPOST repos/a/b/issues/1/comments -F body=@f", ["post"]),
    # Unknown API writes fail closed: a close, repository, collaborator or
    # secret change, or a commit through the contents API always asks.
    ("gh api --method=PATCH repos/a/b/issues/1 -f state=closed", ["destructive"]),
    ("gh api -X PATCH repos/a/b -f visibility=public", ["destructive"]),
    ("gh api -X PUT repos/a/b/collaborators/someone", ["destructive"]),
    ("gh api -X PUT repos/a/b/actions/secrets/TOKEN -f encrypted_value=x", ["destructive"]),
    ("gh api -X PUT repos/a/b/contents/README.md -f message=m -f content=eA==", ["destructive"]),
    ("gh api repos/a/b/issues -f title=t", ["post"]),
    ("gh api repos/a/b/pulls -f title=t -f head=x -f base=main", ["post"]),
    ("gh api repos/a/b/pulls/1/reviews -f event=COMMENT -f body=b", ["post"]),
    ("gh api repos/a/b/pulls/1/comments/5/replies -f body=b", ["post"]),
    ("gh api repos/a/b/issues/comments/5/reactions -f content=+1", ["post"]),
    ("gh api -X PATCH repos/a/b/issues/comments/5 -f body=b", ["post"]),
    ("gh api -X POST repos/a/b/actions/runs/123/rerun-failed-jobs", ["rerun"]),
    ("gh api -X POST repos/a/b/actions/runs/123/rerun", ["rerun"]),
    ("gh api -X POST repos/a/b/actions/jobs/9/rerun", ["rerun"]),
    ("gh run rerun 123 --failed", ["rerun"]),
    ("gh run view 123 --log-failed", []),
    ("gh run cancel 123", ["destructive"]),
    ("gh api -X POST repos/a/b/git/commits -f message=m", ["push"]),
    ("gh api -X PATCH repos/a/b/git/refs/heads/x -f sha=abc", ["push"]),
    ("gh api -X PATCH repos/a/b/git/refs/heads/x -f sha=abc -F force=true", ["destructive"]),
    ("gh api -X DELETE repos/a/b/git/refs/heads/x", ["destructive"]),
    ("gh api -X PUT repos/a/b/pulls/1/merge", ["destructive"]),
    ("gh api graphql -f query='query { viewer { login } }'", []),
    ("gh api graphql -f query='mutation { addComment(input:{}) { x } }'", ["post"]),
    # A query in a file passes here; the gh wrapper reads the file.
    ("gh api graphql -F query=@q.graphql -f path=x", []),
    ("gh api graphql --input body.json", []),
    ("gh api graphql -f query='# a mutation would need a card\nquery { viewer { login } }'", []),
    ("gh api graphql -f query='query { search(query:\"mutation\") { issueCount } }'", []),
    ("gh api graphql -F query=@q.graphql -f note=mutation", []),
    ("NERVE_GITHUB_WRITE=1 python3 -I push.py", ["declared"]),
    ("python3 -I push.py --dry-run", []),
    ("git push origin x && gh pr comment 1 -b done", ["push", "post"]),
    ("git push 'unbalanced", ["unparsed"]),
])
def test_classify_command(command, expected):
    assert kinds(command) == expected


def test_classify_names_what_it_found():
    [write] = gg.classify_command("gh pr comment 9050 -b hi")
    assert write.what == "gh pr comment 9050 -b hi"
    assert "push: git push fork main" in gg.describe(gg.classify_command("git push fork main"))


# --------------------------------------------------------------------- #
#  The person's message                                                  #
# --------------------------------------------------------------------- #

@pytest.mark.parametrize("message, verbs", [
    ("push", {"push"}),
    ("Push", {"push"}),
    ("push it", {"push"}),
    ("yes, push it", {"push"}),
    ("go push", {"push"}),
    ("post", {"post"}),
    ("post both", {"post"}),
    ("post and move", {"post"}),
    ("post the reply on #9050", {"post"}),
    ("ok post and push", {"post", "push"}),
    # Not an order to push or post.
    ("when did i ask you to fucking push it", set()),
    ("draft a plan for the parser fix on a separate branch on octo/nerve", set()),
    ("delete it of course", set()),
    ("don't push", set()),
    ("do not post that", set()),
    ("push?", set()),
    ("posted?", set()),
    ("posted", set()),
    ("post it, not the other one", set()),
    ("push " + "x" * 90, set()),
    ("", set()),
    (None, set()),
])
def test_message_verbs(message, verbs):
    assert gg.message_verbs(message) == frozenset(verbs)


def test_a_telegram_reply_counts_only_the_persons_own_text():
    quoted = '[Reply to assistant: "Should I push the branch?\n\nOr post it?"]'
    assert gg.message_verbs(f"{quoted}\n\npush") == {"push"}
    assert gg.message_verbs(f"{quoted}\n\nwhy?") == frozenset()
    # The quote alone says nothing.
    assert gg.message_verbs(quoted) == frozenset()
    forwarded = "[Forwarded from Sante]\n\npost"
    assert gg.message_verbs(forwarded) == {"post"}


@pytest.mark.parametrize("command, verbs, ok", [
    ("git push origin x", {"push"}, True),
    ("git push origin x", {"post"}, False),
    ("gh pr comment 1 -b x", {"post"}, True),
    ("gh pr comment 1 -b x", {"push"}, False),
    ("git push origin x && gh pr comment 1 -b x", {"push"}, False),
    ("git push origin x && gh pr comment 1 -b x", {"push", "post"}, True),
    ("git push --force origin x", {"push"}, False),
    ("gh pr merge 1", {"post", "push"}, False),
    ("NERVE_GITHUB_WRITE=1 python3 push.py", {"push", "post"}, False),
    ("gh run rerun 1", set(), True),
    ("git push origin x", set(), False),
])
def test_runs_without_card(command, verbs, ok):
    assert gg.runs_without_card(gg.classify_command(command), frozenset(verbs)) is ok


# --------------------------------------------------------------------- #
#  Approvals                                                             #
# --------------------------------------------------------------------- #

def test_an_approval_is_valid_until_it_expires(_gate_dir):
    nonce = gg.issue_approval("push: git push")
    assert gg.approval_valid(nonce)
    path = _gate_dir / "approvals" / nonce
    old = path.stat().st_mtime - gg.APPROVAL_TTL_SECONDS - 5
    os.utime(path, (old, old))
    assert not gg.approval_valid(nonce)


@pytest.mark.parametrize("nonce", [None, "", "abc", "../../etc/passwd", "f" * 32])
def test_an_unknown_approval_is_not_valid(nonce):
    assert not gg.approval_valid(nonce)


def test_approved_command_exports_the_nonce():
    assert gg.approved_command("git push", "ab" * 16) == (
        f"export NERVE_GITHUB_APPROVAL={'ab' * 16}; git push"
    )


# --------------------------------------------------------------------- #
#  Wrappers                                                              #
# --------------------------------------------------------------------- #

@pytest.fixture
def wrappers(tmp_path, _gate_dir, monkeypatch):
    """git/gh wrappers in front of fake git/gh that print their arguments."""
    real = tmp_path / "real"
    real.mkdir()
    for tool in ("git", "gh"):
        fake = real / tool
        fake.write_text(f'#!/bin/sh\necho "REAL-{tool} $*"\ncase "$*" in *@-*|*"--input -"*) printf "STDIN:"; cat;; esac\n')
        fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{real}{os.pathsep}{os.environ['PATH']}")
    bin_dir = gg.install_wrappers()
    assert bin_dir == _gate_dir / "bin"

    def run(tool: str, *args: str, approval: str | None = None, stdin: str = "", cwd=None):
        env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
        env.pop(gg.APPROVAL_ENV, None)
        if approval is not None:
            env[gg.APPROVAL_ENV] = approval
        return subprocess.run([tool, *args], env=env, capture_output=True, text=True, timeout=30,
                              input=stdin, cwd=cwd)
    return run


def test_wrappers_pass_reads_through(wrappers):
    assert wrappers("git", "status").stdout.strip() == "REAL-git status"
    assert wrappers("git", "log", "--grep", "push").stdout.strip() == "REAL-git log --grep push"
    assert wrappers("gh", "pr", "view", "1").stdout.strip() == "REAL-gh pr view 1"
    assert wrappers("gh", "api", "repos/a/b/pulls/1").stdout.strip() == "REAL-gh api repos/a/b/pulls/1"


def test_wrappers_refuse_an_unapproved_write(wrappers):
    pushed = wrappers("git", "push", "origin", "x")
    assert pushed.returncode == gg.REFUSED_EXIT
    assert "GitHub write blocked" in pushed.stderr and "REAL-git" not in pushed.stdout
    posted = wrappers("gh", "api", "repos/a/b/issues/1/comments", "-f", "body=hi")
    assert posted.returncode == gg.REFUSED_EXIT
    assert wrappers("gh", "pr", "comment", "1", "-b", "x", approval="f" * 32).returncode == gg.REFUSED_EXIT


def test_wrappers_let_an_approved_write_through(wrappers):
    nonce = gg.issue_approval("push")
    pushed = wrappers("git", "push", "origin", "x", approval=nonce)
    assert pushed.returncode == 0 and pushed.stdout.strip() == "REAL-git push origin x"


QUERY = "query($p: String!) { repository(owner: \"a\", name: \"b\") { object(expression: $p) { oid } } }"
MUTATION = "mutation { addStar(input: {starrableId: \"x\"}) { clientMutationId } }"


def test_wrappers_read_a_graphql_query_from_its_file(wrappers, tmp_path):
    work = tmp_path / "work"
    (work / "q").mkdir(parents=True)
    (work / "q" / "read.graphql").write_text(QUERY)
    (work / "q" / "star.graphql").write_text("# stars a repo\n" + MUTATION)
    read = wrappers("gh", "api", "graphql", "-F", "query=@q/read.graphql", "-f", "p=main", cwd=work)
    assert read.returncode == 0 and read.stdout.startswith("REAL-gh api graphql")
    star = wrappers("gh", "api", "graphql", "-F", "query=@q/star.graphql", cwd=work)
    assert star.returncode == gg.REFUSED_EXIT and "GraphQL mutation" in star.stderr
    nonce = gg.issue_approval("post")
    assert wrappers("gh", "api", "graphql", "-F", "query=@q/star.graphql", cwd=work, approval=nonce).returncode == 0
    missing = wrappers("gh", "api", "graphql", "-F", "query=@q/nope.graphql", cwd=work)
    assert missing.returncode == gg.REFUSED_EXIT


def test_wrappers_check_a_query_on_stdin_and_pass_it_on(wrappers):
    read = wrappers("gh", "api", "graphql", "-F", "query=@-", stdin=QUERY)
    assert read.returncode == 0 and read.stdout.strip().endswith("STDIN:" + QUERY)
    assert wrappers("gh", "api", "graphql", "-F", "query=@-", stdin=MUTATION).returncode == gg.REFUSED_EXIT


def test_wrappers_read_an_input_body(wrappers, tmp_path):
    import json
    (tmp_path / "read.json").write_text(json.dumps({"query": QUERY, "variables": {"p": "mutation"}}))
    (tmp_path / "star.json").write_text(json.dumps({"query": MUTATION}))
    (tmp_path / "bad.json").write_text("not json")
    assert wrappers("gh", "api", "graphql", "--input", "read.json", cwd=tmp_path).returncode == 0
    assert wrappers("gh", "api", "graphql", "--input", "star.json", cwd=tmp_path).returncode == gg.REFUSED_EXIT
    assert wrappers("gh", "api", "graphql", "--input", "bad.json", cwd=tmp_path).returncode == gg.REFUSED_EXIT
    piped = wrappers("gh", "api", "graphql", "--input", "-", stdin=json.dumps({"query": QUERY}))
    assert piped.returncode == 0 and "STDIN:" in piped.stdout


@pytest.mark.parametrize("document, mutation", [
    (QUERY, False),
    (MUTATION, True),
    ("# mutation\nquery { viewer { login } }", False),
    ('query { search(query: "mutation") { issueCount } }', False),
    ('query { a } mutation M { addStar(input: {}) { x } }', True),
    ('"""a mutation in a block string"""\nquery { a }', False),
])
def test_has_mutation(document, mutation):
    assert gg.has_mutation(document) is mutation


def test_wrappers_let_ci_reruns_through(wrappers):
    rerun = wrappers("gh", "api", "-X", "POST", "repos/a/b/actions/runs/1/rerun-failed-jobs")
    assert rerun.returncode == 0 and rerun.stdout.startswith("REAL-gh api")


def test_install_wrappers_is_idempotent(wrappers, _gate_dir):
    script = (_gate_dir / "bin" / "git").read_text()
    gg.install_wrappers()
    assert (_gate_dir / "bin" / "git").read_text() == script


# --------------------------------------------------------------------- #
#  Claude permission paths                                               #
# --------------------------------------------------------------------- #

class _Hub:
    """The hub surface the gate uses."""

    session_id = "s1"
    snapshot_fn = None

    def __init__(self, message: str | None = None, approve: bool | None = None):
        self.message = message
        self.approve = approve
        self.asked: list[dict] = []

    def turn_person_message(self):
        return self.message

    async def request_github_approval(self, payload):
        self.asked.append(payload)
        if self.approve:
            return SimpleNamespace(approved=True, message="")
        return SimpleNamespace(approved=False, message="Declined by user.")


def _bash(command: str) -> dict:
    return {"command": command, "description": "x"}


def test_gate_check_passes_non_writes():
    assert github_gate_check(_Hub(), "Bash", _bash("git status")) == ("pass", None, [])
    assert github_gate_check(_Hub(), "Read", {"file_path": "/x"}) == ("pass", None, [])


def test_gate_check_asks_without_an_order():
    gate, updated, writes = github_gate_check(_Hub("looks good"), "Bash", _bash("git push o x"))
    assert (gate, updated, [w.kind for w in writes]) == ("ask", None, ["push"])


def test_gate_check_allows_what_the_message_ordered():
    gate, updated, _ = github_gate_check(_Hub("push"), "Bash", _bash("git push o x"))
    assert gate == "allow"
    nonce = updated["command"].split("=", 1)[1].split(";", 1)[0]
    assert updated["command"].endswith("; git push o x") and gg.approval_valid(nonce)
    assert updated["description"] == "x"


def test_gate_check_allows_reruns_as_they_are():
    assert github_gate_check(_Hub(), "Bash", _bash("gh run rerun 5 --failed"))[:2] == ("allow", None)


@pytest.mark.asyncio
async def test_can_use_tool_asks_and_runs_an_approved_write():
    hub = _Hub("looks good", approve=True)
    result = await ClaudeToolPermissions(hub, github_gate=True).can_use_tool(
        "Bash", _bash("gh pr comment 1 -b hi"), None,
    )
    assert isinstance(result, PermissionResultAllow)
    assert result.updated_input["command"].startswith(f"export {gg.APPROVAL_ENV}=")
    [payload] = hub.asked
    assert payload["title"] == "Agent wants to write to GitHub"
    assert payload["item"]["command"] == "gh pr comment 1 -b hi"
    assert payload["reason"].startswith("post: gh pr comment")


@pytest.mark.asyncio
async def test_can_use_tool_denies_a_declined_write():
    hub = _Hub(approve=False)
    result = await ClaudeToolPermissions(hub, github_gate=True).can_use_tool(
        "Bash", _bash("git push o x"), None,
    )
    assert isinstance(result, PermissionResultDeny)
    assert "GitHub write not approved" in result.message


@pytest.mark.asyncio
async def test_can_use_tool_runs_an_ordered_write_without_asking():
    hub = _Hub("push")
    result = await ClaudeToolPermissions(hub, github_gate=True).can_use_tool(
        "Bash", _bash("git push o x"), None,
    )
    assert isinstance(result, PermissionResultAllow) and not hub.asked
    assert result.updated_input["command"].endswith("; git push o x")


@pytest.mark.asyncio
async def test_can_use_tool_without_the_gate_allows_writes():
    hub = _Hub()
    result = await ClaudeToolPermissions(hub).can_use_tool("Bash", _bash("git push o x"), None)
    assert isinstance(result, PermissionResultAllow) and result.updated_input is None
    assert not hub.asked


def _grant_hook(gate: bool, hub):
    config = SimpleNamespace(agent=SimpleNamespace(
        background_agent_permissions=True, cli_max_message_bytes=1 << 20,
        github_write_gate=gate,
    ))
    backend = ClaudeBackend(SimpleNamespace(config=lambda: config))
    spec = SessionSpec(
        session_id="s1", source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp", interactive=hub,
    )
    for matcher in backend._build_hooks(spec)["PreToolUse"]:
        if matcher.matcher is None:
            return matcher.hooks[0]
    raise AssertionError("no catch-all hook")


async def _decide(hook, command: str) -> dict:
    out = await hook({"tool_name": "Bash", "tool_input": _bash(command)}, "t1", None)
    return out["hookSpecificOutput"]


@pytest.mark.asyncio
async def test_grant_hook_leaves_an_unordered_write_to_can_use_tool():
    decided = await _decide(_grant_hook(True, _Hub("ok")), "git push o x")
    assert "permissionDecision" not in decided


@pytest.mark.asyncio
async def test_grant_hook_allows_an_ordered_write_with_its_approval():
    decided = await _decide(_grant_hook(True, _Hub("push")), "git push o x")
    assert decided["permissionDecision"] == "allow"
    assert decided["updatedInput"]["command"].startswith(f"export {gg.APPROVAL_ENV}=")


@pytest.mark.asyncio
async def test_grant_hook_refuses_a_write_with_no_hub_to_ask():
    decided = await _decide(_grant_hook(True, None), "git push o x")
    assert decided["permissionDecision"] == "deny"
    assert decided["permissionDecisionReason"] == UNREACHABLE_MESSAGE


@pytest.mark.asyncio
async def test_grant_hook_allows_reads_and_reruns_as_before():
    hook = _grant_hook(True, _Hub())
    for command in ("git status", "gh run rerun 1"):
        decided = await _decide(hook, command)
        assert decided["permissionDecision"] == "allow" and "updatedInput" not in decided


@pytest.mark.asyncio
async def test_grant_hook_without_the_gate_allows_writes():
    decided = await _decide(_grant_hook(False, _Hub()), "git push o x")
    assert decided["permissionDecision"] == "allow" and "updatedInput" not in decided


def test_cli_env_puts_the_wrappers_first(tmp_path, monkeypatch):
    config = SimpleNamespace(
        agent=SimpleNamespace(github_write_gate=True, agent_teams=False, model_aliases={}),
        provider=SimpleNamespace(is_bedrock=False),
        effective_api_key="k", proxy=SimpleNamespace(enabled=False), anthropic_base_url="",
    )
    backend = ClaudeBackend(SimpleNamespace(config=lambda: config))
    monkeypatch.setattr("nerve.agent.backends.claude.cache_ttl_env", lambda *a: {})
    env = backend._build_env()
    assert env["PATH"].split(os.pathsep)[0] == str(gg.gate_dir() / "bin")
    assert env[gg.GATE_DIR_ENV] == str(gg.gate_dir())


# --------------------------------------------------------------------- #
#  The hub's pause and its relay                                         #
# --------------------------------------------------------------------- #

async def _noop_broadcast(*_args, **_kwargs):
    return None


def _real_hub(sid: str, *, interactive: bool) -> InteractiveToolHandler:
    hub = InteractiveToolHandler(sid, _noop_broadcast, interactive_capable=interactive)
    register_handler(sid, hub)
    return hub


async def _pending_id(hub: InteractiveToolHandler) -> str:
    for _ in range(200):
        if hub._pending:
            return next(iter(hub._pending))
        await asyncio.sleep(0.005)
    raise AssertionError("nothing pending")


@pytest.mark.asyncio
async def test_no_person_means_declined_at_once():
    hub = _real_hub("gh-cron", interactive=False)
    try:
        outcome = await hub.request_github_approval({"item": {"command": "git push"}})
        assert outcome.denied and outcome.message == UNREACHABLE_MESSAGE
    finally:
        unregister_handler("gh-cron")


@pytest.mark.asyncio
async def test_a_telegram_answer_approves_and_reports_back():
    hub = _real_hub("gh-tg", interactive=False)
    finished: list[str] = []
    posted: list[str] = []

    async def relay(interaction_id, payload):
        posted.append(interaction_id)

        async def finish(outcome):
            finished.append(outcome)
        return finish

    hub.approval_relay = relay
    try:
        task = asyncio.create_task(hub.request_github_approval({"item": {"command": "git push"}}))
        interaction_id = await _pending_id(hub)
        assert hub._pending[interaction_id].tool_name == GITHUB_WRITE_TOOL
        assert answer_github_approval(interaction_id, True) == "gh-tg"
        outcome = await task
        assert outcome.approved and posted == [interaction_id] and finished == ["approved"]
        assert answer_github_approval(interaction_id, True) is None  # answered already
    finally:
        unregister_handler("gh-tg")


@pytest.mark.asyncio
async def test_a_telegram_decline_reports_declined():
    hub = _real_hub("gh-tg2", interactive=False)
    finished: list[str] = []

    async def relay(interaction_id, payload):
        async def finish(outcome):
            finished.append(outcome)
        return finish

    hub.approval_relay = relay
    try:
        task = asyncio.create_task(hub.request_github_approval({}))
        answer_github_approval(await _pending_id(hub), False)
        outcome = await task
        assert outcome.denied and finished == ["declined"]
    finally:
        unregister_handler("gh-tg2")


@pytest.mark.asyncio
async def test_a_relay_that_posts_nothing_off_the_web_declines_at_once():
    hub = _real_hub("gh-tg3", interactive=False)

    async def relay(interaction_id, payload):
        return None

    hub.approval_relay = relay
    try:
        outcome = await asyncio.wait_for(hub.request_github_approval({}), timeout=2)
        assert outcome.denied and outcome.message == UNREACHABLE_MESSAGE
        assert not hub._pending
    finally:
        unregister_handler("gh-tg3")


@pytest.mark.asyncio
async def test_on_the_web_the_card_is_enough():
    hub = _real_hub("gh-web", interactive=True)
    try:
        task = asyncio.create_task(hub.request_github_approval({}))
        hub.resolve(await _pending_id(hub), None)
        assert (await task).approved
    finally:
        unregister_handler("gh-web")


@pytest.mark.asyncio
async def test_answering_another_kind_of_pause_is_refused():
    hub = _real_hub("gh-q", interactive=True)
    try:
        task = asyncio.create_task(hub.request_approval("command_approval", {"command": "ls"}))
        interaction_id = await _pending_id(hub)
        assert answer_github_approval(interaction_id, True) is None
        hub.deny(interaction_id)
        await task
    finally:
        unregister_handler("gh-q")


def test_the_turn_message_lasts_one_turn(monkeypatch):
    hub = InteractiveToolHandler("t", _noop_broadcast)
    hub.begin_turn("push", "web")
    assert hub.turn_person_message() == "push" and hub.turn_source == "web"
    hub.end_turn()
    assert hub.turn_person_message() is None and hub.turn_source is None
    hub.begin_turn("push", "telegram")
    assert hub.turn_person_message(max_age=-1) is None  # too old


# --------------------------------------------------------------------- #
#  The approval notification (every channel)                             #
# --------------------------------------------------------------------- #

class _Service:
    """Stands in for NotificationService: records propose_action and dismissals."""

    def __init__(self, delivered):
        self.proposed: list[tuple] = []
        self.dismissed: list[str] = []
        self.status = "pending"
        rows = self

        class _NotifDb:
            async def get_notification(self, nid):
                return {"id": nid, "status": rows.status,
                        "channels_delivered": json.dumps(delivered)}

        self.db = _NotifDb()

    async def propose_action(self, session_id, target_kind, target_id, title, body, **kw):
        self.proposed.append((session_id, target_kind, target_id, title, body, kw))
        return {"notification_id": "approval-1", "status": "sent"}

    async def handle_dismiss(self, nid, **kw):
        self.dismissed.append(nid)
        self.status = "dismissed"
        return True


@pytest.mark.asyncio
async def test_a_write_is_filed_as_an_approval_for_every_channel():
    service = _Service(["web", "telegram", "slack"])
    finish = await file_github_approval(
        service, "s1", "iid-1", {"item": {"command": "git push fork main"}, "reason": "push"},
    )
    [(sid, kind, target, title, body, kw)] = service.proposed
    assert (sid, kind, target) == ("s1", GITHUB_WRITE_KIND, "iid-1")
    assert "GitHub write" in title and "```\ngit push fork main\n```" in body
    assert [o["value"] for o in kw["options"]] == ["approve", "decline"]
    assert kw["expiry_hours"] == 1
    # The interaction ended another way (the web card): the card closes.
    await finish("approved")
    assert service.dismissed == ["approval-1"]


@pytest.mark.asyncio
async def test_an_answered_card_is_left_alone():
    service = _Service(["telegram"])
    finish = await file_github_approval(service, "s1", "iid-1", {})
    service.status = "answered"
    await finish("approved")
    assert service.dismissed == []


@pytest.mark.asyncio
async def test_no_channel_outside_the_web_means_nobody_to_ask():
    service = _Service(["web"])
    assert await file_github_approval(service, "s1", "iid-1", {}) is None
    assert service.dismissed == ["approval-1"]


@pytest.mark.asyncio
async def test_an_answer_from_any_channel_settles_the_write():
    from nerve.notifications import handlers

    assert handlers.get(GITHUB_WRITE_KIND) is not None
    hub = _real_hub("gh-any", interactive=False)

    async def relay(interaction_id, payload):
        async def finish(outcome):
            pass
        return finish

    hub.approval_relay = relay
    dispatch = handlers.get(GITHUB_WRITE_KIND)
    try:
        task = asyncio.create_task(hub.request_github_approval({}))
        iid = await _pending_id(hub)
        result = await dispatch({"id": "approval-1"}, iid, "approve", None, actor=None)
        assert result.ok and (await task).approved
        task = asyncio.create_task(hub.request_github_approval({}))
        iid = await _pending_id(hub)
        result = await dispatch({"id": "approval-2"}, iid, "decline", None, actor=None)
        assert result.ok and (await task).denied
        gone = await dispatch({"id": "approval-3"}, "gone", "approve", None, actor=None)
        assert not gone.ok
    finally:
        unregister_handler("gh-any")


# --------------------------------------------------------------------- #
#  Engine: where the relay posts                                         #
# --------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_engine_relay_files_unless_the_web_card_is_watched(monkeypatch):
    from nerve.agent import engine as engine_mod

    filed: list[str] = []

    async def fake_file(service, sid, interaction_id, payload):
        filed.append(interaction_id)
        return "finish"

    monkeypatch.setattr(engine_mod, "file_github_approval", fake_file)
    fake = SimpleNamespace(notification_service=object())
    relay = engine_mod.AgentEngine._github_approval_relay(fake, "gh-eng")
    hub = _real_hub("gh-eng", interactive=True)
    try:
        monkeypatch.setattr(engine_mod.broadcaster, "has_listeners", lambda sid: True)
        hub.begin_turn("hi", "web")
        assert await relay("i1", {}) is None and filed == []
        for source in ("telegram", "slack"):
            hub.begin_turn("hi", source)
            assert await relay(f"i-{source}", {}) == "finish"
        monkeypatch.setattr(engine_mod.broadcaster, "has_listeners", lambda sid: False)
        hub.begin_turn("hi", "web")
        assert await relay("i3", {}) == "finish"
        assert filed == ["i-telegram", "i-slack", "i3"]
        assert await engine_mod.AgentEngine._github_approval_relay(
            SimpleNamespace(notification_service=None), "gh-eng",
        )("i4", {}) is None
    finally:
        unregister_handler("gh-eng")


def test_engine_end_turn_clears_the_turn_message():
    from nerve.agent import engine as engine_mod

    hub = _real_hub("gh-end", interactive=True)
    try:
        hub.begin_turn("push", "web")
        fake = SimpleNamespace(_turn_watches={})
        engine_mod.AgentEngine._end_turn(fake, "gh-end", "outcome")
        assert hub.turn_person_message() is None
    finally:
        unregister_handler("gh-end")


def test_person_sources():
    from nerve.agent.engine import _PERSON_SOURCES

    assert {"web", "telegram", "slack"} <= _PERSON_SOURCES
    assert not {"cron", "hook", "workflow"} & _PERSON_SOURCES


def test_gate_module_runs_without_nerve(tmp_path):
    """The wrappers run the module as a plain script."""
    out = subprocess.run(
        ["python3", "-I", str(Path(gg.__file__)), "classify", "git", "push", "o", "x"],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0 and out.stdout.startswith("push git push o x")


# --------------------------------------------------------------------- #
#  End to end with the notification service                              #
# --------------------------------------------------------------------- #

class _TgBot:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self._next = 10

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        self._next += 1
        return SimpleNamespace(message_id=self._next)

    async def edit_message_text(self, **kwargs):
        self.calls.append(("edit_message_text", kwargs))

    def named(self, name):
        return [kw for called, kw in self.calls if called == name]


async def _service_with_telegram(db, tmp_path, monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    from nerve.agent import streaming
    from nerve.config import NerveConfig, NotificationsConfig
    from nerve.notifications.service import NotificationService
    from tests.actor_rows import ensure_system_principal

    await ensure_system_principal(db)
    monkeypatch.setattr(streaming, "broadcaster", SimpleNamespace(broadcast=AsyncMock()))
    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.telegram.allowed_users = [4242]
    cfg.notifications = NotificationsConfig(channels=["telegram"], telegram_chat_id=4242)
    bot = _TgBot()
    channel = SimpleNamespace(
        _app=SimpleNamespace(bot=bot), _cache_message=MagicMock(),
        _record_reply_route=AsyncMock(),
    )
    engine = MagicMock()
    engine.sessions.is_running.return_value = False
    engine.router.get_channel.return_value = channel
    return NotificationService(cfg, db, engine), bot


@pytest.mark.asyncio
async def test_a_telegram_answer_through_the_notification_approves(db, tmp_path, monkeypatch):
    service, bot = await _service_with_telegram(db, tmp_path, monkeypatch)
    await db.create_session("gh-e2e", title="e2e", source="slack", actor=None)
    hub = _real_hub("gh-e2e", interactive=False)
    hub.approval_relay = lambda iid, payload: file_github_approval(service, "gh-e2e", iid, payload)
    try:
        task = asyncio.create_task(
            hub.request_github_approval({"item": {"command": "gh pr create"}}),
        )
        await _pending_id(hub)
        for _ in range(200):
            if bot.named("send_message"):
                break
            await asyncio.sleep(0.005)
        (sent,) = bot.named("send_message")
        assert "gh pr create" in sent["text"]
        buttons = [b.callback_data for b in sent["reply_markup"].inline_keyboard[0]]
        nid = buttons[0].split(":")[1]
        assert await service.handle_answer(nid, "approve", "telegram")
        assert (await asyncio.wait_for(task, 2)).approved
        assert (await db.get_notification(nid))["status"] == "answered"
    finally:
        unregister_handler("gh-e2e")


@pytest.mark.asyncio
async def test_a_write_approved_on_the_web_closes_its_card(db, tmp_path, monkeypatch):
    service, bot = await _service_with_telegram(db, tmp_path, monkeypatch)
    await db.create_session("gh-e2w", title="e2w", source="web", actor=None)
    hub = _real_hub("gh-e2w", interactive=True)
    hub.approval_relay = lambda iid, payload: file_github_approval(service, "gh-e2w", iid, payload)
    try:
        task = asyncio.create_task(hub.request_github_approval({"item": {"command": "git push"}}))
        iid = await _pending_id(hub)
        for _ in range(200):
            if bot.named("send_message"):
                break
            await asyncio.sleep(0.005)
        nid = bot.named("send_message")[0]["reply_markup"].inline_keyboard[0][0].callback_data.split(":")[1]
        hub.resolve(iid, None)  # the web card
        assert (await asyncio.wait_for(task, 2)).approved
        assert (await db.get_notification(nid))["status"] == "dismissed"
        assert bot.named("edit_message_text")  # the Telegram card lost its buttons
    finally:
        unregister_handler("gh-e2w")
