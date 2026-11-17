"""GitHub write gate: a shell command that writes to GitHub waits for the person.

The agent runs shell commands through the CLI's ``Bash`` tool. A command that
pushes, posts or changes something on GitHub runs only when one of these holds:

* The person approved it: a card in the web UI, or buttons in Telegram.
* The person's message that started the turn starts with "push" or "post"
  (see :func:`message_verbs`), and the command does only that kind of write.
* It is a CI rerun (``gh run rerun``, or the API's rerun endpoints). A cron
  job that reruns failed CI does these as standing work.

Force pushes, deletions, merges, closes and repository changes always need the
card. A session without a person (cron, hooks) gets a refusal.

The Claude backend checks each ``Bash`` call against :func:`classify_command`.
Writes that happen inside a script (``python3 push.py`` calling ``gh api``)
are not visible in the command, so the CLI gets ``git`` and ``gh`` wrappers on
its PATH (:func:`install_wrappers`). A wrapper lets a write through only when
the command that ran it was approved: the gate then prefixes the command with
``export NERVE_GITHUB_APPROVAL=<nonce>`` and records the nonce in the
approvals directory. A script that writes is marked with
``NERVE_GITHUB_WRITE=1`` so the gate asks for it before it runs.

This guards against mistakes, not against an agent set on getting around it:
the agent runs as the same user that owns the approvals directory.

Standard library only. The wrappers run this file as a script
(``python3 -I github_gate.py wrap-git <real git> <args>``), so it must not
import Nerve.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import shlex
import shutil
import sys
import time
from dataclasses import dataclass
from typing import Callable
from pathlib import Path

PUSH = "push"
POST = "post"
DESTRUCTIVE = "destructive"
RERUN = "rerun"
DECLARED = "declared"
UNPARSED = "unparsed"

#: The kinds a "push"/"post" message lets through without a card.
VERB_KINDS = {"push": PUSH, "post": POST}

APPROVAL_ENV = "NERVE_GITHUB_APPROVAL"
DECLARE_ENV = "NERVE_GITHUB_WRITE"
GATE_DIR_ENV = "NERVE_GITHUB_GATE_DIR"
APPROVAL_TTL_SECONDS = 1800
#: A message longer than this is not "push"/"post" any more.
MAX_VERB_MESSAGE_CHARS = 80
#: The exit code of a wrapper that refused a write.
REFUSED_EXIT = 3


@dataclass(frozen=True)
class Write:
    """One GitHub write found in a command."""

    kind: str   # PUSH, POST, DESTRUCTIVE, RERUN, DECLARED or UNPARSED
    what: str   # what it does, for the card ("git push fork main")


# --------------------------------------------------------------------- #
#  Classifying commands                                                  #
# --------------------------------------------------------------------- #

_OPERATORS = frozenset({";", "&&", "||", "|", "|&", "&", "(", ")", "\n", ";;"})
_SHELLS = frozenset({"bash", "sh", "zsh", "dash", "ksh"})
_DECLARED_RE = re.compile(rf"(?:^|[\s;&|(]){DECLARE_ENV}=(?:1|true|yes)\b")
_HEREDOC_RE = re.compile(r"<<-?[ \t]*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1[^\n]*\n")

# git options that come before the subcommand, and whether they take a value.
_GIT_VALUE_OPTS = frozenset({"-C", "-c", "--git-dir", "--work-tree", "--namespace",
                             "--exec-path", "--config-env", "--super-prefix"})
_GIT_DESTRUCTIVE_FLAGS = frozenset({"--force", "--delete", "--mirror", "--prune",
                                    "--force-if-includes"})

# gh: what each command group's subcommands do.
_GH_GROUPS: dict[str, dict[str, str]] = {
    "pr": {"create": POST, "comment": POST, "review": POST, "edit": POST, "ready": POST,
           "reopen": POST, "merge": DESTRUCTIVE, "close": DESTRUCTIVE, "lock": DESTRUCTIVE,
           "unlock": DESTRUCTIVE, "update-branch": PUSH, "revert": DESTRUCTIVE},
    "issue": {"create": POST, "comment": POST, "edit": POST, "reopen": POST, "pin": POST,
              "unpin": POST, "develop": POST, "close": DESTRUCTIVE, "delete": DESTRUCTIVE,
              "transfer": DESTRUCTIVE, "lock": DESTRUCTIVE, "unlock": DESTRUCTIVE},
    "release": {"create": POST, "edit": POST, "upload": POST, "delete": DESTRUCTIVE,
                "delete-asset": DESTRUCTIVE},
    "repo": {"create": DESTRUCTIVE, "edit": DESTRUCTIVE, "rename": DESTRUCTIVE,
             "archive": DESTRUCTIVE, "unarchive": DESTRUCTIVE, "delete": DESTRUCTIVE,
             "fork": DESTRUCTIVE, "sync": DESTRUCTIVE, "deploy-key": DESTRUCTIVE,
             "autolink": DESTRUCTIVE},
    "gist": {"create": POST, "edit": POST, "rename": POST, "delete": DESTRUCTIVE},
    "label": {"create": POST, "edit": POST, "clone": POST, "delete": DESTRUCTIVE},
    "run": {"rerun": RERUN, "cancel": DESTRUCTIVE, "delete": DESTRUCTIVE},
    "workflow": {"run": DESTRUCTIVE, "enable": DESTRUCTIVE, "disable": DESTRUCTIVE},
    "secret": {"set": DESTRUCTIVE, "delete": DESTRUCTIVE},
    "variable": {"set": DESTRUCTIVE, "delete": DESTRUCTIVE},
    "cache": {"delete": DESTRUCTIVE},
    "project": {"create": POST, "edit": POST, "close": DESTRUCTIVE, "delete": DESTRUCTIVE,
                "copy": POST, "field-create": POST, "field-delete": DESTRUCTIVE,
                "item-add": POST, "item-create": POST, "item-edit": POST,
                "item-archive": DESTRUCTIVE, "item-delete": DESTRUCTIVE, "link": POST,
                "unlink": DESTRUCTIVE, "mark-template": POST},
    "ssh-key": {"add": DESTRUCTIVE, "delete": DESTRUCTIVE},
    "gpg-key": {"add": DESTRUCTIVE, "delete": DESTRUCTIVE},
}
#: gh options that take a value (global and per-command ones we skip over).
_GH_VALUE_OPTS = frozenset({"-R", "--repo", "--hostname", "-X", "--method", "-H", "--header",
                            "-f", "--raw-field", "-F", "--field", "--input", "-q", "--jq",
                            "-t", "--template", "--cache", "-p", "--preview"})
_GH_FIELD_OPTS = frozenset({"-f", "--raw-field", "-F", "--field"})
_RERUN_ENDPOINT = re.compile(
    r"^/?repos/[^/]+/[^/]+/actions/(?:runs/\d+/(?:rerun|rerun-failed-jobs)|jobs/\d+/rerun)/?$",
)
_REF_ENDPOINT = re.compile(r"^/?repos/[^/]+/[^/]+/git/refs(?:/|$)")
_GIT_OBJECT_ENDPOINT = re.compile(r"^/?repos/[^/]+/[^/]+/git/(?:commits|trees|blobs|tags)/?$")
_MERGE_ENDPOINT = re.compile(r"^/?repos/[^/]+/[^/]+/(?:merges|pulls/\d+/merge)/?$")
# The API writes a "post" message covers: new issues and PRs, comments,
# reviews, review replies and reactions, and edits of a comment. Any other
# write (a close, a repository, collaborator or secret change, a commit
# through the contents API) always asks: unknown endpoints fail closed.
_POST_ENDPOINTS = {
    "POST": re.compile(
        r"^/?repos/[^/]+/[^/]+/(?:"
        r"issues|pulls"
        r"|issues/\d+/comments|pulls/\d+/comments(?:/\d+/replies)?"
        r"|pulls/\d+/reviews(?:/\d+/events)?"
        r"|issues/\d+/reactions|issues/comments/\d+/reactions|pulls/comments/\d+/reactions"
        r")/?$",
    ),
    "PATCH": re.compile(r"^/?repos/[^/]+/[^/]+/(?:issues|pulls)/comments/\d+/?$"),
}


def _strip_heredocs(text: str) -> str:
    """Drop here-document bodies: they are data (a commit message, a file),
    not commands, and they often mention ``git push``."""
    out: list[str] = []
    pos = 0
    for match in _HEREDOC_RE.finditer(text):
        if match.start() < pos:
            continue
        out.append(text[pos:match.end()])
        delimiter = match.group(2)
        end = re.compile(rf"^[ \t]*{re.escape(delimiter)}[ \t]*$", re.M).search(text, match.end())
        pos = end.end() if end else len(text)
    out.append(text[pos:])
    return "".join(out)


def _tokens(text: str) -> list[str]:
    lexer = shlex.shlex(text, posix=True, punctuation_chars="();<>|&\n")
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    return list(lexer)


def _simple_commands(tokens: list[str]) -> list[list[str]]:
    commands: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        if token in _OPERATORS or set(token) <= set(";&|()\n"):
            if current:
                commands.append(current)
            current = []
        else:
            current.append(token)
    if current:
        commands.append(current)
    return commands


def _is_named(token: str, name: str) -> bool:
    return token == name or token.endswith("/" + name)


def classify_command(command: str) -> list[Write]:
    """The GitHub writes in a shell command, in order; ``[]`` when none."""
    text = _strip_heredocs(command or "")
    writes: list[Write] = []
    if _DECLARED_RE.search(text):
        writes.append(Write(DECLARED, f"a command marked {DECLARE_ENV}=1"))
    try:
        commands = _simple_commands(_tokens(text))
    except ValueError:
        # Unbalanced quotes: look for the tools by name and ask.
        if re.search(r"\bgit\b[^\n]*\bpush\b|\bgh\s+(?:api|pr|issue|release|repo|run|workflow)\b", text):
            writes.append(Write(UNPARSED, "a command Nerve could not parse"))
        return writes
    for argv in commands:
        writes.extend(_classify_simple(argv, depth=0))
    return writes


def _classify_simple(argv: list[str], depth: int) -> list[Write]:
    """Writes in one simple command. Tools are found by name anywhere in it,
    so ``timeout 60 git push`` and ``env X=1 git push`` count."""
    writes: list[Write] = []
    for i, token in enumerate(argv):
        if _is_named(token, "git"):
            write = classify_git(argv[i + 1:])
            if write:
                writes.append(write)
        elif _is_named(token, "gh"):
            write = classify_gh(argv[i + 1:])
            if write:
                writes.append(write)
        elif depth < 2 and (token in _SHELLS or token.rsplit("/", 1)[-1] in _SHELLS):
            # bash -c "git push": the script is one token.
            rest = argv[i + 1:]
            for j, arg in enumerate(rest):
                if arg == "-c" or (arg.startswith("-") and "c" in arg[1:] and not arg.startswith("--")):
                    if j + 1 < len(rest):
                        writes.extend(_classify_script(rest[j + 1], depth + 1))
                    break
        elif token == "eval" and depth < 2:
            writes.extend(_classify_script(" ".join(argv[i + 1:]), depth + 1))
            break
    return writes


def _classify_script(script: str, depth: int) -> list[Write]:
    try:
        commands = _simple_commands(_tokens(_strip_heredocs(script)))
    except ValueError:
        return [Write(UNPARSED, "a command Nerve could not parse")]
    writes: list[Write] = []
    for argv in commands:
        writes.extend(_classify_simple(argv, depth))
    return writes


def classify_git(args: list[str]) -> Write | None:
    """``git <args>``: a push, or None."""
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in _GIT_VALUE_OPTS:
            i += 2
            continue
        if arg.startswith("-"):
            i += 1
            continue
        break
    if i >= len(args) or args[i] not in ("push", "send-pack"):
        return None
    rest = args[i + 1:]
    what = "git " + " ".join(args[i:])[:200]
    if any(a in ("-n", "--dry-run") for a in rest):
        return None
    for arg in rest:
        if arg in _GIT_DESTRUCTIVE_FLAGS or arg.startswith("--force-with-lease"):
            return Write(DESTRUCTIVE, what)
        if arg.startswith("-") and not arg.startswith("--") and ("f" in arg[1:] or "d" in arg[1:]):
            return Write(DESTRUCTIVE, what)
        if not arg.startswith("-") and (arg.startswith("+") or arg.startswith(":")):
            return Write(DESTRUCTIVE, what)
    return Write(PUSH, what)


def classify_gh(args: list[str], load: Callable[[str], str | None] | None = None) -> Write | None:
    """``gh <args>``: a write, or None for reads and local commands."""
    positional: list[str] = []
    i = 0
    while i < len(args) and len(positional) < 2:
        arg = args[i]
        if arg in _GH_VALUE_OPTS:
            i += 2
            continue
        if arg.startswith("-"):
            i += 1
            continue
        positional.append(arg)
        i += 1
    if not positional:
        return None
    group = positional[0]
    if group == "api":
        return _classify_gh_api(args[args.index("api") + 1:], load)
    sub = positional[1] if len(positional) > 1 else ""
    kind = _GH_GROUPS.get(group, {}).get(sub)
    if kind is None:
        return None
    return Write(kind, "gh " + " ".join(args)[:200])


_GQL_STRIP_RE = re.compile(r'"""(?:[^"]|"(?!""))*"""|"(?:\\.|[^"\\\n])*"|#[^\n]*')


def has_mutation(document: str) -> bool:
    """Whether a GraphQL document has a mutation operation. Comments and
    strings are dropped first, so a ``# mutation`` note or a string that
    says "mutation" doesn't count. Errs on the safe side otherwise: any
    ``mutation`` keyword counts, whichever operation ``operationName`` picks."""
    return re.search(r"\bmutation\b", _GQL_STRIP_RE.sub(" ", document)) is not None


def _input_query(body: str) -> str | None:
    """The ``query`` of a ``--input`` JSON body; None when it isn't one."""
    try:
        data = json.loads(body)
    except ValueError:
        return None
    query = data.get("query") if isinstance(data, dict) else None
    return query if isinstance(query, str) else None


def _classify_gh_api(
    args: list[str], load: Callable[[str], str | None] | None = None,
) -> Write | None:
    """``gh api <args>``. *load* reads a file the call names (``@path``,
    ``--input path``; ``-`` is stdin) and returns None when it can't. The
    command-level check has no loader: a GraphQL query in a file passes
    there, and the gh wrapper, which runs where the file is, reads it."""
    method: str | None = None
    endpoint: str | None = None
    fields: list[str] = []
    has_input = False
    input_path: str | None = None
    i = 0
    while i < len(args):
        arg = args[i]
        value = args[i + 1] if i + 1 < len(args) else ""
        if arg in ("-X", "--method"):
            method = value.upper()
            i += 2
            continue
        if arg.startswith("-X") and len(arg) > 2:
            method = arg[2:].upper()
        elif arg.startswith("--method="):
            method = arg.split("=", 1)[1].upper()
        elif arg in _GH_FIELD_OPTS:
            fields.append(value)
            i += 2
            continue
        elif arg.startswith(("--field=", "--raw-field=")):
            fields.append(arg.split("=", 1)[1])
        elif arg in ("--input",) or arg.startswith("--input="):
            has_input = True
            input_path = value if arg == "--input" else arg.split("=", 1)[1]
            if arg == "--input":
                i += 2
                continue
        elif arg in _GH_VALUE_OPTS:
            i += 2
            continue
        elif not arg.startswith("-") and endpoint is None:
            endpoint = arg
        i += 1
    if endpoint is None:
        return None
    if method is None:
        method = "POST" if (fields or has_input) else "GET"
    what = f"gh api {method} {endpoint}"[:200]
    if endpoint.strip("/").rsplit("/", 1)[-1] == "graphql":
        documents: list[str | None] = []
        for field in fields:
            key, _, value = field.partition("=")
            if key != "query":
                continue  # variables and operationName run nothing
            if value.startswith("@"):
                if load is not None:
                    documents.append(load(value[1:]))
            else:
                documents.append(value)
        if input_path is not None and load is not None:
            body = load(input_path)
            documents.append(None if body is None else _input_query(body))
        # Unreadable or not a query: ask, it could be anything.
        if any(doc is None or has_mutation(doc) for doc in documents):
            return Write(POST, what + " (GraphQL mutation)")
        return None
    if method in ("GET", "HEAD"):
        return None
    if method == "POST" and _RERUN_ENDPOINT.match(endpoint):
        return Write(RERUN, what)
    if method == "DELETE" or _MERGE_ENDPOINT.match(endpoint):
        return Write(DESTRUCTIVE, what)
    if _REF_ENDPOINT.match(endpoint):
        if any(re.match(r"force=(?:true|1)\b", f) for f in fields):
            return Write(DESTRUCTIVE, what + " (force)")
        return Write(PUSH, what)
    if _GIT_OBJECT_ENDPOINT.match(endpoint):
        return Write(PUSH, what)
    post = _POST_ENDPOINTS.get(method)
    if post is not None and post.match(endpoint):
        return Write(POST, what)
    return Write(DESTRUCTIVE, what)


# --------------------------------------------------------------------- #
#  The person's message                                                  #
# --------------------------------------------------------------------- #

_LEAD_RE = re.compile(r"^(?:(?:yes|yep|yeah|ok|okay|go|sure|please)\b[\s,.!:;-]*)+")
_NEGATION_RE = re.compile(r"\b(?:don'?t|do not|not|never|no|without)\b")
_VERB_RE = re.compile(r"\b(push|post)(?:ed|ing|es|s)?\b")
# What Telegram puts before the person's text (nerve/channels/telegram.py,
# _format_reply_context and _format_forward_context), then a blank line.
_CONTEXT_MARKERS = ("[Reply to ", "[Quoted: ", "[Forwarded from ")


def _own_text(message: str) -> str:
    """The person's own words: a Telegram reply or forward context block
    before them is dropped. Unclear cases give ``""``."""
    text = message.strip()
    if not text.startswith(_CONTEXT_MARKERS):
        return text
    head, sep, tail = text.rpartition("\n\n")
    if sep and head.rstrip().endswith("]"):
        return tail
    return ""


def message_verbs(message: str | None) -> frozenset[str]:
    """The writes a person's message orders without a card: ``{"push"}``,
    ``{"post"}``, both, or none.

    The message must start with "push" or "post" (after an optional "yes",
    "ok", "go"…), be at most :data:`MAX_VERB_MESSAGE_CHARS` long, ask no
    question and say no "don't", "not", "never", "no" or "without". "post
    and push" gives both. A Telegram reply's quote of the message replied
    to does not count, only the person's own text after it.
    """
    if not message:
        return frozenset()
    text = " ".join(_own_text(message).lower().replace("’", "'").split())
    if not text or len(text) > MAX_VERB_MESSAGE_CHARS or "?" in text:
        return frozenset()
    if _NEGATION_RE.search(text):
        return frozenset()
    text = _LEAD_RE.sub("", text)
    if not re.match(r"(?:push|post)\b", text):
        return frozenset()
    return frozenset(_VERB_RE.findall(text))


def needs_person(writes: list[Write]) -> bool:
    """Whether any write is more than a CI rerun."""
    return any(w.kind != RERUN for w in writes)


def runs_without_card(writes: list[Write], verbs: frozenset[str]) -> bool:
    """Whether the writes may run without asking: each one is a CI rerun or
    a kind the person's verbs ordered. Destructive, declared and unparsed
    writes always ask."""
    allowed = {VERB_KINDS[v] for v in verbs if v in VERB_KINDS}
    return all(w.kind == RERUN or w.kind in allowed for w in writes)


def describe(writes: list[Write]) -> str:
    """One line per write, for the card."""
    labels = {PUSH: "push", POST: "post", DESTRUCTIVE: "destructive", RERUN: "CI rerun",
              DECLARED: "script that writes", UNPARSED: "unparsed"}
    return "; ".join(f"{labels.get(w.kind, w.kind)}: {w.what}" for w in writes)


# --------------------------------------------------------------------- #
#  Approvals for the wrappers                                            #
# --------------------------------------------------------------------- #

_configured_dir: Path | None = None


def configure(directory: Path) -> None:
    """Set the gate's directory in the Nerve process (``nerve_path("github-gate")``).
    The runtime and the wrappers get it through :data:`GATE_DIR_ENV`."""
    global _configured_dir
    _configured_dir = Path(directory)


def gate_dir() -> Path:
    """The gate's directory: :data:`GATE_DIR_ENV`, else the configured one."""
    configured = os.environ.get(GATE_DIR_ENV)
    if configured:
        return Path(configured)
    if _configured_dir is None:
        raise RuntimeError("the GitHub gate directory is not configured")
    return _configured_dir


def _approvals_dir(base: Path | None = None) -> Path:
    return (base or gate_dir()) / "approvals"


def issue_approval(what: str, base: Path | None = None) -> str:
    """Record an approval; returns the nonce to export into the command."""
    directory = _approvals_dir(base)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    now = time.time()
    for old in directory.iterdir():
        try:
            if now - old.stat().st_mtime > APPROVAL_TTL_SECONDS:
                old.unlink()
        except OSError:
            pass
    nonce = secrets.token_hex(16)
    (directory / nonce).write_text(what[:500] + "\n")
    return nonce


def approval_valid(nonce: str | None, base: Path | None = None) -> bool:
    if not nonce or not re.fullmatch(r"[0-9a-f]{32}", nonce):
        return False
    try:
        age = time.time() - (_approvals_dir(base) / nonce).stat().st_mtime
    except OSError:
        return False
    return 0 <= age <= APPROVAL_TTL_SECONDS


def approved_command(command: str, nonce: str) -> str:
    """The command with the approval exported for the wrappers it runs."""
    return f"export {APPROVAL_ENV}={nonce}; {command}"


# --------------------------------------------------------------------- #
#  Wrappers                                                              #
# --------------------------------------------------------------------- #

_GH_WRITE_GROUPS = ("api",) + tuple(_GH_GROUPS)


def install_wrappers(base: Path | None = None, python: str | None = None) -> Path | None:
    """Write the ``git`` and ``gh`` wrappers; returns their directory, or
    None when neither tool is installed. Idempotent."""
    base = base or gate_dir()
    bin_dir = base / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    module = Path(__file__).resolve()
    python = python or sys.executable or "python3"
    clean_path = os.pathsep.join(
        p for p in os.environ.get("PATH", "").split(os.pathsep)
        if p and Path(p).resolve() != bin_dir.resolve()
    )
    installed = False
    for tool in ("git", "gh"):
        real = shutil.which(tool, path=clean_path)
        target = bin_dir / tool
        if not real:
            if target.exists():
                target.unlink()
            continue
        if tool == "git":
            trigger = (
                'for a in "$@"; do\n'
                '  case "$a" in push|send-pack) '
                f'exec {shlex.quote(python)} -I {shlex.quote(str(module))} wrap-git '
                f'{shlex.quote(real)} "$@";; esac\n'
                'done\n'
            )
        else:
            groups = "|".join(_GH_WRITE_GROUPS)
            trigger = (
                'for a in "$@"; do\n'
                f'  case "$a" in {groups}) '
                f'exec {shlex.quote(python)} -I {shlex.quote(str(module))} wrap-gh '
                f'{shlex.quote(real)} "$@";; esac\n'
                'done\n'
            )
        script = (
            "#!/bin/sh\n"
            "# Written by Nerve (nerve/agent/github_gate.py): GitHub writes need\n"
            "# the approval Nerve's gate gives the command that runs them.\n"
            f"{trigger}"
            f'exec {shlex.quote(real)} "$@"\n'
        )
        if not target.exists() or target.read_text() != script:
            tmp = target.with_suffix(".tmp")
            tmp.write_text(script)
            tmp.chmod(0o755)
            tmp.replace(target)
        installed = True
    return bin_dir if installed else None


def _refuse(writes: list[Write]) -> int:
    sys.stderr.write(
        "nerve: GitHub write blocked: " + describe(writes) + "\n"
        "Writes need the person's approval. Run the write as its own command so Nerve can ask,\n"
        f"or start a script that writes with {DECLARE_ENV}=1.\n"
    )
    return REFUSED_EXIT


def wrap(tool: str, real: str, args: list[str]) -> int:
    """The wrapper's check: run *real* unless a write lacks an approval.

    For gh it reads the files a GraphQL call names, from where it runs. A
    query on stdin is read once and handed on to *real* unchanged."""
    stdin: list[bytes] = []

    def load(path: str) -> str | None:
        try:
            if path == "-":
                if not stdin:
                    stdin.append(sys.stdin.buffer.read())
                return stdin[0].decode("utf-8", "replace")
            return Path(path).expanduser().read_text(errors="replace")
        except OSError:
            return None

    write = classify_git(args) if tool == "git" else classify_gh(args, load)
    writes = [write] if write else []
    if writes and needs_person(writes) and not approval_valid(os.environ.get(APPROVAL_ENV)):
        return _refuse(writes)
    if stdin:
        import subprocess
        return subprocess.run([real, *args], input=stdin[0]).returncode
    os.execv(real, [real, *args])
    return 0  # not reached


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] in ("wrap-git", "wrap-gh"):
        return wrap("git" if argv[0] == "wrap-git" else "gh", argv[1], argv[2:])
    if argv and argv[0] == "classify":
        for write in classify_command(" ".join(argv[1:])):
            print(write.kind, write.what)
        return 0
    sys.stderr.write("usage: github_gate.py wrap-git|wrap-gh <real> <args> | classify <command>\n")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
