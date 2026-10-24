# Setup Guide

## Quick Start

```bash
curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh | bash
```

The installer puts the dependencies in place (Python, Node.js, uv), clones
the repository and runs `nerve init`. From a checkout the same thing is:

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
uv sync                       # creates .venv from uv.lock
source .venv/bin/activate     # puts `nerve` on PATH
nerve init
```

`uv sync` creates `.venv` but does not activate it, so `nerve` is not on `PATH`
until you do. If you'd rather not activate, prefix commands with `uv run` instead —
`uv run nerve init`.

`nerve init` asks three things in the terminal — how Nerve runs here, where
its files live, which port and address it listens on — writes the
machine-local files, builds the web UI if it is not built yet, starts Nerve,
and prints the address of the setup page together with a **setup token**:

```
  ┌──────────────────────────────────────────────────────────────┐
  │  Finish setup in your browser                                │
  │                                                              │
  │  Setup page   http://127.0.0.1:8900/setup                    │
  │  Setup token  <token>                                        │
  │               (copied to your clipboard)                     │
  │                                                              │
  │  The token is a one-time credential. It is pasted into       │
  │  the page and never part of the URL. To see it again:        │
  │  nerve status                                                │
  └──────────────────────────────────────────────────────────────┘

  Open the setup page in your browser now? [Y/n]:
```

Everything else — your account, personal assistant or worker, the model
provider, your time zone, memory, Telegram or Slack, sources, scheduled jobs —
is decided on that page. Three decisions get you to a working assistant:
claim the account, choose a mode, connect a model. The rest can be skipped
and finished later.

Prefer to answer everything in the terminal? `nerve init --terminal` is the
full wizard, unchanged. Have no terminal at all? See
[Unattended installation](#unattended-installation).

## Prerequisites

### Server deployment
- Python 3.13+
- Node.js 22.12+ (to build the web UI; `nerve init` builds it for you)
- A way to reach Claude: an Anthropic API key, a Claude subscription, or AWS
  Bedrock — asked for on the setup page, not in the terminal

### Docker deployment
- Docker with Compose V2 (`docker compose`)
- The same choice of credential, asked for on the setup page

## What `nerve init` decides on the host

Only what the browser cannot: the shape of the install on this machine.

```
  Nerve setup
  Most of the setup happens in your browser. This terminal only decides what the
  browser cannot: how Nerve runs on this machine and where it keeps its files.
  Ctrl+C is safe; answers are saved.

  Step 1 of 3 · How do you want to run Nerve?
    server   Run directly on this machine. Recommended on a server or your own laptop.
    docker   Run in a container. Isolated, easy to remove. Needs Docker.
  Choose [server/docker] (server):

  Step 2 of 3 · Where things live
    Config files   /home/alice/nerve   (config.yaml, config.local.yaml)
    State          /home/alice/.nerve  (database, logs; set NERVE_HOME before running nerve to move it)
  Workspace — the agent's files: memory, skills, settings [~/nerve-workspace]:

  Step 3 of 3 · Network
  Gateway port [8900]:
  Who should be able to open the web UI?
    localhost   Only this computer. Open it here, or through an SSH tunnel from elsewhere.
    lan         Other devices on your network too. You will set a password on the setup page.
  Reachable from [localhost]:
```

- **Deployment** — `server` or `docker`. Docker adds one question, see
  [Docker](#docker).
- **Paths** — the config directory and the state directory are *shown*, not
  asked: they are decided by `-c`/`NERVE_CONFIG_DIR` and `NERVE_HOME` before
  `nerve init` runs. The workspace is asked, default `~/nerve-workspace`.
- **Port** — default 8900. If it is already taken, `nerve init` says so and
  proposes the first free port between 8901 and 8999 as the new default.
- **Reachable from** — `localhost` binds the gateway to `127.0.0.1`; `lan`
  binds to `0.0.0.0`. The default is `localhost`: nothing listens beyond this
  machine until you decide it should, and the setup page is where you set the
  password that makes `lan` safe. (`nerve init --terminal` and
  `--non-interactive` keep their previous default of `0.0.0.0`.)

Answers are saved after every question (`NERVE_HOME/init-state.json`), so
Ctrl+C and a later `nerve init` resumes where you stopped. The file is removed
once the configuration is written.

What it writes:

| File | Keys |
|---|---|
| `config.yaml` (machine-local) | `workspace`, `deployment`, `telegram.enabled: false`, `sync.gmail.accounts: []`; Docker also gets `docker.extra_mounts: []` and `gateway.host`/`port` so host-side commands know the published port |
| `<workspace>/config/settings.yaml` (tracked) | `gateway.host`/`port`; every sync source explicitly `enabled: false` |
| `config.local.yaml` (owner-only, `0600`) | the session-signing secret `auth.jwt_secret` — written **only if the file does not exist**, so a re-run never signs anybody out; Docker also gets a forwarded credential, see below |
| `<workspace>/` | directories, the `config/` scaffold, the bundled skills, `config/cron/gates/` |

Deliberately **not** written here: the mode templates (`SOUL.md` and friends),
`config/cron/system.yaml`, time zone, model, memory settings. Those depend on
decisions made in the browser, and the matching step writes them.

Then `nerve init`:

1. Builds the web UI if `web/dist` is missing and Node.js is installed
   (`npm ci && npm run build`; first time only, about a minute). A failed or
   skipped build is a warning, not an error: the gateway serves a plain page
   at `/` and `/setup` that says how to build it, and `/api/*` works regardless.
2. Starts Nerve (`nerve start --foreground`, detached, logging to
   `NERVE_HOME/nerve.log`) and waits up to 60 s for `/health`.
3. Prints the setup page and the setup token, once. The token is copied to the
   clipboard when `pbcopy`, `wl-copy`, `xclip` or `xsel` is on `PATH` (always
   through stdin, never as a command argument), and a terminal offers to open
   the page.

If Nerve does not answer within 60 s: `Nerve did not answer within 60 s.
Logs: nerve logs. Re-run nerve init after fixing the cause.`

### Flags

| Flag | Effect |
|---|---|
| `--deployment server\|docker`, `--workspace PATH`, `--port N`, `--bind localhost\|lan` | Answer a question from the command line instead of the prompt |
| `--yes`, `-y` | Accept every default without asking |
| `--no-start` | Write the files but do not start Nerve (prints what to do next) |
| `--no-browser` | Do not offer to open the setup page |
| `--no-clipboard` | Do not copy the setup token to the clipboard |
| `--terminal` | The full terminal wizard — see [The terminal wizard](#the-terminal-wizard-nerve-init---terminal) |
| `--non-interactive` | Read everything from the environment — see [Unattended installation](#unattended-installation) |
| `--if-needed` | Exit silently unless this is a fresh install (used by the Docker entrypoint) |
| `--memory-backend memu\|loom` | Word the workspace templates, bundled skills and built-in jobs written now for this memory backend. Init-only: it does not set `memory.backend`, which a platform switching the instance to Loom writes into `config.local.yaml` afterwards. Without it, `nerve init` uses what an existing configuration names, else `memu`. Works with every path above |

A non-TTY stdin still answers the prompts line by line; only `--yes` skips
them. Scripted example:

```bash
nerve init --deployment server --workspace ~/nerve-workspace --port 8900 \
           --bind localhost --no-browser --no-clipboard --yes
```

### Re-running `nerve init`

On an already configured install, `nerve init` changes nothing. It prints
`Nerve is already configured.`, whether Nerve is running, the setup page and
token while the instance is still unclaimed, `Setup checklist: N steps left —
http://…/setup` while the browser checklist is unfinished, and
`Reconfigure everything: nerve init --terminal`. Exit code 0. The signing
secret in `config.local.yaml` is never regenerated by this path.

`nerve init --terminal` on a configured install asks before regenerating; see
[the terminal wizard](#the-terminal-wizard-nerve-init---terminal) for what it
overwrites and what it keeps.

## Finishing setup in the browser

`/setup` is one wizard with a progress rail on the left, Back / Continue /
Skip for now in the footer, and a Review step at the end. It works on a phone.
Each step is saved as you continue; forms open on what the instance already
has, and a stored secret is shown as "Configured · …ab12 — leave blank to
keep", never read back.

| Step | Asks | Writes | Takes effect |
|---|---|---|---|
| **Claim** (local mode only) | username, password or "Keep this installation passwordless", display name, the setup token | the account and the setup-complete record in `nerve.db`; deletes the token | immediately; you are signed in |
| **Mode** | Personal assistant (works for you: your own inboxes, notes about you) or Worker (works for a team, with its own memory, task list and schedule). For a worker, what it should do: checkboxes from the work catalog, by group (code: review pull requests, triage issues, fix issues and open pull requests, keep CI healthy, dependency and security updates; operations: first response to alerts, which needs an alert source; communication: channel digests and answers; knowledge: research and reports, docs and release notes), **Custom…** for work in your own words (at most 4000 characters), and an optional "Anything it must know or never do?" (at most 2000). Nothing ticked is fine: the conversation then offers the same list | the mode's templates (`SOUL.md`, personal: `IDENTITY.md`, `USER.md`, …); a worker's `TASK.md` from its template with what you chose under "Task description (as given)" (the [onboarding conversation](#the-onboarding-conversation) fills the other sections); mode defaults in `settings.yaml` only where absent; `config/cron/system.yaml` with the mode's default jobs if missing. With `memory.backend: loom` the templates, bundled skills and jobs name Loom's memory tools | `memory.categories` is read at startup → restart; the rest is live |
| **Model** | Anthropic API key · Claude subscription token (`claude setup-token`) · the Claude Code login found on this server · AWS Bedrock (region, optional profile); "Test connection"; then a model and effort picker | `anthropic_api_key` / `claude_oauth_token` → `config.local.yaml`; `provider.type`, `provider.aws_region`, `agent.model`, `agent.cron_model` (the picked model, so scheduled jobs run on it too), `agent.effort` → `settings.yaml`; `provider.aws_profile` → `config.yaml` | new sessions; Bedrock's memory model ids need a restart |
| **About you** (worker: **Operator**) | your name, time zone (prefilled from the browser), quiet hours as an on/off switch with two times (personal: on, prefilled 22:00–07:00; worker: off), response language, the agent's name and emoji, a sentence about you or your work | `timezone`, `quiet_start`, `quiet_end` → `settings.yaml` (both empty when quiet hours are off); the matching lines of `USER.md` and `IDENTITY.md` (personal; quiet hours off reads `none`) — only lines still at their template value are touched, your edits are never overwritten; in local mode the display name on your account | `timezone` → restart (the scheduler carries the old zone); the rest is live |
| **Memory** | an OpenAI key for vector search, or "No embeddings" (Claude ranks memories at recall time). With `memory.backend: loom` nothing: the step is done and says memory is kept in Loom | `openai_api_key` → `config.local.yaml` | the key is live; embeddings are built at the next restart |
| **Channels** | a Telegram bot token (verified against Telegram **before** it is saved), pairing; Slack bot and app tokens (verified the same way), who may talk to it | tokens → `config.local.yaml`; `telegram.enabled` / `slack.enabled` → `config.yaml`; Slack allow-lists → `settings.yaml` | Telegram → restart (the bot is built with its token at startup); Slack is live |
| **Sources** (personal) | GitHub, Gmail (addresses), Telegram message sync (API id and hash, then a one-time sign-in to your own Telegram account: phone, the code Telegram sends, your two-step password if you have one) as toggle cards, each with a server-side check of its tool (`gh`, `gog`). On an [instance that keeps no credentials](#instances-that-keep-no-credentials) only the GitHub toggle, and only when the operator provides the token from the environment | `sync.*.enabled` → `settings.yaml`; `sync.gmail.accounts` → `config.yaml`; Telegram API credentials → `config.local.yaml`; the Telegram session → `NERVE_HOME/telegram_sync.session` (owner-only, written by Telethon) | live |
| **Automation** | which optional built-in jobs run; the core set is always on; schedules shown in your time zone. Each job shows its effective state: a job with the same id in `config/cron/jobs.yaml` replaces the built-in job, and the step shows it read-only. For a worker `task-planner` starts off: the onboarding conversation turns it on when the worker should plan through the approval loop | `enabled` flags in `config/cron/system.yaml` (never for a job that `jobs.yaml` replaces) | live (the scheduler reloads) |
| **Agents** (personal) | which coding agents on this server (Codex, Claude Code) get a connection to Nerve; what to do with an existing config | the agents' own config files; `external_agents.*` and `mcp_endpoint.enabled` → `config.yaml` | restart |
| **Review and finish** | a server check-up (Claude CLI, Node, `gh`, `gog`, disk, time zone, web build), what was written where, and the apply button | nothing new | hot reload, or a restart when something above needs one |

A few things worth knowing per step:

- **Mode is chosen once.** Afterwards the step is read-only with the reason:
  changing it means a new workspace (stop Nerve, point `workspace:` in
  `config.yaml` at an empty directory, run setup again). Steps that depend on
  the mode (About you, Sources, Automation, Agents) are shown but locked until
  it is chosen.
- **Model.** "Test connection" runs on the server (not in your browser) and
  reports precisely: a rejected key, a region where Claude is not enabled, no
  network, a timeout. Subscription tokens cannot be tested against the API and
  say so. A subscription token drives chat only: memory, titles and scheduled
  jobs call the API directly and need an Anthropic API key. Paste one on the
  Model step instead (it replaces the token), set `ANTHROPIC_API_KEY` in the
  server's environment, or use the proxy from `nerve init --terminal`, the one
  route that runs those jobs on the subscription; the page says so when it
  applies. "Claude Code login on this server" is
  offered only when the server actually has one; on a Mac, opening the Model
  step may show a Keychain prompt **on the server's screen** once — Deny keeps
  the option hidden. Choosing it copies that credential into
  `config.local.yaml`, exactly as the terminal wizard does. The model you
  pick — or confirm with Continue after saving a credential, the preselected
  default included — also becomes `agent.cron_model`, so scheduled jobs run
  on it; to run them on a cheaper model, set `agent.cron_model` in
  `settings.yaml` afterwards (a later save that picks nothing leaves it
  alone; a switch into or out of Bedrock resets both to the installer's
  defaults, as before).
- **About you** prefills the time zone from your browser. Quiet hours are
  optional: on a personal install the switch starts on with 22:00–07:00 while
  the instance still has the defaults; a worker's starts off. Continuing
  accepts what the form shows; the step counts as decided. Nerve records quiet
  hours for the agent and does not hold messages back itself (see
  [Configuration → Quiet Hours](config.md#quiet-hours)).
- **Channels.** A Telegram token is checked with Telegram before anything is
  written: a well-formed but revoked token would otherwise take the bot — and
  the next start — down. The bot itself starts with the new token after the
  restart at the end of setup, which is why pairing is offered on the final
  "Nerve is ready" screen: it shows the `t.me/<bot>` link and a pairing code,
  and notices when your `/pair <code>` arrives. You can also paste your numeric
  Telegram user id in the step. Slack behind a shared channel gateway
  (`slack.mode: hosted`) shows only the on/off switch.
- **Sources** that are turned on while their tool is missing on the server
  are saved and stay idle until the tool is set up; the card says so.
- **Telegram message sync** reads the chats of your *own* Telegram account,
  so API credentials alone do nothing: after the API id and hash are saved,
  the card offers a sign-in — your phone number, the code Telegram sends,
  and your two-step password if you have one. The sign-in stays on this
  server: Telethon writes the session file under `NERVE_HOME`, owner-only;
  the page shows the phone masked and never stores the code or the
  password. One sign-in is pending at a time, for ten minutes; a new one
  replaces it. **Sign out** revokes the session and removes the file. On the
  server, `nerve setup-telegram` runs the same sign-in in the terminal. While
  the sync is on but nobody is signed in, the Sources summary, the Ready
  screen and `nerve doctor` say so.
- **Automation** lists the core jobs as always-on and the optional ones as
  checkboxes with their schedule rendered in the chosen time zone. A worker's
  `task-planner` is unchecked by default; leave it for the onboarding
  conversation, which turns it on (scoped to the worker's tasks) when the
  worker should plan through the approval loop. A job that `jobs.yaml`
  replaces shows that file's state and cannot be changed here.
- **Review and finish** shows an HTTPS warning when the page is served over
  plain HTTP from an address other than loopback, and a warning when the
  instance is passwordless and listening beyond localhost.

Step status is **derived**, never stored: a step is done because a fact in the
configuration, the workspace or the database says so. That is why an instance
configured at the terminal, from environment variables or from a config
repository shows the same checklist as one configured here, with the parts
already decided marked done. The only stored decisions are *skipped* and
"answered with a value that equals the default", kept in
`NERVE_HOME/setup-state.json` — a file that never contains a configuration
value or a secret.

Several people or tabs can have the wizard open. State lives on the server
only; every save returns the whole checklist and the page refreshes rather
than trusting what it had. The last writer wins per step.

### The ending

The Review step's button reads **Finish** when everything you changed applies
live, or **Restart Nerve** when something is read at startup (the time zone, a
Telegram token, Bedrock's memory models, external agents). The restart
reasons are listed before you click. Then:

- **Finish** reloads the configuration in place, opens the
  [onboarding conversation](#the-onboarding-conversation) and shows **Nerve
  is ready** with **Start the conversation**. When onboarding is already done
  or skipped, a personal install offers "Say hello" (a new chat with a
  suggested first message) and a worker offers "Open chat".
- **Restart Nerve** restarts the daemon and waits for the *new* process to
  answer (see [Restarts from the browser](#restarts-from-the-browser)). The
  page stays where it is, reconnects on its own and ends the same way. The
  new process opens the onboarding conversation when it starts.

### The onboarding conversation

After setup, the agent is set up by talking to it. Finish opens one web chat
session titled **Onboarding** for the person who finished setup, and the agent
speaks first: its first message is the answer to a hidden instruction, which
the chat does not show. What it says first depends on what a worker's Mode
step recorded:

- **With work chosen** (checkboxes, custom work, or both) it offers no menu
  and starts on that work in the same message. It explains before it asks:
  it greets you, restates the work in one sentence, says what it studies and
  sketches the setup it has in mind, and only then shows its first questions.
  It works from the follow-up questions of each item you ticked (which
  repositories, what a review may do, the label vocabulary, how often, and so
  on), asks the questions the items share once (the account it acts as, what
  waits for your approval, where results go), and skips what setup or its
  explorer can tell. What does not fit into two rounds of questions becomes a
  default in its proposal, for you to change. An item whose connection the
  instance lacks (first response to alerts without an alert source) appears
  under what it needs from you.
- **With nothing chosen** (a personal install, or a worker with nothing
  ticked) it says that it is set up by talking to it, offers the work catalog
  by group, each item with its one-line description (a personal install lists
  personal-assistant work first), and asks which apply. A worker with only
  "Anything it must know or never do?" filled in gets the list too.
- **With a note from an older setup page, or from `nerve init --terminal`**
  (free text), a note that describes the work is handled like chosen work; a
  note with only rules ("never merge anything") or only repositories gets the
  list.

Whatever you wrote under "Anything it must know or never do?" (or as rules in
a note) becomes hard rules and approval boundaries in its proposal, and an
answer in the conversation never loosens one without asking.

The agent runs no tool before its first message, so the hidden instruction
also gives it what Nerve knows about the instance when the conversation
opens: the mode and the workspace (cloned repositories go to
`<workspace>/repos/`), whether configuration arrives by pull request, the
model and memory, and each connection, with what is configured and where it
comes from (a file on this machine, the environment, the configuration
repository, or the platform): the GitHub token, the GitHub CLI's login when
`gh auth status` says it is authenticated, the GitHub sync source and its
repositories, Slack (in hosted mode, with the platform gateway's status),
Telegram and Gmail, and the time zone and quiet hours. It never includes a
secret. The agent takes these as facts: it does not ask about them or check
them again before its first message, and it asks only about what is marked
unknown. When the GitHub login is known, it recommends acting as that account
and asks you only to confirm it or name another, and the explorer is told the
same access. Reading the facts adds a moment to Finish: `gh auth status`,
eight seconds at most.

Then:

1. **It explores in the background.** As soon as it knows what to study
   (repositories, documentation, data or alert sources, topics), it starts
   one explorer, a background sub-agent, and says in one line what it
   studies; the explorer's progress shows on the agent line below the chat.
   The explorer only reads. It clones repositories into
   `<workspace>/repos/<name>` (with `gh` when it is signed in, else from the
   public URL) and is told to write nothing else, install nothing, build
   nothing and run no tests: it learns how to build and test from the CI
   configuration, scripts and docs, works for about ten minutes and returns a
   short findings report: how to build and test, CI, contribution and review
   rules, the volume of the work, risks and missing access, a suggested first
   task. If it fails or finds only part, the proposal says so. With nothing to
   clone or fetch (a personal assistant for the inbox and reminders) there is
   no explorer.
2. **It asks in the foreground** while the explorer works, and only what
   neither the explorer nor setup can tell. It explains first, then shows one
   form of up to six questions per round, in at most two rounds, each saying
   why it is asked and each in the shape its answer needs (the Nerve tool
   `ask_form`): repositories as `owner/name` entries, decisions as a few
   options that say what they lead to with its recommendation first and
   **Other…** for your own words, channels and topics as a list, anything
   else as free text. Typical questions: the account it acts as on the code
   host (the login it knows is its recommendation), what waits for your
   approval (its recommendation: drafts you approve for anything public, on
   its own for reading, cloning and local checks), how often to look for new
   work, and where results go. It never asks what setup recorded: the mode,
   your name, time zone and quiet hours, the connected channels and sources.
3. **It proposes the setup as a plan** once the findings and your answers
   are in, in Claude Code's plan mode. The plan has fixed headings: what it
   understood, what it found, the `TASK.md` sections (a worker), the cron
   jobs (schedule, gate, session mode and what one run does, plus the
   built-in jobs it turns on or off, such as whether `task-planner` plans
   through the approval loop), task statuses and tags (the built-in
   `backlog` and `plan_review` and any extra waits), skills, what it will
   remember, what it needs from you, the approval boundaries, and at most
   three optional extras under "Later, if you want". Nerve features appear
   only where this work needs them. The chat shows the plan in a decision
   card: **Apply setup**; **Change something…** with what should change, and
   the agent revises the plan and asks again, as often as it takes, without
   asking what you already wrote (a change request never widens what you
   allowed earlier: when it seems to, the agent keeps the narrower choice and
   asks); or **Edit** to change the plan text yourself and **Apply edited
   plan**, which tells the agent the setup was approved with your edits.
4. **It applies what you approved**: cron jobs in `config/cron/jobs.yaml`
   (then `nerve reload`, so the instance loads them; on an instance whose
   configuration arrives by pull request, it proposes the change instead),
   statuses, skills, tasks for what only you can do, notes in memory and,
   for a worker, the `TASK.md` sections. It is instructed to change nothing
   before you approve; the runtime does not enforce that (plan mode does not
   block file writes here). Then it summarizes every change with numbered
   next steps and calls the `onboarding_done` tool.

The state is one row in `nerve.db` (`onboarding`): pending, running (with the
session id), done or skipped, and the kind: the conversation, or the
[autonomous onboarding](#a-worker-set-up-without-the-browser). A worker
onboarded by an earlier version (the `TASK.md` marker is gone) counts as done.
An install from before the onboarding state that still has its marker, and a
personal install from before, count as pending: the banner offers them the
conversation once setup is complete. Nothing opens without a model credential.
When the agent's first turn fails (a model error), onboarding goes back to
pending, and the next Finish or daemon start opens a new conversation. Until
onboarding is done or skipped:

- the Ready screen leads into the conversation;
- a banner on every page except the conversation says that the agent waits
  for you, with **Continue**. While the agent waits for an answer it says
  what for: "Your agent has a question for you" with **Answer**, or "Your
  agent proposed a setup" with **Review** (`onboarding.awaiting` in
  `GET /api/setup`). Its close button hides it until the next page.
  When setup is complete and the conversation has not started (nobody pressed
  Finish, or an install from before), the banner offers **Start**, which does
  what Finish does and opens the conversation. Every signed-in person sees
  it, because the conversation sets up the shared agent and anyone who can use
  setup can continue or skip it;
- the setup page shows the conversation and **Skip onboarding**, behind a
  confirmation. A skip keeps the session and ends the reminders.

### A worker set up without the browser

`nerve init --terminal` and `nerve init --non-interactive` (`NERVE_TASK`) take
the worker's task description. With a description, the worker sets itself up
without a person: at the first start of the daemon with a model credential,
one session titled **Onboarding** (with the model of scheduled jobs) reads the
description and the worker guide, researches, fills `TASK.md`, creates
skills, cron jobs and the tasks only you can do, notifies you with a summary
and the next steps, and calls `onboarding_done`. It runs once: a restart
during the session does not start it again, and the banner in the web UI
links to the session while it runs. When its first turn fails, the next start
runs it again. Finish on the setup page opens the conversation instead, while
the session has not started.

With an empty task description, the worker waits for its onboarding
conversation in the web UI: open the web UI after the first start, and the
banner leads you to it.

### Coming back later

The wizard is re-enterable. **Setup** sits in the navigation until setup is
finished. After that, a gear button labelled **Setup** at the bottom of the
navigation rail opens it. On a phone, **Setup** stays in the **More** menu.
An instance that has never chosen a mode opens on the setup page: whoever
signs in lands on `/setup` once per visit (a link straight into the wizard
is left where it points) and is free to go anywhere else from there. A partially set-up
instance — Mode chosen, steps left — opens as usual, with a dismissible
banner that reads "Finish setting up Nerve — N steps left" (or "Restart
Nerve to apply your setup" when only the restart is left). The count
includes the steps that are waiting for the Mode step, so it only ever goes
down; steps a lockdown makes read-only are never counted. The
chat page's empty state points at the Model step until a provider is
configured. On the Overview every done step has a **Change** link; a skipped
step offers **Undo skip**. The CLI reports the same checklist: `nerve status`
prints `Setup checklist: N steps left — http://…/setup`, and `nerve doctor`
prints `[OK] Setup checklist complete` or
`[WARN] Setup checklist incomplete (steps: provider, profile) — finish at /setup`.

## The setup token

The first browser step — claiming the account — is the only write Nerve
accepts without a signed-in person, so it is guarded by the setup token
`nerve init` prints:

- It is generated once, kept in `nerve.db` across restarts, and deleted the
  moment the claim succeeds.
- It is sent only in the body of the claim request. Nerve never puts it in a
  URL, a response, a server log or browser storage, and `nerve init` never
  passes it as a command-line argument (the clipboard copy goes through
  stdin).
- `nerve status` prints it again while the instance is unclaimed. `nerve
  doctor` deliberately never does: that report is also relayed by chat
  commands, and a live credential does not belong in a chat log.
- It is a bearer credential. For a remote claim, use HTTPS or a protected
  tunnel; the Review step warns when the page is served over plain HTTP from
  a non-loopback address.

"Keep this installation passwordless" is an explicit choice: anyone who can
reach the gateway is then the owner. It is safe only when access is already
restricted — the `localhost` bind, or a tunnel. A password can be added later
on the Accounts page; it cannot be removed.

Every other setup endpoint requires a signed-in person. System and MCP tokens
are refused (`403`): setup is driven by a human, never by an agent talking to
its own API. Until the claim, those endpoints answer `409`. See
[Accounts and identity](accounts.md) for the account model.

## Restarts from the browser

Most of what the wizard writes is applied by a hot reload, the same one
`nerve reload` performs. A handful of keys are read only at startup —
`timezone`, `telegram.*`, `memory.*`, `external_agents.enabled`,
`mcp_endpoint.*` — and the full list is the restart table in
[Config](config.md#what-still-needs-a-restart); the wizard adds no entry to
it. Whether a restart is pending is computed by comparing the configuration
the running process started with against what is on disk now, so a value you
change and change back disappears from the list, and the list is empty after
a restart by construction.

When a restart is needed, the Review step's **Restart Nerve** calls
`POST /api/system/restart`, which does what `nerve restart` does, from inside
the daemon:

| Where Nerve runs | What happens |
|---|---|
| plain `nerve start` | a detached helper stops the old process and starts a new one; the page polls `/health` until its `generation` changes, up to 2 minutes |
| the generated Docker container | the process exits and Compose brings it back (`restart: unless-stopped`); up to 3 minutes. If you had stopped the container with `docker compose stop`, run `docker compose up -d` |
| systemd | refused, on purpose — the browser cannot see whether the unit says `Restart=`. The page shows the exact command (`systemctl restart nerve.service`, with the unit name read from the process's own cgroup leaf; `systemctl --user restart …` when a systemd user manager started it) and keeps waiting up to 5 minutes for the operator to run it |

`NERVE_SUPERVISOR=exit` or `NERVE_SUPERVISOR=refuse` in the daemon's
environment overrides the detection for other supervisors. The wait is never
unbounded: the overlay offers **Not now** after ten seconds, and "did not
come back" with **Keep waiting** and a `nerve logs` / `nerve start` hint at
the end. Your session survives the restart — the signing secret is pinned and
persisted, and nothing in the wizard rotates it.

## The terminal wizard (`nerve init --terminal`)

The previous `nerve init`, unchanged: it asks everything the browser would —
deployment, credential, mode, OpenAI key, workspace, password or passwordless,
your name and time zone, Telegram, sources, cron jobs, a worker's task,
external agents — and writes it all at the end. It never starts the daemon;
run `nerve start` afterwards. For a Docker deployment it continues inside the
container (`docker compose run nerve nerve init --inside-docker`). Its network
default is still `0.0.0.0`.

Use it when no browser can reach the gateway, or to reconfigure an install
from scratch. Two differences from before: your name is now actually written
into `USER.md` (it used to leave the placeholder), and it is the only path
that regenerates files. On an existing install it asks first:

**What gets overwritten:**
- `config.yaml` and `config.local.yaml` — regenerated from your choices (each
  is copied to `*.bak` first unless it holds nothing)
- `<workspace>/config/cron/system.yaml` — regenerated (picks up new built-in
  cron prompts from Nerve updates)

**What's preserved:**
- All workspace files (`SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, skills, tasks, etc.)
- `<workspace>/config/cron/jobs.yaml` — your custom crons are never touched
- `<workspace>/config/settings.yaml` — only the keys the wizard generates are rewritten; anything else you added stays
- `~/.nerve/nerve.db` and `~/.nerve/memu.sqlite` — databases are preserved

The browser checklist reads a terminal-configured install exactly as it reads
its own work: the steps you answered there are done.

## Unattended installation

For cloud-init, Ansible, image builds and other places with no terminal, run the installer with `--non-interactive` (or `NERVE_NON_INTERACTIVE=1`). It implies `NERVE_YES=1`, never prompts, and finishes with `nerve init --non-interactive`, which reads its configuration from the environment.

```bash
curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh \
  | NERVE_NON_INTERACTIVE=1 \
    NERVE_INSTALL_DIR=/home/agent/nerve \
    NERVE_MODE=worker \
    NERVE_WORKSPACE=/home/agent/nerve-workspace \
    NERVE_TIMEZONE=UTC \
    NERVE_PROVIDER=bedrock \
    NERVE_AWS_REGION=eu-central-1 \
    bash
```

Dependency installation adapts to the environment: as root (a plain `ubuntu:24.04` container, for example) packages are installed directly with no `sudo` needed, and on Debian and Ubuntu `DEBIAN_FRONTEND=noninteractive` is set so `tzdata` and friends cannot open a debconf prompt. As a non-root user, `sudo -n` is used, so a missing password rule fails immediately instead of waiting.

The daemon is not started: unattended installs are normally followed by a service manager that owns the process (see [systemd Service](#systemd-service-optional)). Set `NERVE_START=1` to start it from the installer instead.

Anything the setup wizard would ask comes from the environment. Authentication is required — set `NERVE_PROVIDER=bedrock` (IAM, no key), or `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN` or `NERVE_USE_PROXY=1`. Everything else has a default. `NERVE_PASSWORD` is read once here and stored only as a bcrypt hash in `config.local.yaml`, so it does not need to stay in the environment afterwards. `NERVE_PASSWORDLESS` is also read once here: the choice is recorded in `nerve.db`, and changing the variable later has no effect.

| Variable | Default | Purpose |
|---|---|---|
| `NERVE_MODE` | `personal` | `personal` or `worker` — this is how an unattended install chooses the mode |
| `NERVE_WORKSPACE` | `~/nerve-workspace` | Workspace directory |
| `NERVE_TIMEZONE` | `America/New_York` | Schedule timezone, written to `settings.yaml`. An empty value or a value that is not a time zone name gives `America/New_York`. When the variable is also in the environment of the Nerve process, it is the default time zone ([Configuration](config.md#core)) |
| `NERVE_PROVIDER` | `anthropic` | `anthropic` or `bedrock` |
| `NERVE_AWS_REGION` | `$AWS_REGION`, else `us-east-1` | Bedrock region; sets the model geo-prefix |
| `NERVE_PASSWORD` | unset | Web UI password. Setting it completes setup |
| `NERVE_PASSWORDLESS` | unset | `1` completes setup without a password: anyone who can reach the gateway acts as the owner (see [Accounts and identity](accounts.md)). Not allowed with `NERVE_PASSWORD`. With neither, setup stays incomplete until the browser claim (`nerve status` prints the token once the gateway runs) |
| `NERVE_TASK` | unset | Worker mode task description. With it, the worker sets itself up at its first start; without it, the worker waits for its onboarding conversation in the web UI (see [A worker set up without the browser](#a-worker-set-up-without-the-browser)) |
| `NERVE_EXTERNAL_AGENTS` | unset | Personal mode, e.g. `codex,claude-code` |
| `GH_TOKEN` | unset | GitHub integration |

This path writes the same files it always has and starts no daemon; only a
worker's defaults changed: quiet hours off (`quiet_start` and `quiet_end`
empty) and `task-planner` disabled until onboarding enables it.
The browser checklist afterwards shows what the environment decided as done —
the mode, the provider, the time zone, a Telegram token — and leaves the rest
to skip or fill in. A time zone that is equal to `NERVE_TIMEZONE` in the
environment of the Nerve process is the default, not an answer: About you
stays pending.

A re-run over an existing install keeps the configuration and only upgrades the code.

## Docker

Choose `docker` at the first question. The workspace question then reads
"Workspace on this computer (mounted into the container)", and one more
question follows when this machine has a Claude Code login:

```
  Found a Claude Code login in this machine's keychain (macOS Keychain (OAuth)). Forward it to the container? [Y/n]
```

Yes writes it into `config.local.yaml` as `claude_oauth_token` or
`anthropic_api_key` (the same classification the terminal wizard uses). A
`gh auth` token gets the same question (`Found a GitHub login (gh CLI).
Forward it to the container? [Y/n]`) and is written as `github_token` only on
yes; `--yes` accepts both. Each forward is announced (`✓ Forwarding the GitHub
token (gh CLI) as github_token in config.local.yaml`) — the values themselves
are never printed. The Model step in the browser then shows the credential as
configured. Nothing from `~/.claude` is mounted into the container.

On the host, `config.yaml` names the *container* workspace path
(`/root/nerve-workspace`). `nerve status` and `nerve doctor` derive the setup
checklist from the same files at their bind-mounted host path, read from the
volume in `docker-compose.yml`; when that file or the mount cannot be read,
`status` prints `Setup checklist: open http://…/setup` instead of a count.

Then `nerve init` generates `Dockerfile`, `docker-compose.yml`,
`docker-entrypoint.sh` and `.dockerignore` — carrying the port you chose
(`EXPOSE`, the health check, and the compose `ports:` line, which publishes
on `127.0.0.1:` only when you chose `localhost`) — runs
`docker compose up -d --build`, waits up to 300 s for `/health`, and prints
the same setup block. Existing Docker files are never overwritten; if they
publish a different port, the two lines to edit are printed.

```
    ✓ Nerve started in Docker — ready in 12.0 s
    Container restarts are automatic (restart: unless-stopped). nerve stop is docker compose down.
```

`config.yaml` records the container paths (`workspace: /root/nerve-workspace`)
and, for Docker only, the published `gateway.host`/`port`, so `nerve status`
on the host prints the right address. The tracked `settings.yaml` and the
workspace skeleton are written under the host directory that is bind-mounted
into the container.

**Subsequent starts:**
```bash
docker compose up        # Start
docker compose up -d     # Start in background
docker compose down      # Stop
docker compose logs -f   # Follow logs
```

The entrypoint is unchanged: `nerve init --if-needed --non-interactive`, then
`exec nerve start -f`. With the files the host wrote, `--if-needed` is a
no-op and the container boots straight into an instance that waits for the
browser.

**Non-interactive Docker setup** (CI / automation):
```bash
cp .env.example .env
# Edit .env with your ANTHROPIC_API_KEY (or set NERVE_USE_PROXY=1)
docker compose up
```

The entrypoint's `nerve init --if-needed --non-interactive` then configures
the instance from `.env`, exactly as before.

**Using CLIProxyAPI instead of an API key:**
Set `NERVE_USE_PROXY=1` in your environment (no `ANTHROPIC_API_KEY` required). The proxy authenticates via Claude Code's OAuth — requires a Claude Max/Pro subscription at claude.ai. See [config.md](config.md#proxy-cliproxyapi) for details. The proxy is enabled by `nerve init --terminal` or by hand, not from the browser.

**Volumes:**
| Mount | Purpose |
|-------|---------|
| `.:/nerve` | Application code (bind mount) |
| `<NERVE_HOME>:/root/.nerve` | Databases, logs, PID, sessions (`~/.nerve` by default) |
| `<NERVE_HOME>/claude:/root/.claude` | The container's own Claude Code conversation files, so sessions survive `docker compose down`; kept apart from the host user's `~/.claude` |
| `<workspace>:/root/nerve-workspace` | Workspace files (SOUL.md, tasks, skills, `config/`) |

The container paths are not conventions — the generated Dockerfile sets
`NERVE_HOME=/root/.nerve` and `NERVE_WORKSPACE=/root/nerve-workspace`
explicitly. Point them elsewhere and the mounts must follow.

A restart asked for from the browser makes the container exit, and Compose
starts it again; the page waits for the new process. `nerve restart` on the
host runs `docker compose restart`.

### Docker credential forwarding

`nerve init` resolves a credential on the host and writes it into
`config.local.yaml` for the container, using a priority waterfall:

1. **macOS Keychain — `Claude Code-credentials`** — extracts the OAuth access token from the JSON stored by Claude Code
2. **macOS Keychain — `Claude Code`** — raw API key
3. **`CLAUDE_CODE_OAUTH_TOKEN` env var**
4. **`~/.claude/.credentials.json` file** — where Linux stores Claude credentials
5. **`ANTHROPIC_API_KEY` env var**

The first match wins. The slim flow asks before forwarding; `nerve init
--terminal` passes the same credential to `docker compose run` as an
environment variable and writes it inside the container. Either way the
`~/.claude` directory is NOT mounted into the container — this avoids file
permission issues and macOS Keychain access from within Docker.

The browser's "Claude Code login on this server" option looks in a
different order — the environment variables and the credentials file first,
the Keychain last — because it runs on the gateway host at the click of a
button and a Keychain lookup can raise a prompt there (see the Model step
notes above).

## Pre-configuring connections

An operator can provide a connection before anyone opens the setup page —
from the service environment, from `config.local.yaml` written by
provisioning, from a `settings.yaml` that arrives from a configuration
repository, or by running Slack behind a shared channel gateway. The setup
page then shows that connection as **done and read-only**: the card reads
"Configured by your operator (environment)", "Managed from the configuration
repository" or "Managed by your organisation's channel gateway", with no
Replace or Remove. The page never asks for a value the operator already
provided, and a save that would overwrite one is refused (`409`, naming the
field).

| Connection | Setting(s) and layer | Environment-reference example | How the setup page reports it |
|---|---|---|---|
| Model provider | `anthropic_api_key` or `claude_oauth_token` in `config.local.yaml`; or `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN` in the service environment; Bedrock: `provider.type`, `provider.aws_region` in `settings.yaml` | `anthropic_api_key: "${ANTHROPIC_API_KEY}"` in `settings.yaml` from a configuration repository, or `Environment=ANTHROPIC_API_KEY=…` in the service unit | Model: done · "Configured by your operator (environment)"; the model and effort picker stays open |
| Memory (vector search) | `openai_api_key` in `config.local.yaml` | `openai_api_key: "${OPENAI_API_KEY}"` | Memory: done, read-only |
| Telegram bot | `telegram.bot_token` in `config.local.yaml`; `telegram.enabled` in `config.yaml` | `telegram: {bot_token: "${TELEGRAM_BOT_TOKEN}"}` in `settings.yaml`, or `config.local.yaml` written by provisioning | Channels: done; the Telegram card is read-only, pairing still works |
| Slack with your own app | `slack.bot_token`, `slack.app_token` in `config.local.yaml`; `slack.enabled` in `config.yaml`; allow-lists in `settings.yaml` | `slack: {bot_token: "${SLACK_BOT_TOKEN}", app_token: "${SLACK_APP_TOKEN}"}` | Channels: done; token fields read-only |
| Slack behind a shared channel gateway | `slack.mode: hosted` and `channels.hosted.*` in `settings.yaml` ([config.md](config.md#hosted-mode)) — no tokens on this machine | — | Channels: done · "Managed by your organisation's channel gateway · connected" or "· waiting for the gateway"; the on/off switch is the only control |
| Telegram message sync | `sync.telegram.api_id`, `sync.telegram.api_hash` in `config.local.yaml`; `sync.telegram.enabled` in `settings.yaml`. The signed-in session is runtime state (`NERVE_HOME/telegram_sync.session`), created on the setup page or with `nerve setup-telegram` | `sync: {telegram: {api_id: "${TG_API_ID}", api_hash: "${TG_API_HASH}"}}` | Sources: the credentials read as managed; the sign-in is still offered until a session exists |
| GitHub | `gh auth login` on the server; or `github_token` in `config.local.yaml` (forwarded to a container as `GH_TOKEN`); or `GH_TOKEN` / `GITHUB_TOKEN` in the environment | `GH_TOKEN` in the service environment | Sources: the card shows the login the server-side check found; a token from the environment reads "(environment)" — and on an [instance that keeps no credentials](#instances-that-keep-no-credentials) it is what keeps the GitHub toggle on offer |
| Gmail | `sync.gmail.accounts` in `config.yaml`; `sync.gmail.enabled` in `settings.yaml`; `gog gmail setup <account>` on the server | `sync: {gmail: {accounts: "${GMAIL_ACCOUNTS}"}}` | Sources: done; the addresses are read-only |

How the origin is detected: Nerve reads the configuration files as written,
before `${VAR}` references are resolved, and judges the layer the loader
takes the value from (`config.local.yaml` over `config.yaml` over
`settings.yaml`; under lockdown `settings.yaml` alone). Every masked value in
`GET /api/setup` carries a `source`: `file` (a file on this machine — the
page may replace or remove it), `environment` (a variable Nerve reads
directly, or a `${VAR}` reference in any layer), `config-repo` (the tracked
`settings.yaml` under [lockdown](#lockdown)) or `gateway` (held by the
channel gateway). Secrets stay masked whatever their source.

`nerve doctor` prints the source of every configured connection:

```
[OK] Anthropic API key: configured (environment)
[OK] Telegram bot token: ...ab12 (environment)
[OK] Slack hosted by the channel gateway: agent …, 2 gateway key(s) from …
[OK] Telegram sync: API credentials configured (config-repo)
[WARN] Telegram sync: no signed-in session at ~/.nerve/telegram_sync.session — sign in on the setup page (Sources → Telegram) or run 'nerve setup-telegram' on the server
```

Nothing is appended for a value that came from a file on this machine.

## Instances with external authentication

When a front gateway signs people in and names them to Nerve
(`NERVE_AUTH_MODE=external`, see [Accounts and identity](accounts.md#external-mode)),
the setup page works the same way with these differences:

- **There is no claim step.** There are no local accounts, no password and no
  setup token in this mode; the gateway deletes any token left from an earlier
  local mode at startup. `nerve init` in a shell with `NERVE_AUTH_MODE=external`
  prints the setup page without a token block — "Sign in through your
  gateway." / "No setup token is used in external mode." — and its last line
  reads "Later: `nerve status` prints the page again."
- **The Claude Code login on the server is not offered.** That login is the
  personal credential of whoever runs the server, and here every signed-in
  person may run setup, so the Model step does not offer to import it (the
  server does not even look it up). Paste an API key or a subscription token
  instead, or provision the credential from the environment. An operator who
  wants the option anyway sets `NERVE_SETUP_HOST_LOGIN=1` in the service
  environment.
- **The wizard starts at Mode**, with an overview that says who you are
  ("Signed in as Alice Example through your organisation's sign-in"), what the
  wizard does, and what is already configured. It configures the instance for
  everyone who uses it; nothing in it is personal to your account.
- **Any signed-in person may run setup.** There is no owner. Several people
  may have it open; the state is server-side, every save returns the whole
  checklist, and the last writer wins per step.
- **About you** takes your name from the gateway's sign-in, read-only for the
  account, and uses what you type only for the workspace notes (`USER.md`).
  No account or password control is shown, and nothing in the wizard calls an
  account endpoint.
- **Step status** comes from configuration, workspace files and the state
  file alone — the same facts as in local mode. An instance provisioned from
  the environment (`nerve init --non-interactive`, a credential in the
  process environment) or from a config repository reads as done where those
  decided; a connection the operator provided shows as configured and
  managed, read-only, and the page never asks for it again — see
  [Pre-configuring connections](#pre-configuring-connections) for the
  settings per connection and how each is reported. Slack behind your
  organisation's channel gateway shows the gateway's connection state and an
  on/off switch only.
- **Finish** prefers a hot reload. When a restart is unavoidable, the page
  says what will happen for the supervisor it detects — the process exits and
  the supervisor starts it again, or an operator must run the command shown —
  and reconnects through the gateway's own sign-in; no login form can appear.
  The wait is time-boxed with a clear message and a retry.
- The **Setup** entry and the "Finish setting up" banner render in this mode
  too, and so does the landing: an instance that has never chosen a mode
  opens on the setup page once the gateway has signed the person in; a
  partially set-up one shows the banner.

## Lockdown

Under [lockdown](config.md#lockdown-remote-only-read-only) configuration
arrives by pull request, so the wizard is read-only for anything it would
write to configuration or to a reviewed workspace file. Such steps read
**blocked**, each with the reason ("Configuration is managed from the config
repository on this instance; changes arrive by PR and sync. Configure this
step from the config repository."), and are not counted as "left to do" —
a locked instance that has everything provisioned reads as finished and the
banner stays quiet. What still works: the claim (it writes to `nerve.db`), a
worker's `TASK.md` once the templates have arrived from the repository, your
display name (local mode), skipping, finishing and restarting.

## Instances that keep no credentials

Some instances never hold a credential on the machine Nerve runs on: the model
credential arrives from the service environment, Slack runs through a channel
gateway, and nothing a person pastes into the setup page should end up in
`config.local.yaml`. For those, set `setup.local_secrets: false` in the
tracked `workspace/config/settings.yaml`:

```yaml
setup:
  local_secrets: false
```

or `NERVE_SETUP_LOCAL_SECRETS=0` in the service environment. The variable
works like `NERVE_LOCKDOWN`, one way only: it can turn the storage off over a
file that allows it, never back on over a file that forbids it. The value must
be `true` or `false`: anything else (a typo, a `${VAR}` that resolves to
something else) stops the configuration from loading, as it does for
`lockdown`, and a bare `local_secrets:` reads as `false`. The setting is read
on every request, so a reload applies it. The wizard then says so instead of
offering the fields:

- **Memory**, **Sources** and **Agents** read "Not available on this
  instance", with the reason: *This instance does not store credentials
  entered here. Connections are provided by the operator of this
  deployment.* They are not counted as left to do, so the checklist can
  finish without them.
  Recall is LLM-ranked, as it is on any instance without an OpenAI key.
- **Sources** stays open for one toggle when the operator provides the
  GitHub token from the environment (`GH_TOKEN` or `GITHUB_TOKEN` in the
  service environment, or `github_token: "${VAR}"` in a configuration file):
  switching the GitHub source on stores nothing here. The step then offers
  GitHub alone, with the note *GitHub is provided by the operator of this
  deployment; Gmail and Telegram are not available on this instance.* It
  counts as left to do until it is answered or skipped, and reads done — with
  the switch still open, like the hosted Slack's — once GitHub is on or the
  answer was "no". A Gmail or Telegram field in the same save is refused
  (`409`, naming the field). `GET /api/setup` says which sources are on offer
  in the step's detail (`available`, with `unavailable_reason`).
- **Channels** reads the same way — unless Slack runs through a channel
  gateway (`slack.mode: hosted`), which stores no token on this machine: the
  step is done and the on/off switch stays its one control, as before.
- **Model** still has to be answered. A credential from the environment (or a
  `${VAR}` reference) reads as configured and managed, exactly as under
  [Pre-configuring connections](#pre-configuring-connections); AWS Bedrock
  can be chosen (an IAM role, no secret on disk); the model and effort picker
  stays open. Pasting an API key or a subscription token is refused (`409`,
  naming the field), and so is copying the Claude Code login found on the
  server.
- A step the operator provided — an OpenAI key or a bot token as a `${VAR}`
  reference, a source switched on in the tracked settings — stays done and
  reads read-only.
- The writes behind those steps answer `409` with the same two sentences:
  the Memory, Channels, Sources and Agents saves (the Slack switch in hosted
  mode and the GitHub switch with a token from the environment excepted), the
  OpenAI key test and the bot-token tests, Telegram pairing, the Telegram
  sign-in, and skipping one of the closed steps (there is nothing to decide).
  The Model step's connection test is not among them: Bedrock and a
  credential from the environment are still worth testing.

What stays available: the model provider from the environment, Slack through
a channel gateway, the GitHub source when its token comes from the
environment, the model and effort choice, About you, Automation, the account
(local mode), skipping the steps that are offered, finishing and restarting.
`GET /api/setup` carries `local_secrets: false`, and `nerve doctor` prints one
line about it — naming the Gmail and Telegram sources rather than the whole
Sources step when the GitHub token comes from the environment.

Unlike [lockdown](#lockdown), this does not change which configuration files
are read or what the agent may edit; it only governs what the setup page
stores. `nerve init --non-interactive` is not affected either: what the
environment provides at install time is written as before.

## Manual Configuration

The wizard handles all of this automatically, but you can also configure manually:

```bash
# Create secrets file (gitignored)
cat > config.local.yaml << 'EOF'
anthropic_api_key: sk-ant-...
# claude_oauth_token: sk-ant-oat01-...   # alternative: a Claude subscription token (chat only)
openai_api_key: sk-...           # Optional — enables vector-based memory search

telegram:
  bot_token: "123456:ABC..."

auth:
  password_hash: "$2b$12$..."    # Generate below — see the note under this block
  jwt_secret: "..."              # Optional; if omitted, generated into nerve.db on first start
  jwt_expiry_hours: 720          # Optional — web-session idle timeout (default 30 days)
EOF
```

`auth.password_hash` here is a **starting** password, not where the password
lives. The first start copies it onto the local account and removes the key from
this file; after that, passwords are managed on the accounts screen in the web
UI and this key does nothing. If you leave it out, setup is not complete until
you claim the instance from the browser (the first step of `/setup`).
See [Accounts and identity](accounts.md).

`jwt_expiry_hours` is an **idle** timeout, not a cap on a working session: the
gateway re-mints the token whenever a request arrives past half its lifetime,
so a tab in continuous use is never logged out. Only a tab left untouched for
the whole window comes back to a password prompt. Lower it if the browser is
somewhere you don't fully trust.

### Generate auth credentials

```bash
# Password hash
python -c "import bcrypt; print(bcrypt.hashpw(b'yourpassword', bcrypt.gensalt()).decode())"

# JWT secret
python -c "import secrets; print(secrets.token_hex(32))"
```

## First Run

```bash
nerve status             # Running? Setup page and token while unclaimed; checklist while unfinished
nerve doctor             # Verify everything is set up (never prints the token)
nerve logs               # Tail the daemon log
```

The gateway boots with nothing configured — no provider, no mode — and stays
idle rather than failing: memory warm-up and sync sources wait for their
configuration, the chat page shows "Connect a model to start a conversation",
and a Telegram bot whose token is rejected at startup is logged (token
scrubbed), reported on the Channels step and by `nerve doctor`, and never
stops the gateway from starting.

## HTTPS Setup

```bash
# Install mkcert
sudo apt install libnss3-tools
curl -L https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-linux-arm64 -o mkcert
chmod +x mkcert && sudo mv mkcert /usr/local/bin/

# Create certificates
mkdir -p ~/.nerve/certs
mkcert -install
mkcert -cert-file ~/.nerve/certs/cert.pem -key-file ~/.nerve/certs/key.pem \
  localhost 127.0.0.1 "$(hostname)" "$(hostname).local"
```

Update `config.yaml`:
```yaml
gateway:
  ssl:
    cert: ~/.nerve/certs/cert.pem
    key: ~/.nerve/certs/key.pem
```

### Trust CA on Mac (for remote access)

```bash
# On Pi: copy the CA cert
cat "$(mkcert -CAROOT)/rootCA.pem"

# On Mac: save to file and trust
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain rootCA.pem
```

## Running Nerve

### Daemon Mode (recommended)

Nerve has built-in daemon management. No systemd required for basic usage.

```bash
nerve start           # Start as background daemon (nerve init already did this once)
nerve stop            # Stop the daemon (graceful, 15s timeout)
nerve restart         # Stop + start
nerve status          # Show PID, memory, uptime, setup state
nerve status -f       # Show status then tail logs
nerve logs            # Tail the daemon log

nerve start -f        # Run in foreground (for debugging)
```

**PID file:** `~/.nerve/nerve.pid`
**Lock file:** `~/.nerve/nerve.lock` (the daemon locks it while it runs)
**Log file:** `~/.nerve/nerve.log`

`nerve restart` and the setup page's **Restart Nerve** use the same detached
helper, so a restart typed here and one clicked in the browser are the same
restart.

### systemd Service (optional)

For auto-start on boot, create `/etc/systemd/system/nerve.service`:

```ini
[Unit]
Description=Nerve Personal AI Assistant
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/home/YOUR_USER/nerve
Environment=PATH=/home/YOUR_USER/nerve/.venv/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/home/YOUR_USER/nerve/.venv/bin/nerve start --foreground
Restart=always
RestartSec=10
RestartPreventExitStatus=65

[Install]
WantedBy=multi-user.target
```

Note: Use `--foreground` with systemd since it manages the process lifecycle.
If you set up the instance with `nerve init` first, stop the daemon it
started (`nerve stop`) before enabling the unit. `RestartPreventExitStatus=65`
keeps systemd from restarting a Nerve that refused a database newer than its
code, where every restart would refuse again (see
[Going back to an older Nerve](#going-back-to-an-older-nerve)).

```bash
sudo systemctl daemon-reload
sudo systemctl enable nerve
sudo systemctl start nerve

# Check status
sudo systemctl status nerve
journalctl -u nerve -f
```

Under systemd the setup page does not restart Nerve itself; it shows
`systemctl restart <unit>` for you to run (`systemctl --user restart <unit>`
when a systemd user manager started it) and waits for the new process.
`nerve restart` on the box still stops the process and lets the unit respawn it.

## Dependency versions

`uv.lock` pins the exact version of every dependency, transitive ones included.
`uv sync` installs from it, so the same commit resolves to the same versions
whenever and wherever you install. That is why `uv sync` is the documented install
rather than `uv pip install -e .`.

Two honest limits on "reproducible": the lock is universal but its entries carry
platform and Python markers, so different platforms legitimately get different
*files* for the same pinned versions; and artifacts are not vendored, so an
air-gapped install still needs a populated uv cache or a local wheelhouse.

```bash
uv sync                    # runtime dependencies
uv sync --extra test       # ...plus the test extra
```

`uv sync` creates and manages `.venv` itself, installs Nerve editable, and
removes anything not in the lock — so the environment matches the lock exactly
rather than accumulating leftovers.

> **`uv pip install -e .` does not read `uv.lock`.** uv's pip-compatible layer has
> no lockfile awareness, so that command resolves against the bounds in
> `pyproject.toml` and can install different versions. It still works, and it is
> what you want when deliberately testing against current upstream — but it is not
> a reproducible install.

### Upgrading an installation that predates the lockfile

`nerve upgrade` runs the updater code that is **already loaded in memory**, then
pulls. So the first upgrade across the commit that introduced `uv.lock` still uses
the old, unpinned installer — the lock only takes effect from the *second* upgrade
onward. Two consequences worth knowing before you run it:

- That first upgrade resolves dependencies fresh, so it can pick up something newer than the lock intends.
- If you are on Python 3.12, it will **fail after `git pull` has already advanced the checkout**, because the new `pyproject.toml` requires 3.13+. You are left with new source and an old environment.

Either is straightforward to recover from — do the install step yourself, on 3.13+:

```bash
cd <your nerve checkout>
uv sync --locked --inexact
nerve restart
```

Subsequent `nerve upgrade` runs install from the lock automatically.

### Going back to an older Nerve

Going back to an older Nerve after an upgrade is refused by default: the
database schema is then newer than the code, and older code on a newer schema
fails quietly later rather than loudly at start. The check covers each
migration track on its own, so a Nerve without a fork's migrations also
refuses a database that fork migrated (see
[Migrations](architecture.md#migrations)). The refusal is one line, naming the
track and both versions. `nerve start --foreground`, which is what a
supervisor runs, and every command that opens the database then exit with
status 65. A supervisor can key on it, for example with systemd's
`RestartPreventExitStatus=65`, as in the unit above. The daemonizing
`nerve start` does not pass the status on: it checks the process it started
once, a second later, and only if that process has exited by then does it
print `Nerve failed to start (exit code 65)` and exit with status 1. The
refusal line is in the log file either way. Restore the backup taken before
the upgrade instead, or set `NERVE_ALLOW_NEWER_SCHEMA=1` to start anyway and
accept that risk.

### Changing a dependency

Edit `pyproject.toml`, then relock and commit `uv.lock` alongside it:

```bash
uv lock
```

CI runs `uv sync --locked`, which fails if `uv.lock` and `pyproject.toml` have
drifted apart — so a dependency change without a relock is caught rather than
silently re-resolved.

Relocking preserves existing pins, so it won't sweep in unrelated upgrades. To
move one deliberately:

```bash
uv lock --upgrade-package mcp    # one dependency
uv lock --upgrade                # everything
```

### How CI enforces this

`ci.yml` installs with `uv sync --locked`, which does two jobs at once:

- A PR is never broken by an upstream release that has nothing to do with it — the versions come from the lock.
- A dependency change cannot merge without a relock. `--locked` fails if `uv.lock` disagrees with `pyproject.toml` for *any* reason: a dependency added, removed, or simply re-bounded. Since the relock then moves the pins, the versions CI tests are always the ones the change actually selects.

That second property is why no separate unpinned CI job is needed. Widen a bound and
the lock is invalidated; relock and the pin moves; CI tests the moved pin.

### What is and isn't covered

| Path | Installs from `uv.lock`? |
|---|---|
| `uv sync` (quick start, server install, `install.sh`) | yes |
| `nerve upgrade` | yes — `uv sync --locked --inexact` when a `uv.lock` is present |
| CI (`ci.yml`) | yes — `uv sync --locked` |
| Docker | yes — the image installs from the lock at build time and the entrypoint syncs the project |
| `uv pip install -e .` / plain `pip install -e .` | **no** — resolves from `pyproject.toml` bounds |

Docker keeps its environment at `/opt/nerve-venv`, outside the `/nerve` bind mount,
because a `.venv` under `/nerve` would be shadowed by the host's checkout (and a
host-created one may not be Linux-compatible).

> **Existing Docker deployments need one manual step.** `nerve init` does not
> overwrite Docker files that already exist, so an install generated before this
> change keeps its old pip-based `Dockerfile` and entrypoint. To pick up the locked
> build, delete them and regenerate:
>
> ```bash
> rm Dockerfile docker-entrypoint.sh
> nerve init --terminal            # choose "docker" again; regenerates both
> docker compose build --no-cache
> ```
>
> Keep `docker-compose.yml` — it is unchanged. Check any local edits you made to
> the old files before deleting them.

`nerve upgrade` uses `--inexact`, so it won't uninstall optional extras you added
yourself, and `--frozen`, so it never rewrites `uv.lock` as a side effect of
upgrading. If uv is missing, or the checkout has no `uv.lock`, it falls back to the
previous `pip install -e .` behaviour.

## Troubleshooting

### Setup

**I lost the setup token.** `nerve status` prints it again while the instance
is unclaimed. Nothing else does.

**`/setup` says the web UI has not been built.** Node.js was missing or the
build failed when `nerve init` ran. On the server: `cd web && npm ci && npm run
build` (or `nerve upgrade`), then reload the page. The API — and the rest of
the checklist, once the UI exists — was working the whole time.

**"Port 8900 is already in use by another program."** Accept the proposed
port, or pass `--port`. The address `nerve init` prints carries the port it
actually used; `nerve status` repeats it.

**"Nerve did not answer within 60 s."** Read `nerve logs`, fix the cause, and
run `nerve init` again — it resumes with your saved answers, or prints the
status block if the files were already written.

**"Telegram bot failed to start at the last boot."** The saved token was
rejected by Telegram when the gateway started. The gateway is running without
the bot; replace the token on the Channels step (it is verified before it is
saved) or in `config.local.yaml`, then restart.

**The page says "Nerve did not come back".** The restart was requested but no
new process answered within the time box. On the server, check `nerve logs`
and run `nerve start` if nothing is running, then **Keep waiting** or reload
the page.

**A Keychain prompt appeared on the server's screen (macOS).** Opening the
Model step looks for a Claude Code login once per process; Deny keeps the
option hidden and nothing else changes.

### Database Schema Issues

Nerve auto-migrates the SQLite database on startup. If a migration fails or the schema gets out of sync, you can inspect and fix it manually.

**Check current schema version:**
```bash
sqlite3 ~/.nerve/nerve.db "SELECT version FROM schema_version"
```

**Verify sessions table columns:**
```bash
sqlite3 ~/.nerve/nerve.db "PRAGMA table_info(sessions)"
```

Expected columns (as of V3): `id`, `title`, `created_at`, `updated_at`, `source`, `metadata`, `status`, `sdk_session_id`, `parent_session_id`, `forked_from_message`, `connected_at`, `last_activity_at`, `archived_at`, `message_count`, `total_cost_usd`, `last_memorized_at`.

**Add a missing column manually:**
```bash
sqlite3 ~/.nerve/nerve.db "ALTER TABLE sessions ADD COLUMN last_memorized_at TEXT"
```

After any manual schema fix, restart Nerve: `nerve restart`.
