# Sources

## Overview

Sources are cursor-based data streams that persist records to a local inbox
(`source_messages` table). Most fetch an external service. Channel sources
drain records that Slack or Telegram already delivered to a local buffer. The
architecture follows a **producer/consumer** pattern inspired by Kafka:

- **Producers** (source runners) — Fetch or drain records, preprocess them, persist them to the inbox, condense long ones, and advance the source cursor. No agent processing happens here.
- **Consumers** (agent tools) — Read from the inbox using independent persistent cursors. Multiple consumers can read the same messages without interfering with each other.

```
PRODUCERS (sources):
  Source.fetch(cursor, limit)
      ↓
  Source.preprocess(records)          ← source-specific cleanup (e.g., strip email boilerplate)
      ↓
  InboxFilter.partition(records)      ← guardrail: drop records that fail allow/deny rules
      ↓
  Persist to source_messages table    ← inbox storage (with TTL)
      ↓
  SourceRunner._condense_long_content ← LLM condensation via Haiku (if condense: true)
      ↓
  SourceRunner._run_drop_hook         ← dropped records to the drop hook (if drop_hook is set)
      ↓
  Advance source cursor in SQLite     ← always, no processing dependency

CONSUMERS (agent tools):
  list_sources(consumer="inbox")      → discover sources + unread counts
  poll_source(source, consumer, ...)  → read NEW messages (advances consumer cursor)
  read_source(source, ...)            → browse historical messages (no cursor advancement)
```

## How It Works

### Ingestion (Producer Side)

1. **Fetch** — The source adapter reads an external service (gh CLI, gog CLI, Telethon) or a local channel buffer and returns normalized `SourceRecord` objects with an opaque cursor
2. **Preprocess** (`source.preprocess()`) — Each source can override this for programmatic cleanup. Gmail strips boilerplate paragraphs (legal disclaimers, unsubscribe blocks, tracking URLs). Default: no-op
3. **Guardrail** — An optional `InboxFilter` drops records that fail the source's allow/deny rules (see [Guardrails](#guardrails-inbox-filtering)). Dropped records are never persisted
4. **Persist** — Records are saved to the `source_messages` table with a configurable TTL. A record identical to the stored one is skipped: it is not condensed and not counted as ingested.
5. **LLM condensation** (`condense: true`) — Stored records over 800 chars are sent to a fast model (Haiku) that extracts only essential information, saved as `processed_content`. Configurable per source, runs concurrently with a 30s timeout per record, falls back to original content on failure
6. **Advance** — Source cursor is saved to SQLite after successful persistence

### Consumption (Consumer Side)

Consumers read from the inbox using the `poll_source` agent tool with a named consumer cursor:

1. **Discover** — `list_sources(consumer="inbox")` shows available sources with unread counts for the consumer
2. **Poll** — `poll_source(source="github", consumer="inbox")` returns new messages since the consumer's last read position
3. **Act** — The agent creates tasks, memorizes facts, or ignores noise based on message content
4. **Advance** — Consumer cursor automatically advances to the last message read

**Key properties:**
- Consumer cursors are independent — multiple consumers can read the same source at different positions
- New consumer cursors initialize to the latest message (no backlog flooding)
- Before a source's first run, each consumer that reads every source gets a live cursor on the new source at its newest message (a live cursor that is already there stays as it is), so the first records of the new source are unread for that consumer and its `messages` gates see them. Such a consumer has a cursor on each other source that has messages, as `poll_all_sources` leaves it. A consumer that reads only some sources with `poll_source` gets no cursor on the new source
- Consumer cursors expire after N days of inactivity (configurable, default: 2 days); a `messages` gate that checks the consumer renews the cursors it reads (see [cron.md](cron.md#messages))
- Cross-source deduplication happens naturally because one agent session sees messages from all sources

### Default Consumer: `inbox-processor`

A persistent cron job (`inbox-processor`) runs every 15 minutes:
1. Calls `list_sources(consumer="inbox")` to check for unread messages
2. Polls each source with unread messages
3. Reviews all messages in one session — naturally deduplicates cross-source events (e.g., GitHub notification + email about the same issue)
4. Creates tasks, memorizes facts, or ignores noise

## Built-in Sources

### Gmail
- **Adapter:** `nerve/sources/gmail.py` — uses `gog gmail messages search` + `gog gmail get` CLI
- **Cursor:** Epoch timestamp from Gmail's `internalDate` (the receive timestamp Gmail uses for `after:` filtering)
- **First run:** Fetches emails from the last 24 hours (`newer_than:1d`)
- **Subsequent runs:** Uses `after:<epoch+1>` with client-side dedup (Gmail's `after:` has ~2s tolerance window)
- **Two-step fetch:** Search returns metadata only; body + `internalDate` are fetched per-message via `gog gmail get` (up to 5 concurrent)
- **Default schedule:** `*/15 * * * *` (every 15 min)

### IMAP
- **Adapter:** `nerve/sources/imap.py` — plain `imaplib`, no CLI or provider API
- **Scope:** Any IMAP mailbox the Gmail source cannot reach (that one goes through the `gog` CLI / Gmail API), e.g. GMX, Fastmail, a company server
- **Instances:** One source per configured account (`imap:<label>`), each with an independent cursor
- **Cursor:** `<UIDVALIDITY>:<max_uid>` — UIDs are only stable within a UIDVALIDITY, so a server-side reset is detected and re-baselined instead of trusted
- **First run:** `SINCE` window of `initial_lookback_days` (default 1)
- **Subsequent runs:** `UID SEARCH <last+1>:*`, filtered client-side (the `N:*` range always returns at least the highest UID); oldest UIDs are processed first when a batch is full
- **Failure policy:** A search or message fetch/parse failure fails the batch and leaves the cursor unchanged. `SourceRunner` reports the error and applies its health/backoff policy. Fix or remove a permanently malformed message, then the next run retries it; manually advancing the cursor is an operator recovery action when skipping that UID is confirmed safe
- **Blocking I/O:** the whole IMAP conversation runs in a worker thread via `asyncio.to_thread`
- **Credentials:** passwords live in `sync.imap.passwords` keyed by username (put them in `config.local.yaml`); an account with no password is skipped with a warning instead of failing the sync
- **Optional image pass:** see below
- **Default schedule:** `*/30 * * * *` (every 30 min)
- **Disabled by default**

#### Reading mail that is only legible as an image

Some senders put the part you actually care about in an inline image — a
scan, a photo, a rendered document — so the body text is useless to an
agent. `sync.imap.match` singles those messages out and `sync.imap.vision`
runs a multimodal model over the image at ingest time, so the inbox
consumer gets plain text.

Both are inert by default: with no match rules, this is a plain mailbox
source and no model is ever called.

`vision.prompt` and `vision.answer_key` are a **matched pair** — the prompt
tells the model which label to emit, and the parser reads the line after
exactly that label. Change one without the other and every message silently
comes back as `unknown_answer`, which looks like the model degrading rather
than a config mistake. Leave `answer_key` empty to just take the first
non-empty line.

```yaml
sync:
  imap:
    enabled: true
    accounts:
      - host: imap.example.net
        username: me@example.net
        label: scans              # source name becomes imap:scans
    passwords:                    # config.local.yaml
      me@example.net: "..."
    match:
      sender_contains: ["scans@example.net"]   # substrings of From:
      attachment_contains: ["scan"]            # substrings of Content-ID / filename
      only_matched: true                       # drop everything else at the source
    vision:
      enabled: true
      model: ""                   # empty = memory.fast_model
      prompt: |
        Read the scan. Answer with EXACTLY one line:
        Sender: <company or person, or "unreadable">
      answer_key: "Sender:"
      unknown_answer: "unreadable"
      summary: "[{label}] scan from {answer}"
      content: "{vision}\n\nSubject: {subject}\nDate: {date}"
      summary_unknown: "[{label}] unreadable scan"
      content_unknown: "The image could not be read.\n\nSubject: {subject}"
```

Wording templates accept `{label} {answer} {vision} {subject} {sender}
{date} {body}`, where `{answer}` is the parsed line and `{vision}` the full
model reply. An unknown placeholder logs a warning and leaves the template
visible rather than failing the fetch.

### GitHub
- **Adapter:** `nerve/sources/github.py` — uses `gh api notifications` CLI
- **Cursor:** ISO 8601 `updated_at` of the first notification in the last listing (GitHub lists the most recently notified thread first)
- **Paging:** every page since the cursor is read in one run (`gh api --paginate`), so a backlog after downtime is ingested in full; `batch_size` does not cap this source
- **First run:** Fetches from the last 24 hours
- **Subsequent runs:** Uses `since=<cursor>` (inclusive: the cursor's own second is listed again, and an unchanged re-listed thread is skipped by storage) with `Z` suffix (not `+00:00` — the `+` in a URL query string is decoded as a space, breaking the filter)
- **Filter:** `sync.github.inbox` decides which threads are listed. `participating` (the default) sends `participating=true`: only the threads the account takes part in (assigned, review requested, mentioned, its own, commented on). `all` leaves that parameter out, so GitHub lists the account's whole inbox as its GitHub watch settings produce it: the participating threads plus `subscribed` threads of watched repositories (new issues and pull requests, per the repository's watch or Custom watch setting), `ci_activity` for the account's own workflow runs, and whatever else GitHub puts in the inbox; the guardrails pick within it (see [GitHub reason guardrail](#github-reason-guardrail)). Both modes send `all=true` (read threads too). GitHub marks a thread read as soon as a mail client renders the notification email with its images, which can happen seconds after the notification, so a listing of unread threads alone can miss a thread before anyone has seen it; a re-listed thread whose stored record is unchanged is skipped by storage, and the record carries no read state. A thread that GitHub lists as read and that was last read more than a minute before its update is left out: GitHub moves the thread of the account's own merge or review and leaves it read. Note that in participating mode it never returns `ci_activity` — workflow-run notifications are not "participating", so CI never reaches the inbox through this source in participating mode (verified against a live account: 25 `ci_activity` listed with `all=true` alone, 0 once `participating=true` is added)
- **Enrichment:** Each notification is enriched with actual content from the subject (PR/issue body, state, assignees, labels) and the latest comment, fetched in parallel (up to 5 concurrent `gh api` calls). A `mention`, `assign`, `review_requested` or `team_mention` thread also gets the human comments posted since the readers of the inbox last read a record of the thread, newest first (the latest comment is often a bot's, and a human comment before it would be lost). They are listed with `since`: the listing of one thread's comments takes no sort order and returns the oldest first
- **Mentions:** GitHub's `reason` belongs to the thread, not to the update: once the account was mentioned anywhere in a thread, every later notification of that thread says `mention` (GitHub: "The reason remains as mention, regardless of whether you're ever mentioned again"). For an issue or pull request with that reason, the source reads what was posted since the readers of the inbox last read a record of the thread, and says in the record whether that mentions the account:
  - `mention: new`: it does. With `mention_at`, and with `mention_by` and `mention_url` when the source read the text; the text is quoted in the content under `--- Mention ---`, also when a bot wrote it
  - `mention: old`: it does not, and an earlier mention (`mention_at`) explains GitHub's reason. The summary line ends with `(update, no new mention)` instead of `(mention)`
  - no `mention` key: not known, and the record says `mention` as GitHub does. That is the case when the login could not be read, when a read failed, and when no mention of the account was found at all. A record is moved away from `mention` on evidence only

  `reason` stays `mention` in all three cases, so the [reason guardrail](#github-reason-guardrail) works as before. The description and the comments are checked against GitHub's own mention events of the thread (one GraphQL call), which also date a mention that an edit added. A pull request's reviews and inline review comments are checked by their text, as a whole `@login` outside Markdown code: GitHub writes no mention event for them, though it notifies. The login is the account `gh` acts as, read in every run that has such a thread and never kept between runs, so another account or a renamed one is used from the next run on. Not checked: `team_mention`, and subjects other than issues and pull requests

  **New until read.** Storage keeps one record per thread and replaces it when the thread moves, and a reader comes on its own schedule (by default the inbox job every 30 minutes, this source every 15). So "new" is counted from the last record of the thread that every live consumer of the source has read: a mention that one record says is `new` is still `new`, with its text, in the record that replaces it, and it becomes `old` only in a record that follows one a reader has read. A consumer that stopped reading counts until its cursor expires (`sync.consumer_cursor_ttl_days`); until then a mention can be `new` again in later records of the thread, with the same `mention_at`. Where each look ended is remembered in the process. After a restart a thread is checked for the time since the sync cursor, and no later than a mention that an unread record says is `new`
- **Metadata:** `reason`, `repo_name`, `actors` (every login involved) and `ci_branch` (branch of a CheckSuite run, `""` otherwise) — all four are guardrail-filterable, see [Guardrails](#guardrails-inbox-filtering). A `mention` thread can also carry `mention`, `mention_at`, `mention_by` and `mention_url` (see Mentions above). The summary line and the metadata reach a reader of the inbox as they are, also when the content was condensed
- **Default schedule:** `*/15 * * * *` (every 15 min)
- **Hosted mode:** with `sync.github.mode: hosted` the channel gateway polls GitHub instead; see below

#### Hosted GitHub source

With `sync.github.mode: hosted`, this process does not poll GitHub and needs no
`gh` login. The shared channel gateway (the one that serves
[hosted Slack](config.md#hosted-mode), set up with the same `channels.hosted`
settings) polls GitHub for the agent with the GitHub credential connected to
that agent, builds each record the way the polling source does, enrichment
included, and keeps the records until Nerve reads them, also while Nerve is not
running. Nerve reads them over the hosted channel streams.

- **What arrives:** `github_notification` records, one for each notification
  thread whose `updated_at` moved, with the summary, content and metadata of the
  polling source. With `pr_watch: true` (the default) also `github_pr_activity`
  records: one whenever one of the agent's own open pull requests changes —
  its state, head commit, review decision, CI checks, or a comment or review by
  someone else. A pull request seen for the first time gives no record. Such a
  record has the notification metadata plus `number`, `state`, `head`,
  `review_decision` and `changes`, a list of `state`, `head`, `ci`, `review`
  and `comment`. The agent is always its author, so its `reason` is always
  `author`, and its `actors` are the reviewers and commenters. Only
  `allow_repos` and `deny_repos` apply to it, at the gateway and here; the
  actor, reason and CI branch lists are not checked for it (see
  [Pull request activity records](#github-pull-request-activity-records)).
  A gateway that supports it also delivers `github_thread_item` records: one
  for each comment, review, review comment or description of a thread, with
  its author. Nerve says that it accepts them with a
  `{"provider": "github_thread_items", "filters": {}}` entry next to the
  notifications in its subscriptions, sent while `enabled` and `thread_items`
  are true; a gateway that does not know the entry ignores it. With
  `thread_items: false` the entry is left out, and the gateway keeps sending
  one record per thread. Items add no thread to the inbox: they come only
  from issue and pull request threads whose notification passes the published
  filters, and a gateway that sends them sends each new comment, review and
  description as its own record and leaves that text out of the thread's
  notification record. Such a record carries
  `repo_name`, `repo_url`, `subject_type` (`Issue` or `PullRequest`),
  `subject_url`, `number`, `state`, `title` (absent when the gateway withholds
  it), `item_kind` (`body`, `comment`, `review` or `review_comment`),
  `item_id`, `item_url`, `author`, `author_kind` (`human` or `bot`),
  `author_relation` (`self`, `member`, `team`, `collaborator`, `app`, `agent`,
  `listed` or `outside`), `mentions_agent` (a boolean here) and, on a review,
  `review_state`. Its message starts with fixed lines that show these values
  (the repository, the thread, the item, the author with their relation in
  words, whether it mentions the agent, and the links), then the item's text.
  Its `actors` are its author, so `allow_actors` and `deny_actors` apply to
  the author and `allow_repos` and `deny_repos` to its repository. The reason
  lists bound items as they bound notifications, but at the gateway: it judges
  the thread by `allow_reasons` and `deny_reasons` with the reason of the
  notification that brought the items, before it writes them. The record
  itself has no reason, so the guardrail here does not check the reason and CI
  branch lists for it again. Nerve shows what the gateway sent and decides
  nothing from it. A record without
  these keys and values, or with a control character or a line break in a
  value, is refused on its own.
- **The credential is the switch.** The gateway polls GitHub for an agent only
  while a GitHub credential is connected to that agent and Nerve subscribes,
  which it does while `enabled` is true. Your operator can therefore set
  `sync.github.enabled: true` in `config.yaml` or `config.local.yaml`, which
  outrank the `settings.yaml` that the setup page writes. GitHub then arrives
  once a GitHub credential is connected to the agent and stops when it is
  removed, with nothing to switch in Nerve. The Sources step of the setup page
  shows GitHub as managed by the channel gateway, with no switch, and refuses a
  save that includes it (`409`, naming `github`); Gmail and Telegram work as
  before. Until a gateway stream opens, the step says that it is waiting for
  the channel gateway and that a GitHub credential may still need to be
  connected: Nerve cannot tell those two cases apart. Once a stream is open,
  it says that the stream is connected and that records arrive once a GitHub
  credential is connected to the agent, and never that GitHub is connected,
  also when the page holds the switch: the streams carry every hosted
  provider, so a hosted Slack opens them too, and Nerve cannot see whether a
  credential is connected. A `false` in either file keeps GitHub off the same
  way. Without the key in either file, the Sources step switches GitHub as
  before.
- **The filters are published from this config.** Nerve sends the gateway the
  guardrail patterns of `sync.github` (`allow_repos`, `deny_repos`,
  `allow_actors`, `deny_actors`, `allow_reasons`, `deny_reasons`,
  `deny_ci_branches`), `pr_watch` and `inbox`, and the `github_thread_items`
  entry while `thread_items` is true, whenever a stream opens and again
  on every reload. The gateway keeps the newest set and applies it before it
  stores a record, so a record that the filters drop never reaches this agent and is
  never a reason to start it. The guardrail still runs here too. A list that the
  gateway cannot take — more than 256 patterns, or a pattern longer than 256
  bytes or with a control character — goes to the gateway wider, never
  narrower: an allow list is left out and a deny list keeps the patterns that
  fit, and the local guardrail drops the rest. Blank patterns are not sent; here
  a blank allow pattern matches only an empty value, which GitHub never gives a
  repo, an actor or a reason. If the whole subscription is still larger than
  the gateway's frame limit, it goes with no patterns at all, and the guardrail
  here does all the filtering. A `github_thread_item` record carries no reason,
  so a reason list is applied to it only at the gateway: a reason list that the
  gateway did not get in full does not bound items. With `enabled: false` Nerve
  tells the gateway that it takes no notifications, and with no
  [repository watch](#hosted-repository-watch) either, that it subscribes to
  nothing.
- **The gateway must know the subscriptions.** A gateway that predates hosted
  sources closes a stream that publishes them, hosted Slack traffic included,
  so update the gateway before an agent switches to `mode: hosted`.
- **`inbox: all` needs a gateway that supports it.** The gateway lists the
  threads that `inbox` names, as the polling source does. Nerve leaves the
  `inbox` member out of the subscription unless it is `all`, and the gateway
  reads a missing one as `participating`, so with the default nothing changes
  and an older gateway is unaffected. A gateway that predates `inbox` refuses a
  subscription that carries it and closes the stream, hosted Slack traffic
  included, so update the gateway before you set `inbox: all` in hosted mode.
- **Same source, same name.** The records go to a local buffer, and the
  `source:github` runner drains it. The drop hook, `condense`, health checks,
  consumer cursors, `messages` gates and the Sources page work as they do for
  the polling source. The drain also runs on `sync.github.schedule`, as a
  fallback; that run is not the instance's next scheduled task while nothing
  waits in the buffer (normally the drain on delivery has taken everything), so
  a platform that parks the machine does not wake it for the fallback.
- **Runs on delivery.** As soon as records arrive, the runner drains them, and
  the jobs gated on `messages` for `github`, or for any source, run once
  `cron.delivery_debounce_seconds` (default 60) after the first record of a
  burst. A job whose last run started less than
  `cron.delivery_min_interval_seconds` (default 1800) ago, and whose schedule
  runs it again before that time is over, does not run for the delivery: the
  records wait for the scheduled run. See
  [Runs on delivery](cron.md#runs-on-delivery). Gate a pull request
  monitor on `messages` with `sources: [github]` rather than on
  `github_pr_activity`, which always passes in hosted mode.
- **The events source stays off.** `github_events` reads your own activity
  with `gh` on this machine, which hosted GitHub does not set up. While the
  mode is `hosted`, the GitHub switch of the setup page's Sources step and
  `nerve init` write `sync.github_events.enabled: false`, where in poll mode
  they switch it on and off together with GitHub. On a machine with a `gh`
  login it can still be switched on in the settings, until the GitHub switch
  is saved again.
- **The repository watch is hosted too.** With `sync.github_repos.enabled`,
  the gateway also polls the repositories it lists and delivers their records
  to the `github_repos` source, independently of `sync.github.enabled`. It
  needs a gateway that knows the repository watch; see
  [Hosted repository watch](#hosted-repository-watch).
- **A switched-off hosted Slack holds GitHub back.** Nerve reads the events of
  every hosted provider from one ordered inbox at the gateway, and it stops
  reading while a hosted channel cannot take events. With `slack.mode: hosted`
  and `slack.enabled: false`, Nerve therefore reads nothing from the gateway:
  the GitHub records wait there too, no delivery starts a job, and past the
  gateway's limit of stored records the oldest are dropped. They arrive once
  Slack is switched on again. While GitHub is hosted, keep a hosted Slack
  switched on, or switch both off; `nerve doctor` warns about this
  combination. This is a known limitation: reading each provider's events on
  its own, so that GitHub records arrive while hosted Slack is off, is a
  follow-up.
- **Restart-only.** A change of `mode` takes effect at the next restart. To go
  back from `hosted` to `poll`, switch the source off first: set
  `enabled: false` (and `sync.github_repos.enabled: false`, if the repository
  watch is on) and reload while the mode is still `hosted`. Nerve then
  tells the gateway that it subscribes to nothing (a subscriptions frame of
  `{"version":"1"}`), and the gateway stops polling GitHub for the agent at
  once. Then set `mode: poll`, switch them back on, and restart. In poll
  mode Nerve publishes no subscriptions at all. The gateway also counts an agent
  as subscribed to nothing when a stream opens and publishes no subscriptions
  within a short grace period, or when Nerve no longer serves the stream
  endpoint. Until it notices, though, it keeps polling and delivering records
  that Nerve no longer takes, which is why the reload comes first.

```yaml
sync:
  github:
    mode: hosted
    pr_watch: true
    deny_reasons: ["subscribed"]   # applied by the gateway and again here
```

`nerve doctor` checks the hosted settings and names the jobs that still rely
on the `github_pr_activity` gate.

### GitHub Events
- **Adapter:** `nerve/sources/github_events.py` — uses `gh api /users/{username}/events` CLI
- **Cursor:** Event ID (string, monotonically increasing)
- **First run:** Fetches the latest batch to establish a cursor baseline — no backfill of historical events
- **Subsequent runs:** Fetches newest events, stops at the last-seen event ID
- **Purpose:** Captures the user's **own** GitHub activity (pushes, PR creates/merges, reviews, comments, branch operations). Complements the notification source which only shows what *others* did involving you
- **Username:** Set manually via `username` config, or else the account `gh` is signed in as, read in every run (a failed read falls back to the last login it returned)
- **Repo filter:** Optional `repos` list — empty means all repos
- **Event types:** PushEvent, PullRequestEvent, IssueCommentEvent, IssuesEvent, PullRequestReviewEvent, CreateEvent, DeleteEvent, ForkEvent (with type-specific formatting); other event types get generic formatting
- **Note:** GitHub's Events API returns truncated payloads (e.g., PR titles and URLs may be missing). The source constructs URLs from repo name + number and handles missing fields gracefully
- **Default schedule:** `*/15 * * * *` (every 15 min)

### GitHub Repos

Repositories that the agent follows although its GitHub account takes no part
in them: the new pull requests, issues and releases there arrive as
`github_repos` records, and with `updates: true` so does each new commit,
ready for review, merge, close or reopening of a pull request it reported.
With the `commit` type it also reports the
[new commits on a branch](#new-commits-on-a-branch), one record per branch. It
is the self-service form of GitHub's "Custom watch", which has no API. Nothing
is set on the GitHub account (no watch, no e-mail), and it does not depend on
`sync.github.inbox`. What the account itself takes part in (mentions, review
requests, its own pull requests) still comes through the [GitHub](#github)
notifications, and in hosted mode through `pr_watch`.

- **Adapter:** `nerve/sources/github_repos.py` — `gh api` on this machine. With `sync.github.mode: hosted` the channel gateway polls instead (see [Hosted repository watch](#hosted-repository-watch))
- **Listings:** one request per repository and type, the most recently changed first:
  - `pull_request`: `GET /repos/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100`
  - `issue`: `GET /repos/{repo}/issues?state=all&sort=updated&direction=desc&per_page=100`. The issues endpoint lists the pull requests too; they are skipped
  - `release`: `GET /repos/{repo}/releases?per_page=30`. Draft releases are skipped, prereleases are not
  - `commit`: `GET /repos/{repo}/commits?sha=refs/heads/{branch}&per_page=100`, one request per branch that `branches` names for the repository. The request names the branch's ref, so a tag or a commit that has the same name is never read in its place. A repository without an entry in `branches` reports its default branch, which `GET /repos/{repo}` names at every run
- **One page each:** a repository with more than 100 pull requests (or issues) changed between two runs loses the oldest of those changes. `batch_size` does not cap this source
- **Cursor:** JSON. Per repository and listing: the newest `updated_at` listed (`published_at` for releases), when the listing was read, and the numbers (release ids) of the items created in that newest second. Per repository, the pull requests it follows: a held draft (`include_drafts: false`) and, with `updates`, a reported pull request until it is merged, as head commit, draft and held. One that is closed without a merge stays followed for 30 days after its close, so that its reopening is reported: its entry also holds the time of the close (`closed_at`), and it is forgotten 30 days after that time. At most 1,000 per repository; beyond that the closed ones are forgotten before the others, and of each the one listed longest ago first. And per repository, the head commit of each branch whose commits it follows
- **First run:** the baseline. Each listing's cursor is set and nothing is reported, so switching the source on does not fill the inbox with history; a repository or a type added later takes its own baseline, and one removed is forgotten. An item is new when it was created (a release: published) after the last listing: later than the listing's cursor, or in the cursor's own second and not listed then, since GitHub's times have a resolution of one second. So an older pull request that changes later is never reported as new, and an item is reported once
- **No backfill beyond a day:** nothing older than a day is backfilled, and the last day is. A check more than a day after the last one (a daily schedule, which always comes a moment past the day; a weekday schedule after the weekend; the watch switched off for longer; a long outage) reads the listing as of a day and an hour ago: it reports the items of the last day, with an hour of grace for the scheduler's delay (an item created moments after the last check is not lost), within the one page, and nothing older, and the pull requests it follows stay followed. One thing older than that is judged, in every run: a pull request that the watch keeps as closed, when the listing shows it open, merged, or closed by another close than the one that the watch keeps. One that the watch reported then reports `reopened` or `merged`, however long ago that was, because the watch said that it is closed: a weekday schedule reports on Monday a pull request that was reopened on Friday afternoon. A held draft was never reported, so listed open it is held again or reports `ready`, and merged it is forgotten without a record. One that was reopened and closed again reports nothing, and the watch keeps the later close, so that the 30 days and the ID of the next reopening come from the close that is true. The listing must show the pull request as of the kept close or later: an item whose `updated_at` is before that close comes from a listing that lags behind, and is not judged. One that is still closed by the kept close is not judged again. Switching the watch off and on in poll mode resumes where it left within the day, and beyond it reports the last day's items
- **Failures:** a listing that fails (a repository that does not exist or that the `gh` login cannot read, a branch that does not exist, a rate limit) keeps its cursor and is read again on the next run; the other listings still report. A run that could not read a repository's pulls listing forgets none of that repository's closed pull requests by their age, since it cannot know whether one was reopened; the limit of 1,000 applies in every run. A run in which every listing fails fails, and the source's health shows it
- **Upgrades:** the cursor of the earlier version of this source, one timestamp, becomes the cursor of every listing (one more than a day old reports the last day), so nothing is repeated or skipped; a buffer position left by the hosted mode is no cursor (a new baseline). The earlier version took any `repos` value: one that the watch now refuses (a URL such as `https://github.com/acme/widgets`, a trailing slash, `owner/repo/x`, the same repository twice, more than 50) only logs a warning while the watch is off, and refuses the config once it is on. With `updates: true` or `include_drafts: false`, see [Closed and reopened pull requests](#closed-and-reopened-pull-requests) for the `reopened` event, the ID of a `closed` record, and the cursor after an upgrade or a rollback
- **Default schedule:** `*/15 * * * *` (every 15 min); in hosted mode the fallback cadence of the drain
- **Disabled by default**

```yaml
sync:
  github_repos:
    enabled: true
    repos: ["acme/widgets", "other-org/other-repo"]
    types: [pull_request, issue]    # pull_request | issue | release | commit
    include_bots: false             # PRs and issues a GitHub App or a [bot] account opened
    include_drafts: true            # false: a draft is reported when it is ready for review
    updates: false                  # true: commits, ready, merge, close and reopening of reported PRs
    deny_authors: []                # case-insensitive globs on the author's login
    # branches:                     # with the commit type: the branches to follow, by repository
    #   acme/widgets: [main, release/1.2]   # no entry: the repository's default branch
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github_repos.repos` | list | `[]` | `owner/repo` names as GitHub writes them, each once (case does not matter), at most 50. While the watch is on, a name GitHub would not take, the same repository twice, or more than 50 refuses to load; while it is off, a warning names them. Empty = the source does nothing |
| `github_repos.types` | list | `[pull_request, issue]` | What is reported: `pull_request`, `issue`, `release`, `commit` (the [new commits on a branch](#new-commits-on-a-branch)). Case does not matter; an unknown type, or an empty list, refuses to load |
| `github_repos.branches` | map | `{}` | With the `commit` type: the branches whose new commits are reported, as `owner/repo: [branch, …]` for a repository of `repos` (case does not matter; one branch may be written as a string), at most 8 each. A repository without an entry, or with an empty one, reports its default branch. An entry is the name of a branch, such as `main` or `release/1.2`, and not a ref: it starts with a letter or a digit and goes on with letters, digits, `_`, `.`, `/` and `-`, up to 121 characters, without `..`, not starting with `refs/` and not ending in `.lock`; the repository's name and the branch have at most 194 bytes together. Write a name that YAML reads as something else in quotes: an unquoted `1.10` is the number `1.1`, and `no`, `off` and `0` are no names. [New commits on a branch](#new-commits-on-a-branch) lists what refuses to load. Without the `commit` type the setting is not read: a value that is left there is ignored, with one warning |
| `github_repos.include_bots` | bool | `false` | Also report the pull requests and issues that a GitHub App (`user.type` `Bot`) or an account whose login ends in `[bot]` opened. A release is reported whoever published it: release automation (`github-actions[bot]`, a release bot) is the normal case. `author_kind` still says `bot` on such a release |
| `github_repos.include_drafts` | bool | `true` | `false`: a new draft pull request is held and reported when it is marked ready for review (event `ready`), never as `opened`. A held draft that is closed is not reported and stays held for 30 days after its close: reopened in that time, it is reported when it is ready for review. One that is merged before the watch sees it ready is never reported |
| `github_repos.updates` | bool | `false` | Also report, for each pull request this source reported, a new head commit (`updated`), ready for review (`ready`), its close without a merge (`closed`), its reopening after such a close (`reopened`), and its merge (`merged`), after which it is no longer followed. A pull request that is closed without a merge is followed again when it is reopened within 30 days of its close |
| `github_repos.deny_authors` | list | `[]` | Case-insensitive globs on the author's login, with the rules of the [actor guardrail](#github-actor-guardrail); what they match is never reported, releases included. A pattern that ends in `[bot]` names a GitHub App (`some-ci[bot]` is that App, `*[bot]` every App), so with `include_bots: true` the list leaves out the Apps it names; without it, bot pull requests and issues are dropped already |

Events (the record's `Event:` line and `event` metadata):

| `event` | When | Record ID | `timestamp` |
|---------|------|-----------|-------------|
| `opened` | A pull request or an issue was created since the last run (a draft only with `include_drafts: true`) | `<repo>#<number>:opened:open` | `created_at` |
| `ready` | A held draft (`include_drafts: false`) was marked ready for review; with `updates`, so was a reported draft | `<repo>#<number>:ready:<head sha>` | `updated_at` |
| `updated` | `updates`: a reported pull request has a new head commit | `<repo>#<number>:updated:<head sha>` | `updated_at` |
| `merged` | `updates`: a reported pull request was merged | `<repo>#<number>:merged:merged` | `merged_at` |
| `closed` | `updates`: a reported pull request was closed without a merge | `<repo>#<number>:closed:<closed_at>`; the word `closed` in the place of a time that GitHub does not give | `closed_at` |
| `reopened` | `updates`: a reported pull request that was closed without a merge is open again | `<repo>#<number>:reopened:<closed_at>`, with the time of the close that the reopening ends | `updated_at` |
| `released` | A release was published since the last run | `<repo>#:released:<release id>` | `published_at` |
| `pushed` | `commit` type: the head of a followed branch is another commit than at the last run | `<repo>@<branch>:pushed:<before>..<head>`, with the first 12 characters of the two commits | The time of the run |

A pull request gives one record per run: `merged` or `closed`, else
`reopened`, else `ready`, else `updated`; back to draft gives none, and one
seen for the first time is `opened` whatever its state. Comments, reviews and
labels move `updated_at` and report nothing; issues report only `opened`.
[Closed and reopened pull requests](#closed-and-reopened-pull-requests) says
what follows a close.

An event has one record ID, which does not depend on when the listing is
read. A listing that is read again from the same cursor, as after a run whose
records were not stored, gives an item that did not change in between the
same ID, and its record is stored once. A comment in between changes no ID. A
push in between changes the ID of `updated` and `ready`, which the head
commit names.

The record (type `github_repo_activity`) is built the same way in both modes.
Its summary is, for example, `[acme/widgets] New PR #12: Add a widget cache`,
`[acme/widgets] PR #12 merged: Add a widget cache` or
`[acme/widgets] New release v1.2.0: Widgets 1.2`. Its content has the lines
`Repository:`, `Type:`, `Event:`, `PR: #12 <title>` (`Issue: #7 <title>`,
`Release: <tag> <name>`), `Author:`, `State:` (`merged` for a merged pull
request), `Draft:` and `Base ← head:` for a pull request, `Prerelease:` for a
release, `Labels:`, `Created:`, `Updated:` (`Published:`), `URL:`, then the
description capped at 4,000 characters. Like every source record it is
untrusted content. Its metadata, which the guardrails and gates match on, is
the same in both modes:

| Key | Value |
|-----|-------|
| `repo_name` | `owner/repo`, as configured |
| `number` | The pull request or issue number as a decimal string; empty for a release |
| `event` | `opened`, `ready`, `updated`, `merged`, `closed`, `reopened` or `released` |
| `subject_type` | `PullRequest`, `Issue` or `Release` |
| `author` | The login that opened the item or published the release |
| `author_kind` | `bot` (a GitHub App, or a login ending in `[bot]`) or `human` |
| `head_sha` | The pull request's head commit; empty otherwise |
| `subject_url` | The page of the pull request, issue or release (the `URL:` line), which the Sources page links to |
| `repo_url` | `https://github.com/<owner>/<repo>`, as configured |

The settings are applied before anything is reported or remembered, and the
inbox guardrail applies them again to what arrives (`repo_name` against
`repos`, `author` against `deny_authors`, `author_kind` against
`include_bots` for pull requests and issues, `subject_type` against
`types`), which catches the records delivered before a reload changed them.
Everything here follows a reload.

#### Closed and reopened pull requests

With `updates`, a reported pull request that is closed without a merge stays
followed, so it is followed again when it is reopened within 30 days of its
close, and the reopening is reported. The 30 days count from the close: a
comment on the closed pull request does not move them. The first run after
the 30 days that reads the pulls listing, and whose listing does not show the
pull request open again, forgets it, and says in the log how many closed pull
requests of the repository it forgot. Reopened after that, it reports
nothing.

- **The `reopened` record.** It says that a reported pull request, which the
  watch last saw closed, is open again. Its summary is
  `[acme/widgets] PR #12 reopened: Add a widget cache`. It is built like the
  other pull request records, with the head commit and the draft state of the
  listing that shows the pull request open, and it is the only record of that
  run for the pull request: commits that came with the reopening give no
  `updated`, and ready for review gives no `ready`. After it the pull request
  is followed as before.
- **Jobs that decide by `event`.** A cron job or a prompt that decides by the
  `event` of a record needs a rule for `reopened`. `closed` is not the last
  record of a pull request: `reopened` can follow it, and then `updated`,
  `ready`, `closed` or `merged` as before. `merged` can also follow `closed`
  directly, when the pull request is reopened and merged between two runs or
  in a gap. In its run `reopened` stands in for `updated` and `ready`: the
  head commit is in `head_sha`, and whether the pull request is a draft is in
  the `Draft:` line of the content, not in the metadata.
- **More than one close.** A pull request can be closed and reopened more
  than once. The ID of a `closed` record ends in the time of the close, and so
  does the ID of the `reopened` record that ends that close, so each close
  and each reopening is a record of its own. The ID of a reopening does not
  depend on when the watch sees it, so a listing that is read again gives the
  same ID. A comment on a closed pull request reports nothing.
- **Between two runs.** A pull request that is closed and reopened between two
  runs reports neither. A closed one that is reopened and closed again reports
  nothing, and the watch keeps the later close; one that is reopened and
  merged reports `merged`.
- **First seen closed.** A new pull request that is closed when the watch
  first sees it is `opened` and stays followed the same way. One that is
  merged then is `opened` and not followed.
- **Held drafts.** A held draft (`include_drafts: false`) that is closed stays
  held for the 30 days, with or without `updates`, and reports nothing.
  Reopened as a draft it is held again; it is reported, as `ready`, when it is
  open and ready for review.
- **After a gap, or a listing that failed.** A run that comes more than a day
  after the last one still reports a reopening from before that day, or the
  merge of a pull request that was reopened and merged before it, and it
  keeps the later close of one that was reopened and closed again. Such a
  record carries the time of its event (`updated_at` for `reopened` and
  `ready`, `merged_at` for `merged`), which can be days or weeks back. An
  item whose `updated_at` is before the close that the watch keeps is not
  judged: that listing lags behind the one that showed the close. A run that
  could not read the pulls listing forgets no closed pull request by its age.
  See "No backfill beyond a day" and "Failures" above.
- **Upgrade and rollback.** The records change without a change of the
  settings. With `updates: true`, `reopened` arrives, and the ID of a
  `closed` record ends in the time of the close,
  `acme/widgets#12:closed:2026-10-27T10:04:00Z`, where it ended in the word
  `closed`, `acme/widgets#12:closed:closed`; the reopening that ends that
  close is `acme/widgets#12:reopened:2026-10-27T10:04:00Z`. So an ID holds
  the colons of a time. With `include_drafts: false`, with or without
  `updates`, a held draft that was closed and reopened reports `ready`, where
  it was forgotten at its close. The pull requests that were closed before
  the upgrade stay unfollowed, and their reopening reports nothing. A version
  without `reopened` that reads the cursor drops the closed pull requests
  from it and follows the open ones as before.
- **Hosted mode.** The gateway builds the records, so `reopened` and the
  `closed` ID that ends in the time of the close arrive when the gateway has
  this rule, whatever the version of Nerve. What is said here about the
  cursor is about poll mode.

#### New commits on a branch

With `commit` among the `types`, the watch follows branches: those that
`branches` names for a repository, or the repository's default branch. It is
made for work that should start when a branch moves, such as a build or a
test run: a cron job [gated on the source's unread records](cron.md#messages)
runs when there are new commits and stays idle otherwise.

- **One record per branch and run, never one per commit.** The first run of a
  branch only stores its head. From then on, a run that finds another head
  reports one `pushed` record for the branch, however many commits arrived
  and however long after the last run: the day's limit above is for pull
  requests, issues and releases, not for branches.
- **Which commits it lists.** The watch reads the first 100 commits of the
  branch's listing on GitHub, the head first. `commits` are the newest
  commits of that listing above the head of the last run (`before_sha`), up
  to 50 of them, and `commit_count` counts the commits of the listing above
  `before_sha`, listed or not.
- **Merges.** GitHub lists by commit date, not by ancestry. Across a merge
  the listing interleaves the commits of the merged branches by their dates,
  so a commit that the merge brought in can be listed below `before_sha`:
  `commits` and `commit_count` are not always the exact set of the new
  commits. `subject_url`, the comparison of the two heads on GitHub, is.
- **`truncated`.** `true` when `before_sha` is not among the listed commits,
  after 100 or more new commits or a push that rewrote the branch: then
  `commit_count` is empty and `commits` holds the branch's newest commits.
  Also `true` when `commit_count` is more than the 50 commits that `commits`
  lists. Otherwise `false`.
- **The record's ID names both heads**,
  `<repo>@<branch>:pushed:<before>..<head>` with the first 12 characters of
  `before_sha` and of `head_sha`. So a branch that returns to an earlier
  head, as after a push that was taken back, gives a record of its own, and
  only the same move again, between the same two commits, has the ID of an
  earlier record.
- **Branches only.** A name in `branches` is followed as the branch of that
  name: a tag or a commit that has the same name is never read in its place,
  and without such a branch the listing fails as for a branch that does not
  exist.
- **No author filters.** One record can hold commits of many authors, so
  neither `include_bots` nor `deny_authors` judges it, and its metadata has
  no `author`.
- **A branch that cannot be read** (it does not exist, or the repository has
  no commit yet) fails its listing alone and keeps its head; see Failures
  above. A default branch whose name is outside the shape of `branches` is
  not followed, with a warning.

With the `commit` type, and while the watch is on, `branches` refuses to load
when it holds any of these:

- a value that is not a map of repositories to their branches, such as
  `branches: [main]` or `branches: []`. To name no branch, leave the setting
  out or write `branches: {}`
- a repository that `repos` does not list, or the same repository twice
  (names ignore case)
- a branch that is not a string: what YAML makes of an unquoted `1.10`, `no`,
  `off`, `yes`, `on`, `0` or a date, and an empty item of a list. The message
  says to quote it: `"1.10"`
- a branch name of another shape than the settings table gives, or one that
  starts with `refs/`: write `main`, not `refs/heads/main`
- the same branch twice, or more than 8 branches for one repository
- a repository's name and a branch that have more than 194 bytes together

A blank name is left out. While the watch is off, one warning names what
would refuse and those entries are left out, so the config loads. Without the
`commit` type nothing of `branches` is checked.

The record's summary is `[acme/widgets] 3 new commits on main`, or
`[acme/widgets] main moved to 5c4d3e2f1a0b` when it cannot count them. Its
content has the lines `Repository:`, `Type: Commit`, `Event: pushed`,
`Branch:`, `Commits:` (the count, when there is one), `Head:`, `Before:` and
`URL:`, then the commits, one line each: the commit and the first line of its
message, cut to 120 characters, with a space in the place of each control
character, of the line and the paragraph separator (U+2028, U+2029) and of
each bidirectional control (U+202A to U+202E, U+2066 to U+2069). A record
that is `truncated` ends with `[... not every new commit is listed]`. Its
metadata, the same in both modes:

| Key | Value |
|-----|-------|
| `repo_name` | `owner/repo`, as configured |
| `event` | `pushed` |
| `subject_type` | `Commit` |
| `branch` | The branch |
| `head_sha` | The branch's head |
| `before_sha` | Its head at the last run |
| `commit_count` | The number of commits that the listing shows above `before_sha`, as a decimal string; empty when the listing does not show `before_sha` |
| `commits` | A list of the newest of them, up to 50, in the listing's order, each `<sha> <first line of the message>` |
| `truncated` | `true` or `false`, as above |
| `subject_url` | `https://github.com/<owner>/<repo>/compare/<before_sha>...<head_sha>` |
| `repo_url` | `https://github.com/<owner>/<repo>`, as configured |

#### Hosted repository watch

GitHub is hosted as a whole: with `sync.github.mode: hosted` the channel
gateway polls the watched repositories too, and `gh` is not needed on this
machine for them.

- **Who polls.** The gateway, with the GitHub credential connected to the
  agent (the one of the [hosted GitHub source](#hosted-github-source)), on the
  notifications cadence (60 s), with conditional requests, which cost no rate
  limit while nothing changed. With many repositories it spreads them over a
  few polls. It builds the records described above and keeps them until Nerve
  reads them, also while Nerve is not running; a repository it cannot read is
  paused while the others keep being polled.
- **What is published.** Nerve sends the gateway a `github_repos`
  subscription: each repository with `types`, `include_bots`,
  `include_drafts`, `updates` and `deny_authors`, and with the `commit` type
  the `branches` that the settings name for it, when a stream opens and again
  on every reload, only while `enabled` is true and `repos` lists a
  repository. Switching it off and reloading tells the gateway to stop. A
  `deny_authors` list the gateway cannot take (more than 256 patterns, or a
  pattern longer than 256 bytes or with a control character) goes with the
  patterns that fit, and the inbox guardrail here applies them all.
- **It starts fresh when switched off.** The gateway backfills nothing older
  than a day either (after a gap it reports the last day), and it forgets the
  repositories of an agent that stops subscribing to them, so switching the
  watch off and on in hosted mode starts from a new baseline.
- **The gateway must know the repository watch.** The subscription carries a
  `repos` member that a gateway older than the repository watch refuses: it
  closes the stream, hosted Slack traffic included. Update the gateway before
  an agent switches the watch on in hosted mode. An agent that does not
  switch it on sends the same subscription as before, so it is unaffected.
- **The gateway must know the `commit` type.** Nerve cannot ask the gateway
  which types it knows. A gateway older than the `commit` type refuses a
  subscription that names it: it closes the stream, hosted Slack traffic
  included, each time the agent connects, until the gateway is updated or
  `commit` is taken out of `types`. So name `commit` on a hosted agent only
  once its gateway knows the type. An agent whose `types` do not name
  `commit` sends the same subscription as before, whatever `branches` says.
- **Commits in hosted mode.** The gateway follows the branches as described
  under [New commits on a branch](#new-commits-on-a-branch), with these
  differences: a branch that GitHub does not find rests for an hour before
  the gateway asks again, while the repository's other branches and listings
  go on, and a repository that GitHub does not find is paused as a whole, as
  for the other types; a branch whose listing fails, as while GitHub has an
  outage, rests for a few minutes; and the gateway reads a repository's
  default branch at least once an hour, not at every poll, so a default
  branch that changes is followed, from its head, within the hour.
- **Reopened pull requests in hosted mode.** The gateway builds the records,
  so the `reopened` event and the `closed` ID that ends in the time of the
  close arrive when the gateway has that rule, whatever the version of
  Nerve. See [Closed and reopened pull requests](#closed-and-reopened-pull-requests).
- **Same source, same name.** The records go to their own local buffer, and
  the `source:github_repos` runner drains it, as the `github` runner drains
  the notifications (see [Hosted GitHub source](#hosted-github-source)): it
  runs as soon as records are delivered, and its scheduled run is a fallback
  that is not the instance's next scheduled task while nothing waits, so a
  platform that parks the machine does not wake it for that run. The mode
  follows `sync.github.mode`, which is restart-only.

### Telegram account sync
- **Adapter:** `nerve/sources/telegram.py` — uses Telethon (user account API).
  This is the pull sync for your own Telegram account, and is separate from the
  `telegram:observed` bot-channel source below
- **Mechanism:** Telegram's native `updates.getDifference` API — asks "what's new since this state?" using PTS/QTS/date
- **Cursor:** JSON-encoded Telegram state `{pts, qts, date, seq}` — the server's own update tracking
- **First run:** Calls `updates.getState()` to snapshot current position, returns 0 records (prevents flooding the agent with history)
- **Subsequent runs:** Calls `updates.getDifference(pts, date, qts)` to get only new messages since the saved state. Handles `DifferenceSlice` (large gaps, paginated) and `DifferenceTooLong` (gap too large, reset) gracefully
- **Default schedule:** `*/5 * * * *` (every 5 min)
- **Requires setup:** Run `nerve sync telegram` interactively once to authenticate with Telethon (phone number + code). The session is stored at `~/.nerve/telegram_sync.session`

### Chat channels (Slack, Telegram)

A chat channel can feed the inbox with the group traffic it already receives.

- **Adapter:** `nerve/sources/channel.py` — one `ChannelSource` per enabled
  channel source
- **Mechanism:** for each message delivered by the channel transport, matching
  messages are written to the `channel_observations` buffer. `ChannelSource`
  drains that buffer into the inbox; it does not poll the chat service.
- **Source name:** `<channel>:observed` — `slack:observed`,
  `telegram:observed` — so a cron gate reads `sources: [slack:observed]`.
  Compound like `gmail:<account>`, and deliberately not the bare channel name:
  `telegram` is already the Telethon pull source for your *user* account, and
  sharing a name would mean sharing a cron job id and a cursor key.
- **Config:** `slack.source.*` / `telegram.source.*`, not `sync.*` — see
  [config.md](config.md). The runner carries its own `schedule` because there
  is no `config.sync.<name>` section to look one up in.
- **Default schedule:** `*/5 * * * *`
- **Not the next scheduled task while nothing waits.** Only the running
  instance writes the buffer, so nothing is added to it while the machine is
  parked; what arrives meanwhile is delivered after the next wake. A scheduled
  drain with nothing waiting is therefore not counted in `next` of
  [What runs next](cron.md#what-runs-next) nor in the platform activity
  report's `nextScheduledAt` (it is still listed, with forecast `skip`), so a
  platform that parks an idle machine is not kept up, or woken, for each
  scheduled drain. A drain with messages waiting since the last one counts
  from the next forecast on (a buffered message is not announced, so at most
  one activity report interval later): the machine stays up, or is woken, to
  take them, and the jobs gated on `<channel>:observed` run after it as before.
  While the instance runs, the schedule drains the buffer as it always did.
- **Cursor:** the buffer's autoincrement row id. `AUTOINCREMENT` is
  load-bearing: the buffer is pruned, and a plain SQLite rowid is reused after
  the highest row is deleted, which would reissue ids the cursor has already
  passed and skip everything after them.
- **Idempotent:** the record id is `<conversation>:<message_id>`, so the same
  message collected twice collapses on the inbox's `(source, id)` key.
- **Buffer target:** `max_stored_messages` (default 10 000) is one budget per
  *transport*, covering all of its watched conversations together, not one per
  conversation. Every 100 writes, Nerve drops the oldest rows back to that
  target and logs a warning. The buffer can therefore exceed it by up to 99
  rows. Values below 100 are refused and 100 used instead: `0` reads like "no
  limit" but tells the trim to keep nothing. Daily cleanup removes rows after
  `sync.message_ttl_days`.
- **Backlog:** one run keeps fetching while the buffer reports more, up to 20
  batches, so a chat busier than one `batch_size` per tick still drains. Each
  batch is persisted before the next starts, so a run that stops at the bound
  resumes there and logs a warning. Raise `batch_size` if that repeats.
- **Requires setup (Telegram only):** with privacy mode enabled, a non-admin bot
  does not receive ordinary group messages. Make it an admin or disable privacy
  mode with BotFather. See Telegram's
  [privacy-mode documentation](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get).

#### Routing: which messages get collected

For eligible shared-channel and group-chat messages, the live route and source
route use separate rules. Access rules (`slack.allow_users`,
`telegram.allowed_users`, …) decide who may drive the agent.
`<channel>.source.*` decides whose traffic may reach the inbox. A message can
take either route, both routes, or neither.

| Live route | Source grant | `include_handled_messages` | Result |
|---|---|---|---|
| accepts | no match | — | channel only |
| accepts | matches | `false` (default) | channel only |
| accepts | matches | `true` | channel **and** source |
| refuses | matches | — | source only |
| refuses | no match | — | dropped |

By default, a message accepted by the live route is not also copied to the
source. Set `include_handled_messages: true` only when source consumers should
receive the same message later. "Handled" means accepted for live routing, not
that the agent produced a reply.

Whether the live route accepts differs by transport, and only affects which row
of that table you land in:

- **Slack** accepts a shared-channel message for live routing when it mentions
  the bot or continues a thread with an active session, and the sender passes
  the access policy. A matching source can still collect a message that fails
  the live access policy.
- **Telegram** has no "addressed to me" test: authorization alone decides.
- **Hosted Slack** (`slack.mode: hosted`): the gateway sends a separate
  observation for each message it admits for observation, and a message that
  also starts a turn arrives twice. The live route accepts the turn copy, and
  the table above decides the observation copy. The gateway gives no trusted
  names, so only a channel ID or member ID can grant; a name matches only the
  display name in the event, which can deny but never grant. Nerve
  acknowledges an observation to the gateway only after the buffer row is
  written.
  An observation can also name the person that the gateway linked to the
  Slack user. The inbox record then shows the person's name after the sender
  (`[deployments] ada (Ada Lovelace): ...`), and its metadata has
  `principal_id` and `principal_name`. The person is for attribution only: the
  sender rules match only the member ID and the display name in the event, and
  the observation adds no actor.
  With `slack.source.gateway_grant: true`, the gateway's own grant is enough:
  it sends observations only for channels where the agent's channel policy
  allows them, and Nerve collects all of them without a local allow list. A
  local allow list still narrows that, and the deny lists still apply.

**Never collected, whatever the config says:** direct and group DMs, the
agent's own messages, other bots and apps, join/leave and similar service
events, malformed events, and Telegram `/pair` commands. Duplicate buffered
records collapse to one inbox record on the `(source, id)` key.

**The source is off by default and fail-closed.** An empty allow list collects
nothing rather than everything — the inverse of the access rules, because this
is a standing grant to record other people's messages. `enabled: true` with
nothing listed logs a warning and registers no drain. Hosted Slack with
`gateway_grant: true` is the one exception: there the gateway's grant is the
written one.

#### Threat model

**Everything collected is untrusted input.** Most of it comes from people not
authorized to instruct the agent — that is the usual reason to watch a room —
so treat a buffered message as attacker-controlled text an agent will later
read. What actually protects you:

- `<channel>.source.*` decides **whose messages are collected** before anything
  is written.
- At drain time, Nerve rechecks conversation and sender deny rules against raw
  IDs. Name-based denies apply only before buffering.
- Inbox filters inspect metadata, not content. They cannot distinguish a report
  from an instruction. Polling advances a cursor; it does not delete inbox
  records. Daily cleanup removes them after their TTL expires. Agents see them
  only through explicit source reads or cron gates, never in a live turn.

So scope the grant to conversations whose participants you would already trust
to file a ticket, and treat any workflow that acts on collected content without
review as accepting prompt injection.

**Slack lookup cost.** Names and globs use cached `conversations.info` or
`users.info` lookups. An ID skips the lookup, but only in uppercase. A
lowercase pattern could equally be a name, so it is resolved rather than
assumed. A channel the grant does not name is refused before any sender is
resolved.

**Reply context is not expanded.** Slack stores `thread_ts`; Telegram stores
`reply_to_message_id`. The parent message is not fetched.

**Attachments are named, not fetched.** An upload contributes `[File: <name>]`,
`[Photo]`, or `[Sticker: <emoji>]` ahead of any caption. A message with neither
text nor an attachment is not collected.

## Configuration

Pull sources are configured under the `sync:` key in `config.yaml` /
`config.local.yaml`. Channel sources use `slack.source.*` and
`telegram.source.*` instead.

```yaml
sync:
  message_ttl_days: 7               # How long to keep inbox messages
  consumer_cursor_ttl_days: 2       # Consumer cursors expire after N days of inactivity

  telegram:
    enabled: true
    api_id: 12345678              # From my.telegram.org
    api_hash: "abc123..."         # From my.telegram.org
    schedule: "*/5 * * * *"       # Every 5 minutes
    batch_size: 50                # Max records per fetch
    condense: false
    exclude_chats: []             # Chat IDs to skip
    monitored_folders: []         # Telegram folder names to filter

  gmail:
    enabled: true
    accounts:                     # One source per account, each with own cursor
      - personal@gmail.com
      - work@company.com
    keyring_password: "..."       # gog keyring password (in config.local.yaml)
    schedule: "*/15 * * * *"
    batch_size: 20
    condense: true                # Strip boilerplate + Haiku extraction for long emails

  imap:
    enabled: false                # Off by default
    accounts:                     # One source per mailbox, each with own cursor
      - host: imap.example.net
        username: me@example.net
        label: mailbox            # Source name becomes imap:mailbox
        port: 993
        mailbox: INBOX
    passwords:                    # Keyed by username (in config.local.yaml)
      me@example.net: "..."
    schedule: "*/30 * * * *"
    batch_size: 20
    initial_lookback_days: 1
    condense: false
    match: {}                     # See "Reading mail that is only legible as an image"
    vision: {}

  github:
    enabled: true
    mode: poll                    # poll = gh CLI here; hosted = the channel gateway polls (restart-only)
    pr_watch: true                # Hosted mode: also a record when one of your open PRs changes
    thread_items: true            # Hosted mode: one record per comment, review and description where the gateway offers them
    inbox: participating          # participating = threads you take part in; all = the whole inbox (see "GitHub reason guardrail")
    schedule: "*/15 * * * *"
    batch_size: 30
    condense: true                # Haiku extraction for long notifications
    allow_repos: []               # Guardrail allowlist — empty = all repos. Example: ["ClickHouse/*"]
    deny_repos: []                # Guardrail denylist — always dropped (takes precedence)
    allow_actors: []              # Guardrail allowlist of GitHub logins — empty = all. Example: ["alice", "bob"]
    deny_actors: []               # Guardrail denylist of GitHub logins — always dropped (takes precedence)
    allow_reasons: []             # Guardrail allowlist of GitHub reasons — empty = all. Example: ["mention"]
    deny_reasons: []              # Guardrail denylist of reasons. Example: ["comment", "subscribed"]
    deny_ci_branches: []          # Drop CI runs on these branches. Example: ["main", "master"]
    drop_hook: []                 # Command (argv) that gets the dropped records. See "Drop hook"

  github_events:
    enabled: true
    schedule: "*/15 * * * *"
    repos: []                     # Empty = all repos. Example: ["owner/repo"]
    username: ""                  # Auto-detect from gh auth
    batch_size: 50
    condense: false               # Events are naturally short

  github_repos:
    enabled: false
    schedule: "*/15 * * * *"      # Poll mode; in hosted mode the fallback cadence of the drain
    repos: []                     # Required: owner/repo names, at most 50. Example: ["acme/widgets"]
    types: [pull_request, issue]  # pull_request | issue | release
    include_bots: false           # Also PRs and issues a GitHub App or a [bot] account opened
    include_drafts: true          # false: a draft PR is reported when it is ready for review
    updates: false                # true: also commits, ready, merge, close and reopening of reported PRs
    deny_authors: []              # Globs on the author's login; never reported
    batch_size: 50
    condense: false
```

### Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable this source |
| `schedule` | string | varies | Crontab expression or interval (`2h`, `30m`) |
| `batch_size` | int | `50` | Max records per fetch cycle |
| `condense` | bool | `false` | LLM-condense long stored records via `memory.fast_model` |
| `message_ttl_days` | int | `7` | How long to keep inbox messages |
| `consumer_cursor_ttl_days` | int | `2` | Consumer cursors expire after N days of inactivity |

## Guardrails (Inbox Filtering)

Guardrails programmatically limit **which records ever reach the inbox**. Non-matching
records are dropped at ingestion — they are never persisted to `source_messages`, so the
agent can never see them and they never cost tokens. This shrinks the prompt-injection
attack surface, which matters most in **worker mode** where the agent acts on inbox
content without a human in the loop.

Filtering happens at the choke point in `SourceRunner` (after `source.preprocess()`,
before persist). The source cursor still advances normally. A source with an inclusive
cursor (GitHub) can list a dropped record again on the next run, and the guardrail drops
it again. The number of dropped records is reported in the run summary (visible in the
**Runs** tab and `nerve sync` output).

### How it works

Guardrails are declarative allow/deny rules evaluated against `SourceRecord` fields — a
`metadata` key, or the special `source` / `record_type` attributes. The engine lives in
`nerve/sources/filters.py` (`FieldRule`, `InboxFilter`) and is source-agnostic.

Per-rule semantics:
- **deny wins** — a value matching any `deny` pattern is always dropped.
- **allow is a gate** — if `allow` is non-empty, the value must match one of its patterns,
  otherwise the record is dropped. An absent field fails closed (cannot satisfy `allow`).
- an empty `allow` list means "allow anything not denied".

A record is kept only if it passes **every** rule. Matching is case-insensitive and
supports shell-style globs (`ClickHouse/*`). List-valued metadata (e.g. Gmail `labels`)
matches if any element matches; non-string scalars (e.g. Telegram `chat_id`) are coerced
to strings.

### GitHub repo guardrail

The GitHub notification source matches on `repo_name` (the repo `full_name`, e.g.
`ClickHouse/nerve`):

```yaml
sync:
  github:
    allow_repos: ["ClickHouse/*", "myorg/myrepo"]   # Only these repos reach the inbox
    deny_repos:  ["myorg/noisy-repo"]                # Always dropped, even if allowed above
```

With `allow_repos` empty (the default), all repos pass — behavior is unchanged.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.allow_repos` | list | `[]` | Allowlist of repo globs. Empty = all repos pass |
| `github.deny_repos` | list | `[]` | Denylist of repo globs. Takes precedence over `allow_repos` |

### GitHub actor guardrail

The GitHub notification source also matches on `actors` — the list of every GitHub login
involved in a notification (issue/PR author, assignees, comment/review authors, and on a
`reason=assign` notification the account that performed the assignment). This restricts
**who** can put a notification in front of the worker, so a drive-by `@mention` from an
untrusted account is dropped before the agent ever sees it:

```yaml
sync:
  github:
    allow_actors: ["alice", "bob"]   # Only notifications involving these logins reach the inbox
    deny_actors:  ["noisy-bot"]       # Always dropped, even if otherwise allowed
```

`actors` is list-valued, so a notification is kept when **any** involved login matches the
allowlist. A non-empty `allow_actors` is **fail-closed**: a notification with no
identifiable actor (e.g. enrichment failed) is dropped. With `allow_actors` empty (the
default), all actors pass — behavior is unchanged. The repo and actor rules AND together.

A pattern that ends in `[bot]` names a GitHub App by its login. That ending is matched as
written, not as a character class of one letter: `some-ci[bot]` matches that App and never
`some-cit`. A class anywhere else in a pattern stays a class.

Assignments are worth calling out. An `assign` notification names the assignee but not the
assigner, and carries no comment text, so the assigner is fetched from the thread's event
history and rendered as `Assigned by: <login>`. Without it, an assignment on a subject the
inbox account itself authored has no third-party login anywhere — every candidate is that
one account — and a non-empty `allow_actors` would drop it fail-closed. If you rely on
"a maintainer assigns an issue to the bot" as a work signal, that login needs to be in
`allow_actors`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.allow_actors` | list | `[]` | Allowlist of GitHub login globs. Empty = all actors pass |
| `github.deny_actors` | list | `[]` | Denylist of GitHub login globs. Takes precedence over `allow_actors` |

### GitHub reason guardrail

`reason` is GitHub's own answer to "why am I being told this": `author` (you opened the
thread), `mention`, `team_mention`, `assign`, `review_requested`, `comment` (you commented
on it once), `subscribed` (you only watch it), `manual` (you subscribed by hand),
`ci_activity`, `state_change`, `security_alert`.

A reason describes the thread, not the update, and it can outlive its cause: GitHub keeps
`mention` on a thread after the first mention, so a later comment that mentions nobody
arrives as `mention` too. The guardrail filters on the value as GitHub gives it. Whether
the update itself mentions the account is in the `mention` metadata key (`new` or `old`,
see Mentions under [GitHub](#github)).

```yaml
sync:
  github:
    deny_reasons: ["comment", "subscribed", "manual", "state_change"]
```

That denylist is the compact way to say *"only threads that are mine or that call me by
name"* — it keeps `author`, `mention`, `team_mention`, `assign`, `review_requested` and
`ci_activity`. Prefer `deny_reasons` over `allow_reasons`: the allowlist is fail-closed, so
a reason GitHub adds later would be silently dropped.

In hosted mode the reason lists also bound `github_thread_item` records: the gateway
applies them to a thread, with the reason of its notification, before it writes the
thread's items (see [Hosted GitHub source](#hosted-github-source)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.allow_reasons` | list | `[]` | Allowlist of reason globs. Empty = all reasons pass |
| `github.deny_reasons` | list | `[]` | Denylist of reason globs. Takes precedence over `allow_reasons` |

#### Watched repositories: `inbox: all`

With the default `inbox: participating` the source asks GitHub for participating threads
only, so a new pull request that someone else opens in a repository the account watches
never arrives: GitHub files it under `subscribed`, which `participating=true` leaves out.
`inbox: all` lists the account's whole inbox instead, and the guardrails pick what
reaches the agent:

```yaml
sync:
  github:
    inbox: all
    deny_reasons: [ci_activity, comment, state_change]
```

That keeps the threads that are the account's or that call it by name (`author`,
`mention`, `team_mention`, `assign`, `review_requested`) and adds the `subscribed` ones:
the issues and pull requests of watched repositories, as the account's watch settings
produce them, when they are opened and whenever GitHub notifies the account about them
again. It drops the account's own workflow runs and the churn on threads it only
commented on or whose state it changed. Two settings of the GitHub account matter
here:

- **Automatic watching.** GitHub's "Automatically watch repositories" is on by default:
  every repository the account gets push access to is watched for all activity, and each
  of its threads then arrives as `subscribed` (in poll mode, each costs a few GitHub API
  calls for its content before the guardrail can drop it). Switch it off in the account's
  notification settings, which leaves the repositories already watched as they are
  (https://github.com/watching lists them), or watch the repositories that matter with a
  Custom watch (for example Pull requests only).
- **E-mail.** Independently of Nerve, GitHub also e-mails the notifications of watched
  repositories unless the account's "Watching" notifications are set to web only ("On
  GitHub"). Set that on the account, or its mailbox gets a mail for every watched thread.

In hosted mode the channel gateway must support `inbox` (see
[Hosted GitHub source](#hosted-github-source)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.inbox` | string | `participating` | `participating`: only the threads the account takes part in. `all`: its whole inbox as its watch settings produce it, for the guardrails to pick from |

### GitHub CI-branch guardrail

Keeping `ci_activity` lets in every workflow-run notification, including default-branch
runs that have nothing to do with your work — upstream syncs, scheduled cleanup jobs,
deploys. GitHub has no server-side setting for this: one workflow file serves both the
`push`-to-`main` runs and the `pull_request` runs, and the Actions notification preference
is account-wide.

CheckSuite notifications carry no `subject.url` and can't be enriched, but the branch is
right there in the title (`"CI workflow run failed for main branch"`). The source parses it
into the `ci_branch` metadata key, which is `""` for every other notification:

```yaml
sync:
  github:
    deny_ci_branches: ["main", "master"]
```

You keep CI failures on your own PR branches and drop the default-branch noise. This rule
is **deny-only** by design — `ci_branch` is empty for non-CI records, and a non-empty allow
list is fail-closed, so it would drop the entire feed.

**Inert in participating mode:** with `inbox: participating` (the default) the fetch uses
`participating=true`, which never returns `ci_activity` (see the GitHub adapter notes
above), so no CheckSuite record reaches the guardrail. With `inbox: all` the account's own
workflow runs arrive as `ci_activity`, and this rule is what keeps the default-branch runs
among them out; deny the `ci_activity` reason instead to drop every run.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.deny_ci_branches` | list | `[]` | Denylist of branch globs for CI runs. Empty = all CI notifications pass |

### GitHub pull request activity records

With `sync.github.mode: hosted` and `pr_watch: true`, the channel gateway also
delivers `github_pr_activity` records, one for each change on the agent's own open
pull requests (see [Hosted GitHub source](#hosted-github-source)). The agent is always
the author of those pull requests: the `reason` of such a record is always `author`, and
its `actors` are the reviewers and commenters. So only the repo guardrail applies to it.
`allow_actors`, `deny_actors`, `allow_reasons`, `deny_reasons` and `deny_ci_branches` are
not checked for these records, neither by the gateway nor here. An `allow_actors` list
that names none of the reviewers, or an `allow_reasons` list without `author`, does not
drop them; `deny_repos` and an `allow_repos` list that the repo does not match do.

### Debugging drops

Dropped records are never persisted, so the run summary count is backed by per-record log
lines naming the rule that fired:

```
Source github: guardrail dropped 3/5 records (e.g. '[owner/repo] CI workflow run failed …')
Source github: dropped 19542… on ci_branch='main' — [owner/repo] CI workflow run failed …
```

### Drop hook

A guardrail drop is silent by design: the agent never sees the record, and nothing
answers the person who sent it. A drop hook lets you act on a drop without letting
the content in front of the agent. Typical uses: acknowledge the dropped message
(for example, react to a GitHub comment from an account that is not on
`allow_actors`), or keep an audit trail of drops.

```yaml
sync:
  github:
    allow_actors: ["alice", "bob"]
    drop_hook: ["/usr/local/bin/my-drop-hook", "--source", "github"]
```

`drop_hook` is an argv list. Nerve runs it directly, never through a shell. A bare
string is one program path, not a command line. The hook runs once per batch that
dropped at least one record, and reads one JSON object per dropped record on stdin:

```json
{"source": "github", "id": "1234", "record_type": "github_notification",
 "summary": "[owner/repo] Some issue (mention)", "timestamp": "2026-01-01T00:00:00Z",
 "metadata": {"reason": "mention", "repo_name": "owner/repo", "actors": ["mallory"], "...": "..."},
 "dropped_on": "actors"}
```

`dropped_on` is the field of the rule that fired. The record's `content` is not
sent: the guardrail exists to keep untrusted content away from automation, and the
identifiers and metadata are enough to act on a drop. Do not use a hook to pass
dropped records to an agent. That reopens the hole the guardrail closes.

**Every field is untrusted input**, not only the content that is left out. A GitHub
`summary` carries the issue or PR title, which whoever opened it chose. Never put a
field into a shell command (`sh -c`, `eval`), a templated argument, or an LLM prompt.
Read the JSON and pass values as data.

For GitHub, `id` is the notification **thread** id. It stays the same when new activity
arrives on the thread, so key idempotency on `id` + `timestamp`, not on `id` alone. The
record does not name the comment that triggered it. A hook that needs it can look it up
with `gh api notifications/threads/<id>` (`subject.latest_comment_url`) or by listing
the thread's comments.

Rules a hook can rely on:

- **Best effort.** If the command is missing, cannot start, exits non-zero, or runs
  longer than 60 seconds, Nerve logs a warning (with the tail of the hook's stderr) and
  the sync continues. A hook never fails a sync and never holds back the cursor.
- **At least one attempt, no retry.** The hook runs after the batch is persisted and
  before the cursor advances. A batch whose persist fails is fetched again, and the hook
  gets its drops on that run. A hook that *fails* is not retried. If every drop must be
  handled, spool the records inside the hook and retry from there.
- **Once per `id` + `timestamp`.** A source can list a dropped record again. GitHub
  queries `since=<cursor>` inclusively, so the newest thread comes back on every run
  until a newer one notifies. Nerve remembers the last 4096 `id` + `timestamp` pairs it
  passed to the hook and does not pass them again. New activity on a thread changes its
  `timestamp`, so that reaches the hook. This memory is in the process only. After a
  restart, a source reload, a CLI `nerve sync`, or a crash between the hook and the
  cursor advance, the hook can get a record again. Make the hook idempotent.
- **Own process group.** The hook starts in a new session. On timeout, Nerve kills the
  whole process group, so a child process (a `gh` or `curl` call from a shell script)
  does not keep running. A process that leaves the group (`setsid`, a daemon) is out of
  reach; Nerve then waits at most 5 more seconds. stdout is discarded and stderr goes to
  a temporary file, so a background job the hook leaves behind does not hold the sync.
  That job keeps running, and it is the hook's own business. Read all of stdin before
  you exit, or start background jobs with stdin from `/dev/null`: a job that inherits
  an unread stdin holds the batch until the timeout.
- **Daemon context.** The hook inherits the daemon's environment (including any API
  keys in it), working directory, and privileges. A `gh` call uses the daemon's GitHub
  login, and its API quota: anyone who can trigger drops (for example, by @-mentioning
  the account) can make the hook run, so rate-limit expensive work inside the hook.
- **Validated at startup.** Without a guardrail nothing is dropped, so the hook never
  runs, and Nerve logs a warning. A program that is not an executable (on `PATH` or by
  path) also logs a warning. A value that is not a list of strings, or has an empty
  program name, turns the hook off with a warning.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.drop_hook` | list | `[]` | argv of a command that gets each dropped record as a JSON line on stdin. Passed as is: no shell, nothing stripped. Empty = off |

### Extending to other sources

Adding a guardrail to another source is a config field plus one registry line. In
`build_source_runners()`, build an `InboxFilter` from the relevant metadata key and pass
it to the `SourceRunner`:

```python
from nerve.sources.filters import InboxFilter

# e.g. filter Gmail by label, or Telegram by chat_id
flt = InboxFilter.from_field("labels", allow=cfg.allow_labels, deny=cfg.deny_labels)
runners.append(SourceRunner(..., inbox_filter=flt))
```

A drop hook is one more argument: `SourceRunner(..., inbox_filter=flt, drop_hook=cfg.drop_hook)`.

(Telegram already filters by folder/chat inside `fetch()` via `monitored_folders` /
`exclude_chats`; those remain source-specific.)

## Agent Tools

### `list_sources`
List available sources with message counts and optional consumer cursor status.

```
list_sources(consumer="inbox")
→ - github: 45 messages | consumer "inbox": 3 unread
  - gmail:user@example.com: 120 messages | consumer "inbox": 0 unread
  - telegram: 200 messages | consumer "inbox": 5 unread
```

### `poll_source`
Poll new messages from a specific source using a persistent consumer cursor. Advances the cursor.

```
poll_source(source="github", consumer="inbox", limit=50)
→ ## 3 message(s) from github
  ⚠️ UNTRUSTED DATA — ...
  ### [1/3] github: [myorg/myrepo] Issue #42 (assign)
  ...
```

**Security:** All message content is prefixed with an untrusted data warning. The agent is instructed not to follow instructions embedded in message content.

### `read_source`
Browse historical messages from a source without advancing any cursor. For debugging or review.

```
read_source(source="github", limit=5)
read_source(source="github", before_seq=1234, limit=10)   # paginate backwards
read_source(source="github", after_seq=1000, limit=10)    # paginate forwards
```

### `sync_status` (legacy)
Check the status of sync source fetch cursors. Kept for backward compatibility.

## CLI Usage

```bash
# Run all sources manually
nerve sync

# Run a specific source
nerve sync gmail
nerve sync github
nerve sync telegram
```

Output shows per-source results:
```
Running sync: all
  Running: gmail ... [OK] 5 ingested
  Running: github ... [OK] 3 ingested
  Running: telegram ... [OK] 0 ingested
```

## Monitoring

### Web UI
The Sources page (`/sources`) has three tabs:
- **Inbox** — Messages sorted by timestamp, with source-specific renderers (HTML for email, GitHub cards, markdown)
- **Runs** — Source fetch history, filterable (empty runs hidden by default)
- **Consumers** — Active consumer cursors with unread counts, linked sessions, and expiry times

### Database tables
- `sync_cursors` — Current source fetch cursor per source (producer-side)
- `consumer_cursors` — Per (consumer, source) read position with TTL and session linking
- `source_messages` — Inbox messages with `raw_content` (original HTML), `processed_content` (LLM-condensed), TTL-based expiry
- `source_run_log` — Per-run diagnostics (records ingested, errors, timestamps)
- `channel_observations` — Push buffer for chat messages a channel collected for the inbox, drained by `ChannelSource`, and for the records of the hosted GitHub sources, drained by the `github` and `github_repos` runners. Trimmed toward a per-transport row target and TTL-swept by daily cleanup
- `cron_logs` — Job execution history (source jobs use `source:<name>` as job ID)

### API Endpoints
- `GET /api/sources/overview` — Per-source stats (cursor, message count, storage, run history)
- `GET /api/sources/messages` — Paginated inbox messages
- `GET /api/sources/runs` — Source fetch run history
- `GET /api/sources/consumers` — Active consumer cursors with unread counts
- `POST /api/sources/{source}/sync` — Trigger source sync manually
- `POST /api/sources/sync-all` — Trigger all sources

## Adding a New Source

1. **Create the adapter** at `nerve/sources/mysource.py`:

```python
from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

class MySource(Source):
    source_name = "mysource"

    def __init__(self, config: dict):
        self._config = config

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        # Fetch records from your service
        # cursor is opaque — you define the semantics (timestamp, ID, page token, etc.)
        records = []
        next_cursor = cursor

        # ... fetch logic here ...
        # Build SourceRecord objects:
        # SourceRecord(id, source, record_type, summary, content, timestamp, metadata)

        return FetchResult(
            records=records,
            next_cursor=next_cursor,
            has_more=False,
        )

    async def preprocess(self, records: list[SourceRecord]) -> list[SourceRecord]:
        # Optional: source-specific content cleanup before storage.
        # Default (inherited from Source): returns records unchanged.
        return records

    async def close(self) -> None:
        # Optional cleanup (disconnect clients, etc.)
        pass
```

2. **Add config dataclass** in `nerve/config.py`:

```python
@dataclass
class MySourceSyncConfig:
    enabled: bool = True
    schedule: str = "*/15 * * * *"
    batch_size: int = 50
    condense: bool = False
    # ... source-specific fields ...

    @classmethod
    def from_dict(cls, d: dict) -> MySourceSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            schedule=d.get("schedule", "*/15 * * * *"),
            batch_size=d.get("batch_size", 50),
            condense=d.get("condense", False),
        )
```

Add it to `SyncConfig`:
```python
@dataclass
class SyncConfig:
    telegram: TelegramSyncConfig = ...
    gmail: GmailSyncConfig = ...
    github: GitHubSyncConfig = ...
    mysource: MySourceSyncConfig = field(default_factory=MySourceSyncConfig)
```

3. **Register in the registry** at `nerve/sources/registry.py`:

```python
# In build_source_runners():
ms = config.sync.mysource
if ms.enabled:
    from nerve.sources.mysource import MySource
    source = MySource(config={...})
    runners.append(SourceRunner(
        source=source, db=db,
        batch_size=ms.batch_size,
        condense=ms.condense,
        condense_model=condense_model,
        condense_client_factory=condense_factory,
        ttl_days=ttl_days,
    ))
```

4. **Add config** to `<workspace>/config/settings.yaml`:
```yaml
sync:
  mysource:
    enabled: true
    schedule: "*/15 * * * *"
```

That's it. The source will auto-register as an APScheduler job on next restart. Messages will appear in the inbox and be picked up by the `inbox-processor` consumer.

## Adding a New UI Renderer

The inbox detail panel uses source-specific renderers to display message content. Each source type gets an appropriate renderer (e.g., emails render HTML, GitHub shows a repo card + markdown).

### Renderer Architecture

```
web/src/components/Sources/
├── MessageContent.tsx              ← Dispatcher (picks renderer by source type)
└── renderers/
    ├── index.ts                    ← Registry: source name → renderer type mapping
    ├── MarkdownRenderer.tsx        ← Default: ReactMarkdown with prose styles
    ├── EmailRenderer.tsx           ← Gmail: sandboxed iframe for HTML + text toggle
    └── GitHubRenderer.tsx          ← GitHub: repo/PR header card + markdown body
```

### Steps

1. **Create the renderer** at `web/src/components/Sources/renderers/MyRenderer.tsx`:

```tsx
import { MarkdownRenderer } from './MarkdownRenderer';

interface Props {
  content: string;
  rawContent?: string | null;
  metadata?: Record<string, any>;
  summary: string;
}

export function MyRenderer({ content, metadata, summary }: Props) {
  return (
    <div>
      {/* Custom header/card/etc */}
      <MarkdownRenderer content={content} />
    </div>
  );
}
```

2. **Register the source type** in `renderers/index.ts`
3. **Handle in dispatcher** — add the case to `MessageContent.tsx`

## Architecture

### Key Files

| File | Purpose |
|------|---------|
| `nerve/sources/models.py` | `SourceRecord`, `FetchResult`, `IngestResult` dataclasses |
| `nerve/sources/base.py` | `Source` abstract base class |
| `nerve/sources/runner.py` | `SourceRunner` — pure ingestion pipeline (fetch → preprocess → guardrail → persist → condense → drop hook → advance) |
| `nerve/sources/filters.py` | `InboxFilter` / `FieldRule` — declarative allow/deny guardrails applied before persist |
| `nerve/sources/processor.py` | Legacy agent prompt building (unused by runner, may be used by tools) |
| `nerve/sources/registry.py` | Config → `list[SourceRunner]` factory |
| `nerve/sources/telegram.py` | Telegram adapter (Telethon) |
| `nerve/sources/gmail.py` | Gmail adapter (gog CLI) |
| `nerve/sources/github.py` | GitHub notifications adapter (gh CLI), record builder, GitHub guardrail |
| `nerve/sources/github_hosted.py` | GitHub sources in hosted mode (`github`, `github_repos`): drain the records the channel gateway delivered |
| `nerve/sources/github_events.py` | GitHub user events adapter (gh CLI) |
| `nerve/sources/github_repos.py` | GitHub repository watch adapter (gh CLI), record builder, its guardrail |
| `nerve/agent/tools.py` | Consumer tools: `list_sources`, `poll_source`, `read_source` |
| `web/src/components/Sources/` | Source message renderers (email, GitHub, default markdown) |

### Cursor Design

**Source cursors** (producer-side): Each source owns its cursor semantics. The framework treats cursors as opaque strings stored in the `sync_cursors` SQLite table. Sources return the next cursor in `FetchResult.next_cursor` — the runner stores it as-is. Cursors advance after successful ingestion (persist to inbox).

**Consumer cursors**: Stored in `consumer_cursors` table with composite key `(consumer, source)`. Each consumer has an independent cursor per source, using the implicit `rowid` of `source_messages` as the offset. New cursors initialize to `MAX(rowid)` (no backlog). Cursors expire after `consumer_cursor_ttl_days` of inactivity.

**Inclusive APIs:** Both GitHub and Gmail APIs use inclusive cursor semantics (returning records `>= cursor`). Gmail advances the cursor at query time (+1s) and applies client-side dedup as a safety net. GitHub queries the cursor itself, because a notification can land in the cursor's own second; storage skips the unchanged thread listed again.

**Concurrency safety:** Each `SourceRunner` holds an `asyncio.Lock` to prevent concurrent execution of the same source. If a cron schedule fires while a manual sync is in progress, the second call returns immediately with 0 records.

### Security

All message content returned by `poll_source` and `read_source` is prefixed with an untrusted data warning. Source messages come from external services and may contain prompt injection attempts. The agent is instructed to act only on factual information (sender, subject, PR numbers) and never follow instructions embedded in message content.
