# Platform activity reporting

Some platforms run Nerve on a machine they park when it is idle. Parking
suspends the machine, and the platform wakes it again for the next request or
the next scheduled task. To decide when to park, the platform has to know from
inside the machine whether Nerve is busy and when it next has work. It can
learn this through a **local activity socket**: an HTTP/1.1 endpoint on a Unix
socket inside the machine. When that socket exists, the gateway reports to it.
Everywhere else reporting is off and costs nothing.

The reporter lives in `nerve/platform/activity.py`.

## When it is on

The gateway checks once, at startup, right after the cron service starts.
`platform.activity_socket` (default `/run/nerve/guestd.sock`) must name an
existing Unix socket. Either way the gateway logs one INFO line. If the socket
is present, the line starts with "Reporting activity to the platform's local
activity socket". If it is missing, the line says reporting is off.
`nerve doctor` shows the same check:

```
[OK] Platform activity socket: present (<path>)
[--] Platform activity socket: not present (<path>)
```

A socket that appears after startup is not picked up, and a socket that goes
away is retried (see [Failures](#failures)). The `platform.*` settings are
read once at startup. Changing them needs a restart.

## What is reported

Every report is `POST /v1/activity` with this JSON body:

```json
{
  "runningTurns": 1,
  "activeSessions": 2,
  "nextScheduledAt": "2026-10-22T03:00:00Z",
  "nextScheduledName": "memory-maintenance",
  "scheduledJobs": {
    "state": "running",
    "total": 5,
    "failingCount": 1,
    "failing": [
      {"name": "daily-digest", "since": "2026-10-21T07:00:00Z", "failures": 2, "lastRunAt": "2026-10-22T07:00:00Z"}
    ]
  }
}
```

| Field | Meaning |
|-------|---------|
| `runningTurns` | Sessions running a turn now, plus [workflow runs](workflow-runs.md) that are queued or still running without a turn (for example while background work they started drains). A run counts once. Its turn is counted through its session. Review-loop legs are workflow runs. A turn that starts and ends between two reports is still reported once as running, so the platform can tell that something ran. |
| `activeSessions` | Open web UI connections (authenticated WebSocket clients of `/ws`). Telegram, Slack and hosted channel streams are not client connections and are not counted. |
| `nextScheduledAt` | When the next scheduled task that will actually run is due, in UTC (RFC 3339, whole seconds). `null` when nothing is scheduled. The cron service forecasts it. Jobs whose gates would skip the run are left out; source runners and pending session wakeups are included, except a source whose buffer only the running instance writes (the hosted GitHub source, the channel drains `<channel>:observed`) while nothing delivered waits in it: nothing is added to that buffer while the machine is parked, so its scheduled run is not counted then. The internal `cleanup` and `wakeup_sweep` jobs never count. See [Cron](cron.md). |
| `nextScheduledName` | That task's name: the job id, `source:<name>`, or `wakeup:<first 8 characters of the session id>`. Any character outside `[A-Za-z0-9._:-]` becomes `_`, and the name is cut at 64 characters: the socket refuses a whole report whose name breaks that rule. `null` together with `nextScheduledAt`. |
| `scheduledJobs` | How the scheduled jobs do (see [The summary of the scheduled jobs](#the-summary-of-the-scheduled-jobs)). Absent until it was read once, as when the first read failed. |

### The summary of the scheduled jobs

`scheduledJobs` lets the platform tell a scheduler that does not run, and jobs
that keep failing, from an agent with nothing to do. It holds no error text.

| Field | Meaning |
|-------|---------|
| `state` | `running` while the cron service runs. `failed` when it did not start (the gateway logs "Cron service failed to start") or does not run: then `nextScheduledAt` is `null` too, which alone looks like an agent without jobs. |
| `total` | The enabled jobs that the forecast counts: agent jobs, workflow jobs and source runners. The internal `cleanup` and `wakeup_sweep` jobs never count. `0` with `failed`. |
| `failingCount` | Every failing job, also those that `failing` leaves out. |
| `failing` | At most 16 failing jobs, the one failing longest first, then by name. |
| `failing[].name` | The job id, or `source:<name>`, in the form of `nextScheduledName`. |
| `failing[].since` | When the first failed run of the current streak started, in UTC (RFC 3339, whole seconds). |
| `failing[].failures` | Failed runs in a row. |
| `failing[].lastRunAt` | When the latest failed run started, in UTC. |

When a job fails:

- **Agent job:** its latest finished run failed. The streak is the failed runs
  after its latest successful one in the [run history](cron.md). A missed run
  and a run that is still going neither count nor end a streak. The history
  keeps 14 days of runs, so a longer streak counts from its oldest run kept.
- **Source runner:** its circuit breaker counts failures. A source that backs
  off logs the runs it skips as successful runs that found nothing, so the
  history cannot tell. The breaker lives in memory: after a restart, a streak
  counts from its first failure since then.
- **Workflow job:** never reported as failing. Its run history says only that
  the run was launched.

The summary is read again when a job or source run ends, when the schedule
changes, and with each periodic report, and kept between them, so the reports
for turns and clients never read the database. A read is bounded at five
seconds. A read that fails or times out keeps the last summary, and is logged
like the other failures.

A report is sent:

- when something in it changes: a turn starts or ends, a client connects or
  leaves, the cron schedule changes (start, reload, a job or source run
  finishing, a wakeup being claimed), or a workflow run finishes. At most one
  report goes out every 250 ms. A burst of changes is folded into one report
  that shows the state at its end. The first change after a quiet period goes
  out at once, so a turn start reaches the platform right away. The report that
  follows a run already names the task after it. A wakeup that a session
  schedules during its turn is in the report at the end of that turn.
- every `platform.activity_interval_seconds` (default 30), whatever happened.
- once more at shutdown, saying nothing runs and nobody is connected. The next
  scheduled task is kept, because it is still due after a restart.

Every report except a turn start and the last one at shutdown waits for a new
forecast of the next scheduled task, which normally takes milliseconds: what
runs next can change without the reporter hearing of it. A turn may have
scheduled a wakeup, and a message deferred with "Run later" in the web UI adds
one without any turn, so the report sent when that client leaves names it. If
a forecast is already under way, a report goes out when it finishes, and
another follows the new one. A turn start goes out at once with the last
forecast, so it is never held up by one.

## Keep-awake leases

Background work that is not a turn holds a named lease while it runs. The
gateway takes a lease with `POST /v1/leases {"name": "<name>", "ttlSeconds":
<platform.lease_ttl_seconds>}`. It renews the lease every half lifetime while
the work lasts. When the work ends, and at shutdown, it releases the lease with
`DELETE /v1/leases/<name>`. Several pieces of work with the same name share one
lease. A lease that is not renewed, for example because the gateway died,
expires on its own after its lifetime.

| Lease | Held while |
|-------|------------|
| `nerve-memorize` | Memorization is queued or running. This covers background memorization of a session (after a client is let go, a turn is stopped or fails, a session is archived or deleted, or a cron session is rotated), the periodic and manual memorization sweeps, and the copy of a memorized conversation to xmemory. |
| `nerve-config-sync` | A [workspace sync](config.md#git-backed-workspace-sync) cycle pulls and applies, in the periodic loop and in `POST /api/config/sync`. |
| `nerve-backup` | The scheduled backup (`backup.*`) writes and prunes bundles. |
| `nerve-background` | Any session has a live background agent or task, that is anything still starting, running or stopping in the session's [Agents panel](agents.md): sub-agents, workflows, background shells, monitors, teammates, remote agents and MCP tasks that an agent started. The lease is held after the turn that started them has ended. A task that never ends on its own, such as a dev server in a background shell or a monitor, keeps the machine awake until it is stopped. |

Not covered by a lease:

- **Work in another process.** `nerve backup` and `nerve config sync` run in
  the CLI process, not in the gateway.
- **Work inside a turn or a request.** The `memorize` tool, memory recall, and
  other tool calls run inside a turn, which is already reported as running.
  Calls to the external MCP endpoint from other agents are requests to the
  gateway, not turns, and are not reported.
- **Startup memorization** of sessions left active by the previous process runs
  before the reporter starts.
- **A background task reported in the last few seconds** that has no agent
  record yet (the runtime can announce a task about ten seconds before the
  record exists). The next report or turn end picks it up.
- **Other background loops** hold no lease, because they are short or resume
  where they left off: database retention, notification maintenance, session
  cleanup, the idle-client sweep, external-agents sync, Codex thread sync and
  the model catalog refresh.

Code that adds such work can hold a lease with `keep_awake` (from
`nerve.platform.activity`). It is a context manager, it works from any thread,
and it does nothing without a socket:

```python
from nerve.platform.activity import keep_awake

with keep_awake("nerve-backup"):
    ...
```

## Failures

Reporting is best effort. A request can fail in two ways: the socket answers
with an error, or nothing answers (no socket, a refused connection, or a
two-second timeout). The first failure of each kind (reports, leases, the
forecast) is logged at INFO. Repeats are logged at DEBUG at most every five
minutes, and recovery is logged at INFO again. Nothing is raised into the work
being reported on. If the reporter itself fails to start, the gateway keeps
running without it. A failed lease request is tried again after five seconds,
or after a quarter of the lease lifetime if that is shorter. So a renewal that
fails, even by timing out, is retried before the lease runs out.

If the cron forecast fails, the last known next task is reported again.

A socket that does not know `nextScheduledName` refuses unknown fields with a
400, and with them the whole report. Such a socket does not know
`scheduledJobs` either, which came later. On a 400 the report is sent once
more without both. If that is taken, every later report leaves both out until
the gateway restarts, and one INFO line says so ("activity socket does not
accept task names; reporting times only, without the summary of the scheduled
jobs"). If it is refused as well, they were not the reason. Each later report
is then sent once, without both, until the socket takes one, and the report
after that has them again.

## Switching it off

```yaml
# config.yaml
platform:
  activity_socket: ""
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `platform.activity_socket` | path | `/run/nerve/guestd.sock` | The platform's local activity socket. Reporting is on only when this names an existing Unix socket at startup. `""` switches reporting off. |
| `platform.activity_interval_seconds` | int | `30` | Seconds between reports when nothing changes. Held to 5–300. |
| `platform.lease_ttl_seconds` | int | `120` | Lifetime of a keep-awake lease. Renewed every half lifetime. Held to 10–3600. |

## The socket's side

A platform that wants these reports serves HTTP/1.1 on a Unix socket that the
Nerve process can open:

- `POST /v1/activity`: the body above. It replaces the previous report. A
  socket that ignores the fields it does not know keeps working when a later
  release adds one.
- `POST /v1/leases`: body `{"name", "ttlSeconds"}`. It creates or renews the
  lease. Lease names match `[A-Za-z0-9._-]{1,64}`.
- `DELETE /v1/leases/<name>`: releases the lease. `404` counts as released.

Any 2xx answer is success. An error body of the form
`{"error": "<short_code>"}` is logged with its code. Request bodies are never
logged.
