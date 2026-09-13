# Screenshots — multi-user UI stack

Images-only branch, referenced from the descriptions of the stacked pull
requests. It carries no code and is not meant to be merged. Link to the **raw
URL of a commit**, never of a branch, so a description keeps showing what it
showed when it was written.

Every image below was captured against a **throwaway instance** — its own
`NERVE_HOME`, config directory and workspace, on a loopback port, with two
synthetic accounts, **alice** ("Alice") and **bob** ("Bob"), and fabricated
transcripts. No setup token, API key, real name or internal address appears in
any of them. Chromium via Playwright, `fullPage: false`.

| File | PR | Captured from | What it shows |
|---|---|---|---|
| `438-accounts.png` | [#438](https://github.com/ClickHouse/nerve/pull/438) | `f2b2608`, re-verified against `f167868` | `/accounts` with two accounts: `alice` carrying the **you** badge and `bob` beneath it, the "Add account" form expanded above the list (username / display name / password, with the "no roles" warning), bob's row in the disable-confirmation state, and the own-password form at the foot. |
| `438-login-username.png` | [#438](https://github.com/ClickHouse/nerve/pull/438) | `f2b2608`, re-verified against `f167868` | The login page with two accounts on the instance: `/api/auth/status` reports `login: "username_password"`, so the form collects a **username as well as a password** — the field that only appears in that mode. |
| `440-chat-attribution.png` | [#440](https://github.com/ClickHouse/nerve/pull/440) | `0ee534f` (#440 tip) | The viewer-relative rule, viewed **as Alice**, on a session Bob started that both posted in. Header: the "Started by Bob" chip. Sidebar: "Release notes for 0.4 — Bob", and "Yesterday's notes" — Alice's own — with no marker at all. Transcript: Bob's message labelled **Bob**, Alice's own unlabelled, and the assistant turns carrying no attribution. |
| `441-setup-claim.png` | [#441](https://github.com/ClickHouse/nerve/pull/441) | `2f8c72e` (#441 tip) | A fresh passwordless install: `/setup` renders the claim form alone — username, password, optional display name, and the **mandatory** setup token, required here however the page was reached. The token field is empty, no credential is shown, and the submit stays disabled until it is filled in. The instance withholds every other write until the claim: it is the only one anybody may perform. |
| `mu07-setup-unclaimed.png` | branch 7 (no PR yet) | `7119b1e` | The same first-run state on `alex/mu-07-onboarding-wizard`, where `/setup` is the checklist's first step rather than a page of its own. Same mandatory token; the rest of the list is withheld until the claim. |
| `mu07-setup-checklist-dark.png` | branch 7 (no PR yet) | `7119b1e` | The post-claim checklist, signed in as Alice: account **done** ("The account has a password."), provider **to do**, timezone-and-name **done** ("Alice, UTC" — the zone was written, and this process is still running the old one), Telegram **skipped** and offering "Put back", automation **to do** with the optional crons the installer wrote. At the foot, "Restart to apply": *Waiting on a restart: timezone. Run `nerve restart` on the server, then reload this page.* — **and no button**, because restarting the daemon is an operator action, not a browser one. |
| `mu07-setup-checklist-light.png` | branch 7 (no PR yet) | `7119b1e` | The same checklist in light theme (`localStorage['nerve-theme'] = 'light'`). |
| `mu07-setup-checklist-mobile.png` | branch 7 (no PR yet) | `7119b1e` | The same checklist at 375px wide: cards stack, the per-step controls wrap below the title, and the bottom nav replaces the rail. |

## Superseded

Commit `f6bad6f` also held `441-setup-unclaimed.png`,
`441-setup-checklist.png` and `441-setup-checklist-light.png`, captured from
`f2b2608` — the pre-trim tip of the old #441, before the stack was rewritten.
They are removed rather than kept, because they describe a #441 that no longer
exists: that claim form offered the setup token as *optional* ("only from
another machine"), and the checklist beside it was trimmed out of #441
entirely. The checklist now lives on `alex/mu-07-onboarding-wizard` and is
re-shot above, without the "Restart now" button that version had. The old
files stay readable at `f6bad6f`.
