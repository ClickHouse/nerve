# Screenshots — multi-user UI stack

All captured against a throwaway Nerve instance built from
`alex/mu-06-setup-wizard` (worktree `nerve-mu-06`, tip `f2b2608`), on its own
`NERVE_HOME`/config dir, loopback port 8123. Chromium via Playwright, viewport
1280x800, `fullPage: false`. Two synthetic accounts: **alice** ("Alice") and
**bob** ("Bob"). No setup token, API key or real address appears in any shot —
the instance has no model credentials, which is why the assistant turns in
`440-chat-attribution.png` fail.

| File | What it shows | PR |
|---|---|---|
| `441-setup-unclaimed.png` | Fresh passwordless instance: the root redirect lands on `/setup`, which renders the claim card alone — username, password, optional display name, and the setup-token field that a browser on the machine itself does not need (no token is shown, and none was required from loopback). While unclaimed the wizard deliberately withholds the checklist: the claim is the only write anybody may perform. | #441 |
| `441-setup-checklist.png` | After claiming as `alice` / "Alice": the derived checklist in dark theme, one card per state — account **done** ("The account has a password."), provider **to do**, timezone-and-name **done** ("Alice, America/New_York"), Telegram **skipped** (offering "Put back"), automation **to do** — plus the "Signed in as Alice." line and the restart card reporting "Waiting on a restart: timezone." after a profile write. | #441 |
| `441-setup-checklist-light.png` | The same checklist and restart card in light theme (`localStorage['nerve-theme'] = 'light'`). | #441 |
| `438-accounts.png` | `/accounts` with two accounts: `alice` carrying the **you** badge and `bob` beneath it, the "Add account" create form expanded above the list (username / display name / password, with the "no roles" warning), bob's row in the disable-confirmation state ("Disable bob? Any session they have open stops working at its next request."), and the own-password form at the foot. | #438 |
| `438-login-username.png` | The login page after signing out, with two accounts on the instance: `/api/auth/status` reports `login: "username_password"`, so the form collects a **username as well as a password** — the field that only appears in that mode. | #438 |
| `440-chat-attribution.png` | One session with two senders. Sidebar: "Bob's scratch notes — Bob" and "Yesterday's notes — Alice" (creator markers, shown because the list disambiguates). Header: the "Started by Alice" chip, which renders whenever the id is non-null. Transcript: the user bubbles labelled **Alice** and **Bob**; the assistant rows carry no attribution at all. The two assistant turns fail ("Turn failed: api error") because the throwaway instance has no model credentials — expected, and not what the shot is about. | #440 |
