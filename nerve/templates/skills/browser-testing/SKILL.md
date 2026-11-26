---
name: Browser Testing
description: >
  This skill should be used when the person asks to check a web app in a real
  browser: "test the UI", "screenshot the page", "check it on mobile", "run an
  accessibility check", "check the contrast", "does the sign-up flow work", "find the
  console errors", "verify the frontend change before the PR", "reproduce this UI
  bug". It covers headless Chromium setup, serving the app under test, an audit
  (axe-core, layout, console, requests), user flows with screenshots and a trace,
  and evidence for a PR or a bug report. Not for building a page to share
  (shareable-html) or slides (slide-deck).
version: 1.0.0
---

# Browser Testing

Type checks, linters and unit tests do not see the rendered page. Low contrast, a
box with zero width, a page that scrolls sideways on a phone and a console error
show only in a browser. This skill runs the app in headless Chromium, looks at it,
and gives evidence: counts, screenshots and a trace. Run the scripts with Bash by
path (`skill_run_script` stops after 30 s). `SK=<workspace>/skills/browser-testing`.

| File | Use |
|---|---|
| `scripts/with_server.mjs` | start the app's server, wait until it answers, stop only that server |
| `scripts/snapshot.mjs` | screenshot, accessibility tree and ready locators of one page |
| `scripts/audit.mjs` | contrast, layout, console and request checks over routes, viewports, schemes |
| `assets/flow.mjs` | template for a user flow: a screenshot per step, a trace on failure |
| `scripts/trace_summary.py` | print a `trace.zip` as text (actions, errors, failed requests) |
| `references/recipes.md` | snippets for one-off checks: mocks, dialogs, downloads, devices, focus |

## Rules

- Never install test tooling into the repo under test. `npm install axe-core` there
  changes `package.json` and the lock file, and that diff goes into the PR.
- Stop only the server that you started, by its process id. Never `pkill -f` or
  `killall`: the pattern can match the agent's own shell or another person's server.
- Never put a token or a password in the chat, a PR, or a file in the repo.
- Do not rebuild or restart an instance that people use, to see your change. Serve
  your branch on its own port.
- Run an app's own server only for code that you work on, or when the person asks.
  A repository that you downloaded only to read is data: test its deployed URL.
- On a live site, do not submit forms that send mail, take payments or change data
  without the person's word.

Working folder: `W=~/.local/share/browser-tests/<slug>`. Put screenshots, traces,
the flow copy and sign-in files there, not in the workspace or the repo.

## 1. Browser setup

All bundled skills share one tools folder. This section is the full setup and the
place to look when a browser does not start.

```bash
B=${NERVE_BROWSER_DIR:-$HOME/.local/share/nerve-browser}
mkdir -p "$B" && cd "$B"
[ -f package.json ] || npm init -y >/dev/null
npm install --no-audit --no-fund playwright axe-core
npx playwright install chromium                  # the browser revision that matches this library
# Linux without the browser's libraries: as root, or with sudo when the person allows it
npx playwright install-deps chromium
```

Check it: `node $SK/scripts/snapshot.mjs --url "data:text/html,<h1>Browser works</h1>"
--out /tmp --name browser-check` must print `page.getByRole('heading', { name:
'Browser works' })`. On a bare Ubuntu 24.04 as root, the browser download is about
330 MiB (Chrome for Testing 199 MiB, headless shell 126 MiB, FFmpeg 2 MiB, about
20 s); `install-deps` adds 103 packages (98 MB download, 361 MB on disk, about 20 s).

| Symptom | Cause | Fix |
|---|---|---|
| `playwright is not in ...` from a skill script | setup not done, or another `NERVE_BROWSER_DIR` | run the setup above |
| `Executable doesn't exist at .../chromium_headless_shell-<rev>` | the browser cache holds another revision than the library wants | `npx playwright install chromium` in the tools folder; do not change the library version to match an old browser |
| `Target page, context or browser has been closed` with `error while loading shared libraries: libglib-2.0.so.0` | system libraries are missing | `npx playwright install-deps chromium` as root; with sudo only when the person allows it; else give the person the command. `npx playwright install-deps --dry-run chromium` lists the packages |
| `npm error code E403` and `403 Forbidden - GET https://registry.npmjs.org/...` (or `ETIMEDOUT`, `ECONNRESET`) | the proxy refuses the npm registry | tell the person the host from the error |
| `Error: Download failed: server returned code 403 ... URL: https://cdn.playwright.dev/...` | the proxy refuses the browser download host | tell the person the host from the URL; `npx playwright install --dry-run chromium` lists all URLs. When they name a mirror, set `PLAYWRIGHT_DOWNLOAD_HOST=<mirror URL>` |
| `install-deps` fails in `apt-get` | the proxy refuses the Ubuntu package archive | tell the person; they install the packages from the dry-run list |

Fonts: on a bare Ubuntu, after `install-deps`, `fc-match sans-serif` gives
"WenQuanYi Zen Hei". It has no bold face, so headings render thin. As root (or with
sudo when the person allows it), run `apt-get install -y fonts-dejavu-core`; then
`fc-match sans-serif` gives "DejaVu Sans".

`npx playwright install --only-shell chromium` saves about 200 MiB when you need
only headless runs. Playwright starts Chromium with `--no-sandbox`, so it runs as
root with no extra flag. When a `nerve-cloud` skill is present, read it for the
machine, the proxy and how to serve a port to the agent's members.

## 2. Serve the app under test

When the person gives a URL (a preview or staging deploy), use it and skip this step.

1. Find the dev server command in the README, in `package.json` (`scripts.dev`,
   `start`, `preview`), a Makefile or a Procfile.
2. Install the app's own dependencies without a change to its files: `npm ci`,
   `pnpm install --frozen-lockfile` or `yarn install --immutable`. Then `git status`
   must show no change to package files.
3. Pick a free port: `ss -ltn | grep ':8123 '` must print nothing. A port that
   another session uses is not yours.
4. Make a collision fail loudly. Vite: `--port 8123 --strictPort` (without it, Vite
   prints `Port 8123 is in use, trying another one...` and you test the wrong app).
   `with_server.mjs` also refuses to start when the port already answers.
5. Bind to `127.0.0.1` when the server allows it. For `--url`, use the host and
   port that the server prints when it starts.

One command, server stopped at the end, exit code of the test:

```bash
node $SK/scripts/with_server.mjs --cmd "npm run dev -- --port 8123 --strictPort" \
  --cwd <repo> --url http://127.0.0.1:8123/ --out "$W/server.out" \
  -- node $SK/scripts/audit.mjs --base http://127.0.0.1:8123 --routes /,/settings
```

For several checks in a row, start it once, and stop it at the end also after a
failure:

```bash
node $SK/scripts/with_server.mjs start --cmd "python3 -m http.server 8123 --bind 127.0.0.1" \
  --cwd <site> --url http://127.0.0.1:8123/ --pidfile "$W/server.pid" --out "$W/server.out"
# ... snapshot, audit, flows ...
node $SK/scripts/with_server.mjs stop --pidfile "$W/server.pid"
ss -ltn | grep ':8123 ' || echo "port 8123 closed"
```

The helper runs the command in its own process group and stops that group only.
The command must stay in the foreground. When the server does not answer in time
(`--timeout`, default 60 s), the helper prints the end of its output. Without the
helper, note: a `kill` of the `npm` PID leaves the server child running. Kill the
process that holds your port (`ss -ltnp | grep ':8123 '` shows its `pid=`). `ss` is
missing on some minimal images; the helper's port check works without it.

A plain static page works without a server: `--base file:///path/to/dir`. Pages
that use `fetch` or ES modules need a server.

## 3. Look before you write selectors

```bash
node $SK/scripts/snapshot.mjs --url http://127.0.0.1:8123/ --out "$W/look" --name home
```

It writes `home.png` and `home.aria.yml`, and prints a locator for each control and
heading, the test ids and the console errors:

```
locators (role and name; prefer these to CSS selectors):
  page.getByRole('link', { name: 'Pricing' })
  page.getByRole('textbox', { name: 'Email' })
  page.getByRole('button', { name: 'Subscribe' })
```

Read the screenshot with the Read tool. Then write steps with `getByRole`,
`getByLabel` and `getByTestId`. Use a CSS selector only when nothing else works. A
control with no role or name is itself a finding: a screen reader cannot name it.

## 4. Sign-in

Without a session, an app shows its sign-in page or empty panels, and its API calls
fail with 401 (the audit counts them under `requests=`). Seed the session before the
app starts. A `page.evaluate` after `goto` is too late: the app has already read its
storage and shows "Please sign in".

1. A test account from the person: sign in once with a short script, save
   `context.storageState({ path: "$W/state.json" })` (see `references/recipes.md`),
   then pass `--storage "$W/state.json"` to the scripts.
2. A session cookie or a local storage entry: write the state file by hand:
   `{"cookies": [], "origins": [{"origin": "http://127.0.0.1:8123", "localStorage": [{"name": "acme_token", "value": "..."}]}]}`.
3. A token that the app reads at start: an init file, passed with `--init`. It runs
   before the app's code on every page. Seed only the app's origin:

```js
// $W/seed.js
if (location.origin === 'http://127.0.0.1:8123') {
  localStorage.setItem('acme_token', '<token>');
  // An app with its own theme switch ignores the emulated scheme. Follow it here:
  localStorage.setItem('acme_theme', matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
}
```

Get the token from where the person keeps it (an environment variable, a secrets
file, the app's own command to make one). Do not print it. Check it with one API
call first: `curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer
$TOKEN" http://127.0.0.1:8123/api/me` must print `200`. Make sign-in files with
`umask 077` in `$W` and delete them at the end. A trace or a HAR file of a signed-in
run holds cookies and headers: do not attach it to a public PR or issue.

## 5. Audit

```bash
node $SK/scripts/audit.mjs --base http://127.0.0.1:8123 --routes /,/pricing \
  --viewports desktop,mobile --schemes light,dark --shots "$W/shots"
```

Options: `--routes` (default `/`); `--viewports` `desktop` (1280x800), `mobile`
(375x812, touch) or `<w>x<h>`; `--schemes` emulated `prefers-color-scheme`;
`--axe all` for all axe-core rules (default: `color-contrast` only); `--shots <dir>`
and `--full` for whole-page shots; `--storage`, `--init` (step 4); `--wait <ms>`
after load (default 500, plus up to 5 s for an idle network); `--json <file>` for
all results. Output of a real run on a test page:

```
 ok  /index.html desktop light            contrast=0 zero=0 overflow=0px console=0 requests=0
FAIL /pricing.html desktop light          contrast=1 zero=0 overflow=370px console=1 requests=0

/pricing.html desktop light
  color-contrast .fineprint
                 Element has insufficient color contrast of 2.16 (foreground color: #b0b0b0, ...
  overflow       page is 370px wider than the viewport
                 div.plans right=1616 width=1600
  console: Could not load plan prices: missing currency table
```

Exit code 0 means clean, 1 means findings or a page that did not load, 2 means bad
usage or missing tools. `zero` lists rendered elements with zero width or height
that hold text; `clips` after the size means the box hides its overflow, so the text
is cut off. `overflow` names the outermost elements that stick out. A failed
resource load counts under `requests`, not twice under `console`.

## 6. Triage, fix, run again

Expected hits, not bugs:

- An element that a breakpoint hides on purpose with a zero width or height rule.
- A chart bar with zero height for a zero value.
- Text for screen readers only, in a zero-size box.
- A dev server overlay or hot-reload client: it adds nodes and console lines that a
  production build does not have, so counts can differ by a few.
- The same finding on every route comes from the shared layout. Fix it once.

Always real:

- Contrast under 4.5:1 for normal text, under 3:1 for large text (24 px, or 18.66 px
  bold) and for controls and graphics.
- Any sideways scroll of the page.
- A zero-size element with text that the person should see (`clips` cuts it off).
- A page error, and a failed request that the page needs.

Fix, then run the same command again until it is clean or only expected hits stay.
Put the command and the counts before and after in the PR body ("contrast 86 -> 0
over 4 routes x 2 viewports x 2 schemes"): a reviewer can check a count.

Read every screenshot with the Read tool. A clean audit still misses a page that is
ugly, empty or shows the wrong data.

## 7. User flows

```bash
cp $SK/assets/flow.mjs "$W/flow.mjs"     # then replace the example STEPS
node "$W/flow.mjs" --base http://127.0.0.1:8123 --out "$W/flow-1"
```

Each step is a name and a function with actions and at least one check
(`expectVisible`, `expectText`, `page.waitForURL`), and writes `NN-<step>.png`.
Console and page errors print at the end; `--strict` makes them fail the run.
`--video` records a `.webm`; `--trace always` keeps the trace of a passing run. A
failed step stops the run and writes `NN-<step>-FAILED.png` and `trace.zip`:

```
FAIL 03 open the pricing page (5088 ms)
     locator.waitFor: Timeout 5000ms exceeded.
     Call log:
       - waiting for getByRole('heading', { name: 'Plans and prices' }).first() to be visible
```

`python3 -I $SK/scripts/trace_summary.py "$W/flow-1/trace.zip"` prints each action
with its time and error, then the console errors and failed requests. The person
opens the same file with `npx playwright show-trace trace.zip` on their computer.

## 8. One-off checks

Some questions fit neither the audit nor a flow: "what does the page show when the
API fails", "does Tab reach the menu", "is the date in German format". Write a short
script in `$W` for it. `references/recipes.md` (read it with `skill_read_reference`)
has a skeleton and tested snippets for API mocks, dialogs, downloads, new tabs,
frames, device emulation, focus order and storage state.

## 9. Evidence and delivery

- PR: the audit command, the counts before and after, the flow result, and what you
  saw in the screenshots.
- Bug report: steps, expected result, actual result, a screenshot, the console
  errors, and the trace when the person can open it.
- To send a file, copy it to `<workspace>/.uploads/browser-tests/<name>-<round or
  date>.<ext>` and call `send_file` with that path. Use a new name for each round.
- Clean up: stop your server and delete sign-in files. Keep `$W` while the person
  needs the evidence.

## Known problems

- An app that polls never becomes network-idle. The scripts wait at most 5 s for it
  and then go on; add `--wait` when content comes later.
- axe-core does not rate text over an image or a gradient (it marks it
  "incomplete"). Check that text in the screenshot.
- A server has other fonts than a desktop, so text widths, line breaks and overflow
  can differ from what the person sees. Fix the fonts first (step 1).
- `colorScheme` emulation does nothing for an app that keeps its theme in its own
  setting. Set that setting in an init file (step 4).
- Without `page.once('dialog', ...)`, Playwright dismisses every dialog, so
  `confirm()` returns false.
- In a container without an init process, stopped processes stay as zombies. They
  hold no port; `with_server.mjs` ignores them.
