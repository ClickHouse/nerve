---
name: Browser Testing
description: >
  This skill should be used when the person asks to check a web app in a real
  browser: "test the UI", "screenshot the page", "check it on mobile", "run an
  accessibility check", "check the contrast", "does the sign-up flow work",
  "find the console errors", "verify the frontend change before the PR". It
  covers the headless Chromium setup on this machine, serving the app under
  test, sign-in, the checks, triage and evidence. It also holds the browser
  setup that the other bundled skills use. Not for building a page to share
  (shareable-html) or slides (slide-deck).
version: 1.0.0
---

# Browser testing

Type checks and unit tests do not see the rendered page. Low contrast, a box
with zero width, a page that scrolls sideways on a phone and a console error
show only in a browser.

## Rules

- Never install test tooling into the repo under test: it changes
  `package.json` and the lock file, and that diff goes into the PR. Use the
  tools folder below.
- Stop only the server that you started. Never `pkill -f` or `killall`: the
  pattern can match your own shell or another person's server.
- Do not rebuild or restart an instance that people use to see your change.
  Serve your branch on its own port.
- Never put a token or a password in the chat, a PR or a file in the repo.
- On a live site, do not submit forms that send mail, take payments or change
  data without the person's word.

Keep screenshots, traces and sign-in files in
`~/.local/share/browser-tests/<slug>/`, not in the workspace or the repo.

## Browser setup

All bundled skills use one tools folder:

```bash
B=${NERVE_BROWSER_DIR:-$HOME/.local/share/nerve-browser}
mkdir -p "$B" && cd "$B"
[ -f package.json ] || npm init -y >/dev/null
npm install --no-audit --no-fund playwright axe-core
npx playwright install chromium
```

Scripts load Playwright from that folder
(`createRequire("$B/package.json")("playwright")`). Run them with Bash:
`skill_run_script` stops a script after 30 s.

| Symptom | Fix |
|---|---|
| `Executable doesn't exist at .../chromium_headless_shell-<rev>` | The browser and the library have different revisions: run `npx playwright install chromium` again in the tools folder. Do not change the library to match an old browser. |
| `error while loading shared libraries: libglib-2.0.so.0` (or another library) | `npx playwright install-deps chromium` as root (about 100 packages on a bare Ubuntu 24.04), or with sudo only when the person allows it. Else give the person the command. |
| Headings and bold text look thin in screenshots | A bare Ubuntu's default sans-serif font has no bold face. As root: `apt-get install -y fonts-dejavu-core`. |
| `npm error code E403`, or `Download failed: server returned code 403` | A proxy refuses the npm registry or the browser download host. Tell the person the host from the error. |

The browser download is about 330 MiB. When a `nerve-cloud` skill is present,
read it for the machine, its network and how to serve a port.

## Serve the app

When the person gives a URL (a preview deploy), use it. Else:

1. Take the dev server command from the README or `package.json`. Install
   dependencies without a change to the repo's files (`npm ci`), and check
   that `git status` shows no change.
2. Pick a free port (`ss -ltn`), and make a collision fail: Vite moves to
   another port without `--strictPort`, and you then test the wrong app.
3. Bind to `127.0.0.1`, wait until the URL answers, then test.
4. Stop it at the end. A `kill` of the `npm` process leaves the server child
   running: stop the process that holds your port (`ss -ltnp` shows its pid),
   or start the server in its own process group and stop the group.

A static page needs no server: open it as `file://`.

## Look before you write selectors

Take a screenshot and the accessibility tree (`page.ariaSnapshot()`) of the
page first. Read the screenshot with the Read tool. Then write steps with
`getByRole`, `getByLabel` and `getByTestId`; use CSS selectors only when
nothing else works. A control with no role or name is itself a finding.

## Sign-in

Seed the session before the app starts: `storageState` from a sign-in script,
or a token set in `context.addInitScript`. A `page.evaluate` after `goto` is
too late: the app has already read its storage and shows its sign-in page, and
every API call fails with 401. Check the token with one API call first. Make
sign-in files with `umask 077` and delete them at the end. A trace of a
signed-in run holds cookies and headers: do not attach it to a public PR.

## Checks

For each route, at desktop (1280x800) and phone (375x812) size, in light and
dark (`colorScheme`; an app with its own theme setting needs that setting in
an init script):

- axe-core `color-contrast` (all rules when the person asks for an
  accessibility check);
- sideways overflow of the page, and the elements that stick out;
- visible elements with text and zero width or height;
- console errors, page errors, and failed requests (status 400 or more).

Triage before you fix. Expected: an element a breakpoint hides on purpose, a
zero-value chart bar, text for screen readers only, extra nodes and console
lines of a dev server, the same finding on every route (it is the shared
layout: fix it once). Always real: contrast under 4.5:1 for text, under 3:1
for large text and controls; any sideways scroll; cut-off text; a page error.
Fix, and run the same check again until it is clean.

Read every screenshot with the Read tool. A clean run still misses a page that
is ugly, empty or shows the wrong data.

## Flows and evidence

For a user flow, write a short Playwright script: one screenshot per step, at
least one check per step, and `context.tracing` with the trace saved when a
step fails. For a PR, give the commands, the counts before and after
("contrast 86 -> 0 over 4 routes x 2 sizes x 2 themes") and what the
screenshots show. To send a screenshot, copy it to
`<workspace>/.uploads/browser-tests/<name>-<round>.png` and call `send_file`
with that path.

## Known problems

- An app that polls never becomes network-idle: wait for a visible element,
  not for `networkidle`.
- axe-core does not rate text over an image or a gradient. Check it in the
  screenshot.
- A server has other fonts than a desktop, so line breaks and overflow can
  differ from what the person sees.
- Playwright dismisses every dialog unless you handle it, so `confirm()`
  returns false.
