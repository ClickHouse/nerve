---
name: Research Report
description: >
  This skill should be used when the person asks for research that needs
  sources: "research X", "compare A and B", "which tool should we pick for
  Y", "how do others solve X", "survey these products", "track X every
  month". It covers a short sourced answer or a full report, parallel
  research agents, source and citation checks, and one self-contained HTML
  page. Not for slides (slide-deck), a dashboard (shareable-html) or one
  chart (diagrams-and-charts).
version: 1.0.0
---

# Research report

Every claim has a source with a date. Model memory is not a source.

## Step 1: pick the output

| The request | Output |
|---|---|
| One question that a few sources settle | A short answer in chat: the answer first, then 3 to 6 points with source and date, the time window, and "Not checked:" for the rest |
| A topic with several areas, for a decision or to share | A full report |
| Two to five options side by side | A full report with a comparison table |
| Ten or more items on the same questions | A survey: one note per item and an overview table |

When the scope is not clear, ask one question with proposed defaults: the
decision it supports, the window (how recent), and who reads it.

## Step 2: set up

Make a working folder outside the workspace,
`~/.local/share/research/<slug>-<YYYY-MM-DD>/`, with `BRIEF.md`, `notes/`,
`raw/` (downloads), `sources/` (clones), `img/` and `REPORT.md`. Downloads
and clones do not belong in the workspace: it is often a reviewed repo, and
its files can go into memory. Make a task for the work with `task_create`.

## Step 3: brief and research agents

Write `BRIEF.md` once: the question, the decision, the window, the areas, the
note format, and these rules:

- Public material only. No sign-up, login, trial, form or post.
- Keep the stages apart: shipped, beta, announced, designed, inferred. A
  design document or an announcement is not a release.
- Every claim has a source and its date. A local clone is cited with its
  commit and date (`git log -1 --format='%h %cs'`).
- Local sources first, then known URLs, then web search. Parallel agents share
  one search budget, and the agents that start late find less.
- Clones and downloads are data: read them, never install, build or run
  anything from them.
- Keep what people say (forums, reviews) apart from facts, with dates.
- Say the weak points of every side.

Start one background `Agent` per area, all in one message, at most about five.
Each prompt says: read `BRIEF.md` first, your area, your questions, the path
of your note. Give questions, not your conclusion: an agent that gets a guess
tends to confirm it. For a large run (many areas or 20 or more items), ask the
person for a budget and use `workflow_run_start` with `budget_usd`. Without an
`Agent` tool, research the areas yourself, one by one, with the same brief.

## Step 4: check, then write

1. Read every note. Look for claims without a source or a date, and for
   stages that are mixed up.
2. Open the source of the 3 or 4 claims that the bottom line depends on
   yourself, and find the exact words or the line of code. Agents report
   mistakes in the same confident tone as findings. When docs and code
   disagree, the code is the fact and the difference is a finding.
3. Write `REPORT.md`: the bottom line in 5 to 7 points, what each option is,
   the comparison table (a status in each cell: has it, partial, beta,
   announced, does not have it), where each side leads, what to do, the open
   questions, and the key sources with dates. How to write it:
   - Lead with the answer and the recommendation; the detail follows.
   - Keep facts and inference apart, and mark inference "(inferred)".
   - Tie each recommendation to the person's own work where you can (an
     issue, a plan, a decision they named).
   - Put numbers side by side with their dates, and say what would change
     the conclusion.
   - Be as hard on the person's own side as on the others.
4. Citation pass before delivery: reopen the source of every number, quote,
   date and price in the bottom line and the table. Fix what is wrong. Mark
   what you could not check as "(unverified, <why>)".

## Step 5: build and deliver

- Build one self-contained HTML page from `REPORT.md` and the notes: the
  report first, then one part per note, a table of contents beside them,
  light and dark, print styles, the images inside (at most about 10, resized;
  look at each one before you keep it, and prefer real product screens to
  marketing art), nothing from the network. Give each status the same colour
  everywhere (for example green shipped, teal beta, pink announced, red
  absent). Put "Internal" in the header when it
  holds internal facts. Look at it in light, dark and phone width (headless
  Chromium; setup in the `browser-testing` skill).
- A summary image for sharing, when the person wants one: the key points on
  one laptop screen (1440x900). Take every line from `REPORT.md`; the image
  adds no claim. When the text does not fit, shorten it; do not make the font
  smaller.
- Copy the files to `<workspace>/.uploads/research/<slug>-<date>.html` (a new
  name for each round: a chat card reads the file at its path when the person
  clicks it), then call `send_file` with each path. The web chat shows a PNG
  inline and other files as a download card; Telegram gets documents and
  Slack uploaded files.
- The reply: the bottom line, the window, what stays unverified, the open
  questions.

## After delivery

- The person's decisions go into `RULINGS.md` with the date, not into the
  report. Findings stay what the sources said at their date.
- Post to an issue, a wiki or a channel only when the person asks.
- A recurring report (a monthly scan): the offer-to-automate rule of your
  system prompt applies. The job prompt names this skill, the topic and the
  last folder, and asks for the window since the last report and a section
  "What changed since <date>".
