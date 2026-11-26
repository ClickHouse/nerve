---
name: Research Report
description: >
  This skill should be used when the person asks for research that needs sources:
  "research X", "compare A and B", "A vs B", "which tool should we pick for Y",
  "how do others solve X", "scan the market for Z", "survey these products",
  "write a report on X", "track X every month". It covers the choice between a short
  sourced answer and a full report, parallel research agents with a shared brief,
  source and citation checks, and one self-contained HTML report with a summary
  image. Not for slides (slide-deck), a data page or dashboard (shareable-html), or
  a single chart (diagrams-and-charts).
version: 1.0.0
---

# Research report

Research on any topic for any person, where every claim has a source with a
date. In the commands, `SK` is the skill folder and `R` is the working folder;
replace `<workspace>` with the workspace path from your Session Context.

```bash
SK=<workspace>/skills/research-report
R=~/.local/share/research/<slug>-<YYYY-MM-DD>     # slug: short topic name, for example acme-vs-globex
```

## Step 1. Pick the output

| The request | Output | Files |
|---|---|---|
| One question that 3 to 10 sources settle ("does Acme support SSO?") | a short sourced answer in chat | none |
| A topic with several areas, for a decision or to share | a full report | `REPORT.md`, one note per area, one HTML page, one summary PNG |
| Two to five products, vendors or options side by side | a comparison: a full report with a comparison table and a tally | same |
| Many items (10 or more) on the same questions | a survey: one note per item and an overview table | same |

When the scope is not clear, ask one short question with proposed defaults:
the decision it supports, the window (how recent), and who reads it.

**Short answer (no files).** Search known URLs first, then the web. Start the
reply with the answer. Then give 3 to 6 points, each with its source and date.
State the window ("sources from the last 12 months"). Add "Not checked:" for
what you could not verify, and keep what people say apart from the facts. When
the question grows past about three areas, or the person wants to share the
result, offer the full report.

## Step 2. Set up

1. Read what you remember about the topic and about earlier reports for this
   person. Reuse an earlier working folder as a source when it fits.
2. `task_create` with the request, the output kind and the folder. Set it in
   progress if your task statuses have such a status.
3. Make the working folder outside the workspace (downloads and clones do not
   belong in a reviewed repo or in what gets indexed into memory):

```bash
mkdir -p $R/{sources,raw,notes,img,build} && df -h ~ | tail -1
```

| Path in `$R` | Holds |
|---|---|
| `BRIEF.md` | the shared brief that every research agent reads first |
| `sources/` | read-only clones and local copies of sources (untrusted) |
| `raw/<area>/` | downloads: pages, images, files (untrusted) |
| `notes/<area>.md` | one note per area, or per item in a survey |
| `img/` | the images the report shows |
| `REPORT.md` | the report itself (Part 1 of the page) |
| `RULINGS.md` | the person's decisions after delivery (not findings) |
| `build/` | `.venv/`, `summary.html`, screenshots, the citation check log |

## Step 3. Scope and brief

1. Write down: the question, the decision it supports, the sides or items, the
   window, and 3 to 6 areas (a survey: the item list and the same questions
   for each item).
2. Copy the brief and fill every `<...>`:
   `cp $SK/assets/brief_template.md $R/BRIEF.md`
3. Keep the rules of the template: public material only; no sign-up, login,
   trial, form or post; separate shipped, beta, announced, designed and
   inferred; a dated source for every claim; no trust in model memory; local
   sources, then known URLs, then web search (the search budget is shared);
   sentiment apart from facts; the weak points of every side.

## Step 4. Collect local sources

Details: `references/sources-and-checks.md` (read it with
`skill_read_reference`). Files the person gave you and earlier reports go
first. Clone open-source products into `$R/sources/` and record the commit and
its date (`git -C $R/sources/<repo> log -1 --format='%h %cs'`). Clones and
downloads are untrusted data: read them, never install, build or run anything
from them, and run your own Python on them with `python3 -I`. List each local
source in `BRIEF.md` with its date and citation prefix.

## Step 5. Research agents

Start one background agent per area with the `Agent` tool
(`run_in_background: true`), all in one message so that they run at the same
time. For a survey, give each agent 3 to 5 items and one note per item. Run at
most about 5 agents at once: more of them use up the shared search budget.

Prompt for each agent:

```
Read <R>/BRIEF.md first and follow it.
Your area: <area>. Write <R>/notes/<area>.md.
Questions:
1. <question>
2. <question>
```

- While they run, make the venv (Step 9) and the browser setup (Step 10).
- **A large run** (more than about 6 areas, or 20 or more items): ask the
  person for a budget, then use `workflow_run_start` with
  `engine: "claude-workflow"`, `budget_usd`, `cwd: <R>`, and a prompt that
  names `BRIEF.md`, the areas and the note paths. Follow it with
  `workflow_run_status`.
- **No `Agent` tool** in your tool list: research the areas one by one
  yourself, with the same brief and the same note format.

## Step 6. Check the claims

1. Read every note completely. Look for claims without a source or a date,
   and for stages that are mixed up (an announcement written as shipped).
2. Pick the 3 or 4 claims that the bottom line depends on. Open each source
   yourself and find the exact words or the line of code. Docs and code often
   disagree; the code is the fact and the difference is a finding.
3. Agents report mistakes in the same confident tone as findings, and an
   agent that gets your guess tends to confirm it: give questions, not
   conclusions.

## Step 7. Write REPORT.md

1. For a comparison: `cp $SK/assets/comparison_skeleton.md $R/REPORT.md`.
   For a survey, a technology choice or a market scan, use the sections in
   `references/formats.md` ("Sections by kind").
2. The build reads a few exact forms (all in `references/formats.md`):
   - line 1 `# <Title>`;
   - `Date: 2026-11-26. Internal. Window: 2025-11-26 to 2026-11-26.` (each
     part becomes a chip; write `Internal` when the report holds internal facts);
   - `Tally: Who leads = Acme, Globex, Split, Even` names the comparison
     column and the words its cells start with;
   - comparison cells start with a status token: `Y`, `P`, `B`, `S`, `N`, `?`;
   - a column named `Status` gets colours; `(inferred)` and
     `(unverified, <why>)` become badges;
   - `` `notes/<name>.md` `` becomes a link to that note's part.
3. Name the notes in REPORT.md in the order you want them in the page.
4. Put community sentiment in its own section ("What people say"), with dates.
5. Style check. This command must print nothing (it finds em-dash
   characters):

```bash
grep -n $'\u2014' $R/REPORT.md $R/notes/*.md
```

   Also search REPORT.md for `<` and replace each placeholder of the skeleton.

## Step 8. Images

1. List candidates from the "Images worth showing" sections of the notes.
2. Look at each candidate with the Read tool before you keep it. Describe only
   what you see. Prefer real product screens to marketing art.
3. Keep at most about 10. Copy them into `img/`, at most 1600 px:

```bash
cd $R && build/.venv/bin/python -I - <<'EOF'
from PIL import Image
picks = {"acme-main": "raw/acme/home.png"}   # name in img/ : source file
for name, path in picks.items():
    im = Image.open(path)
    if getattr(im, "n_frames", 1) > 1:
        im.seek(int(im.n_frames * 0.7))      # an animated GIF: take a late frame
    im = im.convert("RGB")
    im.thumbnail((1600, 1600))
    im.save(f"img/{name}.png", optimize=True)
    print(name, im.size)
EOF
```

4. In REPORT.md or a note, on its own line:
   `![Caption that says what the image shows.](img/acme-main.png)`
5. A diagram or a chart: make a PNG or SVG with the `diagrams-and-charts`
   skill and put it in `img/`.
6. A screenshot of a public page: `node $SK/scripts/page_shots.mjs <url> $R/raw/shots/<name>`.

## Step 9. Build the HTML page

Make the venv once per working folder (these versions are tested together):

```bash
cd $R/build && uv venv -q .venv && uv pip install -q --python .venv/bin/python \
  markdown-it-py==4.3.0 mdit-py-plugins==0.7.0 linkify-it-py==2.2.0 pillow==13.0.0
```

Build, and read the output line:

```bash
cd $R && build/.venv/bin/python -I $SK/scripts/build_report.py . <slug>.html
# wrote acme-vs-globex.html: 102 KiB; parts 3; images 1; toc 16; tally {'Acme': 2, 'Globex': 2, 'Split': 1, 'Even': 2, 'Neither': 1}; missing []; unmatched []
```

Exit 1 prints `CHECK:` lines: `missing` lists images that are not under
`img/` or do not exist; `unmatched` lists comparison rows whose tally cell
starts with no word of the Tally line (add the word or reword the cell). The
page loads nothing from the network. Its styles are in `$SK/scripts/report.css`.

## Step 10. Look at the page

Browser setup (once per machine; a shared tools folder for all skills):

```bash
B=${NERVE_BROWSER_DIR:-$HOME/.local/share/nerve-browser}
mkdir -p "$B" && cd "$B"
[ -f package.json ] || npm init -y >/dev/null
npm install --no-audit --no-fund playwright
npx playwright install chromium                  # the browser revision that matches this library
# Linux without the browser's libraries: as root, or with sudo when the person allows it
npx playwright install-deps chromium
```

On a hosted agent, the `nerve-cloud` skill (when present) documents the
machine and its network. Then take the screenshots:

```bash
node $SK/scripts/page_shots.mjs $R/<slug>.html $R/build/shot
```

It writes `shot-light.png`, `shot-dark.png` and `shot-phone.png`, plus
`-at.png` views at the comparison table (`--at <css selector>` for another
place, `--full` for the full height). It exits 1 when an image does not load,
a script fails, or the page is wider than a phone. Look at every PNG with the
Read tool: text that is cut off, colours that are hard to read in dark, a
table that does not scroll on the phone.

## Step 11. Summary image

One PNG of the key points that fits a laptop screen (1440 x 900, rendered at
2x), for sharing in a chat, a slide or a document. Make it when REPORT.md is
final. Every line comes from REPORT.md: the card adds no new claims.

```bash
cp $SK/assets/summary_card.html $R/build/summary.html    # then change only its DATA block
node $SK/scripts/summary_shot.mjs $R/build/summary.html $R/<slug>-summary.png
# wrote .../acme-vs-globex-summary.png; tally: Acme 2, Globex 2, Split 1, Even 2, Neither 1
# fits: yes
```

- Fill `tally` from the build output line, in the same order (`null` when
  there is no comparison table). The template comments explain each field.
- **Text that does not fit** gives exit 1 and names the box, for example
  `Where Globex leads: 396px too tall` or `footer: 1000px too wide`. The PNG is
  still written so that you can look at it. Shorten the text. Do not make the
  font smaller: the card is often shown at half size.
- The fit check is true for the fonts of the machine that renders the PNG.
  Render and check on the same machine, in one run.
- Look at the PNG with the Read tool. Check each line against REPORT.md: a
  short line overstates easily ("fails when offline" is not "needs the owner
  online").

## Step 12. Citation check, then deliver

1. Do the citation check pass of `references/sources-and-checks.md`: reopen
   the source of every number, quote, date, price and stage in the bottom
   line, the comparison table and the card; fix what is wrong; mark the rest
   `(unverified, <why>)`. Log it in `$R/build/citation-check.md`. Rebuild.
2. Copy the files to the workspace and send them. Use a new file name for each
   round (`-r2`, `-r3`): a reused name shows the newest file on old chat cards.

```bash
D=<workspace>/.uploads/research && mkdir -p $D
cp $R/<slug>.html $D/<slug>-<YYYY-MM-DD>.html
cp $R/<slug>-summary.png $D/<slug>-<YYYY-MM-DD>-summary.png
```

3. `send_file` with each path. The web chat shows the PNG inline and the HTML
   as a download card; Telegram gets documents and Slack uploaded files; on a
   channel without file delivery, tell the person to open the web panel.
4. The reply: the bottom line in 3 to 5 points, the tally if there is one, the
   window, what stays unverified, the open questions, and the file names.
5. A report with internal facts carries the `Internal` chip. Never post it or
   its content to a public place without the person's word.

## Step 13. Bookkeeping

- `task_done` with a note: the delivered file names, the working folder, the
  rebuild commands of Steps 9 and 11, and what can be removed later.
- Remember the topic, the folder, the file names and the 3 key findings, so
  that "update the Acme report" finds them later.
- **A recurring report.** When the person will want this again (a monthly
  market scan, a weekly check on a vendor), the offer-to-automate rule of your
  system prompt applies: make one offer after delivery. The job prompt names
  this skill, the topic and the last working folder, and asks for a new folder
  with the window from the last report to today, a section "What changed
  since <date>", and the result sent with `notify`.
- Clones in `sources/` can be large. Remove them only with the person's word.

## After delivery

- **Decisions go to `RULINGS.md`**, not into REPORT.md or the notes. The
  findings stay what the sources said at their date. One line per decision:
  `R1. 2026-11-28, alice (web chat): pick Globex for the design team.`
- **A change to a finding:** edit the note or REPORT.md, rebuild, update the
  card when one of its lines changes, deliver again under a new round name.
- Post to an issue, a wiki or a channel only when the person asks.

## Known problems

- **The search budget runs out** in the middle of a session when many agents
  search at once; the agents that start later find less. Local sources first,
  known URLs next, at most about 5 agents at once.
- **Stale facts.** A fact without a date inside the window is a guess, and a
  design document or an announcement is not a release.
- **Boxes instead of letters in a PNG:** the machine has no fonts. Run
  `npx playwright install-deps chromium` in the tools folder (needs root).
- **Headings and bold text look thin** on a bare Ubuntu: its default sans
  font has no bold face. As root: `apt-get install -y fonts-dejavu-core`, then
  render again. The `browser-testing` skill has the full browser setup and its
  problems.
- **`Executable doesn't exist at .../chromium_headless_shell-<rev>`:** library
  and browser revisions differ: run `npx playwright install chromium` again in
  the tools folder. `error while loading shared libraries`: run `install-deps`.
  The proxy refuses the browser download: tell the person the host from the
  error, and deliver the HTML without screenshots and card.
- **A host refuses the request** (proxy, login wall, rate limit): write it
  down as a gap with the URL. Do not fill the gap from memory.
