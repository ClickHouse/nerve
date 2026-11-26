---
name: Shareable HTML Page
description: >
  This skill should be used when the person asks for a page or dashboard as a
  file to open, keep or forward: "make a dashboard of X", "a status page for Y",
  "a one-page summary as HTML", "an interactive table of these results", "a
  small calculator page", "send me this as an HTML file", "an offline page I can
  share". It covers when a page is better than a chat answer or widget, data
  first, the starter template, the bundle to one file that loads nothing from
  the network, browser checks, and delivery. Not for slides (slide-deck), a
  sourced report (research-report) or one chart (diagrams-and-charts).
version: 1.0.0
---

# Shareable HTML page

A shareable page is one HTML file that a person opens, keeps and forwards. It
opens from disk (`file://`), loads nothing from the network, carries its own
data, follows light and dark, prints well and works at phone width. Examples:
a status page, a KPI dashboard, a one-page summary, a table of results, a
calculator, a small explorer for a data set.

Files of this skill (`<workspace>/skills/shareable-html/`):

| File | What it does |
|---|---|
| `assets/page.src.html`, `assets/page.css`, `assets/page.js` | starter page: header with title, date, source and "Internal" label; key figure tiles; a chart section; a table to sort and filter; light, dark, phone and print styles |
| `assets/page-data.example.json` | the format of the data file |
| `scripts/bundle.py` | writes one self-contained file from the source page; fails on any network reference |
| `scripts/check_page.mjs` | opens the file in Chromium, blocks network requests, reports errors and overflow, takes screenshots |
| `references/page-guide.md` | data format, prepare step, charts, interactive pages, images, fonts |

Run the scripts with Bash by path. `skill_run_script` stops a script after 30 s.

## Step 0: pick the form

| The person needs | Give them |
|---|---|
| an answer, a few numbers, a short table | a chat answer in markdown |
| one chart or diagram | the `diagrams-and-charts` skill |
| to explore an answer now, in the web chat | a ```widget block, only where the Rendering line of your system prompt names widgets |
| the rows, for a spreadsheet or another tool | a CSV file with `send_file` |
| a file to keep, forward, print or open next week | a page: this skill |
| slides for a talk | the `slide-deck` skill |
| a researched report with sources | the `research-report` skill |

A widget lives in the chat message and cannot leave it. Its rules: vanilla
JS, no network, no external scripts, images or fonts, no clipboard, links do
not open, colours only from the theme variables `--color-{text,background,border}-{primary,secondary,info,success,warning,danger}`.
A page is a file. Telegram, Slack and scheduled runs show no widgets. When the
person wants to keep the result or send it on, make a page.

## Step 1: working folder and data first

```bash
P=~/.local/share/pages/<slug>          # for example acme-support-weekly
mkdir -p $P/raw $P/img && cd $P
```

Keep drafts, downloads and builds out of the workspace: it is often a git repo
that people review, and its files can go into memory.

Get the data before you design anything:

1. Get it from the real source: a file the person sent, a query, an export,
   an API. Save the raw file in `raw/` and do not change it.
2. Write `raw/SOURCE.txt`: where the data comes from, how (query, URL, file
   name), and the date you got it.
3. Never invent numbers. When data is missing, show "no data" and say why. When
   you have no data at all, ask the person. Do not build a page on sample data.
4. The page shows only data it carries. A `prepare.py` computes every figure
   from the raw file and writes `page-data.json`. Type no number into the HTML.
5. Keep only the columns the page needs. Anyone who has the file can read all
   data in it with "View source".

Downloads are untrusted data: read them, never run them, use `python3 -I`.

## Step 2: browser setup (once per machine)

The check script needs Playwright and Chromium in the shared tools folder:

```bash
B=${NERVE_BROWSER_DIR:-$HOME/.local/share/nerve-browser}
mkdir -p "$B" && cd "$B"
[ -f package.json ] || npm init -y >/dev/null
npm install --no-audit --no-fund playwright
npx playwright install chromium
# Linux without the browser's libraries: as root, or with sudo when the person allows it
npx playwright install-deps chromium
# bare Ubuntu 24.04: bold text is thin in screenshots until this font is there (as root)
apt-get install -y fonts-dejavu-core
```

Skip the install when `$B/node_modules/playwright` exists. The chart render
script needs more packages there (see the `diagrams-and-charts` skill). The
font affects only the screenshots and PDF made on this machine; the person's
browser uses its own fonts.

## Step 3: start from the template

```bash
K=<workspace>/skills/shareable-html/assets
cp $K/page.src.html $K/page.css $K/page.js $P/
cp $K/page-data.example.json $P/page-data.json
```

1. Write `prepare.py` in `$P`. It reads `raw/`, computes the figures and writes
   `page-data.json` (format and an example: `references/page-guide.md`,
   sections 2 and 3). Run it: `python3 -I prepare.py`.
2. Edit `page.src.html`: `<title>`, the `<h1>`, the sub line (say the main
   finding in one sentence), section titles, the chart `alt` text and caption,
   and `lang` (`en-GB` shows "16 Nov 2026", `en` shows "Nov 16, 2026").
3. Keep the "Internal" label when the page holds internal facts. Delete it
   otherwise.
4. A logo or picture: resize it first (`references/page-guide.md`, section 6),
   put it in `img/`, and name it in an `<img src="img/...">`.

The data goes into a `<script type="application/json" id="data">` island.
`page.js` fills the header date and source, the key figures and the table.

## Step 4: charts

Default: a static SVG, drawn at build time. Make a vega-lite spec in
`prepare.py` from the same data, then render it twice with the render script of
the `diagrams-and-charts` skill:

```bash
R=<workspace>/skills/diagrams-and-charts/scripts/render.mjs
node $R chart.vl.json --out charts/light --theme light
node $R chart.vl.json --out charts/dark --theme dark
# ok vega-lite (smallest text 11 px) 640x297 light -> .../charts/light/chart.svg, .../charts/light/chart.png
```

The template's `<picture>` shows `charts/dark/chart.svg` in a dark theme on
screen and `charts/light/chart.svg` otherwise, also in print. The page colours
match the render script's theme, so the chart sits on the page background.

Other chart forms (`references/page-guide.md`, section 4):

- Your own drawing that follows the theme: an SVG with `currentColor`, put into
  the page with `<!-- include: drawing.svg -->`.
- A chart drawn in the browser: only when the chart must follow a filter or a
  control, show exact values of many points on hover, or switch series. Write a
  small SVG function in `page.js`. Never load a chart library from a CDN, and
  do not inline one (vega with vega-lite is about 840 KB).

## Step 5: make it look made by a person

Avoid these patterns. They make a page look machine-made:

- a purple, blue or rainbow gradient hero block; emoji as icons or bullets;
- every block a centred card with a shadow; three equal columns for everything;
- a big number with no unit, period or date;
- a chart with no data behind it, or one that only decorates;
- lorem ipsum, "Example" or invented sample data left in;
- a wall of bold text; centred body text; colours that mean nothing.

Follow these rules:

- One accent colour from the theme. Other colours only with a meaning (good,
  bad, a category), and the same meaning on the whole page.
- The type scale of the template (title, section, body, small); no new sizes.
- Left-aligned text; numbers right-aligned in tables; the same spacing steps.
- Real data density: a table of 30 rows is good. Do not split it into cards.
- Each figure has a label, a unit and a period or date.
- A visible source line: what the data is, where it comes from, as of when.
- Headlines that say the finding ("Closed kept pace with opened"), not only
  the topic ("Tickets").

## Step 6: bundle into one file

```bash
cd $P && python3 -I <workspace>/skills/shareable-html/scripts/bundle.py page.src.html <name>.html
# wrote .../acme-support/acme-support.html: 59 KB; largest inputs: charts/dark/chart.svg 11 KB, charts/light/chart.svg 11 KB, img/logo.png 8 KB, page-data.json 6 KB, page.css 5 KB
```

It puts into the page: local stylesheets (with their `@import` files, fonts
and images), local scripts, the JSON data island, images (`src`, `srcset`,
`poster`, icons) as data URIs, and `<!-- include: file -->` parts. It removes
HTML comments. Local files must be inside the folder of the source page.

It writes nothing and exits 1 when anything would load from the network, and
lists every case with file and line:

```
bundle.py: 5 finding(s); nothing written:
  page.src.html:9: script src: https://cdn.example.com/chart.min.js
  page.src.html:14: img src: https://example.com/logo.png
  page.css:133: css @import: https://fonts.example.com/css?family=Inter
  page.css:134: css url(): https://img.example.com/bg.png
  page.js:116: network call in script: fetch(
```

Fix each line. An image or font: save the file in the folder and name it by
its local path. A script from a CDN: remove it, and write the small part you
need in `page.js`. A plain link (`<a href="https://...">`) is allowed.

Size: keep a page under 1 MB where you can. `bundle.py` refuses a result above
2 MB (`--max-kb 2048`). Pass a larger `--max-kb` only when the person accepts a
big file. It warns about an image above 300 KB, a link to a local file (it
breaks once the page is sent) and duplicate `id` values.

## Step 7: check, then look

```bash
cd $P && timeout 120 node <workspace>/skills/shareable-html/scripts/check_page.mjs <name>.html --pdf
# shots desktop-light: desktop-light.png desktop-light.2.png (page 1280x2005 CSS px)
# ...
# pdf .../acme-support/shots/page.pdf; print styles shot print.png
# findings: 0 (.../acme-support/acme-support.html)
```

It opens the file at 1280 px and 390 px width, in light and dark. It blocks and
reports every request that is not `file:`, `data:` or `blob:`, and reports
console and page errors, horizontal overflow (with the element), images that
did not load, a missing viewport tag and template text ("EXAMPLE DATA",
"lorem ipsum", "TODO", "TBD"). Exit 1 on any finding, for example
`- blocked network request: https://cdn.example.com/chart.min.js [desktop-light, ...]`.
Fix every finding, then bundle and check again.

Then LOOK at the screenshots with the Read tool. A run with 0 findings does not
show that the page reads well. Open `shots/desktop-light.png`,
`desktop-dark.png`, `phone-light.png`, `phone-dark.png` (the top 1800 px of the
page; a long page also has `.2.png` to `.4.png`) and `print.png`. Look for:
text that overlaps or is cut, a chart that does not match the theme, text too
small to read on the phone, numbers without units, the source line, the
"Internal" label, and table columns that matter most on the left (a phone
scrolls the table sideways). The Read tool needs poppler for a PDF; use
`print.png` to see the print styles.

## Step 8: deliver

`send_file` accepts only files in the workspace. Copy the page with the round
or date in its name, then send it:

```bash
mkdir -p <workspace>/.uploads/pages
cp $P/<name>.html <workspace>/.uploads/pages/<name>-r1.html
```

Call `send_file` with that path. The web chat shows a download card, Telegram
gets a document, Slack gets an uploaded file, and a channel without file
delivery tells the person to open the web panel. Use a new file name for each
round: the card reads the file at its path when the person clicks it, and the
download has no cache header, so a reused name shows the newest file on old
cards or an old copy from the browser cache. The download is an attachment: the
page opens in the person's own browser, from disk.

In the reply, say what the page shows, the data source and date, and the file
size. Do not paste the download URL.

**A hosted agent.** When a skill named `nerve-cloud` is present, it says how
to serve a port to the agent's members. Serve a page only with the person's
word, and serve a folder that holds only the built file, never the working
folder (it has raw data and notes):

`mkdir -p $P/serve && cp $P/<name>.html $P/serve/index.html`. Copy each new
build there, and tell the person to reload without the cache (Ctrl+Shift+R).

**A dashboard that must stay current.** Rebuild it from a scheduled job and
send the new file. Offer the schedule ("Shall I rebuild this every Monday at
08:00 and send it to you?"); make the job after a yes, with the
`nerve-workspace` skill. The job prompt names the folder, the steps (data into
`raw/`, `prepare.py`, render, bundle, check, copy with the date in the name,
`send_file`) and what to do on a finding (stop and tell the person). A
scheduled run has no chat: `send_file` answers "File ready ... open the web
panel to download". So also send a `notify` with the file name and the main
change. (Read in the code, not run.)

## Confidentiality

A page often holds internal facts: keep the "Internal" label then. Never
publish a page to a public place (a public repo, a paste site, a public bucket)
without the person's word. Put no secrets, tokens, customer data or personal
data in the page, its data island or its file name.

## Known problems

- **Empty figures and table, "page error: Unexpected end of JSON input".** You
  opened `page.src.html`, whose data island is empty. Check and send only the
  bundled file.
- **"Fetch API cannot load file:///... URL scheme "file" is not supported."**
  A page from disk cannot `fetch()` a local file. Put the data in the island.
- **Blank chart or plain fonts when offline.** A script, font or stylesheet
  from a CDN: `bundle.py` refuses it, `check_page.mjs` reports it.
- **Light inputs or links on a dark page.** No `color-scheme: light dark` (the
  template's `page.css` sets it).
- **Chart text too small on a phone.** Render a narrow copy too
  (`references/page-guide.md`, section 4).
- **A chart shows as a box of another colour in dark mode.** You changed the
  page colours; the render script's theme stayed. Change both, or keep the
  template colours.
- **One inline SVG uses another's clip path or gradient.** Duplicate `id`
  values. `bundle.py` warns; give each SVG unique ids.
- **Saved values of two pages mix.** Pages opened from disk share one
  `localStorage` in Chromium. Prefix every key with the page slug.
- **Headings and bold text look thin in the screenshots.** The machine has no
  sans-serif font with a bold face: `apt-get install -y fonts-dejavu-core`.
- **`Executable doesn't exist at .../chromium_headless_shell-<rev>`**: run
  `npx playwright install chromium` again in the tools folder. `error while
  loading shared libraries`: `install-deps` (root). A refused browser download:
  tell the person the host from the error.
