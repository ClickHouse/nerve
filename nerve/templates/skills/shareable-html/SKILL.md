---
name: Shareable HTML Page
description: >
  This skill should be used when the person asks for a page or dashboard as a
  file to open, keep or forward: "make a dashboard of X", "a status page for
  Y", "a one-page summary as HTML", "an interactive table of these results", "a
  small calculator page", "send me this as an HTML file". It covers when a page
  is better than a chat answer or a widget, the rules for a file that works
  offline, the check in a browser, delivery and scheduled rebuilds. Not for
  slides (slide-deck), a sourced report (research-report) or one chart
  (diagrams-and-charts).
version: 1.0.0
---

# Shareable HTML page

## Page or something else?

- A few numbers or a short table: answer in chat.
- To explore an answer now, in the web chat: a `widget` block, only where the
  Rendering line of your Session Context names widgets. A widget lives in the
  chat message and cannot leave it; Telegram, Slack and scheduled runs show no
  widgets.
- The rows for a spreadsheet: a CSV file.
- A file to keep, forward, print or open next week: a page, this skill.

## The file

- One HTML file that opens from disk (`file://`) and loads nothing from the
  network: styles, scripts, fonts and images inside. A script or font from a
  CDN does not load offline and tells a third party who opened the page.
- The data inside, in a `<script type="application/json">` block. A page from
  disk cannot `fetch()` a local file.
- Light and dark (`prefers-color-scheme`, and `color-scheme: light dark` so
  that inputs follow), print styles, and a layout that works at phone width
  (390 px); a wide table scrolls inside its own box.
- A header with the title, the date of the data, its source, and "Internal"
  when the page holds internal facts.
- Pages opened from disk share one `localStorage` in Chromium: prefix every
  key with the page's name.
- Keep it under about 1 MB. Resize images before you embed them.

## Data first

Get the data from the real source (a file the person sent, a query, an
export) before you design anything, and keep the raw file and a note of where
and when you got it in a working folder outside the workspace,
`~/.local/share/pages/<slug>/`. Compute every figure from the raw data with a
script. Never type a number into the page, and never ship sample data. Keep
only the columns the page needs: anyone can read all of the data with "View
source".

## Layout

A layout that works for most pages, top to bottom:

1. Title, a one-sentence finding, the date of the data and its source.
2. Three to five key figures, each with its unit, its period and the change
   against the period before.
3. The main chart, with a headline that states what it shows.
4. The table of rows, which the reader can sort and filter (a few lines of
   vanilla JS), with numbers right-aligned.
5. A footer: how the page was made, from what, and when.

A calculator or explorer shows its inputs first, then the result, then how
the result is computed. Save what the reader changes in `localStorage` (keys
with the page's prefix), so a reload keeps it.

## Look

Avoid what makes a page look machine-made: a gradient hero, emoji as icons,
every block a centred card, three equal columns for everything, a big number
without a unit or a date, a chart that only decorates, a wall of bold. Use one
accent colour, left-aligned text, numbers right-aligned in tables, real data
density, and headlines that state the finding ("Closed kept pace with
opened"). Draw charts as inline SVG; the `diagrams-and-charts` skill says how
to render them as files.

## Check

Open the built file in headless Chromium (setup: the `browser-testing`
skill) at 1280 px and 390 px width, in light and dark. Block and list every
request that is not `file:` or `data:`, and list console errors, page errors
and sideways overflow. Fix all of them. Then look at the screenshots with the
Read tool: a clean run does not show that the page reads well.

## Deliver

`send_file` accepts only files in the workspace. Copy the page to
`<workspace>/.uploads/pages/<name>-r<round>.html`, then call `send_file` with
that path. Use a new name for each round: a chat card reads the file at its
path when the person clicks it, and the download has no cache header. The web
chat shows a download card, Telegram gets a document and Slack an uploaded
file. The page opens from disk in the person's own browser. Say what it
shows, the data source and date, and its size.

On a hosted agent, the `nerve-cloud` skill says how to serve a port to the
agent's members. Serve a page only with the person's word, and serve a folder
that holds only the built file, never the working folder.

**A dashboard that must stay current:** offer a schedule (the offer-to-automate
rule of your system prompt), and make the job only after a yes. A scheduled
run has no chat, so `send_file` delivers nothing there: the job sends a
`notify` with the file name and what changed.

## Confidentiality

Never publish a page to a public place (a public repo, a paste site, a public
bucket) without the person's word. No secrets, customer data or personal data
in the page, its data block or its file name.
