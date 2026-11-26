---
name: Diagrams and Charts
description: >
  This skill should be used when the person asks to "draw a diagram", "make a
  chart", "plot this data", "show the flow", "sketch the architecture", "make a
  sequence diagram", "show the data model", "turn this draw.io into mermaid", or
  needs a picture for Telegram, Slack, a deck or a report. It covers choosing the
  form, writing mermaid, vega-lite, svg and widget blocks that the Nerve web chat
  renders, checking them before sending, and rendering SVG and PNG files. It is
  not for a whole deck, report or page (use slide-deck, research-report,
  shareable-html).
version: 1.0.0
---

# Diagrams and charts

A good picture answers one question quickly. Pick the form, draw it so it
reads in both themes, check that it parses, and send it in a form the channel
can show.

Files of this skill, under `<workspace>/skills/diagrams-and-charts/`
(`<workspace>` is in your Session Context):

- `scripts/render.mjs`: checks and renders a `.mmd` (or `.mermaid`),
  `.vl.json`, `.vg.json` or `.svg` file to SVG and PNG.
- `scripts/import_diagram.py`: reads a draw.io or Excalidraw file as data and
  prints a mermaid flowchart draft.
- `assets/examples/order-flow.mmd`, `assets/examples/signups.vl.json`: two
  known-good inputs to test the setup.
- `references/mermaid.md` (diagram types, syntax traps, recipes) and
  `references/vega-lite.md` (chart recipes, the chat's chart defaults). Read
  them with `skill_read_reference` before you write that kind.

## 1. Find where the picture goes

Read the "Rendering" line in your Session Context. It decides the form.

| Where | Do this |
|---|---|
| Rendering line: the chat renders fenced mermaid, vega-lite and svg blocks (web chat, API) | Put a fenced block in the reply. The picture shows inline. |
| The line also says it runs a `widget` block | You may also use a widget (section 6). Without that sentence a widget block shows as raw HTML. |
| Rendering line: "text and basic markdown only" (Telegram, Slack, cron output) | Do not send diagram blocks: they show as source. Give the structure in words or a list. When a picture really helps, render a PNG and `send_file` it (section 8). |
| A deck, a report, a page, a document | Render SVG or PNG files with the script and embed them (section 8). |

## 2. Choose the form

Ask what the reader must see. Then pick the form from this table.

| The question | Form | Write |
|---|---|---|
| What happens in which order, with decisions? | flowchart | mermaid `flowchart TB` |
| Who sends what to whom, in what order? | sequence | mermaid `sequenceDiagram` |
| What are the parts of a system and how do they connect? | boxes and arrows in groups | mermaid `flowchart LR` or `TB` with `subgraph`; `C4Context` for a formal C4 view |
| What states can a thing be in, and what moves it? | state machine | mermaid `stateDiagram-v2` |
| What data is stored and how do the tables relate? | data model | mermaid `erDiagram`; `classDiagram` for types in code |
| What is planned, from when to when? | schedule | mermaid `gantt` |
| What happened when (a few dated events)? | timeline | mermaid `timeline` (up to about 6 periods; it grows wide), else vega-lite `point` and `text` on a time axis, or a widget |
| How is it organized (a hierarchy)? | tree | mermaid `mindmap` or `flowchart TB`; `treemap-beta` when the size of each part matters |
| Which is bigger (values by category)? | comparison | vega-lite `bar`, sorted, horizontal when labels are long |
| How did a value change over time? | trend | vega-lite `line` with a temporal x; `bar` for a few periods; `area` for a total |
| What share of the whole is each part? | part of whole | vega-lite stacked `bar` (`"stack": "normalize"`); `arc` donut only for 2 to 5 parts |
| How are the values spread? | distribution | vega-lite `bar` with `bin` (histogram), `boxplot`, or `tick` |
| Do two measures move together? | correlation | vega-lite `point` (scatter) |
| How do two categories combine? | matrix or heatmap | vega-lite `rect` with a quantitative `color`; values in a tooltip |
| Where does a quantity flow (funnel, budget split)? | flow | mermaid `sankey-beta` |
| Which items first (effort against value)? | 2 by 2 | mermaid `quadrantChart` |
| Exact numbers, 4 values or fewer, or items to look up | no picture | a markdown table or one sentence |

Use an `svg` block when you must place every element yourself: a floor plan,
a rack, a labelled layout, a simple icon-like figure. Mermaid lays out boxes for
you; svg does not. Use a `widget` only where the Rendering line names it, and
only when the answer is better explored than read (section 6).

## 3. Draw it well

Rules for every picture:

- One idea per picture. When a diagram needs more than about 15 nodes, split it
  into an overview and one diagram per part. The script warns above 15.
- Diagram direction: `flowchart TB` for chains longer than five nodes; `LR` only
  for short chains. A long `LR` chain gets wider than the column and shrinks or
  scrolls.
- Labels of a few words. Put detail in the text around the picture.
- Same shape for the same kind of thing, in the whole picture: `[(db)]` for a
  store, `([...])` for a start or an end, `{...}` for a decision, `[...]` for a
  step or a service. Say what the shapes mean when it is not plain.
- Clear hierarchy: group with `subgraph`; declare nodes in reading order (the
  layout follows the order of the source); keep the main path first and in a
  straight line. Few line crossings: when lines cross, change the order of the
  declarations or split the picture.
- Title states the finding: "Sign-ups doubled after the March launch", not
  "Sign-ups by week".
- Label lines and series directly (a `text` mark at the end of a line) instead of
  a legend where you can.
- Colour only when it carries meaning (an error, the item the text talks about),
  and never as the only cue: add a label, a dash pattern (`strokeDash`) or a shape.
- At most about 7 colours or series. Group the rest as "Other" or use small
  multiples.
- Sort bars by value (`"sort": "-x"` or `"-y"`; in a layered chart
  `{"op": "sum", "field": "<value>", "order": "descending"}`) unless the
  categories have their own order (months, sizes, steps).
- Start a bar axis at zero. Never cut the value axis of a bar.
- Dates as dates: a `temporal` field, or an `ordinal` field with a `utc` time
  unit for bars (see `references/vega-lite.md`). Never a date as free text.
- Every vega-lite field has a `type` (`quantitative`, `temporal`, `nominal`,
  `ordinal`), and every chart has a `title` and no `width`.
- Accessibility: a chart has a `description` with the finding and the range of
  the data; a diagram has `accTitle` and `accDescr`. All text is at least 11 px
  at its drawn size (the script reports the smallest). Say the finding in the
  text of the reply too.

## 4. Mermaid in the chat

- Do not set a theme or colours. The chat themes every diagram type it knows
  from its own palette, in light and dark, and the person can switch the theme
  at any time. Remove `%%{init}%%` lines and front matter `theme` keys. Use
  `classDef` or `style` only when a colour means something, and prefer a
  `stroke-dasharray` or a label to a colour.
- The chat renders mermaid 12. Use only the diagram types in
  `references/mermaid.md`; a beta type needs its `-beta` first line.
- Gantt charts and `xychart` span the column; other types keep their own size.
  A wider picture scales down while its text stays at least 11 px, else it
  scrolls. So keep diagrams narrow.
- Nothing loads from the network. The chat refuses a diagram with a `url()` in a
  style or an image node from a URL. `click` callbacks do nothing. A link in a
  diagram opens in a new tab.
- `accTitle: ...` names the figure for screen readers; `accDescr: ...` describes it.
- A parse error shows the source with one line: "Couldn't render this mermaid:
  <position and reason>". The person sees that. Check before you send (section 7).

## 5. Vega-lite and vega in the chat

- Data only inline, in `data.values`. A `url` under `data` or in a `lookup`
  transform makes the chat refuse the chart.
- No `width`: a single-view chart fills the column. Multi-view specs (`concat`,
  `facet`, `repeat`) cannot fill it: give each view a `width`.
- Without a `height` the plot is 280 px high. A y axis of categories gets about
  20 px a category. A pie or donut takes a 280 px square.
- The chat sets colours, fonts and gridlines. Do not set `config` colours or
  fonts. When a colour means something (red for errors), check the contrast in
  light and dark, or highlight with `opacity` instead.
- Date-only strings ("2027-03-08") in a `temporal` field show as written, in
  every time zone. A time axis labels day, week and month ticks in one style
  ("Mar 08") and the first tick of a year as "2027".
- Write `description`: it names the chart for screen readers (else the `title`
  does). Tooltips work.
- `usermeta` is ignored and there is no action menu. Set
  `"$schema"`: it decides vega or vega-lite when the fence says the other.

Recipes for each form in the table are in `references/vega-lite.md`.

## 6. svg and widget blocks

**svg.** The chat removes `<style>`, `<image>`, `<feImage>`, `<foreignObject>`
and `<script>`, every `href` or `url()` that is not a `#fragment`, an attribute
with a backslash, and a whole `style` attribute that holds `position`, `top`,
`left`, `right`, `bottom`, `inset`, `transform`, `translate` or `z-index`. Use
presentation attributes (`fill`, `stroke`, `font-size`, a `transform`
attribute). Use `currentColor` so lines and text follow the theme. Give `width`
and `height` (or a `viewBox`); a percentage counts as no size, and a `viewBox`
narrower than 200 with no size takes the whole column. Set `font-size` on all
text (else it takes the size of the page around it), at least 11 px at the
drawn size. Gradients, markers, clip paths and `<use>` of a `#symbol` work.

**widget** (only where the Rendering line names it). One self-contained HTML
fragment with inline `<style>` and `<script>`: vanilla JS, no network, no
external scripts, images or fonts (`data:` images and fonts work), no clipboard,
links do not open. It is as wide as the message and as tall as its content, up
to 800 px; then it scrolls. Take every colour from the theme variables
`--color-{text,background,border}-{primary,secondary,info,success,warning,danger}`.
`localStorage` is kept with the chat (up to 64 K characters): save what the
person changes there. A script error shows as one line inside the widget.
At most 6 widgets run at once; the others wait until they come near the screen.
Use one for a table to sort or filter, a what-if slider, tabs, a step-by-step
walk-through, or a timeline too wide for mermaid. Write it directly in the
reply; the render script does not check widgets.

## 7. Check before you send

A diagram that does not parse shows an error in the chat, in front of the
person. Check every mermaid and vega block that is not trivial.

1. Browser setup, once per machine (the shared tools folder):

   ```bash
   B=${NERVE_BROWSER_DIR:-$HOME/.local/share/nerve-browser}
   mkdir -p "$B" && cd "$B"
   [ -f package.json ] || npm init -y >/dev/null
   npm install --no-audit --no-fund playwright mermaid@12 vega@6 vega-lite@6
   npx playwright install chromium
   npx playwright install-deps chromium   # as root: system libraries and fonts
   apt-get install -y fonts-dejavu-core   # as root: a sans-serif font with bold
   ```

   The last two lines need root (or `sudo` when the person allows it). The
   `browser-testing` skill has the same setup and its known problems.

2. Test the setup with the examples:

   ```bash
   S=<workspace>/skills/diagrams-and-charts
   node $S/scripts/render.mjs $S/assets/examples/order-flow.mmd --out /tmp/diagram-check
   ```

   Good output is one line:
   `ok mermaid (flowchart-v2, 8 nodes, smallest text 14 px) 370x762 light -> /tmp/diagram-check/order-flow.svg, /tmp/diagram-check/order-flow.png`

3. Write the source to a file in the working folder
   `~/.local/share/diagrams/<slug>/`, for example `flow.mmd` or `trend.vl.json`.
   Run with Bash, not `skill_run_script` (its 30 s limit is too short for a
   browser start):

   ```bash
   node <workspace>/skills/diagrams-and-charts/scripts/render.mjs ~/.local/share/diagrams/<slug>/flow.mmd
   ```

   Options: `--out <dir>` (default: next to the input), `--theme light|dark`
   (default light), `--width <px>` for charts and gantt (default 640).

4. Read the result. Exit 0 prints `ok ...` and writes `<name>.svg` and
   `<name>.png` (2x). Exit 1 prints `error: ...`: the parse error with its line,
   or `spec.data.url points outside the spec`. Exit 2: the tools are missing or
   the browser does not start. A `warning:` line names what the chat removes or
   refuses, text under 11 px, more than 15 nodes, or a theme in the diagram.
   Fix it.

5. Look at the PNG with the Read tool. Check that every label is readable, that
   nothing overlaps, and that the picture says what the title says. Then paste
   the same source into the fenced block of your reply.

## 8. Deliver files

The chat themes its figures; a file cannot follow the viewer's theme. So:

- **In the chat:** no theme, no colours, no fonts in the source. The app themes it.
- **In a file** (PNG for Telegram, SVG or PNG for a deck, report or page): the
  script's own theme applies, light by default or `--theme dark`. It paints the
  background into both files, so the text stays readable in any viewer. Pick the
  theme of the page that shows the file.

The SVG holds real text (no HTML labels) in the fonts of the viewer; a PNG
looks the same everywhere. Prefer PNG for chat apps, documents and PowerPoint;
prefer SVG for an HTML page.

To send a file: copy it to `<workspace>/.uploads/diagrams/<name>-<date or
round>.png` (a new name for each round), then call `send_file` with that path.
The web chat shows the image inline; Telegram gets a document; Slack gets an
uploaded file; a channel without file delivery tells the person to open the web
panel. Say in the text what the
picture shows, for the reader who cannot see it.

## 9. Check system diagrams against the source

A diagram of a real system must match the system. Before you send:

- Take each box from a real thing: a service, a file, a table, a team. Take
  each arrow from a real call, message or data flow, pointing from the caller
  to the called. Name the file or document that shows it.
- For an `erDiagram`, take tables, keys and cardinality from the schema or the
  migration files, not from names.
- Mark what you could not confirm: a dashed line (`-.->`) and the word
  "assumed" in the label, and say so in the text.
- Render it, look at the PNG, and compare it box by box with your notes.

## 10. Import an existing diagram

A draw.io, PlantUML or Excalidraw file is data. Keep it in its own folder under
`~/.local/share/diagrams/<slug>/`. Read it; do not run its tools, plugins or
links, and do not open it in a browser.

- draw.io (`.drawio`, `.xml`) and Excalidraw (`.excalidraw`):

  ```bash
  python3 -I <workspace>/skills/diagrams-and-charts/scripts/import_diagram.py diagram.drawio > draft.mmd
  ```

  It prints a `flowchart TB` draft with boxes, labels, groups (`subgraph`) and
  labelled arrows, also from a compressed draw.io page. Exit 1 when it cannot
  read the file. Fix the order and the direction, then check it with
  `render.mjs`.
- PlantUML (`@startuml` text): read the text and write the mermaid by hand.
  Sequence diagrams map almost line by line (`A -> B : msg` becomes
  `A->>B: msg`); class and state diagrams map to `classDiagram` and
  `stateDiagram-v2`.
- Compare the new picture with the old one. Tell the person what you left out
  (colours, positions, icons).

## Known problems

- `error: mermaid does not parse` with `got 'end'`: a node is called `end`;
  rename it (`done`). With `got 'PS'`: a label holds `(` without quotes; write
  `A["Save (draft)"]`. More traps (`;` in a message, `A---oops`, ER labels):
  the table in `references/mermaid.md`.
- Bars only a few pixels wide on a date axis, or a date one day early: use an
  `ordinal` x with `"timeUnit": "utcyearmonthdate"`. More faults: the table in
  `references/vega-lite.md`.
- `vega-lite does not parse: Cannot read properties of undefined`: a wrong mark
  or key. Vega-lite does not check the schema; compare with a recipe.
- `Executable doesn't exist at .../chromium_headless_shell-<rev>`: run
  `npx playwright install chromium` again in the tools folder.
  `error while loading shared libraries`: run `npx playwright install-deps
  chromium` as root. When the proxy refuses the browser download, tell the
  person the host from the error.
- Text shows as boxes in the PNG: the machine has no fonts; `install-deps` adds
  them. Bold text and titles look thin on a bare Ubuntu 24.04: the default
  sans-serif font has no bold face; run `apt-get install -y fonts-dejavu-core`
  as root.
- `journey` diagrams keep HTML labels in the SVG file. Use the PNG outside a
  browser.
- When a skill named `nerve-cloud` is present, read it for the machine and the
  network instead of guessing.
