---
name: Diagrams and Charts
description: >
  This skill should be used when the person asks to "draw a diagram", "make a
  chart", "plot this data", "show the flow", "sketch the architecture", "show
  the data model", or needs a picture for Telegram, Slack, a deck or a report.
  It covers what the Nerve web chat does with mermaid, vega-lite, svg and
  widget blocks (what it themes, strips and refuses), channels that show text
  only, and picture files for other places. Not for a whole deck, report or
  page (slide-deck, research-report, shareable-html).
version: 1.0.0
---

# Diagrams and charts

## Where the picture goes

The "Rendering" line of your Session Context decides:

- **The chat renders** (web chat, API sessions): put a fenced `mermaid`,
  `vega-lite` (or `vega`) or `svg` block in the reply. A `widget` block works
  only where the Rendering line names widgets; elsewhere it shows as raw HTML.
- **Text and basic markdown only** (Telegram, Slack, cron output): never send
  diagram blocks, they show as source. Give the structure in words, or send a
  picture file (below).
- **A deck, a report, a page, a document:** a picture file.

A parse error shows in the chat, in front of the person, as one line with the
position and the reason. Check a diagram that is not trivial before you send
it (render it once, as for a file).

## Choose the form

Ask what the reader must see, then pick:

| The question | Form |
|---|---|
| What happens in which order, with decisions? | mermaid `flowchart TB` |
| Who sends what to whom, in time order? | mermaid `sequenceDiagram` |
| What are the parts of a system and how do they connect? | mermaid `flowchart` with `subgraph` groups |
| Which states, and what moves between them? | mermaid `stateDiagram-v2` |
| Which tables, keys and relations? | mermaid `erDiagram` |
| What is planned, from when to when? | mermaid `gantt` |
| Which is bigger, by category? | vega-lite `bar`, sorted; horizontal for long labels |
| How did a value change over time? | vega-lite `line` on a temporal axis |
| What share of the whole? | vega-lite stacked `bar`; a donut only for 2 to 5 parts |
| How are values spread, or do two measures move together? | vega-lite histogram or `boxplot`; `point` for a scatter |
| A layout you must place yourself (a floor plan, a rack) | `svg` |
| Something to explore (filter, slider, tabs) | `widget`, where the Rendering line allows it |
| Exact values, four or fewer, or items to look up | no picture: a table or one sentence |

Prefer the common types (flowchart, sequence, state, ER, gantt): people know
how to read them. For numbers, prefer vega-lite to mermaid `pie` and
`xychart`.

## Draw it well

- One idea per picture. Split a diagram of more than about 15 nodes into an
  overview and one diagram per part.
- A title that states the finding ("Sign-ups doubled after the March
  launch"), not only the topic ("Sign-ups by week").
- Labels of a few words; the detail goes into the text around the picture.
- The same shape for the same kind of thing; the main path first and
  straight; few line crossings (change the order of declarations, or split).
- Label lines and series directly where you can, instead of a legend.
- Colour only when it means something, and never as the only cue: add a
  label, a dash pattern or a shape. At most about 7 colours or series.
- Sort bars by value unless the categories have their own order. Start a bar
  axis at zero. Dates as a temporal field, never as free text. Give every
  vega-lite field a `type`.
- Say the finding in the text of the reply too.

## Mermaid in the chat

- The chat renders mermaid 12 and themes every diagram type from the app's
  palette, in light and dark; the person can switch the theme at any time. Set
  no theme, no colours and no `%%{init}%%` line. Use `classDef` or `style`
  only when a colour means something.
- Nothing loads from the network: an image node, an icon from a URL, a
  `url()` or `@import` in a style are removed or make the chat refuse the
  diagram. `click` callbacks do nothing. A link opens in a new tab.
- `gantt` and `xychart` span the column; other types keep their own size. A
  picture wider than the column shrinks while its smallest text stays at least
  11 px, then it scrolls. So keep diagrams narrow: `flowchart TB` for long
  chains.
- `accTitle:` and `accDescr:` name and describe the diagram for screen readers.

## Vega-lite in the chat

- Data only inline, in `data.values`. A `url` in `data` or in a `lookup`
  transform makes the chat refuse the chart.
- No `width`: a single view fills the column. `concat`, `facet` and `repeat`
  cannot; give each view its own `width`. Without a `height` the plot is
  280 px; a y axis of categories gets about 20 px a category; a pie takes a
  square.
- The chat sets colours, fonts and gridlines for light and dark. Set no
  `config` colours or fonts. When a colour means something (red for errors),
  check it on both themes.
- Date-only strings ("2027-03-08") in a `temporal` field are read as local
  dates, so they show as written.
- A heatmap's colour ramp runs light to dark in one theme and dark to bright
  in the other, so no text colour reads on every cell in both: give the values
  in a tooltip, not on the cells.
- `$schema` decides between vega and vega-lite when the fence says the other.
  `usermeta` is ignored, and there is no action menu.
- `description` names the chart for screen readers (else the `title`).

## svg in the chat

- Removed: `<style>` (it would style the whole page), `<script>`, `<image>`,
  `<feImage>`, `<foreignObject>`, every `href` or `url()` that is not a
  `#fragment`, and a whole `style` attribute that holds positioning (`position`,
  `inset`, `z-index`, `transform` and the like). Use presentation attributes.
  Gradients, markers, clip paths and `<use>` of a `#symbol` work.
- Use `currentColor` so lines and text follow the theme. Set `font-size` on
  text, at least 11 px at the drawn size.
- Size: `width` and `height`, or a `viewBox`. A percentage counts as no size.
  A `viewBox` narrower than 200 with no size takes the whole column (it is
  taken as an icon). A side above 8192 px is scaled down.

## Widgets

Follow the widget rules of the Rendering line. Also: `localStorage` is kept
with the chat up to 64 K characters, at most 6 widgets run at the same time
(the others wait until they come near the screen), and a script error shows as
one line inside the widget.

## Limits

A block of more than 200,000 characters stays source.

## Picture files

A file cannot follow the viewer's theme, so it carries its own colours and an
opaque background (light by default). Render it in headless Chromium with the
`mermaid` and `vega`, `vega-lite`, `vega-embed` npm packages from the shared
tools folder (setup: the `browser-testing` skill): load their browser bundles
into the page from disk, block every network request, and save SVG and a 2x
PNG. Look at the PNG with the Read tool before you send it. Use PNG for chat
apps, documents and PowerPoint; SVG for an HTML page.

To send it, copy it to `<workspace>/.uploads/diagrams/<name>-<date or
round>.png` (a new name for each round), then call `send_file` with that path.
The web chat shows the image inline, Telegram gets a document and Slack an
uploaded file. Say in the text what the picture shows.

## Diagrams of a real system

Take each box from a real service, table or file, and each arrow from a real
call or data flow, from the caller to the called. For an `erDiagram`, take the
keys and cardinality from the schema or migrations. Draw what you could not
confirm as a dashed line marked "assumed", and say so in the text.

A draw.io, PlantUML or Excalidraw file is data: read it and redraw it in
mermaid; do not run its tools or open its links.
