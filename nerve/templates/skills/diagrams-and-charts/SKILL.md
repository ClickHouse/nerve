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

A block of more than 200,000 characters stays source. Split big diagrams:
more than about 15 nodes is rarely readable.

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
