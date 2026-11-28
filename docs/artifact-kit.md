# Artifact kit

The artifact kit is a set of static files of the web build that draw a page
of a bundle in the browser: a deck helper, a table page and a Markdown page.
A bundle has them in its reserved folder `nerve-kit/`. The
[Preview tab](web-ui.md#preview) shows a bundle with them.

| File | What it is |
|---|---|
| `nerve-kit/deck.js`, `nerve-kit/deck.css` | The deck helper: one slide at a time, keys, slide numbers, present and print |
| `nerve-kit/table.js` | The table page of a CSV file |
| `nerve-kit/markdown.js` | The Markdown page of a Markdown file |
| `nerve-kit/kit.css` | The styles of the table page and the Markdown page, light and dark |

A page asks for a kit file with a relative address, for example
`nerve-kit/deck.js`. The bundle gets the kit files that its entry page
names: the `index.html` of a folder, or the one `.html` file of a page
bundle. The runtime finds them by a search for the fixed addresses
(`nerve-kit/` and one of the names above) in the first 1 MiB of the entry
file; it does not parse the HTML of the page. So a deck must be the entry page of its
bundle, or the entry page must name the same kit files (another page of
the folder then uses `../nerve-kit/deck.js` from a subfolder). An address
in a code sample of the entry page adds that kit file too, which does no
harm. Only these names are kit files. A workspace folder cannot have its
own `nerve-kit/` folder in a bundle. An address with an encoded separator
(`%2F`, `%5C`) names no file.

Kit files load nothing from the network, set no cookie and use no storage.
They work in a frame without an origin and under a policy that allows only
the files of the bundle.

## A deck

Write the deck as one HTML page, as the entry of its bundle: the
`index.html` of its folder, or a single `.html` file.

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Quarterly review</title>
  <link rel="stylesheet" href="nerve-kit/deck.css">
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <section class="slide">
    <h1>Quarterly review</h1>
  </section>
  <section class="slide">
    <h2>Cost by service</h2>
    <p class="footer">Platform team <span data-slide-number></span></p>
  </section>
  <script src="nerve-kit/deck.js"></script>
</body>
</html>
```

- Each `<section class="slide">` is one slide, in the order of the page.
- A slide is 1280 by 720 CSS pixels. Lay out the content for that size. The
  helper sets the position, the size and the scale of each slide: do not set
  `position`, `width`, `height` or `transform` on a slide.
- Name `nerve-kit/deck.css` in a `<link>` before your own styles, and
  `nerve-kit/deck.js` in a `<script>` after the slides (or with `defer`).
- The helper puts the slide number ("3 / 12") into each element with the
  attribute `data-slide-number`. A slide without one gets a
  `.nerve-slide-number` element at its bottom right. Style it, or hide it
  with `display: none`.
- Keys: Right, Down, Page Down and Space go to the next slide; Left, Up and
  Page Up to the one before; Home and End to the first and the last. Keys in
  a text field stay with the field.
- The print layout has one slide on each page of 1280 by 720 pixels, with
  the colors of the page.
- A slide without a background of its own is white with dark text.

Put your styles, images and fonts in files of the bundle, and name them with
relative addresses. A file of another host does not load.

## Markdown and CSV files

The bundle of a `.md` or `.csv` file has a generated `index.html`, the kit
page that draws it, and the file itself as `source.md` or `data.csv`. The
generated page holds the text of the file as JSON data: the browser draws
it, and no text of the file becomes markup of the page.

- The Markdown page uses the Markdown renderer of the chat (GitHub tables,
  task lists, code colors). It shows raw HTML of the file as text. A block
  of a diagram, a chart or a widget shows as code. An image shows its alt
  text. A link does not open a page (the frame of a kit page cannot open
  one): it shows as plain text, its text and then its address in
  parentheses, for example `the report (https://example.org/page)`, or the
  address once when the text is the address. A person can select and copy
  it. A link inside the page (`#name`, for example a footnote) stays a link.
  A link with an unsafe address (`javascript:`, `data:`) shows its text
  only, without the address.
- The table page shows the first line of the file as the header. It has a
  search over all columns, a sort for each column (a column of numbers sorts
  by value) and the count of rows ("8 of 214 rows"). Each value is shown as
  text. The page draws at most 500 rows at one time; a **Show 500 more
  rows** button draws the next ones, and the count says how many are shown
  ("500 of 9,999 rows shown"). The search and the sort work on all rows.
- The download button of both pages asks the page around the frame for the
  file (`source.md` or `data.csv`): a frame of the kit cannot start a
  download itself. On the Markdown page the button floats at the top right
  of the frame, over the text. It shows while the pointer is over the page
  or the button has the keyboard, and always on a touch screen. The download is the whole file. The page around the
  frame decides by the kind of the bundle (a Markdown or a CSV bundle),
  never by the kind that the page states.
- Limits: the file is UTF-8 text of at most 5 MiB; a CSV file has at most
  10,000 rows after the header row, 200 columns, and 200,000 cells (all
  rows by the widest row). Above a limit the bundle is refused with a clear
  reason. The Markdown page and the table page check the same limits
  themselves (the size in UTF-8 bytes first, before any parse), so a page
  that names a kit script and carries its own data gets the same refusal.
  A field of any length within these limits is fine.

## Messages

A kit page talks to the page around its frame (the host, for example the
Preview tab) with `postMessage` to the target `*`. Each message is an object with
`channel: "nerve-artifact"` and `v: 1`. Slides count from 1.

From the kit page:

```json
{"channel": "nerve-artifact", "v": 1, "type": "ready", "kind": "deck", "canPrint": true, "slides": 12, "slide": 1}
{"channel": "nerve-artifact", "v": 1, "type": "ready", "kind": "table", "canPrint": false}
{"channel": "nerve-artifact", "v": 1, "type": "ready", "kind": "markdown", "canPrint": true}
{"channel": "nerve-artifact", "v": 1, "type": "slide", "slide": 3, "slides": 12}
{"channel": "nerve-artifact", "v": 1, "type": "download"}
```

`ready` comes at load and as the answer to `hello`. A deck without slides
says `ready` without `slides` and `slide`. `download` comes from a table or
Markdown page.

To the kit page:

```json
{"channel": "nerve-artifact", "v": 1, "type": "hello"}
{"channel": "nerve-artifact", "v": 1, "type": "print"}
{"channel": "nerve-artifact", "v": 1, "type": "slide", "to": "next"}
{"channel": "nerve-artifact", "v": 1, "type": "slide", "to": 3}
{"channel": "nerve-artifact", "v": 1, "type": "present", "on": true}
```

`to` is `next`, `prev`, `first`, `last` or a slide number. A page calls
`window.print()` on `print` only when it said `canPrint: true`. `present`
shows the slide of a deck with no margin. A kit page takes a message only
from its parent window, and ignores a message with an unknown type or a
value out of range.

## The build

`npm run build` in `web/` builds the kit after the web UI
(`web/scripts/build-kit.mjs`) into `web/dist/nerve-kit/`. The names are
fixed in `web/src/kit/files.json` and in `KIT_FILES` of
`nerve/artifacts/bundle.py`. Without a build, a bundle that needs a kit file
is refused with `kit_missing`.
