# Page guide: template, data format, charts, images

Detail for the `shareable-html` skill. Read the part you need.

## 1. The template files

| File | What it is | What you change |
|---|---|---|
| `page.src.html` | the page source: header, key figures, chart, table, footer | title, sub line, section titles, chart `alt` and caption, the "Internal" label, `lang` |
| `page.css` | theme (light and dark), layout, phone and print rules | the colours in `:root` and the dark block, if needed |
| `page.js` | fills the header meta, key figures and table from the data island | usually nothing |
| `page-data.example.json` | the format of the data file, with placeholder values | copy to `page-data.json`, then overwrite it from your prepare step |

The page source names local files only. `bundle.py` puts them all in one file.
Open only the bundled file in a browser: the source has no data in its island.

Class names start with `pg-`. Give any new class the same prefix, so it does
not clash with a class of an included part.

## 2. The data island

The source has:

```html
<script type="application/json" id="data" src="page-data.json"></script>
```

A browser does not load `src` for a JSON script. `bundle.py` reads the file,
checks that it parses, and writes its text into the element. `page.js` reads it
with `JSON.parse(document.getElementById('data').textContent)`.

Format of `page-data.json`:

```json
{
  "meta": {
    "source": "Helpdesk export tickets-2026-11.csv",
    "source_url": "https://example.com/reports/42",
    "as_of": "2026-11-16",
    "built": "2026-11-26",
    "note": "Weeks start on Monday."
  },
  "kpis": [
    {"label": "Tickets closed", "value": 207, "unit": "tickets", "period": "week of 16 Nov 2026",
     "change": -0.115, "change_label": "vs previous week", "better": "up"},
    {"label": "Median first reply", "value": 6.5, "unit": "hours", "digits": 1, "period": "week of 16 Nov 2026",
     "change": -0.5, "change_unit": "h", "change_label": "vs previous week", "better": "down"}
  ],
  "columns": [
    {"key": "week_start", "label": "Week starting", "type": "date"},
    {"key": "closed", "label": "Closed", "type": "number"},
    {"key": "reply_h", "label": "Median first reply", "type": "number", "unit": "h", "digits": 1},
    {"key": "share", "label": "Share", "type": "percent"},
    {"key": "queue", "label": "Busiest queue", "type": "text"}
  ],
  "rows": [{"week_start": "2026-11-16", "closed": 207, "reply_h": 6.5, "share": 0.31, "queue": "Billing"}]
}
```

- `meta`: every element with `data-meta="<key>"` gets that value. `as_of` is
  shown as a date. With `source_url`, the source becomes a link.
  `"example": true` shows a warning bar, and `check_page.mjs` reports it.
- `kpis`: `type` is `number` (default) or `percent` (value 0.31 shows 31%).
  `change` is a fraction (−0.115 shows −11.5%) unless `change_unit` is set
  (then it shows the number and the unit, for example "−0.5 h" or "+2 pts").
  `better` is `up` or `down`: it sets green or red. The sign always shows,
  so the colour is not the only signal.
- `columns`: `type` is `text`, `number`, `percent` or `date` (ISO
  `YYYY-MM-DD`). `digits` fixes the decimals. Numbers align right. A click on
  a header sorts; numbers sort high to low first.
- Number and date format follow `<html lang>`: `en` gives "Nov 16, 2026",
  `en-GB` gives "16 Nov 2026", `de` gives "16. Nov. 2026". Set the person's
  locale.

The filter matches the shown text of every cell. The row count says
"12 of 30 rows", also in print, so a filtered printout says so.

## 3. Make the data file from the source data

Write a small `prepare.py` in the working folder. It reads the raw file and
writes `page-data.json` and any chart specs. Compute every figure there; never
type a number into the page. A short example:

```python
import csv, json
rows = list(csv.DictReader(open('raw/tickets-weekly.csv', newline='')))
for r in rows:
    r['opened'], r['closed'] = int(r['opened']), int(r['closed'])
last, prev = rows[-1], rows[-2]
data = {
    "meta": {"source": "raw/tickets-weekly.csv", "as_of": last['week_start'], "built": "2026-11-26"},
    "kpis": [{"label": "Tickets closed", "value": last['closed'], "unit": "tickets",
              "change": round((last['closed'] - prev['closed']) / prev['closed'], 4),
              "change_label": "vs previous week", "better": "up"}],
    "columns": [{"key": "week_start", "label": "Week starting", "type": "date"},
                {"key": "closed", "label": "Closed", "type": "number"}],
    "rows": rows,
}
json.dump(data, open('page-data.json', 'w'), indent=1)
```

Run it with `python3 -I prepare.py`. Keep only the columns the page shows: the
page carries all of its data, and anyone who gets the file can read all of it.

## 4. Charts

### Default: static SVG from the diagrams-and-charts render script

```bash
R=<workspace>/skills/diagrams-and-charts/scripts/render.mjs
node $R chart.vl.json --out charts/light --theme light
node $R chart.vl.json --out charts/dark --theme dark
```

Each run writes `chart.svg` and `chart.png` into its folder. The SVG has a
fixed theme and an opaque background. The page shows the one that matches the
reader's theme:

```html
<picture>
  <source srcset="charts/dark/chart.svg" media="screen and (prefers-color-scheme: dark)">
  <img src="charts/light/chart.svg" alt="What the chart shows and its main point">
</picture>
```

`screen and` makes print use the light chart. The page colours of the template
are the same as the render script's export theme, so the chart background is
the page background. If you change the page colours, the chart shows as a
panel of its own colour.

Width: the default plot width (640 px) suits a page. The page never stretches
a chart. On a phone a wide chart shrinks to 36rem, then scrolls sideways, so
its text stays readable. For a page
that people read mostly on a phone, render a narrow copy too (`--width 340`,
into `charts/light-narrow` and `charts/dark-narrow`) and add `<source>`
elements with `(max-width: 40rem)` before the others.

For the chart spec itself (mark choice, titles, units, colour that means
something), follow the `diagrams-and-charts` skill.

### Your own drawing: an SVG that follows the theme

A small drawing (a bar row, a status strip, a timeline) can be one SVG file
that uses `currentColor` and no fixed colours. Put it into the page as markup:

```html
<figure class="pg-chart"><!-- include: drawing.svg --></figure>
```

`bundle.py` puts the file text in place of the line and removes its XML
header. Inline SVG gets the page CSS: `fill="currentColor"` follows the text
colour, and a `class` can take `var(--accent)`. Give each inline SVG unique
`id` values: two charts with `id="clip1"` take each other's clip path, and
`bundle.py` warns about duplicate ids.

The include line also builds a long page from parts
(`<!-- include: parts/summary.html -->`). Paths are relative to the file that
holds the line, and must stay inside the source folder.

### When an inline JS chart is worth its weight

Use a chart drawn in the browser only when the person must work with the chart
itself:

- the chart must follow the table filter or a control (a calculator, a what-if);
- the person needs exact values of many points on hover;
- the person switches series or ranges.

Then write a small SVG drawing function in `page.js` (40 to 80 lines for bars
or lines: scale, axis ticks with units, a `<title>` on each mark for hover).
Do not add a chart library: vega, vega-lite and vega-embed together are about
840 KB minified, and a library from a CDN fails offline (and `bundle.py`
refuses it). Never paste code from a download into the page.

## 5. Interactive pages (calculators, explorers)

- Inputs: `<input type="number">` with `min`, `max`, `step` and a `<label>`
  with the unit. Compute on `input` events. Show the formula in words under the
  result.
- State: put the inputs in the URL hash (`location.hash`) so a person can send
  a link to one case. `localStorage` works from disk in Chromium browsers, but
  pages opened from disk can share one storage; prefix every key with the page
  slug.
- Defaults: real values from the data, with the source line.

## 6. Images

Resize before you bundle. A photo or screenshot: JPEG, at most 1600 px on the
long edge, quality 80. A logo or icon: PNG, 256 px. Pillow in a venv in the
working folder:

```bash
uv venv -q .venv && uv pip install -q --python .venv/bin/python pillow
.venv/bin/python -I - raw/logo-original.png img/logo.png 256 <<'EOF'
import sys
from PIL import Image
src, out, edge = sys.argv[1], sys.argv[2], int(sys.argv[3])
im = Image.open(src)
im.thumbnail((edge, edge))
if out.lower().endswith(('.jpg', '.jpeg')):
    im.convert('RGB').save(out, quality=80, optimize=True)
else:
    im.save(out, optimize=True)
print(out, im.size)
EOF
```

Base64 makes each file about a third larger in the page. `bundle.py` warns
about an image above 300 KB.

## 7. Fonts

The template uses the system font stack: no font file, no weight. A brand
font costs 20 to 100 KB per weight as WOFF2. If the person wants one, put the
`.woff2` file in `fonts/` and add to `page.css`:

```css
@font-face { font-family: "Brand"; src: url("fonts/brand-400.woff2") format("woff2"); font-weight: 400; }
```

`bundle.py` makes the file a data URI. Check the font licence allows
embedding.
