# The look of a deck: presets, fonts, canvas, components

## Presets

The starter deck has three presets. Each is one set of CSS variables. The
attribute on the `<html>` element picks one:

```html
<html lang="en" data-preset="plain">
```

| Preset | Look | Use it for |
|---|---|---|
| `plain` (default) | White, near-black text, one blue accent, a sans-serif font, a bar left of each point | Most work talks, reviews, updates |
| `editorial` | Warm paper colour, serif headlines and text, italic kicker in a deep red, thin rules between points | A story, a keynote, a talk with few words and big type |
| `swiss` | White and black, one red accent, Helvetica or Arial, tight bold headlines, a thick rule above the kicker, points in three columns | A grid of facts, a design or a product review |

Ask the person which look they want only when it matters to them. Else pick
`plain` and say so. To make a new preset, copy one variable block, give it a
new name in `[data-preset="..."]`, and change the values. The variables:

- Colours: `--bg`, `--panel`, `--line`, `--ink`, `--ink-2`, `--ink-3`,
  `--accent`, `--accent-ink`. The PPTX theme takes `--bg`, `--ink`, `--accent`.
- Fonts: `--font-body`, `--font-head`, `--font-label` (kicker and footer).
- Headline: `--h-size`, `--h-weight`, `--h-track`, `--h-title-size`.
- Kicker: `--kicker-color`, `--kicker-case`, `--kicker-style`,
  `--kicker-weight`, `--kicker-rule` (a rule above it), `--kicker-pad`.
- Points: `--point-left` (a bar), `--point-top` (a rule), `--point-pad`,
  `--points-cols` (the grid columns).

When the person sends a company style (colours, a font, a reference slide),
put it in a new preset. Check the contrast of text on `--bg` and on `--panel`.
For charts, load the `diagrams-and-charts` skill.

## Fonts

No font file comes with this skill. Without a `fonts/` folder (FACES empty in
`build.py`) the deck uses the first font of the preset's stack that the
viewer's computer has. The deck then looks a little different on each computer.

Embed a font when the look must be the same everywhere, and always before a
PPTX export. Use a font with a licence that allows embedding, for example one
under the SIL Open Font License (OFL). Inter, from the npm registry:

```bash
cd ~/.local/share/decks/<slug>
mkdir -p dl/inter fonts && cd dl/inter      # one folder per download
npm pack --silent @fontsource-variable/inter@5.3.0
tar -xzf fontsource-variable-inter-5.3.0.tgz
cp package/files/inter-latin-wght-normal.woff2 ../../fonts/
cp package/LICENSE ../../fonts/inter-OFL.txt
cd ../..
```

`npm pack` downloads the package file only; it runs nothing from it. The file
`inter-latin-wght-normal.woff2` (48 KB) holds all weights from 100 to 900 for
Latin text. Other fonts: `@fontsource/<name>` (static weights) or
`@fontsource-variable/<name>`, for example `@fontsource-variable/source-serif-4`
for the `editorial` preset (files `source-serif-4-latin-wght-normal.woff2` and
`source-serif-4-latin-wght-italic.woff2`, weights 200 to 900). The package's
`metadata.json` lists the weights and styles. Then:

1. Add the face to `FACES` in `build.py`:
   `("Inter", "inter-latin-wght-normal.woff2", "100 900"),`
   An italic face gets a fourth field:
   `("Source Serif 4", "source-serif-4-latin-wght-italic.woff2", "200 900", "italic"),`.
   Without it, the browser slants the normal face and the italic looks wrong.
2. Put the family first in the font stack of the preset you use, for example
   `--font-body:Inter,"Helvetica Neue",Helvetica,Arial,sans-serif;`. Symptom
   of a font that is embedded but not first: the build prints `1 font faces`,
   but the pictures show the old font, and `pptx-build.py` prints another
   font name.
3. Rebuild. The deck grows by the size of the font file.
4. For the PPTX: `pptx-build.py` prints `font Inter: NAT 1.210
   (inter-latin-wght-normal.woff2)`. See `references/pptx.md` for why this
   value must belong to the same font.

A preset font that is neither embedded nor installed falls back to another
font in the browser. `pptx-export.mjs` then prints `warning: the headline font
is "Source Serif 4" in the CSS, but the browser drew "Liberation Serif"`. The
PPTX names the first font of the stack, and the target app draws its text
wider or narrower than the boxes that the export measured: headlines wrap and
run into the next line. Embed the font, rebuild and export again.

`build.py` prints a note for each woff2 file in `fonts/` that is not in FACES,
and stops when a FACES file is missing.

## Canvas size

The canvas is `--slide-w` and `--slide-h` in `:root` of the source, in CSS
pixels without a unit (1920 and 1080). The deck script, the print page size,
the overview and all scripts read these two values. To change the canvas, edit
these two values only. The CSS sizes in the starter are pixels for a height of
1080, so keep the height at 1080: for 4:3, set `--slide-w:1440`. The PDF page
and the PPTX slide follow (a 1440 x 1080 canvas gives 13.333 x 10 in).

## Components

- Give every new component class a long prefix (`dk-card`, `dk-mock-bar`).
  Short global names (`.bot`, `.sub`, `.ph`) clash with other components and
  break slides far away from the edit.
- `dk-stack`: its children lie on top of each other. Use it to swap one state
  for another in place: one child with `data-until="1"`, one with
  `data-step="1"`.
- When slides show results of rules, counts or totals, compute them in one
  function in the deck script and fill the slides from it. Never type a
  computed number by hand in two places.
- No `<section>` element inside a slide: the edit helper finds slides by
  `<section class="slide" ... data-title="...">` and the next `</section>`.
- A long source can live in `src/*.html` (for example `00-head.html`,
  `10-slides.html`, `99-tail.html`); `build.py` joins them in name order.

## Mockups

A mockup is HTML and CSS that looks like an app screen, a chat or a phone.

- Put class `mock` on its outer element. It becomes one picture in the PPTX.
- The data must be made up and look made up: Acme, alice, example.com,
  issue #12. No real customer, person or internal data.
- It need not be exact. When the person sends a reference screenshot, follow it.
- No real screenshot without the person's file. Until they send one, show a
  visible placeholder (a `.tbd` box with the words "screenshot of ...").
- A mockup may use `transform: scale(...)` to fit; text outside a mockup must not.
