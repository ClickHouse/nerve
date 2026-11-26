# PPTX export, LibreOffice preview, native PPTX

Read this before the first PPTX of a deck.

## Which way to go

| The person wants | Do this |
|---|---|
| To present, share or print | The HTML deck and its PDF. No PPTX. |
| To move the deck into Google Slides or PowerPoint, for example to apply the company template | The HTML deck, then the PPTX export below. It gives an editable start. |
| To edit in PowerPoint with the company template from the first slide, often with other editors | A native PPTX built with python-pptx from their template (last section). |

When the person asks which way to go, give a recommendation with the reason.
A deck rebuilt by hand in the target app loses the click builds and the mockups.

## Settings

In `pptx-export.mjs`:

- `MOCK`: classes that become one picture each. The starter uses `mock`. Put
  class `mock` on the outer element of each mockup, or add its own class here.
  Symptom of a missing class: the mockup comes out as many loose text boxes and
  shapes.
- `SKIP`: classes left out (`notes`, `foot`). The target template has its own
  footer and page numbers.
- `HEAD`: headline classes. The biggest one goes into the title placeholder.

In `pptx-build.py`:

- `FONT`, `HEAD_FONT`: theme fonts. `None` takes the first family of the
  deck's font stacks. The builder prints the font and its `NAT`. The exporter
  asks the browser which font it drew; when that is not the first family, it
  prints `warning: the ... font is "X" in the CSS, but the browser drew "Y"`.
  Embed the font (`references/look.md`) and export again.
- `NAT`: the ascent plus descent of a font in em, from the font's `hhea` table.
  The builder reads it from the matching file in `fonts/` (needs `fonttools`
  and `brotli` for woff2); else from the table in the script; else 1.2 with the
  note `default: not measured`. The builder gives a single line of text a box of
  this height. A value from another font moves single lines up or down against
  the HTML. Measure a font file:
  ```bash
  .venv/bin/python -I -c "from fontTools.ttLib import TTFont; t=TTFont('fonts/inter-latin-wght-normal.woff2'); h=t['hhea']; print((h.ascent-h.descent)/t['head'].unitsPerEm)"
  ```
  Measured: Inter 1.21, Jost 1.445, Lato 1.2, Source Serif 4 1.371,
  Liberation Sans 1.117, DejaVu Sans 1.164.
- `MAX_PX` (2000): pictures larger on the long edge are scaled down. Google
  Slides resamples larger pictures badly on import.

## What the export makes

- Text stays editable text, with size, weight, italics, colour, letter spacing
  and alignment. A dotted placeholder line (`.tbd`) becomes a dotted underline.
- The biggest `.h` goes into the title placeholder of a "Title Only" layout.
- `.mock`, `img`, `svg`, `canvas` and `video` become 2x PNG pictures. A mockup
  that changes on a click gets one picture per state, stacked.
- Boxes with a background or a full border become shapes. A border on one side
  only (a rule, a marker bar) becomes a thin rectangle.
- Each click step becomes an Appear or Disappear animation.
- The theme gets the deck's `--ink` (text), `--bg` (background), `--accent` and
  fonts, so a company template can replace them. Text in the theme ink colour
  has no colour of its own.
- The notes get the speaker notes, a list of the click builds, a MEDIA or
  BACKGROUND reminder where the HTML slide has sound, video or a background
  picture, and the HTML slide number and title.
- The slide size is 13.333 in wide; the height follows the canvas
  (`--slide-w`, `--slide-h`). 1920 x 1080 gives 13.333 x 7.5 in.

## Known losses

- Sound and video are lost. Insert them again in the target app.
- Live text (a clock, a counter) is frozen at its value during the export.
- CSS transitions are lost; click builds are Appear and Disappear. LibreOffice
  imports them with the right click counts (`pptx-preview.sh` checks this). The
  Google Slides and PowerPoint imports are not verified, so the notes carry the
  list of builds as a backup.
- In edit view, swapped elements lie on top of each other. That is normal: in
  the slideshow they show one at a time.
- Google Slides always wraps text. The builder adds 25% width to single lines
  and 12% to pills.
- The exporter skips elements with no size. A `data-step` wrapper whose
  children are all absolutely positioned needs a box:
  `style="position:absolute;inset:0;pointer-events:none"`.
- `::before` and `::after` content, CSS background pictures, gradients, shadows
  and transforms are not exported. Use real elements for content. Use transforms
  only inside a mockup (it becomes a picture).
- The PPTX names its fonts but does not hold them. The target app uses its own
  font of that name, or another font when it has none, and text widths shift.
  Use a font that the target app has: Google Slides offers the Google Fonts
  families (Inter is one); PowerPoint needs the font on each computer, or the
  company font. Embed the same font in the HTML (FACES), so that the boxes the
  export measures fit the PPTX text.

## LibreOffice preview (optional)

Pictures of the PPTX as LibreOffice draws it, without PowerPoint. Needs Docker,
and the proxy must allow Docker Hub and the Ubuntu package archive for the
first run.

```bash
cd ~/.local/share/decks/<slug>
bash pptx-preview.sh        # dpi 50; bash pptx-preview.sh 80 for sharper pages
```

The first run builds the image `slide-deck-libreoffice` (about 45 s, 670 MB
on disk). Each run converts in a throwaway container with no network. It
converts the woff2 fonts in `fonts/` to TrueType for LibreOffice. Output in
`pptx-preview/`: `deck.pdf`, `p-N.png`, `sheet.png` (all pages on one picture)
and a line that compares the clicks per slide with the HTML deck. Look at
`sheet.png` with the Read tool. The pages show every shape at once, so swapped
states lie on top of each other. LibreOffice draws close to PowerPoint, not the
same. Remove the image when the deck is done: `docker rmi slide-deck-libreoffice`.

## Native PPTX with python-pptx

Use the venv of the deck folder. Start from the person's template file. A
`.potx` file does not open (`ValueError: file 'x.potx' is not a PowerPoint
file, content type is '...template.main+xml'`). Save it as `.pptx` in
PowerPoint, or change its content type:

```bash
python3 -I - <<'EOF'
import zipfile
src, dst = zipfile.ZipFile('template.potx'), zipfile.ZipFile('template.pptx', 'w', zipfile.ZIP_DEFLATED)
for n in src.namelist():
    d = src.read(n)
    if n == '[Content_Types].xml':
        d = d.replace(b'presentationml.template.main+xml', b'presentationml.presentation.main+xml')
    dst.writestr(n, d)
dst.close()
EOF
```

Then list the layouts and their placeholders, and fill them:

```python
from pptx import Presentation
prs = Presentation('template.pptx')
for i, l in enumerate(prs.slide_layouts):
    print(i, l.name, [(p.placeholder_format.idx, p.name) for p in l.placeholders])
for sid in list(prs.slides._sldIdLst):  # drop the sample slides of the template
    prs.part.drop_rel(sid.rId)
    prs.slides._sldIdLst.remove(sid)
s = prs.slides.add_slide(prs.slide_layouts[1])  # the index of "Title and Content" in this template
s.shapes.title.text = 'Three changes people use every day'
body = s.placeholders[1].text_frame
body.text = 'Export to CSV'
body.add_paragraph().text = 'Saved filters'
s.notes_slide.notes_text_frame.text = 'What to say. The source of each number.'
prs.save('deck.pptx')
```

Keep the same method: the content draft first, edits by slide title, notes for
the detail. For a mockup, draw it as a `.mock` in an HTML deck and run
`pptx-export.mjs`: it writes a 2x PNG of each mockup to `pptx/img/`. Insert that
file with `s.shapes.add_picture(path, left, top, width, height)`. Look at the
result with `pptx-preview.sh`.
