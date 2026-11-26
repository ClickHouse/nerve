# Handoff: <deck name>

Folder: ~/.local/share/decks/<slug>/. Last round: r<N> on <date>, delivered as
<workspace>/.uploads/decks/<name>-r<N>.html (and .pdf, .pptx when asked).
Master copy: this HTML deck | the person's copy in <PowerPoint / Google Slides>.

## Build and check

```bash
cd ~/.local/share/decks/<slug>
python3 -I build.py && node shoot.mjs && python3 -I text-check.py
node pdf.mjs                                                    # when a PDF is asked for
node pptx-export.mjs && .venv/bin/python -I pptx-build.py       # when a PPTX is asked for
```

Preset: <plain | editorial | swiss>. Fonts: <system stack | FACES in build.py>.
Mockup classes in MOCK (pptx-export.mjs): mock<, ...>.

## Slides

Run `python3 -I edits/lib.py` for the current list. At the end of each round, write it here:

1. <title>
2. <title> (clicks: 2)

## Rulings of the person (with the date; do not reopen)

- <date>: <ruling, for example "call it 'saved filters', never 'presets'">

## Removed slides

- <date>: "<title>", kept in deck.src.before-<change>.html

## Open items

- [CHECK] <item>
