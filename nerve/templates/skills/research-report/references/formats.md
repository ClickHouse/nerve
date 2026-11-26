# Formats that build_report.py reads

`scripts/build_report.py` turns a working folder into one HTML page. It reads
only the forms on this page. Everything else is plain Markdown (CommonMark plus
tables, strikethrough and bare links). Raw HTML in the Markdown is shown as
text, never run: notes often quote untrusted pages.

## REPORT.md head

```markdown
# Acme Notes vs Globex Notes

Date: 2026-11-26. Internal. Window: 2025-11-26 to 2026-11-26.
Tally: Who leads = Acme, Globex, Split, Even, Neither
```

- **Line 1** must be `# <Title>`. It is the page title. Without it the build
  stops with exit 2.
- **Date line** (optional). The build splits it at each full stop. The first
  part becomes the chip `Date: 2026-11-26`; each other part becomes a chip as
  written. A part that starts with `Internal` or `Confidential` is red.
- **Tally line** (optional): `Tally: <column header> = <value>, <value>, ...`.
  It names the comparison column and the values its cells start with.
- The Date and Tally lines must come right after line 1 (blank lines between
  them are fine). The build removes them from the text.

## Notes

- Every `notes/*.md` file becomes a part after Part 1, with a sidebar entry
  that lists its `##` headings. Line 1 (`# <Title>`) is the part title.
- Order: the order in which REPORT.md first names each note as
  `notes/<name>.md`, then the notes it does not name, by file name.
- In REPORT.md, `` `notes/<name>.md` `` and `[text](notes/<name>.md)` become
  links to that part.
- A survey of 25 items gives 25 parts. The sidebar shows Part 1 open and the
  notes closed; that keeps it short.

## Comparison table

A table is a comparison table when one header cell equals the Tally column
(case does not matter). In it:

| Cell | Rule |
|---|---|
| A row with text only in the first cell | a group row (grey, small caps) |
| Header of a column that starts with a tally value | a coloured line under it (`Acme Notes` starts with `Acme`) |
| Any other cell (not the first, not the tally column) | may start with a status token, then a space, `(`, `,`, `.`, `;` or `:` |
| Tally column cell | must start with one of the tally values; the build counts it |

Status tokens:

| Token | Meaning |
|---|---|
| `Y` | has it |
| `P` | partial or limited |
| `B` | beta, preview, or off by default |
| `S` | announced or designed, not shipped |
| `N` | does not have it |
| `?` | not known (no public source) |

Tally values:

- Sides take colours 1 to 5 in the order of the Tally line. Neutral words
  (`Even`, `Neither`, `Tie`, `Same`, `None`, `Equal`) are grey; a second
  neutral word is light grey.
- Matching is on the first words of the cell, case-insensitive, whole words,
  longest value first. `Acme, two formats against one` counts as `Acme`.
- A cell that starts with none of the values is listed as `unmatched` and the
  build exits 1. Add the word to the Tally line, or reword the cell.
- Several comparison tables add up into one tally.
- The page header shows the tally with the sentence "The count gives the
  number of rows, not their weight." Say in the bottom line what the count
  does not show (for example that one row decides the choice).

## Status column

In any table, a column whose header is `Status` gets a coloured badge on the
first words of each cell:

| Colour | First words |
|---|---|
| green | Shipped, Generally available, GA, Available, Released, Live, Yes, Done, Merged |
| teal | Beta, Preview, Alpha, Early access, Flag-off, Gated, Partial, Partly, Limited, Prototype, In progress |
| pink | Announced, Designed, Planned, Roadmap, Coming |
| red | Not found, Not available, Not supported, Absent, Removed, Deprecated, None, No |
| dashed | Inferred, Unverified, Unknown, Unclear |

A cell that starts with another word stays plain. Bold (`**Shipped**`) works.

## Flags in text

`(inferred)`, `(inferred, <why>)`, `(unverified)` and `(unverified, <why>)`
anywhere in the text (not in code) become badges. Use them in REPORT.md and in
the notes.

## Images

- `![Caption that says what the image shows.](img/<name>.png)` on its own line
  becomes a figure with the alt text as its caption. Click to zoom.
- Only files under `img/` are embedded: PNG, JPEG, GIF and WebP as a JPEG of at
  most 1500 px (quality 72; transparent parts on white), SVG as is.
- Any other image (a URL, a path outside `img/`, a missing file) is replaced
  by a red "[image not embedded: ...]" line and listed as `missing`; the build
  exits 1. The page never loads anything from the network.

## Sections by kind

**Comparison** (`assets/comparison_skeleton.md`): Bottom line; What each one
is; Comparison (the table); Where A leads; Where B leads; What people say;
Recommendation; Open questions; Sources.

**Survey of many items**: Bottom line; Scope and method (what is in, what is
out, the window, the criteria); Overview (one row per item, with a `Status`
column or tokens, and a link to its note); Patterns (what most items do, what
only a few do); Decisions for the person; Open questions; Sources. One note
per item.

**Technology choice**: Bottom line (the pick, and when it is the wrong pick);
Options; Criteria with weights (say who set them); Comparison (a table per
criterion group, tally column `Best fit`); Risks and exit cost;
Recommendation; Open questions; Sources.

**Market scan or "how do others solve X"**: Bottom line; The problem; How
each group solves it (a table: approach, who, stage, source); What people say;
What to take, and what not to copy; Open questions; Sources.

**Recurring report** (the same topic each week or month): add `## What
changed since <date of the last report>` after the bottom line, with a link
to the earlier file name. Keep the earlier folder; make a new folder per run.

## Output line and exit codes

```
wrote acme-vs-globex.html: 102 KiB; parts 3; images 1; toc 16; tally {'Acme': 2, 'Globex': 2, 'Split': 1, 'Even': 2, 'Neither': 1}; missing []; unmatched []
```

- `parts`: Part 1 plus one per note. `toc`: sidebar entries.
- `tally none`: there is no Tally line.
- Exit 0: nothing to fix. Exit 1: the page is written, but read the `CHECK:`
  lines (images not embedded, unmatched tally cells, or no table with the
  Tally column). Exit 2: wrong arguments, no REPORT.md, or no title on line 1.

## Colours

Light and dark follow the reader's system setting; print is always light. The
five side colours and the greys come from a categorical palette that passes
colour-vision checks in both themes for adjacent blocks. Every coloured badge
also carries its word, so colour never carries the meaning alone. The styles
are in `scripts/report.css`, next to the build script.
