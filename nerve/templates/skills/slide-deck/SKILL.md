---
name: Slide Deck
description: >
  This skill should be used when the person asks for a slide deck or a change
  to one: "build a deck for X", "make slides for the team meeting", "add a slide
  on Y", "cut slide 3", "move this to the speaker notes", "clean up the wording
  of the deck", "a PDF of the deck", "export it to PowerPoint / Google Slides /
  PPTX". It covers the content draft, one offline HTML deck with click steps and
  speaker notes, edit rounds by slide title, picture checks, PDF and PPTX export,
  and delivery. Not for one chart or diagram (diagrams-and-charts), a report
  (research-report) or a one-page dashboard (shareable-html).
version: 1.0.0
---

# Slide deck

Make a deck as **one self-contained HTML file**. It works offline, from disk.
It has click steps, speaker notes, an overview, and print to PDF. A **PPTX
export** gives an editable start in PowerPoint or Google Slides. The work comes
in short rounds in chat: edit the source after a backup, rebuild, look at the
pictures, deliver.

When the person must edit in PowerPoint with a company template from the
start, build a native PPTX with python-pptx instead: see "Native PPTX" in
`references/pptx.md`.

## Files of this skill

`<workspace>/skills/slide-deck/` holds:

| File | What it does |
|---|---|
| `assets/deck.src.html` | Starter deck: markup, presets, runtime (keys, notes, overview, print) |
| `assets/DRAFT.md`, `assets/HANDOFF.md` | Templates for the content draft and the state for the next session |
| `scripts/build.py` | Source to one `deck.html` with fonts and pictures inside |
| `scripts/shoot.mjs` | Pictures of slides and a layout check |
| `scripts/pdf.mjs` | `deck.pdf` plus overview and notes pictures |
| `scripts/text-check.py` | Finds generic machine-made writing and open items |
| `scripts/edit_lib.py` | Helpers for scripted edits by slide title |
| `scripts/pptx-export.mjs`, `scripts/pptx-build.py` | PPTX export, two steps |
| `scripts/pptx-preview.sh` | Optional: the PPTX as LibreOffice draws it (Docker) |
| `references/look.md` | Presets, fonts, canvas size, components, mockups |
| `references/writing.md` | Text rules and the list of patterns to remove |
| `references/pptx.md` | PPTX settings and losses, LibreOffice preview, native PPTX |

Run the scripts with Bash, not with `skill_run_script` (30 s limit).

## Step 1: content draft before HTML

For a new deck, write `DRAFT.md` first (template: `assets/DRAFT.md`):

- A run of show: block, slides, minutes, presenter. Add up the minutes to the slot.
- One section per slide: headline, a few points, notes.
- Status marks for claims: LIVE, BUILT, IN PROGRESS, PLANNED, `[CHECK]`
  (confirm before the talk), `[DECIDE]` (the person's call).
- A source and a date for each number: the query, the report, the page.
  Check each claim about a product or a project against its source.
- Before a rewrite, keep the old draft as `DRAFT.v1.md`, `DRAFT.v2.md`.

Show the draft to the person and get their OK on the structure before you
write slides. When a request is not clear, pick a sensible default, do it,
and say what you chose and what the other option is.

## Step 2: deck folder and kit

One folder per deck, outside the workspace: `~/.local/share/decks/<slug>/`.
Remember the folder and the deck name, so a later session finds them.

```bash
K=<workspace>/skills/slide-deck
D=~/.local/share/decks/<slug>
mkdir -p "$D/edits" && cd "$D"
cp "$K/assets/deck.src.html" "$K/assets/DRAFT.md" "$K/assets/HANDOFF.md" .
cp "$K"/scripts/* . && mv edit_lib.py edits/lib.py
```

The folder then holds: `deck.src.html` (the source, or `src/*.html`), the
scripts, `edits/`, `DRAFT.md`, `HANDOFF.md`, and later `fonts/`, `images/`,
backups `deck.src.before-<change>.html`, `shots/`, `review/`, `pptx/`, and the
outputs `deck.html`, `deck.pdf`, `deck.pptx`. The outputs keep these names;
give them the person's name only when you deliver.

**Browser setup** (once per machine, shared with other skills):

```bash
B=${NERVE_BROWSER_DIR:-$HOME/.local/share/nerve-browser}
mkdir -p "$B" && cd "$B"
[ -f package.json ] || npm init -y >/dev/null
npm install --no-audit --no-fund playwright
npx playwright install chromium
# Linux without the browser's libraries: as root, or with sudo when the person allows it
npx playwright install-deps chromium
# Bare Ubuntu 24.04: bold text renders thin until a font with a bold face is there (as root)
apt-get install -y fonts-dejavu-core
```

The `.mjs` scripts load Playwright from that folder. Symptoms:
`playwright is not in ...`: do the setup. `Executable doesn't exist at
.../chromium_headless_shell-<rev>`: run `npx playwright install chromium` again
in the tools folder. `error while loading shared libraries`: `install-deps`
(needs root). When the proxy refuses the browser download, tell the person the
host from the error. The `browser-testing` skill has more on this setup.
On a bare Ubuntu 24.04, `fc-match sans-serif` gives "WenQuanYi Zen Hei",
which has no bold face: headlines look thin in the pictures and the PDF.
`fonts-dejavu-core` fixes that; an embedded font (FACES) also does for the
deck's own text.

## Step 3: write the slides

Markup contract. The runtime and the scripts depend on it:

- `<section class="slide" data-title="...">`: one slide. The title is the
  name for edits; keep titles unique.
- `data-step="N"`: shows from click N. `data-until="N"`: hides from click N.
  Use both in a `dk-stack` to swap one state for another.
- `<aside class="notes"><p>...</p></aside>`: speaker notes.
- Class `h`: the headline. It goes into the title placeholder of the PPTX.
- Class `mock`: a mockup. It becomes one picture in the PPTX.
- `<span class="tbd">`: a placeholder to fill, with a dotted underline.
- `data-footer` on `#deck`: the footer text, for example
  `Acme · Quarter review · Internal`.
- Keys: arrows, Space, PageDown and PageUp, Home, End, N notes, O overview,
  F full screen. The URL `#12.2` opens slide 12 after 2 clicks.

The look: `data-preset` on `<html>` is `plain` (default), `editorial`
(magazine: serif, paper colour) or `swiss` (grid: black, white, one red).
The canvas size is `--slide-w` and `--slide-h` in `:root`, and nowhere else.
No font file comes with the skill; the deck uses a system font stack until you
embed one. Read `references/look.md` for presets, fonts, the canvas and
mockups.

Text rules (detail in `references/writing.md`):

- Few words on a slide. Headlines of one or two lines. Detail in the notes.
- Notes hold what to say, the source of each number, the click cues, and the
  `[CHECK]` items.
- Do not make up quotes, people, numbers or screenshots. Put a visible
  placeholder. Mockup data is made up and looks made up (Acme, alice).
- Ask before you quote a real person or show a real person's numbers.
- Use the names the person chose, the same way everywhere, notes included.
- New component classes get a long prefix (`dk-card`), never a short name.

## Step 4: build and look

```bash
cd ~/.local/share/decks/<slug>
python3 -I build.py                       # deck.html, with fonts and pictures inside
node shoot.mjs                            # shots/sNN.png, every slide at its last click
W=1280 OUT=review node shoot.mjs 4,8      # smaller pictures of slides 4 and 8 in review/
node shoot.mjs 3.0,3.1                    # slide 3 before and after click 1
```

`build.py` stops on a missing local file and warns about remote addresses
(they do not load offline). `shoot.mjs` prints `errors`, `remote requests` and
`layout findings` (content outside the safe area, into the footer, or
clipped). All three must be `[]`. Then open the pictures with the Read tool
and LOOK at them. An overlap, a clash of colours or a bad line break shows
only in the picture.

## Step 5: text check

```bash
python3 -I text-check.py
```

It prints candidates by slide: filler openers, empty words, "not just X but
Y", lists of three, emoji bullets, a bold word in every line, question
headlines, dense slides, slides without notes, and open placeholders. Fix
what is generic; keep what is a fact. Run it before each delivery.

## Step 6: an edit round

1. **Name each item by slide title.** The person writes "slide 3" or `12.2`.
   Numbers shift after each removal or insertion, also inside one message.
   Map each number to its title with `python3 -I edits/lib.py` (it prints
   the list with click counts). When it is not clear, say which title you edited.
2. **Back up the source first:**
   `cp deck.src.html deck.src.before-<change>.html` (or
   `cp -r src src.before-<change>`). Name it for the change.
3. **Edit.** Use the Edit tool for a small change. For a change in many
   places, write a script in `edits/` (`a_rename.py`, `b_numbers.py`, ...):
   ```python
   import pathlib, sys; sys.path.insert(0, str(pathlib.Path(__file__).parent))
   from lib import load, save, edit, rep, replace_section, insert_after, remove_section
   s = load()
   s = edit(s, "One number", lambda b: rep(b, "What the number measures, in a few words", "Reports exported per week"))
   save(s)
   ```
   Run it with `python3 -I edits/a_rename.py`. `rep()` stops when the old
   text is missing or found more than once (`count=2` or `count=None` to
   allow more); `edit()` stops when the title is missing or repeated. Nothing
   is written then. This matters when two slides share markup. For a `src/`
   deck, set `lib.P` to the file.
4. **Rebuild, shoot the changed slides, look** (step 4), and run the text check.
5. **Deliver** (step 9). Report in a few lines: what changed, by title; what
   you chose where the request was not clear, and the other option.
6. **Update `HANDOFF.md`:** the slide list, new rulings with the date, and each
   removed slide with the backup that holds it.

If the person keeps a master copy elsewhere (for example their Google Slides
copy), ask which copy is the master before a round.

## Step 7: PDF

```bash
node pdf.mjs     # deck.pdf (one page per slide, every click shown), review/overview.png, review/notes.png
```

It prints the page count next to the slide count. A difference means a slide
is taller than the canvas.

## Step 8: PPTX for PowerPoint or Google Slides

Read `references/pptx.md` first. Then:

```bash
cd ~/.local/share/decks/<slug>
uv venv -q .venv && uv pip install -q --python .venv/bin/python python-pptx==1.0.2 pillow==13.0.0 fonttools==4.69.1 brotli==1.2.1   # once
node pptx-export.mjs && .venv/bin/python -I pptx-build.py   # pptx/items.json and pptx/img/, then deck.pptx
bash pptx-preview.sh                                          # optional, needs Docker: pptx-preview/sheet.png
```

Text stays editable; mockups become pictures; click steps become Appear and
Disappear animations; the notes keep the speaker notes and list the click
builds. Read the font lines the two scripts print. `warning: the headline
font is "X" in the CSS, but the browser drew "Y"` or `NAT ... (default: not
measured)` means the PPTX text will not fit its boxes: embed the font (see
`references/look.md`) and export again. Tell the person what the export
loses: sound and video, live text, CSS transitions, background pictures.

## Step 9: deliver

`send_file` accepts only files inside the workspace. Copy the output with the
round in the name, then call `send_file` with that path:

```bash
mkdir -p <workspace>/.uploads/decks
cp deck.html <workspace>/.uploads/decks/<name>-r3.html
cp deck.pdf <workspace>/.uploads/decks/<name>-r3.pdf      # when asked for
```

Use a new file name for each round. A chat card reads the file at its path
when the person clicks it, and the download has no cache header, so a reused
name shows the newest file on old cards or an old copy from the browser
cache. The person opens the HTML file in their own browser, from disk. The
web chat shows a download card, Telegram gets a document and Slack an uploaded
file. A channel without file delivery: tell the person to open the web panel.

**Preview in a browser.** The file is the preview. Serve the deck over HTTP
only with the person's word. On a hosted agent, read the `nerve-cloud` skill
for how to serve a port to the agent's members. Serve only a folder that holds
the built file, never the deck folder (it holds the source, the notes and
`HANDOFF.md`). On the person's own computer:

```bash
mkdir -p serve && cp deck.html serve/
python3 -m http.server 8765 --bind 127.0.0.1 --directory serve
```

Copy each new build into `serve/` and tell the person to reload with
Ctrl+Shift+R (the browser caches). Stop the server when the round is done.

## Confidentiality

A deck often names internal facts, plans, gaps and people. Put "Internal" in
the footer when it does. No secrets, tokens, customer data or personal data in
the slides, notes, mockups or file names. Never publish a deck to a public
place (a public repository, a public URL) without the person's word.

## Known problems

- Slide numbers shift after every removal or insertion: edit by title.
- Two slides with the same title: `edit()` stops. Rename one first.
- Short global class names clash: use long prefixes.
- A `data-step` wrapper with only absolutely positioned children has no size:
  the PPTX export skips it. Give it `position:absolute;inset:0`.
- A font in `fonts/` that is not first in the preset's stack does not show:
  put its family first.
- A picture shows a step half faded: it was taken during the 0.3 s fade. The
  kit scripts wait for it; a script of your own must wait too.
- A reused delivery file name shows old or new content on the wrong card: use a
  new name per round.
- PPTX headlines wrap and run into the next line: the font was not embedded,
  so the browser drew another font than the PPTX names. Embed it (FACES).
- The PPTX loses sound, video, live text and CSS transitions. The Google
  Slides and PowerPoint imports of the animations are not verified; the notes
  carry the list of builds.
- `pptx-preview.sh` needs Docker and, once, access to Docker Hub and the
  Ubuntu package archive.
- Bold text looks thin in pictures and the PDF on a bare Ubuntu: install
  `fonts-dejavu-core` as root (Browser setup in step 2).
