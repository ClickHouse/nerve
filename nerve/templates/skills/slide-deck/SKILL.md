---
name: Slide Deck
description: >
  This skill should be used when the person asks for a slide deck or a
  change to one: "build a deck for X", "add a slide on Y", "cut slide 3",
  "move this to the notes", "a PDF of the deck", "export it to PowerPoint".
  It covers the draft, one offline HTML deck with speaker notes, edit
  rounds, picture checks, PDF, PPTX and delivery. Not for one chart
  (diagrams-and-charts), a report (research-report) or a dashboard
  (shareable-html).
version: 1.0.0
---

# Slide deck

Make the deck as one self-contained HTML file that opens from disk and works
offline. Work in a folder outside the workspace, `~/.local/share/decks/<slug>/`,
and keep the source, its backups and a `HANDOFF.md` there.

## The deck file

- A 16:9 canvas (for example 1920x1080) scaled to the window. Keep the size in
  one CSS variable.
- Keys: arrows, Space and PageDown, Home, End; N shows the speaker notes, O an
  overview. A URL such as `#12.2` opens slide 12 after 2 clicks.
- Click steps on an element (`data-step="2"`), speaker notes in an
  `<aside class="notes">`, and a unique `data-title` on each slide.
- Print styles: one slide per page, every click step shown.
- Nothing from the network: fonts and pictures inside as data URIs, else a
  system font stack. A CDN font or script does not load offline.
- Give each component class a long prefix (`dk-card`). Short global class
  names clash as the deck grows.

## The look

- When the person has a company template or brand, follow it. Else choose
  one look and keep it on every slide: plain (a sans font, white, one accent
  colour), editorial (a serif headline font, a paper-coloured background,
  wide margins) or a Swiss grid (strict columns, black and white, one strong
  accent).
- One idea per slide, and show it: a diagram, a mockup, one big number with
  its unit and date, or a short list. A slide of bullets only usually belongs
  in the notes.
- One alignment grid and one type scale for the whole deck. Text that reads
  from the back of a room: on a 1920x1080 canvas, body text of 24 px or more.
- Mockups of real tools (a chat, an issue, a dashboard) in HTML and CSS: they
  look real and carry invented data, and they stay sharp at any size.
- For a talk, plan about one minute per slide.

## Step 1: content draft first

For a new deck, write `DRAFT.md` before any HTML: the run of show (block,
minutes, presenter, adding up to the slot), and for each slide a headline, a
few points and the notes. Give each claim about a product or project its
status (live, built, in progress, planned) and each number its source and
date. Mark open items `[CHECK]` and the person's decisions `[DECIDE]`. Get the
person's OK on the structure before you write slides.

## Step 2: text rules

- Few words on a slide; headlines of one or two lines; the detail goes into the
  notes. Notes hold what to say, the source of each number and the click cues.
- Never make up quotes, people, numbers or screenshots: put a visible
  placeholder. Mockup data is invented and looks invented (Acme, alice).
- Ask before you quote a real person or show a real person's numbers.
- Use the names the person chose, the same way everywhere, notes included.
- Remove generic machine-made text: filler openers, empty intensifiers, a list
  of three by reflex, "not just X but Y", emoji bullets, a bold word in every
  line, question headlines.
- When a request is not clear, pick a sensible default, do it, and say what
  you chose and what the other option was.

## Step 3: look at it

Take a screenshot of each changed slide at its last click with headless
Chromium (setup: the `browser-testing` skill), and look at each picture with
the Read tool. Check for text outside the slide, text in the footer, overlaps,
console errors and requests to the network. A clash shows only in the
picture. A screenshot taken during a fade catches a step half shown: wait for
the transition to end.

## Step 4: edit rounds

1. Name each change by slide title. The person writes "slide 3", and the
   numbers shift after each removal or insertion, also inside one message.
   Map numbers to titles first, and say which title you changed.
2. Back up the source before each round:
   `cp deck.src.html deck.src.before-<change>.html`.
3. Edit, rebuild, look (step 3).
4. Report in a few lines: what changed, by title, and the defaults you chose.
5. Update `HANDOFF.md`: the slide list, the person's decisions with dates, and
   removed slides with the backup that holds them. Read it first in a later
   session, and do not reopen a decision that it lists.

If the person keeps a master copy elsewhere (Google Slides), ask which copy is
the master before a round.

## PDF and PPTX

- PDF: print the deck with Chromium (`page.pdf`), one page per slide with
  every click step shown. The page count must equal the slide count.
- PPTX, only when asked: build it with python-pptx in a venv in the deck
  folder. Keep text as editable text with the headline in the title
  placeholder, and turn mockups into pictures. Tell the person what is lost:
  sound, CSS transitions, live text, and the fonts when PowerPoint or Google
  Slides does not have them (text widths then change).
- When the person must edit in PowerPoint with a company template from the
  start, build a native PPTX on that template instead of the HTML deck.

## Deliver

`send_file` accepts only files in the workspace. Copy the build to
`<workspace>/.uploads/decks/<name>-r<round>.html` (and `.pdf` or `.pptx` when
asked), then call `send_file` with that path. Use a new name for each round: a
chat card reads the file at its path when the person clicks it, and the
download has no cache header, so a reused name shows the wrong version. The
web chat shows a download card, Telegram gets a document and Slack an uploaded
file. The person opens the HTML from disk in their own browser.

Serve the deck over HTTP only with the person's word, and serve a folder that
holds only the built file, never the deck folder (it has the source and the
notes). On a hosted agent, the `nerve-cloud` skill says how to serve a port to
the agent's members.

## Confidentiality

Decks often name internal plans, gaps and people. Put "Internal" in the footer
when they do. No secrets, customer data or personal data in slides, notes or
file names. Never publish a deck to a public place without the person's word.
