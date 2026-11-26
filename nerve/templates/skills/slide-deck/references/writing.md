# Slide text and the text check

## Rules for the text

- Few words on a slide. A headline of one or two lines with one idea. Points
  of a few words. No paragraph under a diagram or a mockup.
- The detail goes into the notes: what to say, the source and date of each
  number, the click cues ("Click 1: the second point"), and the `[CHECK]`
  items.
- Do not make up quotes, testimonials, people, customers, numbers, logos or
  screenshots. Put a visible placeholder (`<span class="tbd">`) or a TODO slide
  and ask the person for the fact.
- Ask before you quote a real person or show a real person's or team's
  numbers on a slide, also from a public source.
- Use the names the person chose (for a product, a feature, a team) the same
  way on every slide and in the notes. Write each naming ruling in
  `HANDOFF.md` with the date. Do not reopen a ruling that `HANDOFF.md` lists.
- When the person reframes the deck (for example from a status report to a
  plan), change every place: the title slide, the footer, the section
  headers, the closing slide and the notes.
- Plain words, active voice, short sentences. No em dash.

## Patterns of generic machine-made writing

Machine-made slide text has patterns that people notice. Remove them from
the slides and the notes. `text-check.py` finds most of them; it prints
candidates, and you decide.

| Pattern | Example | Do this |
|---|---|---|
| Filler opener | "In today's fast-moving world", "Let's dive in", "It's worth noting that" | Delete it. Start with the fact. |
| Empty intensifier or buzzword | "truly", "seamless", "robust", "unlock", "leverage", "game-changing" | Delete it, or put the fact it stands for: "exports in 2 s" instead of "seamless export". |
| "Not just X, but Y" | "Not just a tool, but a partner" | Say Y. |
| Reflex rule of three | "Fast, simple, and secure" on every slide; three points everywhere | Keep the items that are true and matter. Two or four are fine. |
| Emoji as bullets | "🚀 Faster builds" | Delete the emoji. Use the point style of the preset. |
| A bold word in every line | "**Fast**: ...", "**Simple**: ..." | Bold nothing, or one word on the slide. |
| Question as a headline | "Ready to transform reporting?" | Write the answer as the headline. |
| Exclamation marks | "We shipped it!" | Delete them. |
| Dash for drama | an em dash (U+2014) before a punch line | Two sentences, or a colon. |
| Long headline | more than 12 words | Cut to the idea; move the rest to the notes. |
| Dense slide | more than 40 words outside mockups | Move detail to the notes, or split the slide. |
| Vague claim | "significantly faster", "many customers" | The number with its source, or a `[CHECK]`. |

The script also lists open items: a `.tbd` placeholder, `[CHECK]`, `[DECIDE]`
or `TODO` left in a slide, and slides with no notes. Before the person
presents, every open item is gone or the person knows about it.

## Run the check

```bash
cd ~/.local/share/decks/<slug>
python3 -I text-check.py                # deck.src.html, or src/*.html
python3 -I text-check.py other.html     # another file
```

Output, grouped by slide number and title:

```
2. Three points
   open: placeholder, [CHECK], [DECIDE] or TODO left
4. One number
   open: placeholder, [CHECK], [DECIDE] or TODO left
5 slides, 2 candidates
```

When the person asks to "clean up the wording" or "make it less wordy", run the
check, fix each candidate by title, rebuild, and report what changed by title.
