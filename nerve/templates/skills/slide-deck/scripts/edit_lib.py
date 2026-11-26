"""Helpers for scripted edits of a deck by slide title (slide-deck skill).

Copy this file to <deck folder>/edits/lib.py. An edit script next to it:

    import pathlib, sys; sys.path.insert(0, str(pathlib.Path(__file__).parent))
    from lib import load, save, edit, rep, replace_section, insert_after, remove_section
    s = load()
    s = edit(s, "Three points", lambda b: rep(b, "old text", "new text"))
    save(s)

Run it with: python3 -I edits/a_change.py
Every helper stops with an error when a title or a text is not found exactly as
often as expected, so a wrong edit never writes the file.
List the slides: python3 -I edits/lib.py
"""
import pathlib
import re
import sys

DECK = pathlib.Path(__file__).resolve().parent.parent
P = DECK / "deck.src.html"  # for a source split into src/*.html, set lib.P to the file


def load():
    return P.read_text(encoding="utf-8")


def save(s):
    P.write_text(s, encoding="utf-8")


# A slide: <section class="slide ..." ... data-title="..."> ... </section>. No <section> inside a slide.
SEC = re.compile(r'(<section class="slide[^"]*"[^>]*\bdata-title="([^"]*)"[^>]*>.*?</section>)', re.S)


def sections(s):
    return [(m.start(1), m.end(1), m.group(2)) for m in SEC.finditer(s)]


def get(s, title):
    hits = [x for x in sections(s) if x[2] == title]
    if len(hits) != 1:
        raise SystemExit(f"slide title {title!r}: found {len(hits)} times, expected 1")
    a, b, _ = hits[0]
    return a, b


def edit(s, title, fn):
    """Applies fn to the HTML of one slide, found by its data-title."""
    a, b = get(s, title)
    return s[:a] + fn(s[a:b]) + s[b:]


def rep(block, old, new, count=1):
    """Replaces old with new. Stops unless old occurs exactly count times (count=None: at least once)."""
    n = block.count(old)
    if n == 0:
        raise SystemExit(f"text not found: {old[:80]!r}")
    if count is not None and n != count:
        raise SystemExit(f"text found {n} times, expected {count}: {old[:80]!r}")
    return block.replace(old, new)


def replace_section(s, title, html):
    a, b = get(s, title)
    return s[:a] + html.strip() + s[b:]


def insert_after(s, title, html):
    _, b = get(s, title)
    return s[:b] + "\n\n" + html.strip() + s[b:]


def remove_section(s, title):
    a, b = get(s, title)
    while b < len(s) and s[b] in " \t\r\n":
        b += 1
    return s[:a] + s[b:]


if __name__ == "__main__":
    src = load()
    rows = sections(src)
    for i, (a, b, t) in enumerate(rows, 1):
        steps = [int(x) for x in re.findall(r'data-(?:step|until)="(\d+)"', src[a:b])]
        print(f"{i:3}  {t}" + (f"  (clicks: {max(steps)})" if steps else ""))
    dup = sorted({t for _, _, t in rows if [r[2] for r in rows].count(t) > 1})
    if dup:
        print("duplicate titles (rename them before a scripted edit):", dup)
        sys.exit(1)
