"""Finds patterns of generic machine-made writing on slides and in notes (slide-deck skill).

Run in the deck folder: python3 -I <skill>/scripts/text-check.py [deck.src.html]
Reads deck.src.html (or src/*.html) and prints candidates by slide title. Each
finding is a candidate, not a verdict: read the slide, then rewrite or keep it.
Standard library only.
"""
import html.parser
import pathlib
import re
import sys

# Phrases that open a sentence and say nothing.
FILLER = [
    "in today's", "in a world where", "imagine a world", "let's dive", "let's explore", "let's take a look",
    "it's important to note", "it is important to note", "it's worth noting", "it is worth noting",
    "at the end of the day", "when it comes to", "needless to say", "in conclusion", "in summary",
    "simply put", "here's the thing", "the bottom line is", "without further ado", "buckle up",
]
# Words that add force or shine but no fact.
EMPTY = [
    "truly", "incredibly", "extremely", "seamless", "seamlessly", "robust", "cutting-edge", "state-of-the-art",
    "game-changer", "game-changing", "revolutionize", "revolutionary", "unlock", "unleash", "supercharge",
    "empower", "elevate", "leverage", "delve", "harness", "synergy", "holistic", "best-in-class", "world-class",
    "next-level", "transformative", "paradigm", "landscape", "tapestry", "realm", "plethora", "myriad",
    "ever-evolving", "pivotal", "crucial", "vital", "foster", "journey", "effortless", "effortlessly",
]
CONSTRUCTIONS = [
    (re.compile(r"\bnot (?:just|only|merely)\b[^.?!]{1,60}?\bbut\b", re.I), "'not just X, but Y'"),
    (re.compile(r"\bmore than just\b", re.I), "'more than just'"),
    (re.compile(r"\b(\w+), (\w+),? and (\w+)\b"), "list of three (reflex rule of three?)"),
    (re.compile("\u2014"), "em dash"),
    (re.compile("\\s\u2013\\s"), "en dash used as a dash"),
    (re.compile(r"!"), "exclamation mark"),
]
EMOJI = re.compile("[\U0001F300-\U0001FAFF\u2600-\u27BF\u2B50\u2B55\u2705\u274C]")
MAX_SLIDE_WORDS = 40   # words on the slide, mockups left out
MAX_HEAD_WORDS = 12

SEC = re.compile(r'<section class="slide[^"]*"[^>]*\bdata-title="([^"]*)"[^>]*>(.*?)</section>', re.S)
BLOCK = {"p", "div", "li", "h1", "h2", "h3", "h4", "td", "th", "figcaption", "blockquote"}


class Slide(html.parser.HTMLParser):
    """Splits one slide into its blocks of text: slide text, headline, notes; mockups apart."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []      # (tag, classes)
        self.blocks = []     # [where, text, has_bold]
        self.cur = None

    def where(self):
        cls = [c for _, cl in self.stack for c in cl]
        if "notes" in cls:
            return "notes"
        if "mock" in cls:
            return "mock"
        if "h" in cls:
            return "head"
        return "slide"

    def handle_starttag(self, tag, attrs):
        if tag in ("br", "img", "hr", "meta", "input"):
            return
        cl = (dict(attrs).get("class") or "").split()
        self.stack.append((tag, cl))
        if tag in BLOCK or "h" in cl:
            self.cur = [self.where(), "", False]
            self.blocks.append(self.cur)
        if tag in ("b", "strong") and self.cur:
            self.cur[2] = True

    def handle_endtag(self, tag):
        while self.stack:
            t, _ = self.stack.pop()
            if t == tag:
                break
        if tag in BLOCK:
            self.cur = None

    def handle_data(self, data):
        if not data.strip():
            return
        if self.cur is None:
            self.cur = [self.where(), "", False]
            self.blocks.append(self.cur)
        self.cur[1] += data


def words(t):
    return re.findall(r"[\w'%.-]+", t)


def check(title, body):
    p = Slide()
    p.feed(body)
    out = []
    blocks = [(w, re.sub(r"\s+", " ", t).strip(), b) for w, t, b in p.blocks if t.strip()]
    for where, text, _ in blocks:
        if where == "mock":
            continue
        low = text.lower()
        for f in FILLER:
            if f in low:
                out.append(f"{where}: filler opener '{f}': {text[:70]}")
        for e in EMPTY:
            if re.search(r"\b" + re.escape(e) + r"\b", low):
                out.append(f"{where}: empty word '{e}': {text[:70]}")
        for rx, name in CONSTRUCTIONS:
            if rx.search(text):
                out.append(f"{where}: {name}: {text[:70]}")
        if EMOJI.match(text):
            out.append(f"{where}: emoji as a bullet: {text[:70]}")
        if where == "head" and len(words(text)) > MAX_HEAD_WORDS:
            out.append(f"head: {len(words(text))} words in the headline (more than {MAX_HEAD_WORDS})")
        if where == "head" and text.endswith("?"):
            out.append(f"head: question as a headline: {text[:70]}")
    on_slide = [b for b in blocks if b[0] in ("slide", "head")]
    n = sum(len(words(t)) for _, t, _ in on_slide)
    if n > MAX_SLIDE_WORDS:
        out.append(f"slide: {n} words on the slide (more than {MAX_SLIDE_WORDS}): move detail to the notes")
    bold = [b for b in on_slide if b[0] == "slide"]
    if len(bold) >= 3 and all(b[2] for b in bold):
        out.append("slide: a bold word in every line")
    if not any(w == "notes" for w, _, _ in blocks):
        out.append("notes: none")
    if re.search(r'class="[^"]*\btbd\b', body) or re.search(r"\[(CHECK|DECIDE)\]|\bTODO\b", body):
        out.append("open: placeholder, [CHECK], [DECIDE] or TODO left")
    return out


def main():
    d = pathlib.Path.cwd()
    if len(sys.argv) > 1:
        src = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
    elif (d / "src").is_dir():
        src = "".join(p.read_text(encoding="utf-8") for p in sorted((d / "src").glob("*.html")))
    else:
        src = (d / "deck.src.html").read_text(encoding="utf-8")
    slides = SEC.findall(src)
    total = 0
    for i, (title, body) in enumerate(slides, 1):
        found = check(title, body)
        total += len(found)
        if found:
            print(f"{i}. {title}")
            for f in found:
                print("   " + f)
    three = sum(1 for _, b in slides if len(re.findall(r'class="[^"]*\bdk-point\b', b)) == 3)
    if len(slides) >= 4 and three > len(slides) / 2:
        print(f"deck: {three} of {len(slides)} slides have exactly three points (reflex rule of three?)")
        total += 1
    print(f"{len(slides)} slides, {total} candidates")


main()
