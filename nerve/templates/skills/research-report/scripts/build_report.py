"""Build one self-contained HTML page from a research report folder.

Usage:  <venv>/bin/python -I build_report.py <report-dir> <out.html>

Part 1 is <report-dir>/REPORT.md. Each file in <report-dir>/notes/ is a later
part, in the order in which REPORT.md names them (`notes/<name>.md`), then the
rest by name. Images under <report-dir>/img/ are embedded. The forms this script
reads (head lines, comparison table, Status column, flags) are documented in
references/formats.md of the research-report skill.

Exit 0: page written, nothing to fix. Exit 1: page written, read the CHECK lines.
Exit 2: wrong arguments, or REPORT.md is missing or has no "# Title" on line 1.
Needs: markdown-it-py, mdit-py-plugins, linkify-it-py, pillow.
"""

import base64
import html
import io
import re
import sys
from datetime import datetime
from pathlib import Path

from markdown_it import MarkdownIt
from mdit_py_plugins.anchors import anchors_plugin
from PIL import Image

if len(sys.argv) != 3:
    sys.exit("usage: build_report.py <report-dir> <out.html>")
ROOT, OUT = Path(sys.argv[1]).resolve(), Path(sys.argv[2])
if not (ROOT / "REPORT.md").is_file():
    print(f"no REPORT.md in {ROOT}", file=sys.stderr)
    sys.exit(2)

NEUTRAL = {"even", "neither", "tie", "same", "none", "equal"}
TOKENS = {"Y": "has it", "P": "partial or limited", "B": "beta, preview or off by default",
          "S": "announced or designed, not shipped", "N": "does not have it", "?": "not known"}
TOKEN_RX = re.compile(r"^(Y|P|B|S|N|\?)(?=[\s(,.;:]|$)")
STATUS = [  # first words of a cell in a column named "Status"
    ("ok", "Shipped", r"shipped|generally available|ga|available|released|live|yes|done|merged"),
    ("beta", "Beta", r"beta|preview|alpha|early access|flag-off|gated|partial|partly|limited|prototype|in progress"),
    ("soon", "Announced", r"announced|designed|planned|roadmap|coming"),
    ("no", "Not found", r"not found|not available|not supported|absent|removed|deprecated|none|no"),
    ("inf", "Inferred", r"inferred|unverified|unknown|unclear"),
]
STATUS_RX = [(c, re.compile(rf"(?i)^({w})\b")) for c, _, w in STATUS]
FLAG_RX = re.compile(r"\((inferred|unverified)([,;][^)]*)?\)")

used = set()        # legend boxes this page needs
missing = []        # images that are not embedded
unmatched = []      # comparison rows whose tally cell names no tally value
cache = {}


# ------------------------------------------------------------------ markdown
def render(md_text, prefix, levels):
    md = MarkdownIt("commonmark", {"html": False, "linkify": True, "typographer": False})
    md.enable(["table", "linkify", "strikethrough"])

    def slug(s):
        s = re.sub(r"[^\w\s-]", "", s.lower()).strip()
        return prefix + re.sub(r"[\s_]+", "-", s)
    md.use(anchors_plugin, min_level=2, max_level=3, slug_func=slug)
    tokens = md.parse(md_text)
    toc = [(int(t.tag[1]), t.attrGet("id"), tokens[i + 1].content)
           for i, t in enumerate(tokens) if t.type == "heading_open" and int(t.tag[1]) in levels]
    body = md.renderer.render(tokens, md.options, {})
    body = re.sub(r'<a href="(https?://[^"]+)"', r'<a href="\1" target="_blank" rel="noopener noreferrer"', body)
    return body, toc


def text_of(cell_html):
    return html.unescape(re.sub(r"<[^>]+>", "", cell_html)).strip()


def edit_tables(body, fn):
    def one(m):
        return '<div class="table-wrap">' + fn(m.group(0)) + "</div>"
    return re.sub(r"<table>.*?</table>", one, body, flags=re.S)


def header(table):
    head = re.search(r"<thead>(.*?)</thead>", table, re.S)
    return [text_of(c) for c in re.findall(r"<th[^>]*>(.*?)</th>", head.group(1), re.S)] if head else []


def edit_rows(table, fn):
    """fn(col, cell_html, texts) -> cell_html for each <td>; marks group rows."""
    def row(m):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", m.group(0), re.S)
        texts = [text_of(c) for c in cells]
        idx = iter(range(len(cells)))
        new = re.sub(r"(<td[^>]*>)(.*?)</td>", lambda c: c.group(1) + fn(next(idx), c.group(2), texts) + "</td>",
                     m.group(0), flags=re.S)
        if texts and texts[0] and not any(texts[1:]):
            new = new.replace("<tr>", '<tr class="group">', 1)
        return new
    head, sep, rest = table.partition("<tbody>")
    return head + sep + re.sub(r"<tr>.*?</tr>", row, rest, flags=re.S)


# ------------------------------------------------------------------ head lines
lines = (ROOT / "REPORT.md").read_text(encoding="utf-8").splitlines()
if not lines or not lines[0].startswith("# "):
    print("REPORT.md line 1 must be '# <Title>'", file=sys.stderr)
    sys.exit(2)
TITLE = lines[0][2:].strip()
chips, tally_col, tally_values = [], "", []
i = 1
while i < len(lines) and (not lines[i].strip() or re.match(r"^(Date|Tally):", lines[i])):
    key, _, value = lines[i].partition(":")
    if key == "Date":
        parts = [p.strip() for p in re.split(r"\.(?:\s+|$)", value.strip()) if p.strip()]
        chips = ["Date: " + parts[0]] + parts[1:] if parts else []
    elif key == "Tally":
        tally_col, _, vals = value.partition("=")
        tally_col = tally_col.strip()
        tally_values = [v.strip() for v in vals.split(",") if v.strip()]
    i += 1
report_md = "\n".join(lines[i:])

# Tally value -> CSS class: sides take c1..c5 in order; the first neutral word
# is grey (cn), later ones light grey (cn2).
klass, n = {}, 0
for v in tally_values:
    if v.lower() in NEUTRAL:
        klass[v] = "cn2" if "cn" in klass.values() else "cn"
    else:
        n += 1
        klass[v] = f"c{min(n, 5)}"
tally = {v: 0 for v in tally_values}


def tally_value(text):
    for v in sorted(tally_values, key=len, reverse=True):
        if re.match(rf"(?i){re.escape(v)}(?!\w)", text):
            return v
    return None


def comparison(table):
    cols = [c.lower() for c in header(table)]
    if not tally_col or tally_col.lower() not in cols:
        return table
    used.add("compare")
    ti = cols.index(tally_col.lower())

    def th(m):
        v = tally_value(text_of(m.group(2)))
        return f'<th class="col {klass[v]}"{m.group(1)}>{m.group(2)}</th>' if v and klass[v][1] != "n" else m.group(0)
    head, sep, rest = table.partition("<tbody>")
    head = re.sub(r"<th([^>]*)>(.*?)</th>", th, head, flags=re.S)

    def cell(col, content, texts):
        if not any(texts[1:]):
            return content
        if col == ti:
            v = tally_value(texts[col])
            if v is None:
                unmatched.append(texts[0] or texts[col][:40])
                return content
            tally[v] += 1
            return f'<span class="edge {klass[v]}">{content}</span>'
        if col > 0:
            return TOKEN_RX.sub(lambda t: f'<span class="st st-{"u" if t.group(1) == "?" else t.group(1).lower()}">'
                                f"{t.group(1)}</span>", content, count=1)
        return content
    return edit_rows(head + sep + rest, cell)


def status_column(table):
    cols = [c.lower() for c in header(table)]
    if "status" not in cols:
        return table
    si = cols.index("status")

    def cell(col, content, texts):
        if col != si:
            return content
        lead = re.match(r"^(<strong>)?", content).group(0)
        rest = content[len(lead):]
        for cls, rx in STATUS_RX:
            m = rx.match(rest)
            if m:
                used.add("status")
                return lead + f'<span class="rs rs-{cls}">{m.group(1)}</span>' + rest[m.end():]
        return content
    return edit_rows(table, cell)


def flags(body):
    out, in_code = [], 0
    for part in re.split(r"(<[^>]+>)", body):
        if part.startswith("<"):
            in_code += 1 if re.match(r"<(code|pre)\b", part) else -1 if re.match(r"</(code|pre)>", part) else 0
            out.append(part)
        elif in_code:
            out.append(part)
        else:
            new = FLAG_RX.sub(lambda m: f'<span class="badge f-{m.group(1)}">{m.group(1)}{m.group(2) or ""}</span>', part)
            if new != part:
                used.add("flags")
            out.append(new)
    return "".join(out)


# ------------------------------------------------------------------ images
def embed(rel):
    path = (ROOT / rel).resolve()
    if rel not in cache:
        if path.suffix.lower() == ".svg":
            data, mime = path.read_bytes(), "image/svg+xml"
        else:
            im = Image.open(path)
            if im.mode in ("RGBA", "LA", "P"):
                im = im.convert("RGBA")
                bg = Image.new("RGB", im.size, "white")
                bg.paste(im, mask=im.getchannel("A"))
                im = bg
            im = im.convert("RGB")
            im.thumbnail((1500, 1500))
            buf = io.BytesIO()
            im.save(buf, "JPEG", quality=72, optimize=True)
            data, mime = buf.getvalue(), "image/jpeg"
        cache[rel] = f"data:{mime};base64," + base64.b64encode(data).decode()
    return cache[rel]


def figures(body):
    img_dir = (ROOT / "img").resolve()

    def fig(m):
        src, alt = html.unescape(m.group(1)), m.group(2)
        path = (ROOT / src).resolve()
        if not src.startswith("img/") or img_dir not in path.parents or not path.is_file():
            missing.append(src)
            return f'<span class="missing">[image not embedded: {alt or html.escape(src)}]</span>'
        return (f'<figure><img src="{embed(src)}" alt="{alt}" loading="lazy">'
                f"<figcaption>{alt}</figcaption></figure>")
    body = re.sub(r'<p><img src="([^"]+)" alt="([^"]*)"[^>]*></p>', fig, body)
    return re.sub(r'<img src="((?!data:)[^"]+)" alt="([^"]*)"[^>]*>', fig, body)


def finish(body):
    body = edit_tables(body, lambda t: status_column(comparison(t)))
    return figures(flags(body))


# ------------------------------------------------------------------ parts
note_files = {p.stem: p for p in sorted((ROOT / "notes").glob("*.md"))}
order = [n for n in dict.fromkeys(re.findall(r"notes/([\w.-]+)\.md", report_md)) if n in note_files]
order += [n for n in note_files if n not in order]

r_body, r_toc = render(report_md, "r-", (2, 3))
r_body = finish(r_body)
for name in order:
    r_body = r_body.replace(f"<code>notes/{name}.md</code>", f'<a href="#part-{name}"><code>notes/{name}.md</code></a>')
    r_body = r_body.replace(f'href="notes/{name}.md"', f'href="#part-{name}"')

parts = [("part-report", "Part 1", "Report", r_body, r_toc, True)]
for k, name in enumerate(order, start=2):
    n_lines = note_files[name].read_text(encoding="utf-8").splitlines() or [""]
    n_title = n_lines[0].lstrip("# ").strip() or name
    body, toc = render("\n".join(n_lines[1:]), f"{name}-", (2,))
    parts.append((f"part-{name}", f"Part {k}", n_title, finish(body), toc, False))


# ------------------------------------------------------------------ page
def esc(s):
    return html.escape(s, quote=True)


def toc_html():
    out = ['<nav class="toc" aria-label="Contents"><details open id="toc"><summary>Contents</summary>']
    for anchor, kicker, title, _, toc, is_open in parts:
        out.append(f'<details class="part"{" open" if is_open else ""}><summary>'
                   f'<a href="#{anchor}">{esc(kicker)}. {esc(title)}</a></summary><ul>')
        for level, hid, text in toc:
            out.append(f'<li class="l{level}"><a href="#{hid}">{esc(re.sub(r"[`*]", "", text))}</a></li>')
        out.append("</ul></details>")
    out.append("</details></nav>")
    return "".join(out)


def legend_html():
    boxes = []
    if "compare" in used:
        rows = "".join(f'<div><span class="st st-{"u" if t == "?" else t.lower()}">{esc(t)}</span> {esc(d)}</div>'
                       for t, d in TOKENS.items())
        boxes.append(f'<div class="box"><b>Comparison: status</b>{rows}</div>')
    if "status" in used or "flags" in used:
        rows = "".join(f'<div><span class="rs rs-{c}">{esc(label)}</span> and similar words</div>'
                       for c, label, _ in STATUS) if "status" in used else ""
        if "flags" in used:
            rows += ('<div><span class="badge f-inferred">inferred</span> read from the source, not tested</div>'
                     '<div><span class="badge f-unverified">unverified</span> not checked in a source</div>')
        boxes.append(f'<div class="box"><b>Status in tables and text</b>{rows}</div>')
    return (f'<details open id="legend"><summary>Legend</summary><div class="legend">{"".join(boxes)}</div></details>'
            if boxes else "")


def tally_html():
    total = sum(tally.values())
    if not total:
        return ""
    counts = " ".join(f'<span class="edge {klass[v]}">{esc(v)} {c}</span>' for v, c in tally.items())
    return (f'<p class="tally">{esc(tally_col)}, over {total} rows: {counts}. '
            "The count gives the number of rows, not their weight.</p>")


chip_html = "".join(f'<span class="chip{" internal" if re.match(r"(?i)internal|confidential", c) else ""}">'
                    f"{esc(c)}</span>" for c in chips)
parts_html = "".join(f'<div class="part-head" id="{a}"><div class="kicker">{esc(k)}</div><h1>{esc(t)}</h1></div>'
                     f'<section class="content">{b}</section>' for a, k, t, b, _, _ in parts)
CSS = (Path(__file__).with_name("report.css")).read_text(encoding="utf-8")
page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>{esc(TITLE)}</title>
<style>{CSS}</style>
</head>
<body id="top">
<div class="layout">
{toc_html()}
<main>
<header class="hero">
<h1>{esc(TITLE)}</h1>
<div class="chips">{chip_html}</div>
{legend_html()}
{tally_html()}
</header>
{parts_html}
<footer>Built {datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")} from REPORT.md and {len(order)} notes.
Every claim has its source in the text; see the sources section of Part 1.</footer>
</main>
</div>
<div class="zoom" id="zoom"><img alt=""></div>
<a class="top" href="#top">Top</a>
<script>
const z = document.getElementById('zoom'), zi = z.querySelector('img');
document.querySelectorAll('figure img').forEach(i => i.addEventListener('click', () => {{ zi.src = i.src; z.classList.add('on'); }}));
z.addEventListener('click', () => z.classList.remove('on'));
document.addEventListener('keydown', e => {{ if (e.key === 'Escape') z.classList.remove('on'); }});
if (matchMedia('(max-width: 1000px)').matches) document.querySelectorAll('#toc, #legend').forEach(d => d.removeAttribute('open'));
</script>
</body>
</html>
"""
OUT.write_text(page, encoding="utf-8")
print(f"wrote {OUT}: {len(page.encode()) // 1024} KiB; parts {len(parts)}; images {len(cache)}; "
      f"toc {sum(len(p[4]) for p in parts)}; tally {tally if tally_values else 'none'}; "
      f"missing {missing}; unmatched {unmatched}")
problems = []
if missing:
    problems.append(f"images not embedded (only files under img/ are): {missing}")
if unmatched:
    problems.append(f"'{tally_col}' cell starts with none of {tally_values} in rows: {unmatched}")
if tally_col and "compare" not in used:
    problems.append(f"no table has a column named '{tally_col}' (Tally line of REPORT.md)")
for p in problems:
    print("CHECK: " + p)
sys.exit(1 if problems else 0)
