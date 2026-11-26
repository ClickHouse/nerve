#!/usr/bin/env python3
"""Write one self-contained HTML file from a source page (shareable-html skill).

Usage:
    python3 -I bundle.py <source.html> <out.html> [--max-kb N]

What it does, in this order:
  1. Expands <!-- include: path --> with the text of that file (HTML parts,
     SVG drawings). Paths are relative to the file that holds the line.
  2. Puts each local <link rel="stylesheet" href="x.css"> in a <style> block,
     with its @import files and url() files (fonts, images) inside.
  3. Puts each local <script src="x.js"> in an inline <script>.
  4. Puts each local <script type="application/json" src="x.json"> in the
     page as a data island (the JSON is checked first).
  5. Makes data URIs of local files in src, srcset, poster, href (icon,
     SVG image and use) and in url() of style blocks and style attributes.
  6. Removes HTML comments.

It writes nothing and exits 1 when a source file names anything that would
load from the network (script src, link href, img src, srcset, url() in CSS,
@import, fetch(, XMLHttpRequest, import(, an import statement, WebSocket,
EventSource, sendBeacon, Worker), a local file that is missing or outside the
source folder, a JSON file that does not parse, or
when the result is larger than --max-kb (default 2048). A plain link
(<a href="https://...">) loads nothing and is allowed.

Standard library only. Run with python3 -I.
"""
import base64
import json
import mimetypes
import pathlib
import re
import sys

MIME = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".avif": "image/avif",
    ".svg": "image/svg+xml", ".ico": "image/x-icon",
    ".woff2": "font/woff2", ".woff": "font/woff", ".ttf": "font/ttf", ".otf": "font/otf",
    ".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".wav": "audio/wav",
    ".mp4": "video/mp4", ".webm": "video/webm",
    ".json": "application/json", ".csv": "text/csv", ".txt": "text/plain",
}
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")
RAW = re.compile(r"<!--.*?-->|(<(script|style)\b[^>]*>)(.*?)(</\2\s*>)", re.S | re.I)
TAG = re.compile(r"<([a-zA-Z][a-zA-Z0-9:-]*)((?:\s+[^\s=>/]+(?:\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s>]+))?)*)\s*/?>")
ATTR = re.compile(r"([^\s=>/]+)(?:\s*=\s*(\"[^\"]*\"|'[^']*'|[^\s>]+))?")
INCLUDE = re.compile(r"<!--\s*include:\s*(\S+?)\s*-->")
CSS_URL = re.compile(r"url\(\s*(\"[^\"]*\"|'[^']*'|[^)\s]*)\s*\)", re.I)
CSS_IMPORT = re.compile(r"@import\s+(?:url\(\s*)?([\"']?)([^\"')\s;]+)\1\s*\)?\s*([^;]*);", re.I)
JS_NET = re.compile(r"\b(fetch|XMLHttpRequest|importScripts|WebSocket|EventSource|sendBeacon|Worker)\s*\("
                    r"|\bimport\s*\(|^\s*(?:import|export)\s+(?:[\w*{}\s,]+\s+from\s+)?[\"']", re.M)
URL_ATTRS = {"src", "srcset", "poster", "href", "xlink:href", "data", "action", "formaction", "background"}

findings = []   # (where, kind, value)
warnings = []   # text lines
inlined = []    # (name, bytes)


def line_of(text, pos):
    return text.count("\n", 0, pos) + 1


def unquote(v):
    v = v.strip()
    return v[1:-1] if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'" else v


def srcset_items(value):
    """(url, descriptor) pairs, split the way browsers split srcset."""
    items, i, n = [], 0, len(value)
    while i < n:
        while i < n and (value[i].isspace() or value[i] == ","):
            i += 1
        j = i
        while j < n and not value[j].isspace():
            j += 1
        url, desc = value[i:j], ""
        if url.endswith(","):
            url = url.rstrip(",")
        else:
            k = value.find(",", j)
            k = n if k == -1 else k
            desc, j = value[j:k].strip(), k + 1
        if url:
            items.append((url, desc))
        i = j
    return items


def is_local(url):
    u = url.strip()
    return bool(u) and not u.startswith(("#", "//")) and not SCHEME.match(u)


def is_safe(url):
    """True when the value loads nothing from the network."""
    u = url.strip().lower()
    return (not u) or u.startswith(("#", "data:", "blob:", "about:")) or is_local(u)


class Bundler:
    def __init__(self, root):
        self.root = root

    def resolve(self, ref, base, where):
        """Path of a local file, or None (with a finding) when missing or outside."""
        name = re.split(r"[?#]", ref.strip(), maxsplit=1)[0]
        p = (base / name).resolve()
        try:
            p.relative_to(self.root)
        except ValueError:
            findings.append((where, "outside the source folder", ref))
            return None
        if not p.is_file():
            findings.append((where, "missing local file", ref))
            return None
        return p

    def data_uri(self, ref, base, where):
        p = self.resolve(ref, base, where)
        if p is None:
            return ref
        raw = p.read_bytes()
        inlined.append((str(p.relative_to(self.root)), len(raw)))
        mime = MIME.get(p.suffix.lower()) or mimetypes.guess_type(p.name)[0] or "application/octet-stream"
        return f"data:{mime};base64,{base64.b64encode(raw).decode()}"

    def read(self, p):
        text = p.read_text(encoding="utf-8")
        inlined.append((str(p.relative_to(self.root)), len(text.encode())))
        return text

    # -- includes ---------------------------------------------------------
    def includes(self, text, base, depth=0):
        def one(m):
            if depth > 8:
                findings.append((m.group(1), "include nested too deep", m.group(1)))
                return ""
            p = self.resolve(m.group(1), base, f"include {m.group(1)}")
            if p is None:
                return ""
            part = self.read(p)
            scan_file(part, p, self.root)
            if p.suffix.lower() == ".svg":
                part = re.sub(r"<\?xml.*?\?>|<!DOCTYPE[^>]*>", "", part, flags=re.S | re.I).strip()
            return self.includes(part, p.parent, depth + 1)
        return INCLUDE.sub(one, text)

    # -- CSS --------------------------------------------------------------
    def css(self, text, base, where, depth=0):
        def imp(m):
            ref, media = m.group(2), m.group(3).strip()
            if not is_local(ref) or depth > 8:
                return m.group(0)
            p = self.resolve(ref, base, where)
            if p is None:
                return ""
            sub = self.read(p)
            scan_file(sub, p, self.root)
            body = self.css(sub, p.parent, str(p.relative_to(self.root)), depth + 1)
            return f"@media {media}{{{body}}}" if media else body

        def url(m):
            ref = unquote(m.group(1))
            if not is_local(ref):
                return m.group(0)
            return f"url({self.data_uri(ref, base, where)})"

        return CSS_URL.sub(url, CSS_IMPORT.sub(imp, text))

    # -- markup -----------------------------------------------------------
    def srcset(self, value, base, where):
        out = []
        for ref, desc in srcset_items(value):
            new = self.data_uri(ref, base, where) if is_local(ref) else ref
            out.append(f"{new} {desc}".strip())
        return ", ".join(out)

    def tag(self, m, base, where):
        name, attrs = m.group(1).lower(), m.group(2)
        pairs = [(a.group(1), a.group(2)) for a in ATTR.finditer(attrs)]
        low = {k.lower(): unquote(v) if v is not None else "" for k, v in pairs}
        if name == "link" and "stylesheet" in low.get("rel", "").lower().split() and is_local(low.get("href", "")):
            p = self.resolve(low["href"], base, where)
            if p is None:
                return ""
            sub = self.read(p)
            scan_file(sub, p, self.root)
            media = f' media="{low["media"]}"' if low.get("media") else ""
            return f"<style{media}>\n{self.css(sub, p.parent, str(p.relative_to(self.root)))}\n</style>"
        out = []
        for k, v in pairs:
            key = k.lower()
            if v is None:
                out.append(k)
                continue
            val = unquote(v)
            if key == "style":
                val = self.css(val, base, where)
            elif key == "srcset":
                val = self.srcset(val, base, where)
            elif key in ("src", "poster") or (key in ("href", "xlink:href") and name in ("link", "image", "use", "feimage")):
                if is_local(val):
                    val = self.data_uri(val, base, where)
            elif name == "a" and key == "href" and is_local(val):
                warnings.append(f"warning: link to a local file {val!r} breaks once the page is sent; "
                                "put the content in the page or link to a public URL")
            q = "'" if '"' in val else '"'
            out.append(f"{k}={q}{val}{q}")
        end = "/>" if m.group(0).endswith("/>") else ">"
        return f"<{m.group(1)}{''.join(' ' + a for a in out)}{end}"

    def script(self, open_tag, body, base, where):
        m = TAG.match(open_tag)
        pairs = [(a.group(1), a.group(2)) for a in ATTR.finditer(m.group(2))] if m else []
        low = {k.lower(): unquote(v) if v is not None else "" for k, v in pairs}
        src = low.get("src", "")
        if not is_local(src):
            return None
        p = self.resolve(src, base, where)
        if p is None:
            return ""
        keep = "".join(f" {k}={v}" if v is not None else f" {k}" for k, v in pairs
                       if k.lower() not in ("src", "defer", "async"))
        text = self.read(p)
        kind = low.get("type", "").lower()
        if "json" in kind:
            try:
                json.loads(text)
            except ValueError as e:
                findings.append((str(p.relative_to(self.root)), "JSON does not parse", str(e)))
            return f"<script{keep}>{text.strip().replace('</', '<' + chr(92) + '/')}</script>"
        scan_file(text, p, self.root)
        text = re.sub(r"</script", "<\\\\/script", text, flags=re.I)
        return f"<script{keep}>\n{text}\n</script>"

    def html(self, text, base, where):
        out, pos = [], 0
        for m in RAW.finditer(text):
            out.append(TAG.sub(lambda t: self.tag(t, base, where), text[pos:m.start()]))
            pos = m.end()
            if m.group(1) is None:      # an HTML comment: drop it
                continue
            open_tag, kind, body, close = m.group(1), m.group(2).lower(), m.group(3), m.group(4)
            if kind == "style":
                out.append(open_tag + self.css(body, base, where) + close)
                continue
            new = self.script(open_tag, body, base, where)
            out.append(open_tag + body + close if new is None else new)
        out.append(TAG.sub(lambda t: self.tag(t, base, where), text[pos:]))
        return "".join(out)


# -- scanning for network loads ---------------------------------------------

def scan_css(text, where, offset_text=None, offset=0):
    full = offset_text if offset_text is not None else text
    spans = []
    for m in CSS_IMPORT.finditer(text):
        spans.append(m.span())
        if not is_local(m.group(2)):
            findings.append((f"{where}:{line_of(full, offset + m.start())}", "css @import", m.group(2)))
    for m in CSS_URL.finditer(text):
        ref = unquote(m.group(1))
        if any(a <= m.start() < b for a, b in spans):
            continue
        if not is_safe(ref):
            findings.append((f"{where}:{line_of(full, offset + m.start())}", "css url()", ref))


def scan_js(text, where, offset_text=None, offset=0):
    full = offset_text if offset_text is not None else text
    for m in JS_NET.finditer(text):
        findings.append((f"{where}:{line_of(full, offset + m.start())}", "network call in script", m.group(0).strip()))


def scan_html(text, where):
    def markup(chunk, off):
        for t in TAG.finditer(chunk):
            name = t.group(1).lower()
            for a in ATTR.finditer(t.group(2)):
                key, val = a.group(1).lower(), unquote(a.group(2) or "")
                at = f"{where}:{line_of(text, off + t.start())}"
                if key == "style":
                    scan_css(val, at)
                elif key == "srcset":
                    for ref, _ in srcset_items(val):
                        if not is_safe(ref):
                            findings.append((at, f"{name} srcset", ref))
                elif key in URL_ATTRS and not (name == "a" and key in ("href", "xlink:href")):
                    if not is_safe(val):
                        findings.append((at, f"{name} {key}", val))
                elif key == "content" and name == "meta" and re.search(r"url\s*=\s*https?:", val, re.I):
                    findings.append((at, "meta refresh", val))

    pos = 0
    for m in RAW.finditer(text):
        markup(text[pos:m.start()], pos)
        pos = m.end()
        if m.group(1) is None:
            continue
        kind, open_tag = m.group(2).lower(), m.group(1)
        markup(open_tag, m.start())
        if kind == "style":
            scan_css(m.group(3), where, text, m.start(3))
        elif "json" not in open_tag.lower():
            scan_js(m.group(3), where, text, m.start(3))
    markup(text[pos:], pos)


def scan_file(text, path, root):
    where = str(path.relative_to(root))
    suffix = path.suffix.lower()
    if suffix == ".css":
        scan_css(text, where)
    elif suffix in (".js", ".mjs"):
        scan_js(text, where)
    else:
        scan_html(re.sub(r"<!--(?!\s*include:).*?-->", lambda m: "\n" * m.group(0).count("\n"), text, flags=re.S), where)


def main(argv):
    args = [a for a in argv if not a.startswith("--")]
    max_kb = 2048
    if "--max-kb" in argv:
        i = argv.index("--max-kb")
        max_kb = int(argv[i + 1])
        args.remove(argv[i + 1])
    if len(args) != 2:
        print(__doc__.split("\n\n")[1].strip(), file=sys.stderr)
        return 2
    src, out = pathlib.Path(args[0]).resolve(), pathlib.Path(args[1]).resolve()
    root = src.parent
    b = Bundler(root)
    text = src.read_text(encoding="utf-8")
    scan_file(text, src, root)
    text = b.includes(text, root)
    result = b.html(text, root, src.name)
    if not findings:
        scan_html(result, f"bundled result ({out.name})")
        for m in re.finditer(r"<!--", result):
            findings.append((f"bundled result:{line_of(result, m.start())}", "comment left", "<!--"))
    size_kb = len(result.encode()) / 1024
    if size_kb > max_kb:
        findings.append((out.name, f"size {size_kb:.0f} KB is above --max-kb {max_kb}", "resize or drop images"))
    if findings:
        seen = set()
        print(f"bundle.py: {len(findings)} finding(s); nothing written:", file=sys.stderr)
        for where, kind, value in findings:
            if (where, kind, value) in seen:
                continue
            seen.add((where, kind, value))
            print(f"  {where}: {kind}: {value[:160]}", file=sys.stderr)
        return 1
    ids = re.findall(r"<[^>]*\sid=[\"']([^\"']+)[\"']", RAW.sub(lambda m: m.group(1) or "", result))
    dups = sorted({i for i in ids if ids.count(i) > 1})
    out.write_text(result, encoding="utf-8")
    big = sorted(inlined, key=lambda x: -x[1])[:5]
    print(f"wrote {out}: {size_kb:.0f} KB; largest inputs: "
          + ", ".join(f"{n} {s / 1024:.0f} KB" for n, s in big))
    for n, s in inlined:
        if s > 300 * 1024 and pathlib.Path(n).suffix.lower() in (".png", ".jpg", ".jpeg", ".gif", ".webp"):
            print(f"warning: {n} is {s / 1024:.0f} KB; resize it (see the skill)")
    for w in dict.fromkeys(warnings):
        print(w)
    if dups:
        print(f"warning: duplicate id values {dups[:10]}: an SVG url(#id) or a label can point at the wrong element")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
