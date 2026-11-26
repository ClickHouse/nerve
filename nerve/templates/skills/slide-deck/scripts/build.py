"""Builds one self-contained HTML file from the deck source (slide-deck skill).

Copy this file into the deck folder and run it there: python3 -I build.py
Source: deck.src.html, or src/*.html joined in name order when src/ exists.
Output: deck.html in the same folder. Rename it only when you deliver it.

Fonts: each FACES entry becomes an @font-face rule at the /*FONTS*/ marker, with
the woff2 file from fonts/ inside as a data URI. With FACES empty (and no fonts/
folder) the deck uses the system font stack of its preset.
Pictures and sounds: every src="<local file>" and url(<local file>) becomes a
data URI. The build stops when a local file is missing and warns about every
remote address, because the deck must work offline.
Standard library only.
"""
import base64
import mimetypes
import pathlib
import re
import sys

d = pathlib.Path(__file__).resolve().parent
OUT = d / "deck.html"
FACES = [  # (font-family name, file in fonts/, weight range[, "italic"]), for example:
    # ("Inter", "inter-latin-wght-normal.woff2", "100 900"),
    # ("Source Serif 4", "source-serif-4-latin-wght-italic.woff2", "200 900", "italic"),
]

missing_fonts = [f[1] for f in FACES if not (d / "fonts" / f[1]).is_file()]
if missing_fonts:
    sys.exit(f"font files not found in fonts/: {missing_fonts}")
css = "".join(
    f"@font-face{{font-family:'{f[0]}';font-style:{f[3] if len(f) > 3 else 'normal'};font-weight:{f[2]};font-display:block;"
    f"src:url(data:font/woff2;base64,{base64.b64encode((d / 'fonts' / f[1]).read_bytes()).decode()}) format('woff2')}}\n"
    for f in FACES)
unused = sorted(p.name for p in (d / "fonts").glob("*.woff2") if p.name not in {f[1] for f in FACES}) if (d / "fonts").is_dir() else []
if unused:
    print("note: fonts/ files not in FACES (not embedded):", ", ".join(unused))

parts = sorted((d / "src").glob("*.html")) if (d / "src").is_dir() else [d / "deck.src.html"]
src = "".join(p.read_text(encoding="utf-8") for p in parts)
if src.count("/*FONTS*/") != 1:
    sys.exit("the source needs exactly one /*FONTS*/ marker")

LOCAL = re.compile(r"^(?!data:|https?:|//|#|about:|blob:)")


def data_uri(m):
    name = m.group(2)
    f = d / name
    if not LOCAL.match(name) or not f.is_file():
        return m.group(0)
    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
    return m.group(1) + "data:" + mime + ";base64," + base64.b64encode(f.read_bytes()).decode() + m.group(3)


src = re.sub(r'(\bsrc=")([^"]+)(")', data_uri, src)
src = re.sub(r"(new Audio\(')([^']+)('\))", data_uri, src)
src = re.sub(r"""(url\(['"]?)([^'")]+)(['"]?\))""", data_uri, src)

missing = [n for n in re.findall(r'\bsrc="([^"]+)"', src) + re.findall(r"""url\(['"]?([^'")]+)""", src)
           if LOCAL.match(n)]
if missing:
    sys.exit(f"local files not found: {missing}")
remote = sorted(set(re.findall(r"""(?:\bsrc=|<link[^>]*\bhref=|url\(|@import\s)['"]?((?:https?:)?//[^'")\s>]+)""", src)))
if remote:
    print("warning: remote addresses (they do not load offline):", ", ".join(remote))

OUT.write_text(src.replace("/*FONTS*/", css), encoding="utf-8")
n = len(re.findall(r'<section class="slide[ "]', src))
print("built", OUT.name, OUT.stat().st_size, "bytes;", n, "slides;", len(FACES), "font faces")
