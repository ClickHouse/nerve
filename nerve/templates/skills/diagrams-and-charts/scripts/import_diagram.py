"""Read a draw.io or Excalidraw file as data and print a mermaid flowchart draft.

    python3 -I import_diagram.py <file.drawio|file.xml|file.excalidraw> > draft.mmd

Standard library only. It reads boxes, their labels, groups and arrows; it runs
nothing from the file and loads nothing it names. The draft keeps the source's
order; check every box and arrow against the picture, then edit the layout.
Exit 1 when the file is not a format it knows.
"""
import base64
import html
import json
import re
import sys
import urllib.parse
import xml.etree.ElementTree as ET
import zlib


def clean(text):
    text = re.sub(r"<br\s*/?>|</div>|</p>", " ", text or "", flags=re.I)
    text = html.unescape(re.sub(r"<[^>]+>", "", text))
    return re.sub(r"\s+", " ", text).strip().replace('"', "#quot;")


def shape(label, style):
    style = style or ""
    if "rhombus" in style or "diamond" in style:
        return '{"%s"}' % label
    if "cylinder" in style or "datastore" in style:
        return '[("%s")]' % label
    if "ellipse" in style:
        return '(["%s"])' % label
    return '["%s"]' % label


def emit(title, nodes, edges):
    """nodes: id -> (label, style, parent id or None); edges: (source, target, label)."""
    ids = {key: f"n{i + 1}" for i, key in enumerate(nodes)}
    groups = {p for (_, _, p) in nodes.values() if p in nodes}
    out = [f"%% {title}: {len(nodes)} boxes, {len(edges)} arrows", "flowchart TB"]

    def write(parent, indent):
        for key, (label, style, p) in nodes.items():
            if p != parent and not (parent is None and p not in nodes):
                continue
            if key in groups:
                out.append(f'{indent}subgraph {ids[key]} ["{label or key}"]')
                write(key, indent + "  ")
                out.append(f"{indent}end")
            else:
                out.append(f"{indent}{ids[key]}{shape(label or key, style)}")

    write(None, "  ")
    for source, target, label in edges:
        if source in ids and target in ids:
            arrow = f' -->|"{label}"| ' if label else " --> "
            out.append(f"  {ids[source]}{arrow}{ids[target]}")
    print("\n".join(out))


def drawio(text):
    root = ET.fromstring(text)
    pages = root.findall(".//diagram") or [root]
    for page in pages:
        model = page.find(".//mxGraphModel")
        if model is None and (page.text or "").strip():  # compressed page
            raw = zlib.decompress(base64.b64decode(page.text.strip()), -15).decode("utf-8")
            model = ET.fromstring(urllib.parse.unquote(raw))
        if model is None:
            model = root
        nodes, edges = {}, []
        # A cell with extra data sits inside <object> or <UserObject>, which holds its id and label.
        inner = {c for el in model.iter() if el.tag in ("object", "UserObject") for c in el.findall("mxCell")}
        for el in model.iter():
            cell = el if el.tag == "mxCell" else el.find("mxCell") if el.tag in ("object", "UserObject") else None
            if cell is None or el in inner:
                continue
            key = el.get("id") or cell.get("id")
            label = clean(el.get("label") if el is not cell else cell.get("value"))
            if cell.get("vertex") == "1":
                nodes[key] = (label, cell.get("style"), cell.get("parent"))
            elif cell.get("edge") == "1":
                edges.append((cell.get("source"), cell.get("target"), label))
        emit(f"draw.io page {page.get('name', '1')}", nodes, edges)


def excalidraw(text):
    elements = [e for e in json.loads(text).get("elements", []) if not e.get("isDeleted")]
    labels = {e["containerId"]: clean(e.get("text")) for e in elements if e.get("type") == "text" and e.get("containerId")}
    nodes, edges = {}, []
    for e in elements:
        if e.get("type") in ("rectangle", "ellipse", "diamond"):
            nodes[e["id"]] = (labels.get(e["id"], ""), e["type"], None)
        elif e.get("type") in ("arrow", "line"):
            start, end = e.get("startBinding") or {}, e.get("endBinding") or {}
            edges.append((start.get("elementId"), end.get("elementId"), labels.get(e["id"], "")))
    emit("excalidraw", nodes, edges)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__.strip().splitlines()[2].strip())
    path = sys.argv[1]
    text = open(path, encoding="utf-8").read()
    try:
        if path.endswith(".excalidraw") or text.lstrip().startswith("{"):
            excalidraw(text)
        else:
            drawio(text)
    except (ET.ParseError, ValueError, KeyError, zlib.error) as err:
        print(f"error: {path} is not a draw.io or Excalidraw file this reads ({err})", file=sys.stderr)
        sys.exit(1)


main()
