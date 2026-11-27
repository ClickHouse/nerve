"""The kit of a bundle and the bundles of Markdown and CSV files
(``nerve/artifacts/bundle.py``): the generated page, the escaping of the
source text, the rule that adds kit files, and the kit build."""

from __future__ import annotations

import json
import re
import traceback
from html.parser import HTMLParser
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from nerve.artifacts import bundle as bundle_module
from nerve.artifacts.bundle import (
    CSV_SOURCE,
    KIT_FILES,
    KIT_SCAN_BYTES,
    MAX_TABLE_CELLS,
    MAX_TABLE_COLUMNS,
    MARKDOWN_SOURCE,
    BundleError,
    BundleKind,
    BundleRefusal,
    FileSource,
    describe_bundle,
    kit_names,
    kit_page,
    read_bundle_file,
)

REPO = Path(__file__).resolve().parent.parent
FILES_JSON = REPO / "web" / "src" / "kit" / "files.json"
BUILT_KIT = REPO / "web" / "dist" / "nerve-kit"
CSV_CASES = REPO / "web" / "src" / "kit" / "csv-cases.json"


@pytest.fixture
def kit(tmp_path) -> Path:
    folder = tmp_path / "kit"
    folder.mkdir()
    for name in KIT_FILES:
        (folder / name).write_text(f"/* {name} */")
    return folder


@pytest.fixture
def ws(tmp_path) -> Path:
    workspace = tmp_path / "ws"
    (workspace / "reports").mkdir(parents=True)
    return workspace


def _paths(bundle) -> list[str]:
    return [f.path for f in bundle.files]


def _refused(workspace, path, kit) -> BundleError:
    with pytest.raises(BundleError) as caught:
        describe_bundle(workspace, path, kit_dir=kit)
    return caught.value


class _Blocks(HTMLParser):
    """The scripts of a page as a browser parser splits them: type and text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.scripts: list[tuple[dict, str]] = []
        self._open: dict | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self._open = dict(attrs)
            self.scripts.append((self._open, ""))

    def handle_data(self, data):
        if self._open is not None:
            attrs, text = self.scripts[-1]
            self.scripts[-1] = (attrs, text + data)

    def handle_endtag(self, tag):
        if tag == "script":
            self._open = None


def _source_of(page: bytes) -> dict:
    parser = _Blocks()
    parser.feed(page.decode("utf-8"))
    data = [text for attrs, text in parser.scripts if attrs.get("type") == "application/json"]
    assert len(data) == 1
    return json.loads(data[0])


HOSTILE = (
    "# Notes\n\n</script><script>window.bad = 1</script>\n"
    "</SCRIPT >\n<!-- <script> -->\n&lt;b&gt; &     \"quotes\" \\ back\n"
)


class TestSourceBundles:
    def test_a_markdown_file(self, ws, kit):
        (ws / "reports" / "notes.md").write_text(HOSTILE)
        b = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        assert (b.kind, b.root, b.entry, b.path) == (BundleKind.MARKDOWN, "reports", "index.html", "reports/notes.md")
        assert _paths(b) == ["index.html", "nerve-kit/kit.css", "nerve-kit/markdown.js", MARKDOWN_SOURCE]
        assert b.file("index.html").source is FileSource.GENERATED
        assert b.file(MARKDOWN_SOURCE).media_type == "text/markdown"
        assert read_bundle_file(b, MARKDOWN_SOURCE) == HOSTILE.encode("utf-8")
        assert read_bundle_file(b, "nerve-kit/markdown.js") == b"/* markdown.js */"

    def test_a_csv_file(self, ws, kit):
        text = 'name,note\nalice,"</script><b>x</b>"\nbob,2\n'
        (ws / "reports" / "cost.csv").write_text(text)
        b = describe_bundle(ws, "reports/cost.csv", kit_dir=kit)
        assert (b.kind, b.root, b.entry) == (BundleKind.CSV, "reports", "index.html")
        assert _paths(b) == [CSV_SOURCE, "index.html", "nerve-kit/kit.css", "nerve-kit/table.js"]
        assert b.file(CSV_SOURCE).media_type == "text/csv"
        assert read_bundle_file(b, CSV_SOURCE) == text.encode()
        page = read_bundle_file(b, "index.html")
        assert b.file("index.html").size == len(page)
        assert _source_of(page) == {"name": "cost.csv", "text": text}

    @pytest.mark.parametrize("kind", [BundleKind.MARKDOWN, BundleKind.CSV])
    def test_no_source_text_ends_the_data_block(self, kind):
        page = kit_page(kind, "a</title><script>x()</script>.md", HOSTILE)
        assert _source_of(page) == {"name": "a</title><script>x()</script>.md", "text": HOSTILE}
        block = page.split(b'id="nerve-kit-source">', 1)[1].split(b"</script>", 1)[0]
        assert b"</" not in block and b"<!" not in block
        # The escapes cost little: the text grows by the escape of each mark only.
        plain = kit_page(kind, "a.md", "x" * 1000)
        marked = kit_page(kind, "a.md", "</" * 500)
        assert len(marked) - len(plain) == 500
        # The page has the data block and the one kit script, and no other.
        parser = _Blocks()
        parser.feed(page.decode())
        assert [a.get("type") or a.get("src") for a, _ in parser.scripts] == [
            "application/json", f"nerve-kit/{'markdown' if kind is BundleKind.MARKDOWN else 'table'}.js",
        ]
        assert b"<title>a&lt;/title&gt;&lt;script&gt;x()&lt;/script&gt;.md</title>" in page

    def test_a_byte_order_mark_is_left_out_of_the_data(self, ws, kit):
        (ws / "a.csv").write_bytes(b"\xef\xbb\xbfx\n1\n")
        b = describe_bundle(ws, "a.csv", kit_dir=kit)
        assert _source_of(read_bundle_file(b, "index.html"))["text"] == "x\n1\n"
        assert read_bundle_file(b, CSV_SOURCE).startswith(b"\xef\xbb\xbf")

    def test_the_fingerprint_follows_the_source(self, ws, kit):
        source = ws / "reports" / "notes.md"
        source.write_text("one")
        first = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        assert describe_bundle(ws, "reports/notes.md", kit_dir=kit).fingerprint == first.fingerprint
        source.write_text("two!")
        assert describe_bundle(ws, "reports/notes.md", kit_dir=kit).fingerprint != first.fingerprint

    def test_no_page_bytes_are_kept(self, ws, kit):
        (ws / "reports" / "notes.md").write_text("# kept nowhere\n" * 1000)
        b = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        assert not any(isinstance(getattr(f, name, None), bytes) for f in b.files for name in f.__dataclass_fields__)
        assert b.file("index.html").origin == MARKDOWN_SOURCE
        assert _source_of(read_bundle_file(b, "index.html"))["text"] == "# kept nowhere\n" * 1000

    def test_a_changed_source_refuses_the_generated_page(self, ws, kit):
        source = ws / "reports" / "notes.md"
        source.write_text("one")
        b = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        source.write_text("two, longer")
        with pytest.raises(BundleError) as caught:
            read_bundle_file(b, "index.html")
        assert caught.value.code is BundleRefusal.CHANGED

    def test_a_write_with_the_same_size_and_time_changes_the_fingerprint(self, ws, kit):
        import os

        source = ws / "reports" / "notes.md"
        source.write_text("one")
        st = source.stat()
        first = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        source.write_text("two")
        os.utime(source, ns=(st.st_atime_ns, st.st_mtime_ns))
        again = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        assert again.fingerprint != first.fingerprint
        assert _source_of(read_bundle_file(again, "index.html"))["text"] == "two"

    def test_the_generated_page_takes_the_identity_of_its_source(self, ws, kit):
        source = ws / "reports" / "notes.md"
        source.write_text("x")
        st = source.stat()
        b = describe_bundle(ws, "reports/notes.md", kit_dir=kit)
        for f in (b.file("index.html"), b.file(MARKDOWN_SOURCE)):
            assert (f.mtime_ns, f.ctime_ns, f.inode, f.device) == (st.st_mtime_ns, st.st_ctime_ns, st.st_ino, st.st_dev)
        kit_file = b.file("nerve-kit/markdown.js")
        assert kit_file.inode == (kit / "markdown.js").stat().st_ino

    def test_more_rows_than_a_table_page_shows(self, ws, kit, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_TABLE_ROWS", 3)
        (ws / "ok.csv").write_text("h\n1\n\n2\n3\n")
        assert describe_bundle(ws, "ok.csv", kit_dir=kit).kind is BundleKind.CSV
        (ws / "big.csv").write_text("h\n1\n2\n3\n4\n")
        e = _refused(ws, "big.csv", kit)
        assert e.code is BundleRefusal.SOURCE_TOO_LARGE
        assert "4 rows" in e.message and "at most 3 rows" in e.message

    def test_a_table_wider_than_the_page_shows(self, ws, kit):
        header = ",".join(f"c{i}" for i in range(5000))
        (ws / "wide.csv").write_text(header + "\n" + "x\n" * 9999)
        e = _refused(ws, "wide.csv", kit)
        assert e.code is BundleRefusal.SOURCE_TOO_LARGE
        assert f"more than {MAX_TABLE_COLUMNS} columns" in e.message

    def test_more_cells_than_the_page_shows(self, ws, kit):
        line = ",".join(["1"] * 100)
        rows = MAX_TABLE_CELLS // 100
        (ws / "ok.csv").write_text("\n".join([line] * rows))
        assert describe_bundle(ws, "ok.csv", kit_dir=kit).kind is BundleKind.CSV
        (ws / "big.csv").write_text("\n".join([line] * (rows + 1)))
        e = _refused(ws, "big.csv", kit)
        assert e.code is BundleRefusal.SOURCE_TOO_LARGE and "cells" in e.message

    def test_a_field_longer_than_128_kib_is_a_table(self, ws, kit):
        text = 'id,note\n1,"' + "long text, with a comma\n" * 8000 + '"\n2,short\n'
        (ws / "notes.csv").write_text(text)
        b = describe_bundle(ws, "notes.csv", kit_dir=kit)
        assert _source_of(read_bundle_file(b, "index.html"))["text"] == text

    @pytest.mark.parametrize("case", json.loads(CSV_CASES.read_text()), ids=lambda c: c["name"])
    def test_the_counts_agree_with_the_kit_parser(self, case):
        # The same cases as web/src/kit/csv.test.ts.
        assert bundle_module.table_shape(case["text"]) == (case["rows"], case["width"])

    def test_a_refusal_is_new_at_each_description_and_keeps_no_bytes(self, ws, kit, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_TABLE_ROWS", 1)
        (ws / "big.csv").write_text("h\n1\n2\n")
        first = _refused(ws, "big.csv", kit)
        second = _refused(ws, "big.csv", kit)
        assert first is not second and first.message == second.message
        depth = len(traceback.extract_tb(second.__traceback__))
        assert len(traceback.extract_tb(_refused(ws, "big.csv", kit).__traceback__)) == depth
        for value in bundle_module._PAGE_SIZES._values.values():
            assert isinstance(value, (int, tuple))
            assert not any(isinstance(v, (bytes, BaseException)) for v in (value if isinstance(value, tuple) else ()))

    def test_the_shown_time_leaves_the_kit_out(self, ws, kit):
        import os

        source = ws / "notes.md"
        source.write_text("x")
        os.utime(source, ns=(10**18, 10**18))
        os.utime(kit / "markdown.js", ns=(2 * 10**18, 2 * 10**18))
        b = describe_bundle(ws, "notes.md", kit_dir=kit)
        assert b.modified_ns == 10**18

    def test_a_quoted_line_break_is_one_row(self, ws, kit, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_TABLE_ROWS", 1)
        (ws / "a.csv").write_text('h\n"one\ntwo\nthree"\n')
        assert describe_bundle(ws, "a.csv", kit_dir=kit).kind is BundleKind.CSV

    @pytest.mark.parametrize("name", ["big.md", "big.csv"])
    def test_a_source_above_the_byte_limit(self, ws, kit, monkeypatch, name):
        monkeypatch.setattr(bundle_module, "MAX_SOURCE_BYTES", 10)
        (ws / name).write_text("x" * 11)
        e = _refused(ws, name, kit)
        assert e.code is BundleRefusal.SOURCE_TOO_LARGE
        assert "larger than" in e.message

    def test_a_source_that_is_not_utf8(self, ws, kit):
        (ws / "latin.csv").write_bytes(b"caf\xe9\n")
        assert _refused(ws, "latin.csv", kit).code is BundleRefusal.NOT_TEXT

    def test_a_link_as_the_source_is_refused(self, ws, kit, tmp_path):
        (tmp_path / "outside.md").write_text("secret")
        (ws / "notes.md").symlink_to(tmp_path / "outside.md")
        assert _refused(ws, "notes.md", kit).code is BundleRefusal.LINK

    def test_a_missing_kit_is_said(self, ws, tmp_path):
        (ws / "notes.md").write_text("x")
        e = _refused(ws, "notes.md", tmp_path / "no-kit")
        assert e.code is BundleRefusal.KIT_MISSING
        assert "npm run build" in e.message


class TestKitRule:
    def _kit_of(self, ws, kit, files: dict[str, str], path: str = "site") -> list[str]:
        for name, text in files.items():
            (ws / "site" / name).parent.mkdir(parents=True, exist_ok=True)
            (ws / "site" / name).write_text(text)
        return [p for p in _paths(describe_bundle(ws, path, kit_dir=kit)) if p.startswith("nerve-kit/")]

    def test_kit_files_are_added_exactly_when_the_entry_names_them(self, ws, kit):
        assert self._kit_of(ws, kit, {"index.html": "<p>no kit</p>"}) == []
        deck = ('<link rel="stylesheet" href="nerve-kit/deck.css"><section class="slide"></section>'
                '<script src="nerve-kit/deck.js"></script>')
        assert self._kit_of(ws, kit, {"index.html": deck}) == ["nerve-kit/deck.css", "nerve-kit/deck.js"]

    def test_a_page_that_is_not_the_entry_adds_nothing(self, ws, kit):
        files = {"index.html": "<p>start</p>", "part/page.html": '<script src="../nerve-kit/table.js"></script>'}
        assert self._kit_of(ws, kit, files) == []

    def test_the_kit_files_of_a_page_bundle(self, ws, kit):
        (ws / "deck.html").write_text("<script src='./nerve-kit/deck.js?v=1#x'></script>")
        b = describe_bundle(ws, "deck.html", kit_dir=kit)
        assert (b.kind, _paths(b)) == (BundleKind.PAGE, ["deck.html", "nerve-kit/deck.js"])
        assert b.file("nerve-kit/deck.js").source is FileSource.KIT
        assert read_bundle_file(b, "nerve-kit/deck.js") == b"/* deck.js */"

    def test_only_the_first_part_of_the_entry_is_searched(self, ws, kit):
        head = "<!-- " + "x" * (KIT_SCAN_BYTES - 40) + " -->"
        found = self._kit_of(ws, kit, {"index.html": head + '<script src="nerve-kit/deck.js"></script>'})
        assert found == ["nerve-kit/deck.js"]
        late = "<!-- " + "x" * KIT_SCAN_BYTES + " -->" + '<script src="nerve-kit/table.js"></script>'
        assert self._kit_of(ws, kit, {"index.html": late}) == []

    def test_an_address_in_a_code_sample_adds_the_file(self, ws, kit):
        page = "<pre>&lt;script src=\"nerve-kit/table.js\"&gt;</pre>"
        assert self._kit_of(ws, kit, {"index.html": page}) == ["nerve-kit/table.js"]

    def test_only_the_fixed_names_are_kit_files(self):
        assert kit_names(b"nerve-kit/unknown.js nerve-kit/sub/deck.js nerve-kit/../deck.js /etc/passwd") == frozenset()
        assert kit_names(b"nerve-kit/kit.css nerve-kit/markdown.js") == frozenset({"kit.css", "markdown.js"})
        # A plain search: an encoded separator before the address is a false hit, which adds the file only.
        assert kit_names(b"%2Fnerve-kit/deck.js") == frozenset({"deck.js"})

    def test_a_folder_cannot_have_its_own_kit_folder(self, ws, kit):
        site = ws / "site"
        (site / "nerve-kit").mkdir(parents=True)
        (site / "index.html").write_text("<p>x</p>")
        (site / "nerve-kit" / "deck.js").write_text("not the kit")
        e = _refused(ws, "site", kit)
        assert e.code is BundleRefusal.RESERVED_PATH

    def test_a_link_in_the_kit_folder_is_not_a_kit_file(self, ws, tmp_path):
        folder = tmp_path / "linked-kit"
        folder.mkdir()
        (tmp_path / "elsewhere.js").write_text("x")
        (folder / "deck.js").symlink_to(tmp_path / "elsewhere.js")
        (ws / "deck.html").write_text("<script src=nerve-kit/deck.js></script>")
        assert _refused(ws, "deck.html", folder).code is BundleRefusal.KIT_MISSING

    def test_a_change_of_a_kit_file_changes_the_fingerprint(self, ws, kit):
        (ws / "deck.html").write_text("<script src=nerve-kit/deck.js></script>")
        first = describe_bundle(ws, "deck.html", kit_dir=kit).fingerprint
        (kit / "deck.js").write_text("/* a new build of deck.js */")
        assert describe_bundle(ws, "deck.html", kit_dir=kit).fingerprint != first

    def test_an_image_gets_no_kit(self, ws, kit):
        (ws / "logo.svg").write_text("<svg><script href='nerve-kit/deck.js'/></svg>")
        assert _paths(describe_bundle(ws, "logo.svg", kit_dir=kit)) == ["logo.svg"]


class TestTheBuild:
    def test_the_build_and_the_runtime_have_the_same_kit_names(self):
        files = json.loads(FILES_JSON.read_text())
        assert set(files["scripts"]) | set(files["styles"]) == KIT_FILES
        for source in [*files["scripts"].values(), *files["styles"].values()]:
            assert (REPO / "web" / source).is_file(), source

    def test_the_pages_check_the_limits_of_the_runtime(self):
        kit_src = REPO / "web" / "src" / "kit"
        source = (kit_src / "source.ts").read_text()
        csv_ts = (kit_src / "csv.ts").read_text()
        assert "MAX_SOURCE_BYTES = 5 * 1024 * 1024;" in source
        assert bundle_module.MAX_SOURCE_BYTES == 5 * 1024 * 1024
        for name, value in (("MAX_TABLE_ROWS", bundle_module.MAX_TABLE_ROWS),
                            ("MAX_TABLE_COLUMNS", MAX_TABLE_COLUMNS), ("MAX_TABLE_CELLS", MAX_TABLE_CELLS)):
            assert f"export const {name} = {value:_};" in csv_ts, name

    def test_the_generated_pages_name_kit_files(self):
        for kind in (BundleKind.MARKDOWN, BundleKind.CSV):
            assert kit_names(kit_page(kind, "x", "")) == bundle_module._PAGE_KIT[kind]
            assert len(bundle_module._PAGE_KIT[kind]) == 2

    @pytest.mark.skipif(not BUILT_KIT.is_dir(), reason="the web UI is not built")
    def test_the_runtime_finds_each_file_of_the_build(self, tmp_path):
        assert {p.name for p in BUILT_KIT.iterdir()} == KIT_FILES
        assert bundle_module.KIT_DIR == BUILT_KIT
        workspace = tmp_path / "ws"
        workspace.mkdir()
        (workspace / "deck.html").write_text(
            '<link rel=stylesheet href="nerve-kit/deck.css"><script src="nerve-kit/deck.js"></script>'
        )
        (workspace / "notes.md").write_text("# x")
        (workspace / "rows.csv").write_text("a\n1\n")
        for path in ("deck.html", "notes.md", "rows.csv"):
            b = describe_bundle(workspace, path)
            for f in b.files:
                if f.source is FileSource.KIT:
                    assert read_bundle_file(b, f.path) == (BUILT_KIT / f.path.split("/", 1)[1]).read_bytes()

    @pytest.mark.skipif(not BUILT_KIT.is_dir(), reason="the web UI is not built")
    def test_no_kit_file_loads_from_the_network(self):
        allowed = (
            "http://www.w3.org/", "https://react.dev/errors/",
            "https://github.com/syntax-tree/hast-util-to-jsx-runtime", "https://github.com/highlightjs/highlight.js/",
        )
        for path in BUILT_KIT.iterdir():
            text = path.read_text()
            for address in re.findall(r"(?:https?:)?//[a-z0-9.-]+\.[a-z]{2,}[^\s\"'`)]*", text, re.IGNORECASE):
                assert address.startswith(allowed), (path.name, address)
            if path.suffix == ".css":
                assert "@import" not in text and not re.search(r"url\((?!\s*[\"']?data:)", text), path.name
            else:
                assert not re.search(r"\b(?:fetch|sendBeacon|importScripts)\s*\(|\bimport\s*\(", text), path.name


# --------------------------------------------------------------------------- #
#  The preview routes with the kit                                            #
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def api(tmp_path, bypass_auth, monkeypatch, kit):
    from types import SimpleNamespace

    from fastapi import FastAPI

    import nerve.config as cfgmod
    from nerve.config import NerveConfig
    from nerve.gateway.routes import _deps as deps_module
    from nerve.gateway.routes.preview import router

    workspace = tmp_path / "ws-api"
    workspace.mkdir()
    (workspace / "cost.csv").write_text("a,b\n1,2\n")
    config = NerveConfig.from_dict({"workspace": str(workspace), "artifacts": {"preview": True}})
    monkeypatch.setattr(cfgmod, "_config", config, raising=False)
    monkeypatch.setattr(bundle_module, "KIT_DIR", kit)
    from nerve.gateway.routes import preview as preview_routes

    monkeypatch.setattr(preview_routes, "descriptions", preview_routes.DescriptionCache())
    deps = deps_module.RouteDeps(engine=SimpleNamespace(config=config), db=None, preview_serving=True)
    monkeypatch.setattr(deps_module, "_deps", deps)
    app = FastAPI()
    app.include_router(router)
    bypass_auth(app)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
        http.workspace = workspace
        yield http


@pytest.mark.asyncio
class TestRoutes:
    async def test_the_bundle_of_a_csv_file_and_its_files(self, api):
        body = (await api.get("/api/preview/bundle", params={"path": "cost.csv"})).json()
        assert (body["kind"], body["entry"]) == ("csv", "index.html")
        assert [f["path"] for f in body["files"]] == [CSV_SOURCE, "index.html", "nerve-kit/kit.css", "nerve-kit/table.js"]
        kept = {"path": "cost.csv", "fingerprint": body["fingerprint"]}
        page = await api.get("/api/preview/file", params={**kept, "file": "index.html"})
        assert page.headers["content-disposition"] == "attachment"
        assert _source_of(page.content)["text"] == "a,b\n1,2\n"
        assert len(page.content) == next(f["size"] for f in body["files"] if f["path"] == "index.html")
        source = await api.get("/api/preview/file", params={**kept, "file": CSV_SOURCE})
        assert source.content == b"a,b\n1,2\n"
        script = await api.get("/api/preview/file", params={**kept, "file": "nerve-kit/table.js"})
        assert (script.content, script.headers["content-type"].split(";")[0]) == (b"/* table.js */", "text/javascript")

    async def test_a_changed_source_makes_the_kept_page_stale(self, api):
        body = (await api.get("/api/preview/bundle", params={"path": "cost.csv"})).json()
        (api.workspace / "cost.csv").write_text("a,b\n3,4\n5,6\n")
        kept = {"path": "cost.csv", "fingerprint": body["fingerprint"]}
        for file in ("index.html", CSV_SOURCE):
            r = await api.get("/api/preview/file", params={**kept, "file": file})
            assert (r.status_code, r.json()["detail"]["code"]) == (409, "stale_description")
        # The kit file of the kept description is still the described one.
        assert (await api.get("/api/preview/file", params={**kept, "file": "nerve-kit/table.js"})).status_code == 200
        fresh = (await api.get("/api/preview/bundle", params={"path": "cost.csv"})).json()
        assert fresh["fingerprint"] != body["fingerprint"]
        page = await api.get("/api/preview/file", params={"path": "cost.csv", "fingerprint": fresh["fingerprint"], "file": "index.html"})
        assert _source_of(page.content)["text"] == "a,b\n3,4\n5,6\n"

    async def test_a_kit_file_that_no_page_names_is_not_served(self, api):
        body = (await api.get("/api/preview/bundle", params={"path": "cost.csv"})).json()
        r = await api.get(
            "/api/preview/file", params={"path": "cost.csv", "fingerprint": body["fingerprint"], "file": "nerve-kit/deck.js"},
        )
        assert (r.status_code, r.json()["detail"]["code"]) == (404, "not_in_bundle")


def test_a_cache_put_cannot_remove_the_key_of_a_get_in_another_thread():
    """The preview routes describe bundles in worker threads. A get that read
    its key must move it before a put of another thread can remove it: the
    get here waits between its read and its move while the put runs."""
    import threading
    import time
    from collections import OrderedDict

    in_move = threading.Event()

    class SlowMove(OrderedDict):
        def move_to_end(self, key, last=True):
            if threading.current_thread().name == "reader":
                in_move.set()
                time.sleep(0.2)
            super().move_to_end(key, last)

    recent = bundle_module._Recent(1)
    recent._values = SlowMove()
    recent.put(("a",), 1)
    errors: list[BaseException] = []

    def read() -> None:
        try:
            recent.get(("a",))
        except BaseException as e:  # noqa: BLE001 - the test reports any failure
            errors.append(e)

    reader = threading.Thread(target=read, name="reader")
    reader.start()
    assert in_move.wait(2)
    recent.put(("b",), 2)  # size 1: this put removes ("a",)
    reader.join(5)
    assert errors == []
    assert recent.get(("b",)) == 2
