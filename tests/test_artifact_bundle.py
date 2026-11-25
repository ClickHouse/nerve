"""The bundle of a workspace path (``nerve/artifacts/bundle.py``): which
files it has, and which paths are refused."""

from __future__ import annotations

import os
import socket

import pytest

from nerve.artifacts import bundle as bundle_module
from nerve.artifacts.bundle import (
    BundleError,
    BundleKind,
    BundleRefusal,
    describe_bundle,
    open_bundle_file,
    read_bundle_file,
)


@pytest.fixture
def ws(tmp_path):
    workspace = tmp_path / "ws"
    deck = workspace / "decks" / "demo"
    (deck / "img").mkdir(parents=True)
    (deck / "index.html").write_text("<!doctype html><link rel=stylesheet href=style.css>")
    (deck / "style.css").write_text("body{color:red}")
    (deck / "img" / "chart.png").write_bytes(b"\x89PNG\r\n")
    (deck / ".notes.txt").write_text("left out")
    (deck / ".cache").mkdir()
    (deck / ".cache" / "x.js").write_text("left out")
    (workspace / "report.html").write_text("<p>one page</p>")
    (workspace / "logo.svg").write_text("<svg xmlns='http://www.w3.org/2000/svg'/>")
    (workspace / "notes.txt").write_text("text")
    return workspace


def _paths(bundle) -> list[str]:
    return [f.path for f in bundle.files]


def _refused(workspace, path) -> BundleRefusal:
    with pytest.raises(BundleError) as caught:
        describe_bundle(workspace, path)
    return caught.value.code


class TestRules:
    def test_a_folder_has_all_regular_files_below_it_and_index_html_as_entry(self, ws):
        b = describe_bundle(ws, "decks/demo")
        assert (b.kind, b.root, b.entry, b.path) == (BundleKind.FOLDER, "decks/demo", "index.html", "decks/demo")
        assert _paths(b) == ["img/chart.png", "index.html", "style.css"]
        assert b.file("style.css").media_type == "text/css"
        assert b.file("img/chart.png").size == 6
        assert b.size == sum(f.size for f in b.files)

    def test_index_html_names_its_folder(self, ws):
        a = describe_bundle(ws, "decks/demo")
        b = describe_bundle(ws, "decks/demo/index.html")
        assert (b.kind, b.root, b.entry) == (BundleKind.FOLDER, "decks/demo", "index.html")
        assert _paths(a) == _paths(b)
        assert a.fingerprint == b.fingerprint

    def test_another_html_file_is_that_one_file(self, ws):
        b = describe_bundle(ws, "report.html")
        assert (b.kind, b.root, b.entry, _paths(b)) == (BundleKind.PAGE, "", "report.html", ["report.html"])
        assert b.root_path == ws.resolve()

    def test_an_image_is_that_one_file(self, ws):
        b = describe_bundle(ws, "logo.svg")
        assert (b.kind, b.entry, _paths(b)) == (BundleKind.IMAGE, "logo.svg", ["logo.svg"])
        assert b.files[0].media_type == "image/svg+xml"

    def test_an_absolute_path_below_the_workspace(self, ws):
        b = describe_bundle(ws, str(ws / "decks" / "demo" / "index.html"))
        assert b.root == "decks/demo"

    def test_a_workspace_reached_through_a_link(self, ws, tmp_path):
        link = tmp_path / "ws-link"
        link.symlink_to(ws)
        assert describe_bundle(link, str(link / "report.html")).entry == "report.html"
        assert describe_bundle(link, str(ws / "report.html")).entry == "report.html"

    def test_the_fingerprint_changes_when_a_file_changes(self, ws):
        before = describe_bundle(ws, "decks/demo").fingerprint
        assert describe_bundle(ws, "decks/demo").fingerprint == before
        css = ws / "decks" / "demo" / "style.css"
        st = css.stat()
        css.write_text("body{color:blue}")
        os.utime(css, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
        assert describe_bundle(ws, "decks/demo").fingerprint != before

    def test_the_fingerprint_changes_for_a_same_size_replacement_with_the_mtime_restored(self, ws):
        """A copy that keeps the timestamps (``cp -p``, a build tool): same
        size, same mtime, other bytes."""
        css = ws / "decks" / "demo" / "style.css"
        before = describe_bundle(ws, "decks/demo").fingerprint
        st = css.stat()
        replacement = ws / "decks" / "demo" / "style.tmp"
        replacement.write_text("body{color:tan}")
        assert replacement.stat().st_size == st.st_size
        os.replace(replacement, css)
        os.utime(css, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert css.stat().st_mtime_ns == st.st_mtime_ns
        assert describe_bundle(ws, "decks/demo").fingerprint != before

    def test_the_fingerprint_changes_for_a_same_size_write_in_place_with_the_mtime_restored(self, ws):
        css = ws / "decks" / "demo" / "style.css"
        before = describe_bundle(ws, "decks/demo").fingerprint
        st = css.stat()
        css.write_text("body{color:tan}")
        os.utime(css, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert describe_bundle(ws, "decks/demo").fingerprint != before

    def test_the_fingerprint_changes_when_a_file_is_added(self, ws):
        before = describe_bundle(ws, "decks/demo").fingerprint
        (ws / "decks" / "demo" / "app.js").write_text("1")
        assert describe_bundle(ws, "decks/demo").fingerprint != before

    def test_a_hidden_file_changes_nothing(self, ws):
        before = describe_bundle(ws, "decks/demo").fingerprint
        (ws / "decks" / "demo" / ".swap").write_text("1")
        assert describe_bundle(ws, "decks/demo").fingerprint == before


class TestRefusals:
    @pytest.mark.parametrize("path", ["../outside.html", "decks/../../outside.html", "/etc/hosts", "/"])
    def test_outside_the_workspace(self, ws, path):
        assert _refused(ws, path) is BundleRefusal.OUTSIDE_WORKSPACE

    @pytest.mark.parametrize("path", [".", "decks/..", "index.html"])
    def test_the_workspace_root(self, ws, path):
        (ws / "index.html").write_text("root page")
        assert _refused(ws, path) is BundleRefusal.WORKSPACE_ROOT

    def test_the_workspace_root_as_an_absolute_path(self, ws):
        assert _refused(ws, str(ws)) is BundleRefusal.WORKSPACE_ROOT

    @pytest.mark.parametrize("path", ["", "  ", "a\x00b.html"])
    def test_an_empty_or_invalid_path(self, ws, path):
        assert _refused(ws, path) is BundleRefusal.INVALID_PATH

    def test_a_missing_path(self, ws):
        assert _refused(ws, "nothing.html") is BundleRefusal.NOT_FOUND
        assert _refused(ws, "report.html/x.html") is BundleRefusal.NOT_FOUND

    def test_a_file_of_another_type(self, ws):
        assert _refused(ws, "notes.txt") is BundleRefusal.UNSUPPORTED

    def test_a_folder_without_index_html(self, ws):
        (ws / "empty").mkdir()
        assert _refused(ws, "empty") is BundleRefusal.NO_ENTRY

    @pytest.mark.parametrize("path", [".hidden.html", ".site/index.html", "decks/demo/.notes.txt"])
    def test_a_hidden_path(self, ws, path):
        (ws / ".site").mkdir()
        (ws / ".site" / "index.html").write_text("x")
        (ws / ".hidden.html").write_text("x")
        assert _refused(ws, path) is BundleRefusal.HIDDEN

    def test_a_link_as_the_path(self, ws, tmp_path):
        (tmp_path / "secret.html").write_text("outside")
        (ws / "linked.html").symlink_to(tmp_path / "secret.html")
        assert _refused(ws, "linked.html") is BundleRefusal.LINK

    def test_a_link_on_the_way(self, ws):
        (ws / "via").symlink_to(ws / "decks")
        assert _refused(ws, "via/demo") is BundleRefusal.LINK

    def test_a_link_in_the_folder(self, ws, tmp_path):
        (tmp_path / "secret.txt").write_text("outside")
        (ws / "decks" / "demo" / "data.txt").symlink_to(tmp_path / "secret.txt")
        assert _refused(ws, "decks/demo") is BundleRefusal.LINK

    def test_a_linked_folder_in_the_folder(self, ws, tmp_path):
        (tmp_path / "other").mkdir()
        (ws / "decks" / "demo" / "more").symlink_to(tmp_path / "other")
        assert _refused(ws, "decks/demo") is BundleRefusal.LINK

    def test_a_special_file_in_the_folder(self, ws):
        os.mkfifo(ws / "decks" / "demo" / "pipe")
        assert _refused(ws, "decks/demo") is BundleRefusal.SPECIAL_FILE

    def test_a_socket_in_the_folder(self, ws, tmp_path):
        # A short path: a socket address has a small limit.
        path = ws / "decks" / "demo" / "s"
        with socket.socket(socket.AF_UNIX) as server:
            try:
                server.bind(str(path))
            except OSError:
                pytest.skip("this system binds no socket at that path")
            assert _refused(ws, "decks/demo") is BundleRefusal.SPECIAL_FILE

    def test_a_special_file_as_the_path(self, ws):
        os.mkfifo(ws / "pipe.html")
        assert _refused(ws, "pipe.html") is BundleRefusal.SPECIAL_FILE

    def test_a_file_too_large(self, ws, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_FILE_BYTES", 4)
        assert _refused(ws, "report.html") is BundleRefusal.FILE_TOO_LARGE
        assert _refused(ws, "decks/demo") is BundleRefusal.FILE_TOO_LARGE

    def test_too_many_files(self, ws, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_FILES", 2)
        assert _refused(ws, "decks/demo") is BundleRefusal.TOO_MANY_FILES

    def test_too_many_entries_also_left_out_ones(self, ws, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_SCANNED_ENTRIES", 3)
        assert _refused(ws, "decks/demo") is BundleRefusal.TOO_MANY_FILES

    def test_a_large_folder_is_not_read_past_the_allowance(self, ws, monkeypatch):
        """The walk takes at most the allowance plus one entry from the
        directory iterator, and refuses then: it does not read and sort the
        whole folder first."""
        monkeypatch.setattr(bundle_module, "MAX_SCANNED_ENTRIES", 50)
        real_scandir = os.scandir
        taken = {"count": 0}

        class Counting:
            def __init__(self, path):
                self.inner = real_scandir(path)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self.inner.close()

            def __iter__(self):
                for entry in self.inner:
                    taken["count"] += 1
                    yield entry

        flat = ws / "flat"
        flat.mkdir()
        (flat / "index.html").write_text("x")
        for i in range(500):
            (flat / f"f{i:04}.png").write_bytes(b"")
        monkeypatch.setattr(bundle_module.os, "scandir", Counting)
        assert _refused(ws, "flat") is BundleRefusal.TOO_MANY_FILES
        assert taken["count"] == 51

    def test_a_path_too_long(self, ws, monkeypatch):
        monkeypatch.setattr(bundle_module, "MAX_PATH_BYTES", 8)
        assert _refused(ws, "decks/demo") is BundleRefusal.PATH_TOO_LONG

    def test_a_request_path_too_long(self, ws):
        assert _refused(ws, "a/" * 3000 + "x.html") is BundleRefusal.PATH_TOO_LONG

    def test_a_name_with_a_backslash(self, ws):
        (ws / "decks" / "demo" / "a\\b.css").write_text("x")
        assert _refused(ws, "decks/demo") is BundleRefusal.INVALID_PATH


class TestOpen:
    def test_reads_a_file_of_the_bundle(self, ws):
        b = describe_bundle(ws, "decks/demo")
        assert read_bundle_file(b, "style.css") == b"body{color:red}"
        with open_bundle_file(b, "img/chart.png") as handle:
            assert handle.read() == b"\x89PNG\r\n"

    def test_refuses_a_file_that_is_not_in_the_description(self, ws):
        b = describe_bundle(ws, "decks/demo")
        for path in (".notes.txt", "../../report.html", "missing.css", ".cache/x.js"):
            with pytest.raises(BundleError) as caught:
                read_bundle_file(b, path)
            assert caught.value.code is BundleRefusal.NOT_FOUND

    def test_does_not_follow_a_link_that_appeared_after_the_description(self, ws, tmp_path):
        b = describe_bundle(ws, "decks/demo")
        (tmp_path / "secret.css").write_text("outside")
        css = ws / "decks" / "demo" / "style.css"
        css.unlink()
        css.symlink_to(tmp_path / "secret.css")
        with pytest.raises(BundleError):
            read_bundle_file(b, "style.css")

    def test_does_not_follow_a_linked_folder_that_appeared_after_the_description(self, ws, tmp_path):
        b = describe_bundle(ws, "decks/demo")
        (tmp_path / "img").mkdir()
        (tmp_path / "img" / "chart.png").write_bytes(b"outside")
        img = ws / "decks" / "demo" / "img"
        (img / "chart.png").unlink()
        img.rmdir()
        img.symlink_to(tmp_path / "img")
        with pytest.raises(BundleError):
            read_bundle_file(b, "img/chart.png")

    def test_refuses_a_fifo_that_took_the_place_of_the_file(self, ws):
        b = describe_bundle(ws, "decks/demo")
        css = ws / "decks" / "demo" / "style.css"
        css.unlink()
        os.mkfifo(css)
        with pytest.raises(BundleError) as caught:
            read_bundle_file(b, "style.css")
        assert caught.value.code is BundleRefusal.SPECIAL_FILE

    @pytest.mark.parametrize("change", ["grow", "same_size_restored_mtime", "replace"])
    def test_refuses_a_file_that_is_not_the_described_one(self, ws, change):
        b = describe_bundle(ws, "decks/demo")
        css = ws / "decks" / "demo" / "style.css"
        st = css.stat()
        if change == "grow":
            css.write_text("body{color:red}" * 1000)
        elif change == "same_size_restored_mtime":
            css.write_text("body{color:tan}")
            os.utime(css, ns=(st.st_atime_ns, st.st_mtime_ns))
        else:
            other = css.with_name("other.tmp")
            other.write_text("body{color:red}")
            os.utime(other, ns=(st.st_atime_ns, st.st_mtime_ns))
            os.replace(other, css)
        with pytest.raises(BundleError) as caught:
            read_bundle_file(b, "style.css")
        assert caught.value.code is BundleRefusal.CHANGED

    def test_refuses_more_than_the_limit(self, ws):
        b = describe_bundle(ws, "decks/demo")
        with pytest.raises(BundleError) as caught:
            read_bundle_file(b, "style.css", max_bytes=4)
        assert caught.value.code is BundleRefusal.FILE_TOO_LARGE
