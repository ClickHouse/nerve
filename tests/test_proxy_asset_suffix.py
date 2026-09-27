"""The release asset the proxy downloader picks out of a CLIProxyAPI release.

Two ways this went wrong:

*Wrong name.* CLIProxyAPI publishes 64-bit ARM builds as ``aarch64``; the
downloader asked for ``arm64`` and so found nothing on exactly the two platforms
Nerve is most likely to run on — an Apple Silicon Mac and an ARM VPS. The
failure surfaced during ``nerve init`` as "No CLIProxyAPI asset found for
darwin_arm64", which reads like the build is missing rather than misnamed, and
pushed the operator toward buying a separate API key.

*Wrong build.* Each platform ships twice — a full build and a stripped
``_no-plugin`` one — and the suffix is a substring of both names. The match had
no tiebreak, so the winner was whichever GitHub listed first: the no-plugin
build, in every one of the last five releases.

Names verified against release v7.2.131.
"""

from __future__ import annotations

import pytest

from nerve.proxy.service import _detect_asset_suffix, _select_asset_url

# The tar.gz builds the downloader can actually install. Windows assets ship as
# .zip, which the downloader cannot open — and _detect_asset_suffix never
# returns a windows suffix — so they are deliberately absent.
PUBLISHED = {
    "darwin_aarch64", "darwin_amd64",
    "linux_aarch64", "linux_amd64",
    "freebsd_amd64",
}

VERSION = "7.2.131"


def _asset(name: str) -> dict[str, str]:
    return {
        "name": name,
        "browser_download_url": f"https://example.invalid/{name}",
    }


@pytest.mark.parametrize(
    "system,machine,expected",
    [
        ("Darwin", "arm64", "darwin_aarch64"),
        ("Darwin", "aarch64", "darwin_aarch64"),
        ("Darwin", "x86_64", "darwin_amd64"),
        ("Linux", "aarch64", "linux_aarch64"),
        ("Linux", "arm64", "linux_aarch64"),
        ("Linux", "x86_64", "linux_amd64"),
    ],
)
def test_suffix_matches_a_published_asset(monkeypatch, system, machine, expected):
    monkeypatch.setattr("platform.system", lambda: system)
    monkeypatch.setattr("platform.machine", lambda: machine)

    suffix = _detect_asset_suffix()
    assert suffix == expected
    assert suffix in PUBLISHED, f"{suffix} is not a name the project publishes"


def test_unsupported_platform_still_raises(monkeypatch):
    monkeypatch.setattr("platform.system", lambda: "SunOS")
    monkeypatch.setattr("platform.machine", lambda: "sparc")
    with pytest.raises(RuntimeError):
        _detect_asset_suffix()


def test_windows_is_unsupported_not_silently_mismatched(monkeypatch):
    """Windows ships .zip only, so it must fail loudly rather than half-work."""
    monkeypatch.setattr("platform.system", lambda: "Windows")
    monkeypatch.setattr("platform.machine", lambda: "AMD64")
    with pytest.raises(RuntimeError):
        _detect_asset_suffix()


@pytest.mark.parametrize("suffix", ["linux_aarch64", "linux_amd64", "darwin_aarch64"])
@pytest.mark.parametrize("no_plugin_first", [True, False])
def test_full_build_wins_over_no_plugin_in_either_order(suffix, no_plugin_first):
    full = _asset(f"CLIProxyAPI_{VERSION}_{suffix}.tar.gz")
    stripped = _asset(f"CLIProxyAPI_{VERSION}_{suffix}_no-plugin.tar.gz")
    pair = [stripped, full] if no_plugin_first else [full, stripped]

    # Padded with the other platforms so the match cannot succeed by luck.
    assets = [
        _asset(f"CLIProxyAPI_{VERSION}_{other}.tar.gz")
        for other in sorted(PUBLISHED - {suffix})
    ]
    assets += pair
    assets.append(_asset(f"CLIProxyAPI_{VERSION}_windows_amd64.zip"))
    assets.append(_asset("checksums.txt"))

    assert _select_asset_url(assets, suffix) == full["browser_download_url"]


def test_missing_asset_returns_none():
    assets = [_asset(f"CLIProxyAPI_{VERSION}_linux_amd64.tar.gz")]
    assert _select_asset_url(assets, "linux_aarch64") is None
