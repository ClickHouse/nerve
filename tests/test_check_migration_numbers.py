"""scripts/check_migration_numbers.py: added migrations must be numbered above the
base's highest, no two migrations of a track share a number, and upstream ships no
fork migration."""

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from nerve.db.migrations.runner import FORK_TRACK, discover_migrations

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_migration_numbers.py"
_spec = importlib.util.spec_from_file_location("check_migration_numbers", _SCRIPT)
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)

BASE = ["__init__.py", "runner.py", "v001_a.py", "v003_c.py"]


@pytest.mark.parametrize("added, bad", [
    ("v002_b.py", ["v002_b.py"]),
    ("v003_x.py", ["v003_x.py"]),
    ("v004_d.py", []),
    ("v010_e.py", []),  # gaps are fine
])
def test_added_migration_must_be_above_the_base_highest(added, bad):
    assert check.misnumbered(BASE, BASE + [added]) == (3, bad)


def test_migrations_already_on_the_base_are_not_checked():
    assert check.misnumbered(BASE, BASE) == (3, [])


@pytest.mark.parametrize("base, head", [(["runner.py"], BASE), (BASE, ["runner.py"])])
def test_no_migrations_on_either_side_fails(base, head):
    with pytest.raises(SystemExit):
        check.misnumbered(base, head)


@pytest.mark.parametrize("track, directory", [
    ("upstream", check.MIGRATIONS_DIR), ("fork", check.FORK_DIR),
])
def test_reads_the_versions_the_runner_applies(track, directory):
    names = [p.name for p in (check.ROOT / directory).iterdir()]
    found = {n.removesuffix(".py"): v for n, v in check.versions(names).items()}
    applied = discover_migrations() if track == "upstream" else discover_migrations(FORK_TRACK)
    assert found == {name: v for v, name in applied}


@pytest.mark.parametrize("names, found", [
    (BASE, {}),
    (BASE + ["v003_x.py"], {3: ["v003_c.py", "v003_x.py"]}),
    (["v001_a.py", "v001_b.py", "v001_c.py", "v2_d.py", "v002_e.py"],
     {1: ["v001_a.py", "v001_b.py", "v001_c.py"], 2: ["v002_e.py", "v2_d.py"]}),
])
def test_duplicate_numbers(names, found):
    assert check.duplicates(names) == found


def _git_repo(tmp_path, files):
    """A repository whose base commit holds ``files`` and the script."""
    if not shutil.which("git"):
        pytest.skip("git not available")
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t"]
    for path in files:
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text("")
    subprocess.run(git + ["init", "-q"], cwd=tmp_path, env=env, check=True)
    subprocess.run(git + ["add", "-A"], cwd=tmp_path, env=env, check=True)
    subprocess.run(git + ["commit", "-qm", "base"], cwd=tmp_path, env=env, check=True)
    (tmp_path / "scripts").mkdir()
    shutil.copy(_SCRIPT, tmp_path / "scripts")

    def run(*args):
        return subprocess.run(
            [sys.executable, "scripts/check_migration_numbers.py", *args],
            cwd=tmp_path, env=env, capture_output=True, text=True,
        )

    return run


@pytest.mark.parametrize("added, rc", [("v004_x.py", 1), ("v005_x.py", 0)])
def test_cli_against_a_git_base(tmp_path, added, rc):
    mig = check.MIGRATIONS_DIR
    # git ls-tree quotes a non-ASCII name unless -z is given.
    run = _git_repo(tmp_path, [f"{mig}/v001_a.py", f"{mig}/v004_\u00e9.py"])
    (tmp_path / mig / added).write_text("")
    r = run("HEAD")
    assert r.returncode == rc, r.stderr
    assert (added in r.stderr) if rc else ("highest on HEAD: v004" in r.stdout)


_UPSTREAM_BASE = [
    f"{check.MIGRATIONS_DIR}/v001_a.py",
    f"{check.MIGRATIONS_DIR}/v004_b.py",
    f"{check.FORK_DIR}/__init__.py",
    f"{check.FORK_DIR}/README.md",
]


@pytest.mark.parametrize("args", [["HEAD"], ["--fork", "HEAD"]])
def test_a_fork_directory_with_only_its_package_files_passes(tmp_path, args):
    r = _git_repo(tmp_path, _UPSTREAM_BASE)(*args)
    assert r.returncode == 0, r.stderr
    assert "highest on HEAD: v004" in r.stdout


@pytest.mark.parametrize("args, rc", [(["HEAD"], 1), (["--fork", "HEAD"], 0)])
def test_upstream_ships_no_fork_migration(tmp_path, args, rc):
    """Without ``--fork`` (upstream, and CI) any fork migration fails; a fork of
    Nerve passes ``--fork`` for its own."""
    run = _git_repo(tmp_path, _UPSTREAM_BASE)
    (tmp_path / check.FORK_DIR / "v001_mine.py").write_text("")
    r = run(*args)
    assert r.returncode == rc, r.stderr + r.stdout
    if rc:
        assert f"{check.FORK_DIR}/v001_mine.py: upstream ships no fork migration" in r.stderr
        assert "above v004" in r.stderr
    else:
        assert r.stderr == ""


@pytest.mark.parametrize("directory, args", [
    (check.FORK_DIR, ["--fork", "HEAD"]),
    (check.MIGRATIONS_DIR, ["HEAD"]),
])
def test_two_migrations_with_one_number_fail(tmp_path, directory, args):
    run = _git_repo(tmp_path, _UPSTREAM_BASE)
    for name in ("v007_one.py", "v007_two.py"):
        (tmp_path / directory / name).write_text("")
    r = run(*args)
    assert r.returncode == 1
    assert f"{directory}: v007_one.py, v007_two.py share v007" in r.stderr


def test_the_fork_directory_may_be_absent(tmp_path):
    """A base from before the fork track."""
    r = _git_repo(tmp_path, [f"{check.MIGRATIONS_DIR}/v001_a.py"])("HEAD")
    assert r.returncode == 0, r.stderr
