"""scripts/check_migration_numbers.py: added migrations must be numbered above the base's highest."""

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from nerve.db.migrations.runner import discover_migrations

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


def test_reads_the_versions_the_runner_applies():
    names = [p.name for p in (check.ROOT / check.MIGRATIONS_DIR).iterdir()]
    found = {n.removesuffix(".py"): v for n, v in check.versions(names).items()}
    assert found == {name: v for v, name in discover_migrations()}


@pytest.mark.parametrize("added, rc", [("v004_x.py", 1), ("v005_x.py", 0)])
def test_cli_against_a_git_base(tmp_path, added, rc):
    if not shutil.which("git"):
        pytest.skip("git not available")
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t"]
    mig = tmp_path / check.MIGRATIONS_DIR
    mig.mkdir(parents=True)
    # git ls-tree quotes a non-ASCII name unless -z is given.
    for name in ("v001_a.py", "v004_\u00e9.py"):
        (mig / name).write_text("")
    subprocess.run(git + ["init", "-q"], cwd=tmp_path, env=env, check=True)
    subprocess.run(git + ["add", "-A"], cwd=tmp_path, env=env, check=True)
    subprocess.run(git + ["commit", "-qm", "base"], cwd=tmp_path, env=env, check=True)
    (tmp_path / "scripts").mkdir()
    shutil.copy(_SCRIPT, tmp_path / "scripts")
    (mig / added).write_text("")
    r = subprocess.run(
        [sys.executable, "scripts/check_migration_numbers.py", "HEAD"],
        cwd=tmp_path, env=env, capture_output=True, text=True,
    )
    assert r.returncode == rc, r.stderr
    assert (added in r.stderr) if rc else ("highest on HEAD: v004" in r.stdout)
