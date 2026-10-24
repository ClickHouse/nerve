#!/usr/bin/env python3
"""Check how this branch numbers its migrations against a base revision.

There are two migration tracks (see nerve/db/migrations/fork/README.md):

* nerve/db/migrations/: a migration added on this branch must be numbered above
  the base's highest. The runner skips every migration at or below the
  database's current version, so such a migration never runs on a database
  already migrated to the base.
* nerve/db/migrations/fork/: upstream never ships a migration here, so any
  migration fails the check. With --fork the checkout is a fork of Nerve, whose
  own migrations belong there.

In each track, two migrations must not share a number: a database that already
ran one of them skips the others.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = "nerve/db/migrations"
FORK_DIR = f"{MIGRATIONS_DIR}/fork"
# The file names discover_migrations() turns into versions.
_MIGRATION = re.compile(r"v(\d+)_.*\.py")


def versions(names: list[str]) -> dict[str, int]:
    return {n: int(m.group(1)) for n in names if (m := _MIGRATION.fullmatch(n))}


def misnumbered(base_names: list[str], head_names: list[str]) -> tuple[int, list[str]]:
    """Return the base's highest version and the added migrations numbered at or below it."""
    base, head = versions(base_names), versions(head_names)
    if not base or not head:
        raise SystemExit("No migrations found: wrong base revision or working tree?")
    top = max(base.values())
    return top, sorted(n for n, v in head.items() if n not in base and v <= top)


def duplicates(names: list[str]) -> dict[int, list[str]]:
    """Map each number that more than one migration uses to those migrations."""
    by_version: dict[int, list[str]] = {}
    for name, version in sorted(versions(names).items()):
        by_version.setdefault(version, []).append(name)
    return {v: group for v, group in by_version.items() if len(group) > 1}


def _names(directory: Path) -> list[str]:
    return [p.name for p in directory.iterdir()] if directory.is_dir() else []


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "base_rev", nargs="?", default="origin/main",
        help="the revision to compare with (default: origin/main)",
    )
    parser.add_argument(
        "--fork", action="store_true",
        help="this checkout is a fork of Nerve: allow its own migrations in fork/",
    )
    args = parser.parse_args()

    base_names = subprocess.run(
        ["git", "ls-tree", "-z", "--name-only", f"{args.base_rev}:{MIGRATIONS_DIR}"],
        cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True,
    ).stdout.split("\0")
    head_names = _names(ROOT / MIGRATIONS_DIR)
    fork_names = _names(ROOT / FORK_DIR)
    top, bad = misnumbered(base_names, head_names)

    problems = [
        f"{MIGRATIONS_DIR}/{name}: renumber above v{top:03d}, the highest migration on "
        f"{args.base_rev}; a database already at v{top:03d} never runs it."
        for name in bad
    ]
    for directory, names in ((MIGRATIONS_DIR, head_names), (FORK_DIR, fork_names)):
        problems += [
            f"{directory}: {', '.join(group)} share v{version:03d}; a database that "
            f"already ran one of them skips the others."
            for version, group in duplicates(names).items()
        ]
    if not args.fork:
        problems += [
            f"{FORK_DIR}/{name}: upstream ships no fork migration; number it above "
            f"v{top:03d} in {MIGRATIONS_DIR} instead (a fork of Nerve passes --fork)."
            for name in sorted(versions(fork_names))
        ]
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    print(f"Migration numbers OK (highest on {args.base_rev}: v{top:03d})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
