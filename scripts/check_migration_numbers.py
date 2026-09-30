#!/usr/bin/env python3
"""Fail when a migration added on this branch is not numbered above the base's highest.

The runner skips every migration at or below the database's current version, so
such a migration never runs on a database already migrated to the base.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = "nerve/db/migrations"
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


def main() -> int:
    base_rev = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    base_names = subprocess.run(
        ["git", "ls-tree", "-z", "--name-only", f"{base_rev}:{MIGRATIONS_DIR}"],
        cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True,
    ).stdout.split("\0")
    head_names = [p.name for p in (ROOT / MIGRATIONS_DIR).iterdir()]
    top, bad = misnumbered(base_names, head_names)
    for name in bad:
        print(
            f"{MIGRATIONS_DIR}/{name}: renumber above v{top:03d}, the highest migration on "
            f"{base_rev}; a database already at v{top:03d} never runs it.",
            file=sys.stderr,
        )
    if bad:
        return 1
    print(f"Migration numbers OK (highest on {base_rev}: v{top:03d})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
