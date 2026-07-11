#!/usr/bin/env python3
"""Verify the installed Codex app-server schema against Nerve's contract."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
META_PATH = ROOT / "tests" / "fixtures" / "codex_schema_meta.json"


def canonical_hash(path: Path) -> str:
    payload = json.loads(path.read_text(encoding="utf-8"))
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def main() -> int:
    meta = json.loads(META_PATH.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="nerve-codex-schema-") as tmp:
        subprocess.run(
            ["codex", "app-server", "generate-json-schema", "--out", tmp],
            check=True,
        )
        schema_path = Path(tmp) / "codex_app_server_protocol.v2.schemas.json"
        digest = canonical_hash(schema_path)
        if digest != meta["canonical_v2_schema_sha256"]:
            raise SystemExit(
                "Codex app-server schema changed: "
                f"expected {meta['canonical_v2_schema_sha256']}, got {digest}"
            )
        text = schema_path.read_text(encoding="utf-8")
        missing = [method for method in meta["required_methods"] if method not in text]
        if missing:
            raise SystemExit(f"Codex schema lacks required methods: {missing}")
    print(f"Codex schema contract OK ({digest})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
