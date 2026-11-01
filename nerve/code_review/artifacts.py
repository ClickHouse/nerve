"""Write-once review artifacts, separate from task bodies and worker output."""

from __future__ import annotations

import json
import os
from pathlib import Path

from nerve.utils.fs import atomic_write_text

STAGES = ("description.md", "diagrams.md", "review.md", "verdict.json")
SEVERITIES = ("none", "low", "medium", "high", "critical")


def read_output(directory: Path):
    result, errors = {}, []
    for name in STAGES:
        path = directory / name
        try:
            if not path.exists():
                continue
            # Model output is untrusted: refuse symlinks, special files and oversized data.
            import stat
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > 2_000_000:
                raise ValueError(f"unsafe or oversized artifact: {name}")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd) as handle:
                result[name] = handle.read(2_000_001)
        except (OSError, ValueError, UnicodeError) as error:
            errors.append(f"{name}: {error}")
    return result, errors


def validate(output, snapshot):
    if any(not output.get(name, "").strip() for name in STAGES):
        raise ValueError("partial review: required stages are missing")
    verdict = json.loads(output["verdict.json"])
    if not isinstance(verdict, dict):
        raise ValueError("verdict must be a JSON object")
    if verdict.get("head") != snapshot["head"] or verdict.get("base") != snapshot["base"]:
        raise ValueError("review SHA does not match its input version")
    if verdict.get("severity") not in SEVERITIES:
        raise ValueError("invalid review severity")
    if not isinstance(verdict.get("behavior_changed"), bool):
        raise ValueError("behavior_changed must be a boolean")
    for field in ("coverage", "limitations", "compatibility", "enablement", "invariants", "tests", "previous_findings"):
        if not verdict.get(field):
            raise ValueError(f"review lacks {field}")
    if not isinstance(verdict.get("findings"), list):
        raise ValueError("findings must be a list")
    for finding in verdict["findings"]:
        if not isinstance(finding, dict):
            raise ValueError("each finding must be a JSON object")
        if finding.get("severity") not in SEVERITIES[1:] or finding.get("sha") != snapshot["head"]:
            raise ValueError("finding lacks severity or exact head SHA")
        for field in ("path", "line", "condition", "impact", "evidence"):
            if not finding.get(field):
                raise ValueError(f"finding lacks {field}")
        if type(finding["line"]) is not int or finding["line"] <= 0:
            raise ValueError("finding line must be a positive integer")
        if not isinstance(finding["path"], str) or finding["path"].startswith("/") or ".." in Path(finding["path"]).parts:
            raise ValueError("finding path must be repository-relative")
    severity = max((f["severity"] for f in verdict["findings"]), key=SEVERITIES.index, default="none")
    if verdict["severity"] != severity:
        raise ValueError("overall severity must match the most severe finding")
    if "sequenceDiagram" not in output["diagrams.md"]:
        raise ValueError("sequence diagram is missing")
    if verdict.get("behavior_changed") is True and not (
        "rect rgb(220, 252, 231)" in output["diagrams.md"] and "новое" in output["diagrams.md"]
    ):
        raise ValueError("changed behavior needs green rect and the label новое")
    return verdict


def freeze(path, value):
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)
    if path.exists():
        if path.read_text() != text:
            raise ValueError("immutable artifact already exists with different contents")
    else:
        atomic_write_text(path, text, mode=0o400)
    # Read back before the database can advertise a completed artifact.
    if json.loads(path.read_text()) != value:
        raise ValueError("artifact read-back failed")
