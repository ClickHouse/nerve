"""The worker guide that the onboarding conversation is given in full.

It is read from the checkout's ``docs/`` with a packaged copy as the fallback
for wheel installs, and the two copies must not drift. The conversation itself
is tested in ``tests/test_onboarding.py``.
"""

from __future__ import annotations

from pathlib import Path

from nerve import workspace as workspace_module
from nerve.workspace import read_worker_guide, worker_guide_paths

REPO = Path(__file__).resolve().parent.parent
SOURCE_GUIDE = REPO / "docs" / "worker-best-practices.md"
PACKAGED_GUIDE = REPO / "nerve" / "templates" / "guides" / "worker-best-practices.md"


class TestGuideCopy:
    def test_packaged_copy_is_byte_identical_to_the_source(self) -> None:
        assert SOURCE_GUIDE.read_bytes() == PACKAGED_GUIDE.read_bytes(), (
            "docs/worker-best-practices.md and nerve/templates/guides/worker-best-practices.md "
            "differ. The docs file is the source: copy it over the packaged one."
        )

    def test_the_source_copy_is_read_first(self) -> None:
        source, packaged = worker_guide_paths()
        assert (source, packaged) == (SOURCE_GUIDE, PACKAGED_GUIDE)
        assert read_worker_guide() == SOURCE_GUIDE.read_text(encoding="utf-8")

    def test_falls_back_to_the_packaged_copy(self, tmp_path: Path, monkeypatch) -> None:
        """A wheel install has no ``docs/`` beside the package."""
        monkeypatch.setattr(
            workspace_module, "worker_guide_paths",
            lambda: (tmp_path / "docs" / "worker-best-practices.md", PACKAGED_GUIDE),
        )
        assert read_worker_guide() == PACKAGED_GUIDE.read_text(encoding="utf-8")
        monkeypatch.setattr(
            workspace_module, "worker_guide_paths",
            lambda: (tmp_path / "missing-a.md", tmp_path / "missing-b.md"),
        )
        assert read_worker_guide() == ""
