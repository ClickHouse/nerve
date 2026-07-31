"""Tests for cron hot-reload and the reserved job-id namespace."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
import yaml

from nerve.cron.service import CronService
from nerve.cron.jobs import CronJob, is_reserved_job_id


def _write_jobs(path: Path, jobs: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")


@pytest_asyncio.fixture
async def svc(tmp_path):
    cron_dir = tmp_path / "cron"
    cron_dir.mkdir(parents=True)
    jobs_file = cron_dir / "jobs.yaml"
    _write_jobs(jobs_file, [])

    config = MagicMock()
    config.timezone = "UTC"
    config.cron.jobs_file = jobs_file
    config.cron.system_file = cron_dir / "system.yaml"  # absent
    config.cron.gate_plugins_dir = cron_dir / "gates"    # absent → no-op

    db = AsyncMock()
    db.get_last_successful_cron_run = AsyncMock(return_value=None)

    service = CronService(config, AsyncMock(), db)
    # Start the scheduler paused so add/remove/replace_existing hit the real
    # jobstore (as in production) without actually firing jobs. An unstarted
    # scheduler keeps jobs in a "pending" list where replace_existing is a no-op.
    service.scheduler.start(paused=True)
    try:
        yield service, jobs_file
    finally:
        service.scheduler.shutdown(wait=False)


def _job_dict(job_id="j1", schedule="1h", **kw):
    return {"id": job_id, "schedule": schedule, "prompt": "do stuff", **kw}


# A gate plugin, for the reload paths that have to keep the registry in step with
# the schedule. It blocks, so a job that loses it goes from never running to
# running every time.
_GATE_PLUGIN = '''
from nerve.cron.gates import CronGate


class BlockingGate(CronGate):
    type = "reload_test"

    async def is_satisfied(self, ctx):
        return False

    def describe(self):
        return "blocking (test plugin)"

    @classmethod
    def from_config(cls, spec):
        return cls()
'''


def _write_gate_plugin(gates_dir: Path) -> Path:
    """Drop the plugin above into *gates_dir* (the svc fixture's plugins dir)."""
    gates_dir.mkdir(parents=True, exist_ok=True)
    plugin = gates_dir / "blocking.py"
    plugin.write_text(_GATE_PLUGIN, encoding="utf-8")
    return plugin


def _gated_job(job_id: str, **kw) -> dict:
    return _job_dict(job_id, run_if=[{"type": "reload_test"}], **kw)


def _source_runner(name: str):
    return SimpleNamespace(
        job_id=f"source:{name}",
        source=SimpleNamespace(source_name=name),
        set_notification_service=lambda *a, **k: None,
    )


class TestReload:
    @pytest.mark.asyncio
    async def test_add_job(self, svc):
        service, jobs_file = svc
        result = await service.reload()  # empty → nothing
        assert result["added"] == [] and result["enabled"] == 0

        _write_jobs(jobs_file, [_job_dict("j1")])
        result = await service.reload()
        assert result["added"] == ["j1"]
        assert service.scheduler.get_job("j1") is not None
        assert result["enabled"] == 1

    @pytest.mark.asyncio
    async def test_remove_job(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1")])
        await service.reload()
        assert service.scheduler.get_job("j1") is not None

        _write_jobs(jobs_file, [])
        result = await service.reload()
        assert result["removed"] == ["j1"]
        assert service.scheduler.get_job("j1") is None

    @pytest.mark.asyncio
    async def test_disable_job_unschedules(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1")])
        await service.reload()

        _write_jobs(jobs_file, [_job_dict("j1", enabled=False)])
        result = await service.reload()
        # Still present in config (so not "removed"), but unscheduled + not enabled.
        assert result["removed"] == []
        assert "j1" not in result["added"] and "j1" not in result["updated"]
        assert service.scheduler.get_job("j1") is None
        assert result["enabled"] == 0

    @pytest.mark.asyncio
    async def test_reenable_job(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1", enabled=False)])
        await service.reload()
        assert service.scheduler.get_job("j1") is None

        _write_jobs(jobs_file, [_job_dict("j1", enabled=True)])
        result = await service.reload()
        assert result["added"] == ["j1"]
        assert service.scheduler.get_job("j1") is not None

    @pytest.mark.asyncio
    async def test_reschedule_job_reports_updated(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1", schedule="1h")])
        await service.reload()
        first_trigger = str(service.scheduler.get_job("j1").trigger)

        _write_jobs(jobs_file, [_job_dict("j1", schedule="0 3 * * *")])
        result = await service.reload()
        assert result["updated"] == ["j1"]
        assert str(service.scheduler.get_job("j1").trigger) != first_trigger

    @pytest.mark.asyncio
    async def test_identical_file_still_reschedules(self, svc):
        """A reload rebuilds every enabled job rather than diffing against the
        previous config, so an unchanged file reports its jobs as updated."""
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1")])
        await service.reload()

        result = await service.reload()  # identical file
        assert result == {
            "added": [], "removed": [], "updated": ["j1"], "enabled": 1,
            "rejected": [],
        }
        assert service.scheduler.get_job("j1") is not None


class TestReloadSafety:
    @pytest.mark.asyncio
    async def test_new_arrives_disabled_is_noop(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1", enabled=False)])
        result = await service.reload()
        assert result == {
            "added": [], "removed": [], "updated": [], "enabled": 0, "rejected": [],
        }
        assert service.scheduler.get_job("j1") is None

    @pytest.mark.asyncio
    async def test_disabled_stays_disabled_no_remove(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1", enabled=False)])
        await service.reload()
        # Reload again, still disabled — must not try to remove a job that was
        # never scheduled (the get_job guard), and must not report it removed.
        result = await service.reload()
        assert result["removed"] == []

    @pytest.mark.asyncio
    async def test_edited_definition_reaches_the_scheduler(self, svc):
        """The scheduler runs the CronJob object it holds, so a reload has to
        replace that object and not just the copy in ``_jobs``. Uses a workflow
        block because it decides what the run actually does — engine, prompt and
        budget — and none of it is re-read at fire time."""
        service, jobs_file = svc
        w = {"engine": "claude", "prompt": "go", "budget_usd": 1.0}
        _write_jobs(jobs_file, [_job_dict("j1", workflow=w)])
        await service.reload()
        _write_jobs(
            jobs_file, [_job_dict("j1", workflow={**w, "budget_usd": 25.0})],
        )
        result = await service.reload()
        assert result["updated"] == ["j1"]
        assert service.scheduler.get_job("j1").args[0].workflow["budget_usd"] == 25.0
        assert service._jobs[0].workflow["budget_usd"] == 25.0

    @pytest.mark.asyncio
    async def test_reserved_id_collision_ignored(self, svc):
        service, jobs_file = svc
        # Register a fake source runner id + rely on cleanup/wakeup_sweep.
        fake_runner = MagicMock()
        fake_runner.job_id = "source:gmail"
        service._source_runners = [fake_runner]
        # Simulate the internal cleanup job already occupying the slot.
        service.scheduler.add_job(
            lambda: None, "interval", seconds=3600, id="cleanup",
        )

        _write_jobs(jobs_file, [
            _job_dict("cleanup", schedule="1h"),
            _job_dict("source:gmail", schedule="1h"),
            _job_dict("legit", schedule="1h"),
        ])
        result = await service.reload()
        # Only the non-reserved job is scheduled; reserved ids are ignored.
        assert result["added"] == ["legit"]
        assert service.scheduler.get_job("legit") is not None
        # The internal cleanup job is untouched (still the lambda interval job).
        assert service.scheduler.get_job("cleanup") is not None

    @pytest.mark.asyncio
    async def test_reserved_removal_does_not_delete_internal_job(self, svc):
        service, jobs_file = svc
        # User previously had a 'cleanup' cron in self._jobs (as start would keep).
        service._jobs = [CronJob(id="cleanup", schedule="1h", prompt="x")]
        service.scheduler.add_job(
            lambda: None, "interval", seconds=3600, id="cleanup",
        )
        _write_jobs(jobs_file, [])  # user removed their cleanup cron
        result = await service.reload()
        # Internal cleanup job must survive; not reported removed.
        assert "cleanup" not in result["removed"]
        assert service.scheduler.get_job("cleanup") is not None

    @pytest.mark.asyncio
    async def test_source_namespace_reserved_while_runner_is_down(self, svc):
        service, jobs_file = svc
        # No source runners registered at all — the whole `source:` namespace is
        # still off limits, otherwise the job would be silently replaced the
        # moment that source is turned back on.
        assert service._source_runners == []

        _write_jobs(jobs_file, [
            _job_dict("source:telegram", schedule="1h"),
            _job_dict("legit", schedule="1h"),
        ])
        result = await service.reload()
        assert result["added"] == ["legit"]
        assert result["enabled"] == 1
        assert service.scheduler.get_job("source:telegram") is None
        assert "source:telegram" not in [j.id for j in service._jobs]

    @pytest.mark.asyncio
    async def test_reserved_dropped_on_the_merged_path(self, svc):
        """The system+user merge — every install after `nerve init` — filters too."""
        service, jobs_file = svc
        system_file = service.config.cron.system_file
        _write_jobs(system_file, [
            _job_dict("wakeup_sweep", schedule="1h"),
            _job_dict("sys-job", schedule="1h"),
        ])
        _write_jobs(jobs_file, [
            _job_dict("source:github", schedule="1h"),
            _job_dict("user-job", schedule="1h"),
        ])
        result = await service.reload()
        assert sorted(result["added"]) == ["sys-job", "user-job"]
        assert sorted(j.id for j in service._jobs) == ["sys-job", "user-job"]
        assert service.scheduler.get_job("source:github") is None
        # The daemon's own wakeup sweep is never displaced by a same-named job.
        assert service.scheduler.get_job("wakeup_sweep") is None

    @pytest.mark.asyncio
    async def test_reload_names_the_jobs_it_refused(self, svc):
        """A refused job is missing from the schedule *and* from added/removed/
        updated, so without this it disappears from the reload entirely and the
        only trace is a log line on a box nobody is tailing.
        """
        service, jobs_file = svc
        _write_jobs(jobs_file, [
            _job_dict("source:gmail", schedule="1h"),
            _job_dict("cleanup", schedule="1h"),
            _job_dict("legit", schedule="1h"),
        ])
        result = await service.reload()
        assert result["rejected"] == ["source:gmail", "cleanup"]
        assert result["enabled"] == 1  # only 'legit' — not 3

        # Renaming the job clears the refusal on the next reload.
        _write_jobs(jobs_file, [
            _job_dict("my-gmail", schedule="1h"),
            _job_dict("legit", schedule="1h"),
        ])
        result = await service.reload()
        assert result["rejected"] == []
        assert result["added"] == ["my-gmail"] and result["enabled"] == 2

    @pytest.mark.asyncio
    async def test_a_source_reload_cannot_take_a_colliding_job_off_the_schedule(
        self, svc, monkeypatch,
    ):
        """Source runners schedule with ``replace_existing=True``, so before the
        whole ``source:`` namespace was reserved, turning a source on removed a
        user job's trigger while every later reload kept counting it as enabled —
        unrecoverable short of a restart. Both halves are checked here: the job
        never gets a trigger to lose, and the reload says so.
        """
        import nerve.sources.registry as registry

        service, jobs_file = svc
        _write_jobs(jobs_file, [
            _job_dict("source:gmail", schedule="1h"),
            _job_dict("legit", schedule="1h"),
        ])
        result = await service.reload()
        assert result["rejected"] == ["source:gmail"]
        assert result["enabled"] == 1

        runner = MagicMock()
        runner.job_id = "source:gmail"
        runner.source.source_name = "gmail"
        service.config.sync.gmail.schedule = "5m"
        monkeypatch.setattr(
            registry, "build_source_runners", lambda config, db: [runner],
        )
        await service.reload_sources()

        # The trigger under that id belongs to the source runner, and the reload
        # after it still reports one enabled job — the one that is really live.
        assert service.scheduler.get_job("source:gmail") is not None
        result = await service.reload()
        assert result["enabled"] == 1
        assert result["rejected"] == ["source:gmail"]
        assert service.scheduler.get_job("legit") is not None

    @pytest.mark.asyncio
    async def test_reserved_rejection_is_logged(self, svc, caplog):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("source:gmail", schedule="1h")])
        with caplog.at_level(logging.WARNING, logger="nerve.cron.service"):
            await service.reload()
        # An operator who hand-edited jobs.yaml must be able to find out why.
        assert any(
            "source:gmail" in r.getMessage() and "reserved" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_unchanged_job_with_missing_trigger_is_rescheduled(self, svc):
        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1")])
        await service.reload()

        # Something dropped the trigger behind reload's back. An unchanged job
        # must not be left stranded while the summary keeps calling it enabled.
        service.scheduler.remove_job("j1")
        result = await service.reload()
        # Newly scheduled, so "added" — the summary reports what the scheduler
        # did, not what changed in the file.
        assert result["added"] == ["j1"]
        assert result["updated"] == []
        assert result["enabled"] == 1
        assert service.scheduler.get_job("j1") is not None

    @pytest.mark.asyncio
    async def test_jobs_refreshed_without_scheduler_report_as_added(self, svc):
        service, jobs_file = svc
        # run_job()/rotate_session() refresh _jobs from disk without touching the
        # scheduler. The next reload schedules those jobs for the first time, so
        # they are added, not updated.
        _write_jobs(jobs_file, [_job_dict("j1"), _job_dict("j2")])
        service._jobs = service._load_merged_jobs()
        assert service.scheduler.get_job("j1") is None

        result = await service.reload()
        assert sorted(result["added"]) == ["j1", "j2"]
        assert result["updated"] == []

    @pytest.mark.asyncio
    async def test_malformed_yaml_refused(self, svc):
        from nerve.config import ConfigError

        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1")])
        await service.reload()

        # Corrupt the file — reload must refuse and leave the schedule intact.
        jobs_file.write_text("jobs: [ this is: not valid: yaml\n", encoding="utf-8")
        with pytest.raises(ConfigError):
            await service.reload()
        assert service.scheduler.get_job("j1") is not None  # still scheduled

    @pytest.mark.asyncio
    async def test_trigger_build_failure_leaves_schedule_intact(self, svc):
        """A job whose trigger won't build must not take the others down with it.

        Interval jobs anchor their timer to the last successful run, so building
        a trigger reads the database — and that read can fail long after the
        config parsed fine. If the reload had already unscheduled the removed
        jobs by then, the daemon would be left half-reloaded with no way for an
        operator to tell which crons are still live.
        """
        service, jobs_file = svc
        _write_jobs(jobs_file, [
            _job_dict("keep-a", schedule="1h"),
            _job_dict("keep-b", schedule="30m"),
            _job_dict("doomed", schedule="2h"),
        ])
        await service.reload()
        before = {
            jid: str(service.scheduler.get_job(jid).trigger)
            for jid in ("keep-a", "keep-b", "doomed")
        }

        # A reload that touches all three paths: keep-a rescheduled, keep-b
        # removed, doomed's trigger unbuildable.
        _write_jobs(jobs_file, [
            _job_dict("keep-a", schedule="15m"),
            _job_dict("doomed", schedule="3h"),
        ])

        async def flaky_last_run(job_id):
            if job_id == "doomed":
                raise RuntimeError("database is locked")
            return None

        service.db.get_last_successful_cron_run = AsyncMock(
            side_effect=flaky_last_run,
        )

        with pytest.raises(RuntimeError):
            await service.reload()

        # Every job that was running still runs, on its original trigger.
        live = {jid: service.scheduler.get_job(jid) for jid in before}
        assert [jid for jid, j in live.items() if j is None] == []
        assert {jid: str(j.trigger) for jid, j in live.items()} == before
        # And the recorded job list still matches the live schedule.
        assert sorted(j.id for j in service._jobs) == [
            "doomed", "keep-a", "keep-b",
        ]

    @pytest.mark.asyncio
    async def test_invalid_job_entry_refused(self, svc):
        from nerve.config import ConfigError

        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1")])
        await service.reload()
        # A job missing required fields (no prompt/prompt_file) → strict raise.
        jobs_file.write_text(
            yaml.safe_dump({"jobs": [{"id": "bad", "schedule": "1h"}]}),
            encoding="utf-8",
        )
        with pytest.raises(ConfigError):
            await service.reload()
        assert service.scheduler.get_job("j1") is not None

    @pytest.mark.asyncio
    async def test_invalid_schedule_refused(self, svc):
        """A crontab the scheduler rejects refuses the reload, like bad YAML.

        It used to be accepted as a 2h interval, so the reload "succeeded" and
        the job ran on a cadence the operator never wrote down.
        """
        from nerve.config import ConfigError

        service, jobs_file = svc
        _write_jobs(jobs_file, [_job_dict("j1", schedule="1h")])
        await service.reload()
        before = str(service.scheduler.get_job("j1").trigger)

        _write_jobs(jobs_file, [
            _job_dict("j1", schedule="30m"),
            _job_dict("typo", schedule="99 * * * *"),
        ])
        with pytest.raises(ConfigError) as ei:
            await service.reload()

        assert "typo" in str(ei.value)
        # All-or-nothing: j1 keeps its old trigger, the typo is never scheduled.
        assert str(service.scheduler.get_job("j1").trigger) == before
        assert service.scheduler.get_job("typo") is None


class TestRefusedReloadRestoresGates:
    """All-or-nothing has to cover GATE_REGISTRY, which is process-global.

    Plugin gates must be replaced before jobs are rebuilt — a CronJob builds its
    gates at construction time — which puts the registry ahead of every check
    that can still refuse the reload. Leaving the scheduler alone is then not
    enough on its own: the registry has to be put back by hand.
    """

    @pytest.mark.asyncio
    async def test_registry_is_restored(self, svc, clean_registry):
        from nerve.config import ConfigError
        from nerve.cron.gates import GATE_REGISTRY

        service, jobs_file = svc
        plugin = _write_gate_plugin(jobs_file.parent / "gates")
        _write_jobs(jobs_file, [_gated_job("j1")])
        await service.reload()
        registered = GATE_REGISTRY["reload_test"]

        # One edit deletes the plugin and breaks the YAML.
        plugin.unlink()
        jobs_file.write_text("jobs: [ this is: not valid: yaml\n", encoding="utf-8")
        with pytest.raises(ConfigError):
            await service.reload()

        # The same class object, not just the same type name: a rollback puts the
        # snapshot back, where a re-import would install an equivalent new class.
        assert GATE_REGISTRY["reload_test"] is registered

    @pytest.mark.asyncio
    async def test_rollback_covers_a_failure_after_the_load(
        self, svc, clean_registry,
    ):
        """The guard spans the planning pass, not just the strict load.

        A trigger that won't build raises at the far end of it, long after the
        plugins have been replaced.
        """
        from nerve.cron.gates import GATE_REGISTRY

        service, jobs_file = svc
        plugin = _write_gate_plugin(jobs_file.parent / "gates")
        _write_jobs(jobs_file, [_gated_job("j1")])
        await service.reload()

        # The plugin and the job that used it go away together. Removing only the
        # plugin would have the strict load refuse this reload before the planning
        # pass is reached, which is a different failure than the one under test.
        plugin.unlink()
        _write_jobs(jobs_file, [_job_dict("doomed")])

        async def flaky_last_run(job_id):
            if job_id == "doomed":
                raise RuntimeError("database is locked")
            return None

        service.db.get_last_successful_cron_run = AsyncMock(
            side_effect=flaky_last_run,
        )
        with pytest.raises(RuntimeError):
            await service.reload()

        assert "reload_test" in GATE_REGISTRY

    @pytest.mark.asyncio
    async def test_no_vanished_gate_is_announced(self, svc, clean_registry, caplog):
        """A refused reload must not announce a gate that is still registered.

        The warning reports that a gate stopped being available. A refused reload
        puts it back, so saying so would describe a regression that this very
        reload prevented — and the operator would go looking for a plugin that is
        sitting where it always was.
        """
        from nerve.config import ConfigError

        service, jobs_file = svc
        plugin = _write_gate_plugin(jobs_file.parent / "gates")
        _write_jobs(jobs_file, [_gated_job("j1")])
        await service.reload()

        plugin.unlink()
        jobs_file.write_text("jobs: [ this is: not valid: yaml\n", encoding="utf-8")
        with caplog.at_level(logging.WARNING):
            with pytest.raises(ConfigError):
                await service.reload()

        assert "reload_test" not in caplog.text
        # And the claim it would have made is indeed false — j1 still has its gate.
        assert len(next(j for j in service._jobs if j.id == "j1").gates) == 1

    @pytest.mark.asyncio
    async def test_a_manual_run_afterwards_keeps_its_gate(self, svc, clean_registry):
        """What the rollback is for: run_job goes back to disk for a missing id.

        Rebuilding a job there takes its gates from the live registry, so a
        registry left stripped by a refused reload makes every gated job fail to
        build — and since run_job replaces the whole of _jobs, they all drop out
        at once. The instance would lose jobs off a reload that returned 400,
        having agreed to change nothing.
        """
        from nerve.config import ConfigError

        service, jobs_file = svc
        plugin = _write_gate_plugin(jobs_file.parent / "gates")
        _write_jobs(jobs_file, [_gated_job("gated")])
        await service.reload()

        # The operator adds a second gated job, deletes the plugin, and mistypes
        # a schedule badly enough to be refused — all in one sync.
        plugin.unlink()
        _write_jobs(jobs_file, [
            _gated_job("gated"),
            _gated_job("added"),
            _job_dict("typo", schedule="99 * * * *"),
        ])
        with pytest.raises(ConfigError):
            await service.reload()

        service._run_job_wrapper = AsyncMock()
        await service.run_job("added")     # not in _jobs → reloads from disk

        rebuilt = {j.id: len(j.gates) for j in service._jobs}
        assert rebuilt["added"] == 1
        assert rebuilt["gated"] == 1

    @pytest.mark.asyncio
    async def test_a_committed_reload_still_announces_it(
        self, svc, clean_registry, caplog,
    ):
        """Holding the warning back until the commit must not amount to losing it.

        The reload has to be one that commits, which now means the plugin and the
        last job using it went away together — removing the plugin alone refuses
        the reload, and its 400 does the telling. This is the case where the
        warning is the only notice that a gate stopped being available.
        """
        from nerve.cron.gates import GATE_REGISTRY

        service, jobs_file = svc
        plugin = _write_gate_plugin(jobs_file.parent / "gates")
        _write_jobs(jobs_file, [_gated_job("j1")])
        await service.reload()

        plugin.unlink()
        _write_jobs(jobs_file, [_job_dict("j1")])   # same job, no longer gated
        with caplog.at_level(logging.WARNING):
            await service.reload()          # valid config → applies

        assert "reload_test" not in GATE_REGISTRY
        assert "reload_test" in caplog.text


@pytest_asyncio.fixture
async def sources(svc, monkeypatch):
    """The service with a gmail (crontab) and a github (interval) source live.

    ``sync`` is a plain namespace rather than the fixture's MagicMock, because a
    source whose config section is absent has to read as absent.
    """
    import nerve.sources.registry as registry

    service, _jobs_file = svc
    service.config.sync = SimpleNamespace(
        gmail=SimpleNamespace(schedule="*/15 * * * *"),
        github=SimpleNamespace(schedule="30m"),
    )
    monkeypatch.setattr(
        registry, "build_source_runners",
        lambda config, db: [_source_runner("gmail"), _source_runner("github")],
    )
    service._register_source_runners()
    return service, registry


class TestSourceReloadIsAllOrNothing:
    """A source reload the daemon cannot carry out must change nothing.

    Every old source job was removed before a single new schedule had been
    parsed, and the scheduling pass logged and skipped whatever it could not
    build. One typo therefore destroyed that source's working trigger, moved
    the others onto whatever their strings happened to parse to, and returned a
    response naming sources that were never scheduled — which the reload route
    rendered as ``ok: true``.
    """

    @pytest.mark.asyncio
    async def test_a_bad_crontab_refuses_the_whole_reload(self, sources):
        service, _registry = sources
        before = {
            jid: str(service.scheduler.get_job(jid).trigger)
            for jid in ("source:gmail", "source:github")
        }
        service.config.sync.gmail.schedule = "99 * * * *"

        from nerve.config import ConfigError

        with pytest.raises(ConfigError) as ei:
            await service.reload_sources()

        assert "gmail" in str(ei.value) and "99 * * * *" in str(ei.value)
        assert {
            jid: str(service.scheduler.get_job(jid).trigger) for jid in before
        } == before

    @pytest.mark.asyncio
    async def test_an_unusable_interval_is_refused_not_defaulted(self, sources):
        """``_parse_interval`` answers 'hourly' with its 2h default, so this
        reload used to move the source from 30 minutes to 2 hours and report
        success. The value nobody wrote down is not an outcome to return ok for.
        """
        service, _registry = sources
        before = str(service.scheduler.get_job("source:github").trigger)
        service.config.sync.github.schedule = "hourly"

        from nerve.config import ConfigError

        with pytest.raises(ConfigError) as ei:
            await service.reload_sources()

        assert "github" in str(ei.value) and "hourly" in str(ei.value)
        assert str(service.scheduler.get_job("source:github").trigger) == before

    @pytest.mark.asyncio
    async def test_the_response_names_only_what_is_scheduled(
        self, sources, monkeypatch,
    ):
        """A runner whose source has no config section is not scheduled. The
        response was built from the runners that were built, so it named that
        one under ``sources`` and left it out of ``removed``."""
        service, registry = sources
        monkeypatch.setattr(
            registry, "build_source_runners",
            lambda config, db: [_source_runner("gmail"), _source_runner("ghost")],
        )

        result = await service.reload_sources()

        assert service.scheduler.get_job("source:ghost") is None
        assert result["sources"] == ["source:gmail"]
        assert result["removed"] == ["source:github"]

    @pytest.mark.asyncio
    async def test_a_reload_it_can_carry_out_still_applies(self, sources):
        service, _registry = sources
        service.config.sync.github.schedule = "45m"

        result = await service.reload_sources()

        assert result["sources"] == ["source:github", "source:gmail"]
        assert result["removed"] == []
        assert "0:45:00" in str(service.scheduler.get_job("source:github").trigger)


class TestInvalidScheduleAtStartup:
    """Startup answers the same error differently from reload(), on purpose."""

    @pytest.mark.asyncio
    async def test_start_falls_back_rather_than_dropping_the_source(
        self, svc, monkeypatch,
    ):
        """Boot keeps the lenient reading of an interval string it cannot use:
        a source on a conservative 2h cadence beats one that never runs. Only
        the reload path is strict, where an operator is waiting on an answer and
        the old cadence is still running until they get one.
        """
        import nerve.sources.registry as registry

        service, _jobs_file = svc
        service.config.sync = SimpleNamespace(github=SimpleNamespace(schedule="hourly"))
        monkeypatch.setattr(
            registry, "build_source_runners",
            lambda config, db: [_source_runner("github")],
        )

        service._register_source_runners()

        job = service.scheduler.get_job("source:github")
        assert job is not None
        assert "2:00:00" in str(job.trigger)

    @pytest.mark.asyncio
    async def test_start_skips_only_the_offending_job(
        self, tmp_path, monkeypatch, caplog,
    ):
        """One typo must not cost the other jobs their daemon."""
        import nerve.sources.registry as registry

        monkeypatch.setattr(registry, "build_source_runners", lambda *a, **k: [])

        cron_dir = tmp_path / "cron"
        jobs_file = cron_dir / "jobs.yaml"
        _write_jobs(jobs_file, [
            _job_dict("typo", schedule="99 * * * *"),
            _job_dict("legit", schedule="1h"),
        ])

        config = MagicMock()
        config.timezone = "UTC"
        config.cron.jobs_file = jobs_file
        config.cron.system_file = cron_dir / "system.yaml"  # absent
        config.cron.gate_plugins_dir = cron_dir / "gates"   # absent → no-op

        db = AsyncMock()
        db.get_last_successful_cron_run = AsyncMock(return_value=None)

        service = CronService(config, AsyncMock(), db)
        with caplog.at_level(logging.ERROR, logger="nerve.cron.service"):
            await service.start()  # must not raise
        try:
            assert service.scheduler.get_job("legit") is not None
            assert service.scheduler.get_job("typo") is None
            # Kept in _jobs so list_jobs still shows it — with no next run —
            # rather than dropping it out of sight.
            assert sorted(j.id for j in service._jobs) == ["legit", "typo"]
        finally:
            service.scheduler.shutdown(wait=False)

        logged = " ".join(r.getMessage() for r in caplog.records)
        assert "typo" in logged and "99 * * * *" in logged

    @pytest.mark.asyncio
    async def test_start_skips_only_the_offending_source(
        self, tmp_path, monkeypatch, caplog,
    ):
        """Same for sync.<source>.schedule, and the later sources still register.

        The registration loop sits inside a blanket `except Exception`, so an
        error escaping one runner would silently abandon every runner after it.
        """
        import nerve.sources.registry as registry

        def _runner(name):
            runner = MagicMock()
            runner.source.source_name = name
            runner.job_id = f"source:{name}"
            return runner

        # Bad one first: whatever follows it is what a swallowed error costs.
        monkeypatch.setattr(
            registry, "build_source_runners",
            lambda *a, **k: [_runner("gmail"), _runner("slack")],
        )

        cron_dir = tmp_path / "cron"
        jobs_file = cron_dir / "jobs.yaml"
        _write_jobs(jobs_file, [])

        config = MagicMock()
        config.timezone = "UTC"
        config.cron.jobs_file = jobs_file
        config.cron.system_file = cron_dir / "system.yaml"  # absent
        config.cron.gate_plugins_dir = cron_dir / "gates"   # absent → no-op
        config.sync.gmail.schedule = "0 99 * * *"
        config.sync.slack.schedule = "*/15 * * * *"

        db = AsyncMock()
        db.get_last_successful_cron_run = AsyncMock(return_value=None)

        service = CronService(config, AsyncMock(), db)
        with caplog.at_level(logging.ERROR, logger="nerve.cron.service"):
            await service.start()
        try:
            assert service.scheduler.get_job("source:gmail") is None
            assert service.scheduler.get_job("source:slack") is not None
        finally:
            service.scheduler.shutdown(wait=False)

        logged = " ".join(r.getMessage() for r in caplog.records)
        assert "gmail" in logged and "0 99 * * *" in logged


class TestReloadRoute:
    @pytest.mark.asyncio
    async def test_503_when_no_service(self, monkeypatch):
        import nerve.gateway.server as srv
        from fastapi import HTTPException

        from nerve.gateway.routes.cron import reload_cron_jobs

        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        with pytest.raises(HTTPException) as ei:
            await reload_cron_jobs(user={})
        assert ei.value.status_code == 503

    @pytest.mark.asyncio
    async def test_returns_summary(self, monkeypatch):
        import nerve.gateway.server as srv

        from nerve.gateway.routes.cron import reload_cron_jobs

        fake = MagicMock()
        fake.reload = AsyncMock(
            return_value={"added": ["a"], "removed": [], "updated": [], "enabled": 1}
        )
        monkeypatch.setattr(srv, "_cron_service", fake, raising=False)
        result = await reload_cron_jobs(user={})
        assert result["reloaded"] is True
        assert result["added"] == ["a"]

    @pytest.mark.asyncio
    async def test_400_on_config_error(self, monkeypatch):
        import nerve.gateway.server as srv
        from fastapi import HTTPException

        from nerve.config import ConfigError
        from nerve.gateway.routes.cron import reload_cron_jobs

        fake = MagicMock()
        fake.reload = AsyncMock(side_effect=ConfigError("bad cron file"))
        monkeypatch.setattr(srv, "_cron_service", fake, raising=False)
        with pytest.raises(HTTPException) as ei:
            await reload_cron_jobs(user={})
        assert ei.value.status_code == 400
        assert "bad cron file" in ei.value.detail

    @pytest.mark.asyncio
    async def test_400_on_invalid_schedule(self, monkeypatch):
        """A typo'd schedule is a bad request, not a server error."""
        import nerve.gateway.server as srv
        from fastapi import HTTPException

        from nerve.cron.service import InvalidScheduleError
        from nerve.gateway.routes.cron import reload_cron_jobs

        fake = MagicMock()
        fake.reload = AsyncMock(
            side_effect=InvalidScheduleError("Cron job 'typo': bad minute"),
        )
        monkeypatch.setattr(srv, "_cron_service", fake, raising=False)
        with pytest.raises(HTTPException) as ei:
            await reload_cron_jobs(user={})
        assert ei.value.status_code == 400
        assert "typo" in ei.value.detail


class TestReservedIds:
    @pytest.mark.parametrize("job_id", [
        "cleanup", "wakeup_sweep",
        "source:gmail", "source:telegram", "source:gmail:me@example.com",
        "source:",
    ])
    def test_reserved(self, job_id):
        assert is_reserved_job_id(job_id)

    @pytest.mark.parametrize("job_id", [
        "cleanup-inbox", "my_wakeup_sweep", "sources:gmail", "source-gmail",
        "morning-briefing",
    ])
    def test_not_reserved(self, job_id):
        assert not is_reserved_job_id(job_id)

    @pytest.mark.asyncio
    async def test_start_drops_reserved_jobs(self, tmp_path, monkeypatch, caplog):
        """Startup must reject reserved ids too, not just reload."""
        import nerve.sources.registry as registry

        monkeypatch.setattr(registry, "build_source_runners", lambda *a, **k: [])

        cron_dir = tmp_path / "cron"
        jobs_file = cron_dir / "jobs.yaml"
        _write_jobs(jobs_file, [
            _job_dict("cleanup", schedule="1h"),
            _job_dict("source:gmail", schedule="1h"),
            _job_dict("legit", schedule="1h"),
        ])

        config = MagicMock()
        config.timezone = "UTC"
        config.cron.jobs_file = jobs_file
        config.cron.system_file = cron_dir / "system.yaml"  # absent
        config.cron.gate_plugins_dir = cron_dir / "gates"   # absent → no-op

        db = AsyncMock()
        db.get_last_successful_cron_run = AsyncMock(return_value=None)

        service = CronService(config, AsyncMock(), db)
        with caplog.at_level(logging.WARNING, logger="nerve.cron.service"):
            await service.start()
        try:
            assert [j.id for j in service._jobs] == ["legit"]
            # No user job in the source namespace, and the daemon's own cleanup
            # job — not the user's — owns the 'cleanup' slot.
            assert service.scheduler.get_job("source:gmail") is None
            assert service.scheduler.get_job("cleanup").name == "Cleanup expired data"
            assert service.scheduler.get_job("legit") is not None
        finally:
            service.scheduler.shutdown(wait=False)

        logged = " ".join(r.getMessage() for r in caplog.records)
        assert "cleanup" in logged and "source:gmail" in logged

    def test_source_runner_ids_are_always_inside_the_namespace(self):
        """The reservation only holds if every runner really lives under it."""
        import inspect

        from nerve.sources.runner import SourceRunner

        # No caller may hand a runner an id outside the reserved namespace.
        assert "job_id" not in inspect.signature(SourceRunner.__init__).parameters

        source = MagicMock()
        source.source_name = "gmail:me@example.com"
        runner = SourceRunner(source=source, db=AsyncMock())
        assert runner.job_id == "source:gmail:me@example.com"
        assert is_reserved_job_id(runner.job_id)


class TestReservedIdsInCli:
    """The daemon-log warning never reaches someone running the CLI."""

    @staticmethod
    def _config_dir(tmp_path, system_jobs, user_jobs):
        cron_dir = tmp_path / "cron"
        _write_jobs(cron_dir / "system.yaml", system_jobs)
        _write_jobs(cron_dir / "jobs.yaml", user_jobs)
        (tmp_path / "config.yaml").write_text(
            f"cron:\n"
            f"  system_file: {cron_dir / 'system.yaml'}\n"
            f"  jobs_file: {cron_dir / 'jobs.yaml'}\n",
            encoding="utf-8",
        )
        return tmp_path

    def test_cron_listing_flags_reserved_jobs(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        import nerve.agent.engine as engine_mod
        import nerve.db as db_mod
        from nerve.cli import main

        # The listing itself needs neither an engine nor a database.
        monkeypatch.setattr(db_mod, "init_db", AsyncMock(return_value=AsyncMock()))
        monkeypatch.setattr(db_mod, "close_db", AsyncMock())
        monkeypatch.setattr(
            engine_mod, "AgentEngine", MagicMock(return_value=AsyncMock()),
        )

        cfg = self._config_dir(
            tmp_path, [], [_job_dict("source:rss"), _job_dict("legit")],
        )
        result = CliRunner().invoke(main, ["-c", str(cfg), "cron"])
        assert result.exit_code == 0, result.output
        # The job is listed (so the reader can see it was read at all) but is
        # never described as enabled.
        assert "source:rss" in result.output
        assert "RESERVED ID" in result.output
        assert "source:rss: 1h (enabled)" not in result.output
        assert "legit: 1h (enabled)" in result.output

    def test_doctor_excludes_reserved_from_the_enabled_count(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        cfg = self._config_dir(
            tmp_path, [], [_job_dict("source:rss"), _job_dict("legit")],
        )
        # doctor exits non-zero on this bare config (no API key, no workspace);
        # only its cron reporting is under test here.
        result = CliRunner().invoke(main, ["-c", str(cfg), "doctor"])
        assert "Cron jobs: 1/1 enabled" in result.output
        assert "source:rss" in result.output
        assert "reserved" in result.output
