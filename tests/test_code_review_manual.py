"""Manual review consent is single-use, version-bound and independent of polling."""

import asyncio
from types import SimpleNamespace

import aiosqlite
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from nerve.code_review.config import CodeReviewConfig, ReviewProfile
from nerve.code_review.service import CodeReviewService
from nerve.db import Database
from nerve.db.migrations import runner
from nerve.db.migrations.fork.v001_review_requests import up
from nerve.gateway.auth import require_auth
from nerve.gateway.routes import code_review as routes
from tests.test_code_review import db, service, pr, snapshot, complete_output  # noqa: F401

pytestmark = pytest.mark.asyncio


@pytest.fixture
def manual(service):
    service.profile('example').review_mode = 'manual'
    service.started = True
    return service


async def request(service, row=None, key='click-1', version=None):
    row = row or await service.observe(service.profile('example'), 1)
    return await service.request_review(row['id'], version or row['current_version'], key, service.db.system_actor)


async def test_safe_default_and_config_validation():
    profile = ReviewProfile('example', 'alice/example', 'draft:false', enabled=True)
    assert profile.review_mode == 'manual'
    assert CodeReviewConfig.from_dict({'profiles': [dict(id='example', repository='alice/example', query='draft:false')]}).profiles[0].review_mode == 'manual'
    with pytest.raises(ValueError, match='review_mode'):
        ReviewProfile('example', 'alice/example', 'draft:false', review_mode='typo')


@pytest.mark.parametrize('status', ['queued', 'retry', 'waiting_budget', 'error'])
async def test_legacy_queue_poll_resume_day_and_restart_never_start(manual, db, status):
    row = await manual.observe(manual.profile('example'), 1)
    await db._write('UPDATE code_review_versions SET status=?,retry_at=?', (status, ''))
    manual.scheduler.start = lambda: None
    await manual._startup()
    await manual.tick()
    await manual.pause('example')
    await manual.resume('example')
    await asyncio.gather(*list(manual._tasks))
    manual.day = lambda profile: '2099-01-02'
    await manual.tick()
    assert (await db.get_task(row['task_id']))['status'] == 'in_progress'
    manual.runs.start_run.assert_not_called()
    assert not await db.cr_rows('SELECT * FROM code_review_attempts')
    # The persistence layer also defaults to fail-closed without an intent.
    assert not await db.cr_reserve('example', '2099-01-02', 5, 30, 3)


async def test_lifecycle_and_new_inputs_are_observed_without_models(manual, db):
    await manual.poll('example')
    row = await db.cr_one('SELECT * FROM code_review_prs')
    task = await db.get_task(row['task_id'])
    await manual.poll('example')
    assert (await db.get_task(row['task_id']))['revision'] == task['revision']
    assert (await db.cr_one('SELECT * FROM code_review_prs'))['content_updated_at'] == row['content_updated_at']
    for data, snap in [(pr(draft=True), snapshot()), (pr(), snapshot(ci={'success': True})),
                       (pr(head={'sha': 'd' * 40}), snapshot(head='d' * 40)),
                       (pr(state='closed', merged=True), snapshot()), (pr(), snapshot())]:
        manual.github.pr.return_value, manual.github.snapshot.return_value = data, snap
        await manual.poll('example')
        await manual.tick()
        assert (await db.get_task(row['task_id']))['status'] == ('done' if data['state'] == 'closed' else 'in_progress')
    assert len(await db.cr_rows('SELECT * FROM code_review_prs')) == 1
    assert len(await db.cr_rows('SELECT * FROM code_review_versions')) == 2
    manual.runs.start_run.assert_not_called()


async def test_concurrent_clicks_one_request_and_one_reservation(manual, db, tmp_path):
    row = await manual.observe(manual.profile('example'), 1)
    other = Database(tmp_path / 'state.db')
    await other.connect()
    try:
        args = ('example', row['id'], row['current_version'], db.system_actor.actor_id, 'same-key', manual.day(manual.profile('example')), 5, 30)
        first, replay = await asyncio.gather(db.cr_request_review(*args), other.cr_request_review(*args))
        assert first['id'] == replay['id']
        with pytest.raises(ValueError, match='уже запрошена'):
            await request(manual, row, 'different-key')
        claims = await asyncio.gather(*[
            store.cr_reserve('example', args[5], 5, 30, 3, row['current_version'], request_id=first['id'])
            for store in (db, other)
        ])
        assert sum(c is not None for c in claims) == 1
        assert len(await db.cr_rows('SELECT * FROM code_review_attempts')) == 1
    finally:
        await other.close()


async def test_request_survives_restart_launches_selected_pr_once(manual, db):
    await db.cr_observe('example', pr(2), snapshot(pr(2)))
    row = await manual.observe(manual.profile('example'), 1)
    intent = await request(manual, row)
    restarted = CodeReviewService(manual.config, db, manual.runs, github=manual.github)
    restarted.started = True
    await restarted.tick()
    call = manual.runs.start_run.call_args
    assert call.kwargs['title'].startswith('Code Review #1 ')
    assert call.kwargs['attribution'].caused_by_actor_id == db.system_actor.actor_id
    await restarted.tick()  # missing outbox workflow is interrupted, not retried
    await restarted.tick()
    manual.runs.start_run.assert_awaited_once()
    assert (await db.cr_one('SELECT * FROM code_review_requests'))['state'] == 'finished'
    assert (await request(manual, row))['id'] == intent['id']


@pytest.mark.parametrize('change', ['head', 'body', 'failure', 'draft', 'closed'])
async def test_changed_input_cancels_without_spend(manual, db, change):
    row = await manual.observe(manual.profile('example'), 1)
    intent = await request(manual, row)
    if change == 'head':
        manual.github.pr.return_value = pr(head={'sha': 'd' * 40})
        manual.github.snapshot.return_value = snapshot(head='d' * 40)
    elif change == 'body':
        manual.github.snapshot.return_value = snapshot(body='Changed requirements')
    elif change == 'failure':
        manual.github.snapshot.return_value = snapshot(failures=[{'name': 'build', 'conclusion': 'failure'}])
    else:
        manual.github.pr.return_value = pr(draft=True) if change == 'draft' else pr(state='closed')
    await manual.tick()
    await manual.tick()
    assert (await db.cr_one('SELECT * FROM code_review_requests WHERE id=?', (intent['id'],)))['state'] == 'cancelled'
    manual.runs.start_run.assert_not_called()
    assert not await db.cr_rows('SELECT * FROM code_review_attempts')


async def test_stale_click_is_rejected_and_key_not_rebound(manual, db):
    row = await manual.observe(manual.profile('example'), 1)
    manual.github.snapshot.return_value = snapshot(body='Changed requirements')
    rejected = await request(manual, row)
    assert rejected['state'] == 'rejected'
    current = await db.cr_one('SELECT * FROM code_review_prs')
    with pytest.raises(ValueError, match='другой версии'):
        await request(manual, current)
    await manual.tick()
    manual.runs.start_run.assert_not_called()


async def test_pause_revokes_pending_request_and_resume_only_polls(manual, db):
    await request(manual)
    await manual.pause('example')
    await manual.resume('example')
    await asyncio.gather(*list(manual._tasks))
    await manual.tick()
    manual.runs.start_run.assert_not_called()
    assert (await db.cr_one('SELECT * FROM code_review_requests'))['state'] == 'cancelled'


async def test_expired_request_cannot_start_next_day(manual, db):
    await request(manual)
    manual.day = lambda profile: '2099-01-02'
    await manual.tick()
    manual.runs.start_run.assert_not_called()
    assert (await db.cr_one('SELECT * FROM code_review_requests'))['state'] == 'cancelled'


async def test_budget_exhaustion_is_terminal_for_request(manual, db):
    row = await manual.observe(manual.profile('example'), 1)
    await request(manual, row)
    # Spend after admission but before reservation (another profile/run).
    await db.cr_observe('example', pr(2), snapshot(pr(2)))
    second = await db.cr_one('SELECT * FROM code_review_prs WHERE number=2')
    claim = await db.cr_reserve('example', manual.day(manual.profile('example')), 5, 30, 3, second['current_version'], review_mode='automatic')
    await db.cr_settle(claim['run_id'], 30, manual.day(manual.profile('example')), 'complete', '')
    await manual.tick()
    assert (await db.cr_one('SELECT * FROM code_review_requests'))['state'] == 'cancelled'
    manual.day = lambda profile: '2099-01-02'
    await manual.tick()
    manual.runs.start_run.assert_not_called()


async def test_failure_requires_new_click_and_preserves_remaining_budget(manual, db):
    row = await manual.observe(manual.profile('example'), 1)
    await request(manual, row)
    manual.runs.start_run.side_effect = RuntimeError('Temporary executor failure')
    await manual.tick()
    await manual.tick()
    assert (await db.cr_one('SELECT * FROM code_review_versions'))['status'] == 'error'
    manual.runs.start_run.assert_awaited_once()
    await db._write('UPDATE code_review_attempts SET spent=2')
    await request(manual, row, 'retry-click')
    await manual.tick()
    assert manual.runs.start_run.await_count == 2
    assert manual.runs.start_run.call_args.args[2] == 3
    assert len(await db.cr_rows('SELECT * FROM code_review_attempts')) == 2


async def test_guard_rejects_legacy_unrequested_run_and_changed_claim(manual, db):
    row = await manual.observe(manual.profile('example'), 1)
    claim = await db.cr_reserve('example', manual.day(manual.profile('example')), 5, 30, 3, review_mode='automatic')
    run = {'id': claim['run_id'], 'created_by': 'code-review:example'}
    assert 'authorization' in await manual.guard_dispatch(run)
    await db.cr_settle(claim['run_id'], 0, manual.day(manual.profile('example')), 'retry', '')
    await request(manual, row)
    claim = await manual.manual_claim(manual.profile('example'))
    run['id'] = claim['run_id']
    assert await manual.guard_dispatch(run) == ''
    manual.github.pr.return_value = pr(draft=True)
    assert 'changed' in await manual.guard_dispatch(run)
    assert (await db.cr_one('SELECT * FROM code_review_requests'))['state'] == 'cancelled'


async def test_complete_and_saved_admission_cannot_be_requested_again(manual, db):
    row = await manual.observe(manual.profile('example'), 1)
    await request(manual, row)
    claim = await manual.manual_claim(manual.profile('example'))
    directory = manual.root / claim['pr_id'] / claim['id'] / 'attempts' / claim['run_id'] / 'work'
    directory.mkdir(parents=True)
    for name, content in complete_output().items():
        (directory / name).write_text(content)
    manual.runs.get_run.return_value = {'status': 'done', 'spent_usd': 1}
    manual.runs.settled_spend.return_value = 1
    manual.github.pr.side_effect = RuntimeError('temporary')
    await manual.reconcile()
    assert (await db.cr_one('SELECT * FROM code_review_versions'))['status'] == 'awaiting_validation'
    manual.github.pr.side_effect = None
    rejected = await request(manual, row, 'second-click')
    assert rejected['state'] == 'rejected' and 'Отчёт' in rejected['reason']
    await db._write("UPDATE code_review_versions SET retry_at=''")
    await manual.tick()
    assert (await db.cr_one('SELECT * FROM code_review_versions'))['status'] == 'complete'
    rejected = await request(manual, row, 'third-click')
    assert rejected['state'] == 'rejected' and 'уже проверена' in rejected['reason']
    manual.runs.start_run.assert_not_called()


async def test_manual_api_auth_attribution_validation_and_replay(manual, db, monkeypatch):
    app = FastAPI()
    app.include_router(routes.router)
    monkeypatch.setattr(routes, 'get_service', lambda: manual)
    monkeypatch.setattr(routes, 'get_deps', lambda: SimpleNamespace(db=db))
    row = await manual.observe(manual.profile('example'), 1)
    path = f"/api/code-review/prs/{row['id']}/requests"
    body = {'expected_version': row['current_version'], 'idempotency_key': 'click'}
    async with AsyncClient(transport=ASGITransport(app), base_url='http://example.com') as client:
        # Authentication dependency must run, including on this mutation.
        from fastapi import HTTPException
        def denied():
            raise HTTPException(401, 'unauthenticated')
        app.dependency_overrides[require_auth] = denied
        assert (await client.post(path, json=body)).status_code == 401
        app.dependency_overrides[require_auth] = lambda: db.system_actor
        assert (await client.post(path, json={**body, 'idempotency_key': ''})).status_code == 422
        first = await client.post(path, json=body)
        assert first.status_code == 202
        replay = await client.post(path, json=body)
        assert replay.json()['request']['id'] == first.json()['request']['id']
        assert replay.json()['request']['actor_id'] == db.system_actor.actor_id
        assert (await client.post(path, json={**body, 'idempotency_key': 'another'})).status_code == 409
        detail = (await client.get(f"/api/code-review/prs/{row['id']}")).json()
        assert detail['pr']['review_action']['state'] == 'queued'
        assert not detail['pr']['review_action']['enabled']
        await manual.pause('example')
        assert (await client.post(path, json=body)).status_code == 409


async def test_fork_migration_upgrades_existing_database_without_rewriting(tmp_path):
    path = tmp_path / 'old.db'
    async with aiosqlite.connect(path) as conn:
        await runner.run_migrations(conn, tracks=(runner.UPSTREAM_TRACK,))
        await conn.execute("INSERT INTO code_review_profiles(id,paused) VALUES ('example',1)")
        await conn.execute("INSERT INTO code_review_prs(id,profile,repository_id,repository,number,task_id,title,url,state,head,base,base_ref,last_polled_at,pr_updated_at,content_updated_at,queue_since) VALUES ('pr','example',1,'alice/example',1,'pr','Synthetic','https://example.com','open','head','base','main','now','now','now','now')")
        await conn.execute("INSERT INTO code_review_versions(id,pr_id,fingerprint,head,base,snapshot,created_at,updated_at) VALUES ('version','pr','hash','head','base','{}','now','now')")
        await conn.execute("INSERT INTO code_review_attempts(run_id,version_id,profile,day,reserved,spent,settled,created_at) VALUES ('run','version','example','2026-01-01',5,2,1,'now')")
        await conn.commit()
        before = [await (await conn.execute('SELECT * FROM ' + table)).fetchall() for table in ('code_review_profiles','code_review_prs','code_review_versions','code_review_attempts')]
        await runner.run_migrations(conn)
        await up(conn)
        await up(conn)
        assert await runner.get_fork_version(conn) == 1
        after = [await (await conn.execute('SELECT * FROM ' + table)).fetchall() for table in ('code_review_profiles','code_review_prs','code_review_versions','code_review_attempts')]
        assert after == before


@pytest.mark.parametrize('during_refresh', ['midnight', 'pause-resume', 'overrun'])
async def test_final_guard_rechecks_after_network_wait(manual, db, during_refresh):
    row = await manual.observe(manual.profile('example'), 1)
    await request(manual, row)
    claim = await manual.manual_claim(manual.profile('example'))
    original = manual.observe

    async def observe(profile, number):
        result = await original(profile, number)
        if during_refresh == 'midnight':
            manual.day = lambda profile: '2099-01-02'
        elif during_refresh == 'pause-resume':
            await manual.pause('example')
            await db._write("UPDATE code_review_profiles SET paused=0 WHERE id='example'")
        else:
            await db._write('UPDATE code_review_attempts SET spent=31')
        return result

    manual.observe = observe
    assert await manual.guard_dispatch({'id': claim['run_id'], 'created_by': 'code-review:example'})
    manual.runs.start_run.assert_not_called()


async def test_human_request_cause_reaches_workflow(manual, db):
    from nerve.identity import Actor
    actor = Actor('human-example', 'human')
    await db.ensure_external_actor(actor.actor_id, 'human')
    row = await manual.observe(manual.profile('example'), 1)
    await manual.request_review(row['id'], row['current_version'], 'human-click', actor)
    await manual.tick()
    attribution = manual.runs.start_run.call_args.kwargs['attribution']
    assert attribution.actor_id == db.system_actor.actor_id
    assert attribution.caused_by_actor_id == actor.actor_id


async def test_reserved_run_waiting_for_global_executor_is_still_queued(manual, db):
    row = await manual.observe(manual.profile('example'), 1)
    await request(manual, row)
    claim = await manual.manual_claim(manual.profile('example'))
    for run_status, expected in [('pending', 'queued'), ('running', 'running')]:
        manual.runs.get_run.return_value = {'id': claim['run_id'], 'status': run_status}
        action = await manual.review_action(row)
        assert action['state'] == expected and not action['enabled']
    # An already executing attempt remains executing when its billing day changes.
    manual.day = lambda profile: '2099-01-02'
    assert (await manual.review_action(row))['state'] == 'running'
