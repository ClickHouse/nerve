"""Session-bound review I/O works without shell execution or arbitrary file access."""

import json
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from nerve.agent.tools import build_default_registry
from nerve.agent.tools.handlers.code_review import input_handler, source_handler, checkpoint_handler
from nerve.agent.tools.registry import ToolContext
from nerve.code_review.github import GitHub
from tests.test_code_review import db, service, complete_output  # noqa: F401

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def worker(db, service):
    row = await service.observe(service.profile('example'), 1)
    claim = await db.cr_reserve('example', '2026-01-01', 5, 30, 3)
    run_id = claim['run_id']
    sid = 'workflow:' + run_id
    await db.create_session(sid, source='workflow', actor=None)
    await db.create_workflow_run(run_id, 'codex-ultracode', {}, 5, created_by='code-review:example')
    await db.transition_workflow_run(run_id, 'running', session_id=sid)
    work = service.root / row['id'] / row['current_version'] / 'attempts' / run_id / 'work'
    work.mkdir(parents=True)
    sources = [{'side': 'after', 'sha': 'a' * 40, 'path': 'src/example.cc', 'content': 'one\ntwo\nthree\n'}]
    (work / 'sources.json').write_text(json.dumps(sources))
    ctx = ToolContext(session_id=sid, db=db, config=service.config)
    return ctx, work, run_id


async def test_tools_registered_and_bound_input(worker):
    ctx, work, _ = worker
    registry = build_default_registry()
    result = await registry.invoke('code_review_input', ctx, {})
    assert not result.is_error
    data = json.loads(result.text_content)
    assert data['snapshot']['repository'] == 'alice/example'
    assert data['sources'] == [{'side': 'after', 'sha': 'a' * 40, 'path': 'src/example.cc'}]
    assert data['previous_review'] is None


@pytest.mark.parametrize('restriction', ['outside', 'source', 'run_session', 'creator', 'stopped', 'settled', 'paused', 'disabled'])
async def test_unavailable_attempt_rejected_for_all_operations(worker, db, restriction):
    ctx, work, run_id = worker
    if restriction == 'outside':
        ctx.session_id = 'external'
    elif restriction == 'source':
        await db._write("UPDATE sessions SET source='external' WHERE id=?", (ctx.session_id,))
    elif restriction == 'run_session':
        await db.create_session('workflow:another', source='workflow', actor=None)
        await db.update_workflow_run(run_id, {'session_id': 'workflow:another'})
    elif restriction == 'creator':
        await db._write("UPDATE workflow_runs SET created_by='user' WHERE id=?", (run_id,))
    elif restriction == 'stopped':
        await db.transition_workflow_run(run_id, 'done')
    elif restriction == 'settled':
        await db._write('UPDATE code_review_attempts SET settled=1 WHERE run_id=?', (run_id,))
    elif restriction == 'paused':
        await db._write('UPDATE code_review_profiles SET paused=1')
    else:
        ctx.config.code_review.profiles[0].enabled = False
    for handler, args in [(input_handler, {}), (source_handler, {'revision': 'head', 'path': 'src/example.cc'}),
                          (checkpoint_handler, {'stage': 'description.md', 'content': 'text'})]:
        assert (await handler(ctx, args)).is_error
    assert not (work / 'description.md').exists()


async def test_source_lines_and_pinned_neighbor_get(worker, monkeypatch):
    ctx, _, _ = worker
    get = AsyncMock(return_value='neighbor\ncontract')
    monkeypatch.setattr(GitHub, 'get', get)
    response = await source_handler(ctx, {'path': 'src/example.cc', 'revision': 'head', 'limit': 2})
    data = json.loads(response.text_content)
    assert data['content'] == '1: one\n2: two' and data['next_line'] == 3 and data['total_lines'] == 3
    get.assert_not_called()
    response = await source_handler(ctx, {'path': 'include/contract.h', 'revision': 'base'})
    assert json.loads(response.text_content)['sha'] == 'b' * 40
    get.assert_awaited_once_with('repos/alice/example/contents/include/contract.h?ref=' + 'b' * 40, raw=True)


@pytest.mark.parametrize('args', [
    {'path': '../secret', 'revision': 'head'}, {'path': '/etc/secret', 'revision': 'head'},
    {'path': 'src//file', 'revision': 'head'}, {'path': 'src/file', 'revision': 'main'},
    {'path': 'src/file', 'revision': 'head', 'start_line': 0},
    {'path': 'src/file', 'revision': 'head', 'limit': 401},
])
async def test_source_rejects_escape_and_unbounded_reads(worker, monkeypatch, args):
    ctx, _, _ = worker
    get = AsyncMock()
    monkeypatch.setattr(GitHub, 'get', get)
    assert (await source_handler(ctx, args)).is_error
    get.assert_not_called()


async def test_pause_during_source_fetch_closes_access(worker, db, monkeypatch):
    ctx, _, _ = worker
    async def get(*args, **kwargs):
        await db._write('UPDATE code_review_profiles SET paused=1')
        return 'content'
    monkeypatch.setattr(GitHub, 'get', get)
    assert (await source_handler(ctx, {'path': 'include/contract.h', 'revision': 'base'})).is_error


async def test_checkpoint_order_validation_and_controller_admission(worker, db, service):
    ctx, work, run_id = worker
    output = complete_output()
    assert (await checkpoint_handler(ctx, {'stage': 'review.md', 'content': 'too early'})).is_error
    for name, content in output.items():
        result = await checkpoint_handler(ctx, {'stage': name, 'content': content})
        assert not result.is_error, result.text_content
        assert (work / name).read_text() == content
    invalid = json.loads(output['verdict.json']); invalid['head'] = 'd' * 40
    assert (await checkpoint_handler(ctx, {'stage': 'verdict.json', 'content': json.dumps(invalid)})).is_error
    for malformed in ([], {**json.loads(output['verdict.json']), 'findings': [None]}):
        assert (await checkpoint_handler(ctx, {'stage': 'verdict.json', 'content': json.dumps(malformed)})).is_error
    assert (work / 'verdict.json').read_text() == output['verdict.json']
    await db.transition_workflow_run(run_id, 'done', spent_usd=0.5)
    service.runs.get_run = db.get_workflow_run
    service.runs.settled_spend = AsyncMock(return_value=0.5)
    await service.reconcile()
    version = await db.cr_one('SELECT * FROM code_review_versions')
    assert version['status'] == 'complete'
    assert json.loads((service.root / version['artifact_path']).read_text())['output'] == output
    assert (await db.cr_one('SELECT * FROM code_review_attempts'))['spent'] == 0.5


async def test_symlink_and_arbitrary_output_paths(worker, tmp_path):
    ctx, work, _ = worker
    victim = tmp_path / 'victim'
    victim.write_text('untouched')
    (work / 'description.md').symlink_to(victim)
    assert not (await checkpoint_handler(ctx, {'stage': 'description.md', 'content': 'report'})).is_error
    assert victim.read_text() == 'untouched'
    assert not (work / 'description.md').is_symlink()
    assert (await checkpoint_handler(ctx, {'stage': '../../victim', 'content': 'bad'})).is_error
    (work / 'sources.json').unlink()
    (work / 'sources.json').symlink_to(victim)
    assert (await input_handler(ctx, {})).is_error
    work.rename(work.with_name('original-work'))
    work.symlink_to(work.with_name('original-work'), target_is_directory=True)
    assert (await checkpoint_handler(ctx, {'stage': 'description.md', 'content': 'bad'})).is_error
    assert (work / 'description.md').read_text() == 'report'
