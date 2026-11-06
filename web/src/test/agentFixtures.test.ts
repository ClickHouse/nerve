import { describe, expect, it } from 'vitest';
import {
  FX_CHILD, FX_CODEX, FX_COMPLETED, FX_FAILED, FX_PARENT, FX_SESSION_ID, FX_SHELL, FX_STOPPED_BY_USER, FX_WAVE,
  makeAgentStoreSeed, makeAgentView, makeSnapshot, makeTranscriptItems, makeTranscriptState,
} from './agentFixtures';

describe('agent fixtures', () => {
  it('derive the server fields the way to_view does', () => {
    expect(makeAgentView({ name: null, description: '', command: 'npm run e2e', kind: 'shell' }).label).toBe('npm run e2e');
    expect(makeAgentView({ status: 'failed' })).toMatchObject({ attention: 'failed', end_reason: 'error' });
    expect(FX_STOPPED_BY_USER.controls).toEqual({ stop: false, transcript: true, output: false, message: false });
    expect(FX_COMPLETED.controls.message).toBe(true);
    expect(FX_SHELL.controls).toEqual({ stop: true, transcript: false, output: true, message: false });
    expect(FX_CODEX.controls).toEqual({ stop: false, transcript: false, output: false, message: false });
    expect(FX_CHILD.parent_agent_id).toBe(FX_PARENT.id);
    expect(FX_FAILED.attention).toBe('failed');
  });

  it('give every wave agent a unique id and spawn id in the fixture session', () => {
    expect(FX_WAVE).toHaveLength(8);
    expect(new Set(FX_WAVE.map(a => a.id)).size).toBe(8);
    expect(new Set(FX_WAVE.map(a => a.spawn_tool_use_id)).size).toBe(8);
    expect(FX_WAVE.every(a => a.session_id === FX_SESSION_ID)).toBe(true);
  });

  it('summarise, snapshot and seed consistently', () => {
    const snapshot = makeSnapshot(FX_WAVE);
    expect(snapshot.summary).toMatchObject({ total: 8, live: 5, completed: 1, failed: 1, stopped: 1, shells_live: 1, attention: 2 });
    const seed = makeAgentStoreSeed(FX_WAVE);
    expect(Object.keys(seed.agents!)).toHaveLength(8);
    expect(seed.bySpawn![FX_PARENT.spawn_tool_use_id!]).toBe(FX_PARENT.id);
  });

  it('build contiguous transcripts', () => {
    const items = makeTranscriptItems(6, 11);
    expect(items.map(i => i.seq)).toEqual([11, 12, 13, 14, 15, 16]);
    expect(items.map(i => i.kind)).toEqual(['text', 'tool_call', 'tool_result', 'text', 'tool_call', 'tool_result']);
    expect(makeTranscriptState(items)).toMatchObject({ status: 'ready', firstSeq: 11, lastSeq: 16, hasMoreBefore: true });
  });
});
