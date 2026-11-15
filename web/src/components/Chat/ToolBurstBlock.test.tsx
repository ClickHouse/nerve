import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import type { MessageBlock, ToolCallBlockData } from '../../types/chat';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));

const { BlockRenderer } = await import('./BlockRenderer');

function call(tool: string, input: Record<string, unknown>, extra: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return { type: 'tool_call', toolUseId: `tu-${tool}-${JSON.stringify(input)}`, tool, input, result: 'ok', status: 'complete', ...extra };
}
const text = (content: string): MessageBlock => ({ type: 'text', content });

const BURST: MessageBlock[] = [
  { type: 'thinking', content: 'Look at the login code first.' },
  call('Read', { file_path: 'src/login.ts' }),
  call('Bash', { command: 'npm test' }, { isError: true, result: '1 test failed' }),
  { type: 'thinking', content: 'The timeout is too short.' },
  call('Edit', { file_path: 'src/login.ts', old_string: 'a', new_string: 'b' }),
  call('Bash', { command: 'npm test -- login' }),
];
const SUMMARY = /^Read 1 file, ran 2 commands, edited 1 file\W+1 failed$/;

function show(blocks: MessageBlock[], streaming = false) {
  const view = render(<MemoryRouter><BlockRenderer blocks={blocks} streaming={streaming} rollUpTools /></MemoryRouter>);
  return {
    rerender: (next: MessageBlock[], nextStreaming = streaming) =>
      view.rerender(<MemoryRouter><BlockRenderer blocks={next} streaming={nextStreaming} rollUpTools /></MemoryRouter>),
  };
}
const line = () => screen.getByRole('button', { name: SUMMARY });

describe('ToolBurstBlock', () => {
  it('folds a finished burst to one line and opens it on a click', async () => {
    show([text('I check the login test.'), ...BURST, text('The test passes.')]);
    expect(line()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText('src/login.ts')).toBeNull();
    expect(screen.getByText('The test passes.')).toBeInTheDocument();

    await userEvent.click(line());
    expect(line()).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getAllByText('src/login.ts')).toHaveLength(2);
    expect(screen.getByText('npm test')).toBeInTheDocument();
  });

  it('shows the failed calls in the error colour', () => {
    show(BURST);
    expect(screen.getByText('1 failed')).toHaveClass('text-error');
  });

  it('stays open while it is the end of a running turn, and folds when text follows', () => {
    const view = show([text('Start.'), ...BURST], true);
    expect(line()).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('npm test -- login')).toBeInTheDocument();

    view.rerender([text('Start.'), ...BURST, text('Next')], true);
    expect(line()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText('npm test -- login')).toBeNull();
  });

  it('folds when the turn ends', () => {
    const view = show(BURST, true);
    expect(line()).toHaveAttribute('aria-expanded', 'true');
    view.rerender(BURST, false);
    expect(line()).toHaveAttribute('aria-expanded', 'false');
  });

  it('keeps the choice of the person over the live state', async () => {
    const view = show(BURST, true);
    await userEvent.click(line());
    expect(line()).toHaveAttribute('aria-expanded', 'false');
    view.rerender([...BURST, call('Grep', { pattern: 'timeout' })], true);
    expect(screen.getByRole('button', { name: /^Read 1 file, ran 2 commands, edited 1 file, searched once/ }))
      .toHaveAttribute('aria-expanded', 'false');
  });

  it('never hides a question card in a finished turn', async () => {
    show([
      call('Read', { file_path: 'a.ts' }), call('Bash', { command: 'ls' }),
      call('mcp__nerve__ask_user', { title: 'Merge the PR now?' }, { result: 'Question sent (ask-0000beef).' }),
      call('Read', { file_path: 'b.ts' }), call('Bash', { command: 'pwd' }),
      text('I wait for the answer.'),
    ]);
    await act(async () => {}); // the card reads its question
    expect(screen.getByText('Merge the PR now?')).toBeInTheDocument();
    const lines = screen.getAllByRole('button', { name: 'Read 1 file, ran 1 command' });
    expect(lines.map(b => b.getAttribute('aria-expanded'))).toEqual(['false', 'false']);
  });

  it('says "Running" to a screen reader while a call of a folded burst runs', () => {
    const view = show([
      ...BURST.slice(0, -1),
      call('Bash', { command: 'npm test -- login' }, { status: 'running', result: undefined }),
    ]);
    const button = screen.getByRole('button', { name: /^Read 1 file, ran 2 commands, edited 1 file\W+1 failed\W*Running$/ });
    expect(button).toHaveAttribute('aria-expanded', 'false');
    expect(screen.getByText('Running')).toHaveClass('sr-only');

    view.rerender(BURST);
    expect(line()).toBeInTheDocument();
    expect(screen.queryByText('Running')).toBeNull();
  });

  it('shows a burst of one call as its row, with no summary line', () => {
    show([text('One look.'), call('Read', { file_path: 'a.ts' }), text('Fine.')]);
    expect(screen.getByText('a.ts')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Read 1 file' })).toBeNull();
  });
});
