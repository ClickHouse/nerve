import { describe, expect, it } from 'vitest';
import type { MessageBlock } from '../types/chat';
import { findPullRequests, MAX_PR_CARDS } from './pullRequestLinks';

const text = (content: string): MessageBlock => ({ type: 'text', content });
const tool = (result: string): MessageBlock => ({
  type: 'tool_call', toolUseId: 't1', tool: 'Bash', input: {}, result, status: 'complete',
});

const urls = (blocks: MessageBlock[]) => findPullRequests(blocks).map(pr => pr.url);

describe('findPullRequests', () => {
  it('finds a pull request in text with its repo and number', () => {
    expect(findPullRequests([text('Opened https://github.com/example/repo/pull/12 for review.')]))
      .toEqual([{ url: 'https://github.com/example/repo/pull/12', repo: 'example/repo', number: 12 }]);
  });

  it('finds links in markdown, angle brackets and JSON tool results', () => {
    expect(urls([
      text('See [the PR](https://github.com/example/repo/pull/1) and <https://github.com/example/repo/pull/2>.'),
    ])).toEqual([
      'https://github.com/example/repo/pull/1',
      'https://github.com/example/repo/pull/2',
    ]);
    expect(urls([
      tool('[{"type":"text","text":"https://github.com/example/other-repo/pull/3\\n"}]'),
      text('Opened the pull request.'),
    ])).toEqual(['https://github.com/example/other-repo/pull/3']);
  });

  it('cuts a longer address to the pull request URL', () => {
    expect(urls([text('https://github.com/example/repo/pull/12/files#diff-1 and https://github.com/example/repo/pull/13#issuecomment-5')]))
      .toEqual(['https://github.com/example/repo/pull/12', 'https://github.com/example/repo/pull/13']);
  });

  it.each([
    ['the end of the text', ''],
    ['a path', '/files'],
    ['a query', '?w=1'],
    ['a fragment', '#discussion_r1'],
    ['a period', '.'],
    ['a comma', ', then'],
    ['a parenthesis', ')'],
    ['a bracket', ']'],
    ['an angle bracket', '>'],
    ['a colon', ': done'],
    ['an exclamation mark', '!'],
    ['a quote', '"'],
    ['a backtick', '`'],
    ['bold markers', '**'],
    ['a line break', '\nnext'],
  ])('finds the URL when %s follows the number', (_, suffix) => {
    expect(urls([text(`https://github.com/example/repo/pull/12${suffix}`)]))
      .toEqual(['https://github.com/example/repo/pull/12']);
  });

  it.each(['12abc', '12x', '12-fix', '12A'])('ignores /pull/%s', (tail) => {
    expect(urls([text(`https://github.com/example/repo/pull/${tail}`)])).toEqual([]);
  });

  it('lists each pull request once', () => {
    expect(urls([
      text('Done: https://github.com/example/repo/pull/9 and https://github.com/Example/Repo/pull/7'),
      text('Again https://github.com/example/repo/pull/9 and https://github.com/example/repo/pull/7'),
    ])).toEqual([
      'https://github.com/example/repo/pull/9',
      'https://github.com/Example/Repo/pull/7',
    ]);
  });

  it('reads the tool results only when the text links to no pull request', () => {
    const list = tool([7, 8, 9].map(n => `https://github.com/example/repo/pull/${n}`).join('\n'));
    expect(urls([list, text('Merge https://github.com/example/repo/pull/12 first.')]))
      .toEqual(['https://github.com/example/repo/pull/12']);
    expect(urls([tool('https://github.com/example/repo/pull/13'), text('Opened the pull request.')]))
      .toEqual(['https://github.com/example/repo/pull/13']);
  });

  it('reads only a tool result that links to exactly one pull request', () => {
    const listed = [7, 8, 9].map(n => `{"url":"https://github.com/example/repo/pull/${n}"}`).join(',');
    expect(urls([tool(`[${listed}]`), text('There are three open pull requests.')])).toEqual([]);
    expect(urls([
      tool(`[${listed}]`),
      tool('https://github.com/example/repo/pull/13\nhttps://github.com/example/repo/pull/13'),
      text('Opened the pull request.'),
    ])).toEqual(['https://github.com/example/repo/pull/13']);
  });

  it(`stops at ${MAX_PR_CARDS}`, () => {
    const many = Array.from({ length: 8 }, (_, i) => `https://github.com/example/repo/pull/${i + 1}`).join(' ');
    expect(urls([text(many)])).toHaveLength(MAX_PR_CARDS);
  });

  it.each([
    'http://github.com/example/repo/pull/12',
    'https://gitlab.com/example/repo/pull/12',
    'https://github.com/example/repo/issues/12',
    'https://github.com/example/repo/pulls',
    'https://github.com/example/repo/pull/0',
    'https://github.com/-example/repo/pull/12',
    'https://github.com/example/../pull/12',
    'https://github.com/example/repo/pull/12345678901',
  ])('ignores %s', (address) => {
    expect(urls([text(address)])).toEqual([]);
  });

  it('ignores tool inputs, thinking and running tool calls', () => {
    expect(urls([
      { type: 'thinking', content: 'https://github.com/example/repo/pull/1' },
      {
        type: 'tool_call', toolUseId: 't2', tool: 'Bash', status: 'running',
        input: { command: 'gh pr view https://github.com/example/repo/pull/2' },
      },
    ])).toEqual([]);
  });
});
