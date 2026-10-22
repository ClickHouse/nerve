import { describe, expect, it } from 'vitest';
import {
  addItems, answerOf, draftFor, EMPTY_DRAFT, formQuestions, isAskFormTool, isValidUrl, normalizeRepo,
  parseFormResult, problemOf, splitItems,
} from './formQuestions';

const [REPOS, CADENCE, REPORT, NOTES] = formQuestions({
  questions: [
    { id: 'repos', kind: 'repos', label: 'Repositories', question: 'Which repositories?' },
    { id: 'cadence', kind: 'choice', label: 'Cadence', question: 'How often?', options: [{ label: 'Hourly' }, { label: 'Daily' }] },
    { id: 'report', kind: 'multi', label: 'Report', question: 'Where to?', options: [{ label: 'This chat' }, { label: 'Notifications' }] },
    { id: 'notes', kind: 'text', label: 'Notes', question: 'Anything else?', required: false },
  ],
});

describe('formQuestions', () => {
  it('fills in the defaults for the keys a call leaves out', () => {
    expect(CADENCE).toMatchObject({
      required: true, allowOther: true, help: '', placeholder: '', defaultValue: null,
      options: [{ label: 'Hourly', description: '' }, { label: 'Daily', description: '' }],
    });
    expect(NOTES).toMatchObject({ required: false, allowOther: false, options: [] });
    const [text] = formQuestions({
      questions: [{ id: 'x', kind: 'text', label: 'X', question: 'X?', allow_other: true, options: [{ label: 'a' }] }],
    });
    expect(text).toMatchObject({ allowOther: false, options: [] });
  });

  it('reads questions sent as a JSON string, and skips entries without an id or with a repeated one', () => {
    const questions = formQuestions({
      questions: JSON.stringify([
        { id: 'a', kind: 'url', label: 'A', question: 'A?' },
        { kind: 'text', label: 'No id', question: '?' },
        { id: 'a', kind: 'text', label: 'Again', question: '?' },
        { id: 'b', kind: 'unknown', question: 'B?' },
      ]),
    });
    expect(questions.map(q => [q.id, q.kind, q.label])).toEqual([['a', 'url', 'A'], ['b', 'text', 'b']]);
    expect(formQuestions({ questions: 'not json' })).toEqual([]);
    expect(formQuestions({ arguments: '{}' })).toEqual([]);
  });

  it('knows the tool by its MCP name', () => {
    expect(isAskFormTool('mcp__nerve__ask_form')).toBe(true);
    expect(isAskFormTool('ask_form')).toBe(true);
    expect(isAskFormTool('mcp__nerve__ask_user')).toBe(false);
    expect(isAskFormTool('AskUserQuestion')).toBe(false);
  });
});

describe('items and addresses', () => {
  it('turns a GitHub address into owner/name and leaves anything else as typed', () => {
    expect(normalizeRepo(' https://github.com/org/repo-a ')).toBe('org/repo-a');
    expect(normalizeRepo('https://www.github.com/org/repo-a.git/')).toBe('org/repo-a');
    expect(normalizeRepo('github.com/org/repo-a')).toBe('org/repo-a');
    expect(normalizeRepo('git@github.com:org/repo-a.git')).toBe('org/repo-a');
    expect(normalizeRepo('https://github.com/org/repo-a/pull/12')).toBe('https://github.com/org/repo-a/pull/12');
    expect(normalizeRepo('https://code.example.com/org/repo-a')).toBe('https://code.example.com/org/repo-a');
  });

  it('splits at commas and line breaks, dropping blanks and repeats', () => {
    expect(splitItems('a, b,,\nc\r\n')).toEqual(['a', 'b', 'c']);
    // Repositories compare without case, as GitHub compares them.
    expect(addItems(['org/Repo-A'], ['org/repo-a', 'org/repo-b'], 'repos')).toEqual(['org/Repo-A', 'org/repo-b']);
    expect(addItems(['Reviews'], ['reviews', ' Reviews '], 'list')).toEqual(['Reviews', 'reviews']);
  });

  it('takes an http(s) address with a host and no whitespace', () => {
    expect(isValidUrl('https://docs.example.com/a?b=c')).toBe(true);
    expect(isValidUrl(' http://localhost:8080 ')).toBe(true);
    for (const bad of ['https://', 'https://docs example.com', 'HTTPS://docs.example.com', 'ftp://docs.example.com', 'docs.example.com']) {
      expect(isValidUrl(bad)).toBe(false);
    }
  });
});

describe('drafts and answers', () => {
  it('gives a string for a choice and a list for a multi, the own words last', () => {
    expect(answerOf(CADENCE, { ...EMPTY_DRAFT, chosen: [1] })).toBe('Daily');
    expect(answerOf(CADENCE, { ...EMPTY_DRAFT, chosen: [1], other: ' Weekly ' })).toBe('Weekly');
    expect(answerOf(REPORT, { ...EMPTY_DRAFT, chosen: [1, 0], other: 'email' })).toEqual(['This chat', 'Notifications', 'email']);
    expect(answerOf(REPOS, { ...EMPTY_DRAFT, items: ['org/repo-a'], text: 'org/repo-b' })).toEqual(['org/repo-a', 'org/repo-b']);
    expect(answerOf(NOTES, { ...EMPTY_DRAFT, text: '   ' })).toBeNull();
  });

  it('names what holds a draft back', () => {
    expect(problemOf(CADENCE, EMPTY_DRAFT)).toBe('missing');
    expect(problemOf(NOTES, EMPTY_DRAFT)).toBeNull();
    expect(problemOf(REPORT, { ...EMPTY_DRAFT, chosen: [0], other: '' })).toBe('other');
    expect(problemOf(REPOS, { ...EMPTY_DRAFT, items: ['org/repo-a', 'repo-b'] })).toBe('repos');
    expect(problemOf(REPOS, { ...EMPTY_DRAFT, text: 'repo-b' })).toBe('repos');
  });

  it('reads a default (or an answer) back into a draft', () => {
    expect(draftFor(CADENCE, 'Daily').chosen).toEqual([1]);
    expect(draftFor(CADENCE, 'Weekly').other).toBe('Weekly');
    expect(draftFor(REPORT, ['Notifications', 'email', 'This chat'])).toMatchObject({ chosen: [0, 1], other: 'email' });
    expect(draftFor(REPOS, 'org/repo-a, https://github.com/org/repo-b').items).toEqual(['org/repo-a', 'org/repo-b']);
    // A value that is no option, where Other… is off: dropped as a default, shown as an answer.
    const closed = { ...CADENCE, allowOther: false };
    expect(draftFor(closed, 'Weekly').other).toBeNull();
    expect(draftFor(closed, 'Weekly', true).other).toBe('Weekly');
  });
});

describe('parseFormResult', () => {
  const questions = [REPOS, CADENCE, NOTES];
  const text = [
    'The person answered the form.',
    'Repositories: org/repo-a, org/repo-b',
    'Cadence: Daily',
    'Notes: (skipped)',
    '',
    '{"answers": {"repos": ["org/repo-a", "org/repo-b"], "cadence": "Daily"}, "skipped": ["notes"]}',
  ].join('\n');

  it('reads the JSON line, inside its MCP content block or bare', () => {
    const expected = { answers: { repos: ['org/repo-a', 'org/repo-b'], cadence: 'Daily' }, skipped: ['notes'] };
    expect(parseFormResult(JSON.stringify([{ type: 'text', text }]), questions)).toEqual(expected);
    expect(parseFormResult(text, questions)).toEqual(expected);
  });

  it('reads a pretty-printed JSON object too', () => {
    const pretty = `The person answered the form.\n\n${JSON.stringify({ answers: { cadence: 'Hourly' }, skipped: [] }, null, 2)}`;
    expect(parseFormResult(pretty, questions)).toEqual({ answers: { cadence: 'Hourly' }, skipped: [] });
  });

  it('falls back to the readable lines, a multi-line answer included', () => {
    const lines = 'The person answered the form.\nRepositories: org/repo-a\nCadence: (skipped)\nNotes: Never merge.\n  Ask first.';
    expect(parseFormResult(lines, questions)).toEqual({
      answers: { repos: ['org/repo-a'], notes: 'Never merge.\nAsk first.' },
      skipped: ['cadence'],
    });
  });

  it('finds nothing in a result without either', () => {
    expect(parseFormResult('Done.', questions)).toBeNull();
    expect(parseFormResult(undefined, questions)).toBeNull();
  });
});
