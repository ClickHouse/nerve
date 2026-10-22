import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ToolCallBlockData } from '../../../types/chat';

vi.mock('../../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    answerInteraction: vi.fn(),
  },
}));

const { ws } = await import('../../../api/websocket');
const { useChatStore } = await import('../../../stores/chatStore');
const { FormBlock } = await import('./FormBlock');
const { ToolCallBlock } = await import('../ToolCallBlock');

type Q = Record<string, unknown>;

const REPOS: Q = {
  id: 'repos', kind: 'repos', label: 'Repositories', question: 'Which repositories should it watch?',
  help: 'Owner and name, as on GitHub.',
};
const CADENCE: Q = {
  id: 'cadence', kind: 'choice', label: 'Cadence', question: 'How often should it look?',
  options: [
    { label: 'Every 30 minutes (recommended)', description: 'Quick enough for reviews' },
    { label: 'Hourly' },
    { label: 'Daily' },
  ],
};
const REPORT: Q = {
  id: 'report', kind: 'multi', label: 'Report', question: 'Where should results go?',
  options: [{ label: 'This chat' }, { label: 'Notifications' }],
};
const TOPICS: Q = { id: 'topics', kind: 'list', label: 'Topics', question: 'Which topics matter?' };
const DOCS: Q = { id: 'docs', kind: 'url', label: 'Docs', question: 'Where are the team docs?', required: false };
const NOTES: Q = { id: 'notes', kind: 'text', label: 'Notes', question: 'Anything it must know?', required: false };

/** The call as the model made it: keys may be left out. */
function form(questions: Q[], overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: 'tu-form', tool: 'mcp__nerve__ask_form',
    input: { title: 'A few facts', questions }, status: 'running', ...overrides,
  };
}

/** The server's copy of a question: every key, the contract's defaults filled in. */
function normalized(q: Q): Q {
  const offersOptions = q.kind === 'choice' || q.kind === 'multi';
  return {
    help: '', placeholder: '', required: true, default: null, ...q,
    options: offersOptions ? (q.options as Q[]).map(o => ({ description: '', ...o })) : [],
    allow_other: offersOptions ? q.allow_other !== false : false,
  };
}

/** The agent's turn waits on a form asking these questions. */
function pause(questions: Q[]) {
  useChatStore.setState({
    pendingInteraction: {
      interactionId: 'i-form', interactionType: 'form', toolName: 'ask_form',
      toolInput: { title: 'A few facts', questions: questions.map(normalized) },
    },
  });
}

/** A live form card, waiting for these questions. */
function show(questions: Q[]) {
  pause(questions);
  return render(<FormBlock block={form(questions)} />);
}

/** The tool result as the server writes it: one MCP text block. */
function resultOf(lines: string[], json?: object): string {
  const text = ['The person answered the form.', ...lines, ...(json ? ['', JSON.stringify(json)] : [])].join('\n');
  return JSON.stringify([{ type: 'text', text }]);
}

const group = (name: unknown) => within(screen.getByRole('group', { name: name as string }));
const fieldOf = (q: Q) => group(q.question).getByRole('textbox', { name: q.question as string });
const submitButton = () => screen.getByRole('button', { name: 'Submit' });
const answered = (answers: Record<string, string | string[]>) =>
  expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-form', { answers });

/** The folded card's answer line for the question with this chip. */
function answerTo(label: string): HTMLElement {
  const term = screen.getAllByRole('term').find(t => t.textContent === label);
  if (!term?.parentElement) throw new Error(`no answer line for ${label}`);
  return within(term.parentElement).getByRole('definition');
}

beforeEach(() => {
  vi.mocked(ws.answerInteraction).mockClear();
  useChatStore.setState({ activeSession: 'session-1', pendingInteraction: null });
});

describe('ask_form: answering', () => {
  it('answers every kind with the shape the server takes', async () => {
    show([REPOS, CADENCE, REPORT, TOPICS, DOCS, NOTES]);
    expect(screen.getByRole('form', { name: 'A few facts' })).toBeInTheDocument();
    await userEvent.type(fieldOf(REPOS), 'org/repo-a{Enter}org/repo-b,');
    await userEvent.click(group(CADENCE.question).getByRole('button', { name: /^Hourly/ }));
    await userEvent.click(group(REPORT.question).getByRole('button', { name: /^This chat/ }));
    await userEvent.click(group(REPORT.question).getByRole('button', { name: /^Notifications/ }));
    // The last item needs no comma or Enter.
    await userEvent.type(fieldOf(TOPICS), 'reviews, releases');
    await userEvent.type(fieldOf(DOCS), 'https://docs.example.com/guide');
    // Enter is a new line in a text answer.
    await userEvent.type(fieldOf(NOTES), 'Never merge.{Enter}Ask first.');
    await userEvent.click(submitButton());

    answered({
      repos: ['org/repo-a', 'org/repo-b'],
      cadence: 'Hourly',
      report: ['This chat', 'Notifications'],
      topics: ['reviews', 'releases'],
      docs: 'https://docs.example.com/guide',
      notes: 'Never merge.\nAsk first.',
    });
  });

  it('leaves an optional question out of the answers when it is left empty', async () => {
    show([CADENCE, DOCS, NOTES]);
    expect(group(NOTES.question).getByText('Optional')).toBeInTheDocument();
    expect(fieldOf(NOTES)).not.toBeRequired();
    await userEvent.click(screen.getByRole('button', { name: /^Daily/ }));
    await userEvent.click(submitButton());
    answered({ cadence: 'Daily' });
  });

  it('keeps Submit disabled until every required question has an answer', async () => {
    show([REPOS, CADENCE, NOTES]);
    expect(fieldOf(REPOS)).toBeRequired();
    expect(submitButton()).toBeDisabled();
    await userEvent.type(fieldOf(REPOS), 'org/repo-a{Enter}');
    expect(submitButton()).toBeDisabled();
    await userEvent.keyboard('{Control>}{Enter}{/Control}');
    expect(ws.answerInteraction).not.toHaveBeenCalled();

    await userEvent.click(screen.getByRole('button', { name: /^Every 30 minutes/ }));
    expect(submitButton()).toBeEnabled();
  });

  it('takes Other… on a choice in place of the option', async () => {
    show([CADENCE]);
    const cadence = group(CADENCE.question);
    await userEvent.click(cadence.getByRole('button', { name: /^Hourly/ }));
    await userEvent.click(cadence.getByRole('button', { name: /^Other…/ }));
    expect(cadence.getByRole('button', { name: /^Hourly/ })).toHaveAttribute('aria-pressed', 'false');
    const words = cadence.getByRole('textbox', { name: 'Your answer' });
    expect(words).toHaveFocus();
    expect(submitButton()).toBeDisabled();

    await userEvent.type(words, 'Every 2 hours');
    await userEvent.click(submitButton());
    answered({ cadence: 'Every 2 hours' });
  });

  it('drops the own words of a choice when an option is chosen instead', async () => {
    show([CADENCE]);
    const cadence = group(CADENCE.question);
    await userEvent.click(cadence.getByRole('button', { name: /^Other…/ }));
    await userEvent.type(cadence.getByRole('textbox', { name: 'Your answer' }), 'somebody decides');
    await userEvent.click(cadence.getByRole('button', { name: /^Daily/ }));
    expect(cadence.queryByRole('textbox')).not.toBeInTheDocument();
    await userEvent.click(submitButton());
    answered({ cadence: 'Daily' });
  });

  it('adds Other… on a multi after the options, and holds Submit while it is empty', async () => {
    show([REPORT]);
    const report = group(REPORT.question);
    await userEvent.click(report.getByRole('button', { name: /^Notifications/ }));
    await userEvent.click(report.getByRole('button', { name: /^This chat/ }));
    await userEvent.click(report.getByRole('button', { name: /^Other…/ }));
    expect(submitButton()).toBeDisabled();

    await userEvent.type(report.getByRole('textbox', { name: 'Your answer' }), 'a weekly email');
    await userEvent.click(submitButton());
    // In the options' order, whatever the clicks' order; the own words last.
    answered({ report: ['This chat', 'Notifications', 'a weekly email'] });
  });

  it('offers no Other… when the call turns it off', () => {
    show([{ ...CADENCE, allow_other: false }, { ...REPORT, allow_other: false }]);
    expect(screen.queryByRole('button', { name: /^Other…/ })).not.toBeInTheDocument();
  });

  it('starts from the defaults, ready to send as they are', async () => {
    show([
      { ...REPOS, default: ['org/repo-a'] },
      { ...CADENCE, default: 'Hourly' },
      { ...REPORT, default: ['Notifications', 'a weekly email'] },
      { ...TOPICS, default: ['reviews'] },
      { ...DOCS, default: 'https://docs.example.com' },
      { ...NOTES, default: 'Ask before merging.' },
    ]);
    expect(group(CADENCE.question).getByRole('button', { name: /^Hourly/ })).toHaveAttribute('aria-pressed', 'true');
    // A default that is no option is the Other… words: filled in, not focused.
    const words = group(REPORT.question).getByRole('textbox', { name: 'Your answer' });
    expect(words).toHaveValue('a weekly email');
    expect(words).not.toHaveFocus();

    await userEvent.click(submitButton());
    answered({
      repos: ['org/repo-a'],
      cadence: 'Hourly',
      report: ['Notifications', 'a weekly email'],
      topics: ['reviews'],
      docs: 'https://docs.example.com',
      notes: 'Ask before merging.',
    });
  });
});

describe('ask_form: repositories and lists', () => {
  it('marks an item that is not owner/name, and holds Submit until it is removed', async () => {
    show([REPOS]);
    const field = fieldOf(REPOS);
    await userEvent.type(field, 'org/repo-a{Enter}repo-b{Enter}');
    expect(screen.getByTitle('Not owner/name')).toHaveTextContent('repo-b');
    expect(field).toHaveAttribute('aria-invalid', 'true');
    expect(field).toHaveAccessibleDescription(/owner\/name, like org\/repo-a/);
    expect(submitButton()).toBeDisabled();

    await userEvent.click(screen.getByRole('button', { name: 'Remove repo-b' }));
    expect(field).not.toHaveAttribute('aria-invalid');
    await userEvent.click(submitButton());
    answered({ repos: ['org/repo-a'] });
  });

  it('takes a GitHub address as owner/name', async () => {
    show([REPOS]);
    await userEvent.type(fieldOf(REPOS), 'https://github.com/org/repo-a.git{Enter}git@github.com:org/repo-b.git{Enter}');
    expect(screen.getByRole('button', { name: 'Remove org/repo-a' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Remove org/repo-b' })).toBeInTheDocument();
    expect(submitButton()).toBeEnabled();
  });

  it('splits a paste at commas and line breaks; Backspace in the empty field takes the last item back', async () => {
    show([REPOS]);
    const field = fieldOf(REPOS);
    await userEvent.click(field);
    await userEvent.paste('org/repo-a, org/repo-b\norg/repo-c');
    expect(field).toHaveValue('');
    expect(screen.getByRole('button', { name: 'Remove org/repo-c' })).toBeInTheDocument();

    await userEvent.keyboard('{Backspace}');
    expect(screen.queryByRole('button', { name: 'Remove org/repo-c' })).not.toBeInTheDocument();
    await userEvent.click(submitButton());
    answered({ repos: ['org/repo-a', 'org/repo-b'] });
  });

  it('keeps what is typed as an item when the field is left, without repeating one', async () => {
    show([TOPICS, CADENCE]);
    await userEvent.type(fieldOf(TOPICS), 'reviews{Enter}reviews');
    await userEvent.tab();
    expect(screen.getAllByRole('button', { name: 'Remove reviews' })).toHaveLength(1);
    expect(fieldOf(TOPICS)).toHaveValue('');
  });

  it('adds an item with Enter rather than submitting', async () => {
    show([TOPICS]);
    await userEvent.type(fieldOf(TOPICS), 'reviews{Enter}');
    expect(screen.getByRole('button', { name: 'Remove reviews' })).toBeInTheDocument();
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });
});

describe('ask_form: addresses', () => {
  it('flags an address that is not http(s) and holds Submit until it is one', async () => {
    show([{ ...DOCS, required: true }]);
    const field = fieldOf(DOCS);
    // On its way to https://: no complaint yet.
    await userEvent.type(field, 'http');
    expect(field).not.toHaveAttribute('aria-invalid');
    expect(submitButton()).toBeDisabled();

    await userEvent.clear(field);
    await userEvent.type(field, 'docs.example.com');
    expect(field).toHaveAttribute('aria-invalid', 'true');
    expect(field).toHaveAccessibleDescription(/starting with https:\/\//);
    expect(submitButton()).toBeDisabled();

    await userEvent.clear(field);
    await userEvent.type(field, 'https://docs.example.com/guide');
    expect(field).not.toHaveAttribute('aria-invalid');
    await userEvent.click(submitButton());
    answered({ docs: 'https://docs.example.com/guide' });
  });

  it('flags an unfinished address once the field is left, optional or not', async () => {
    show([DOCS, NOTES]);
    const field = fieldOf(DOCS);
    await userEvent.type(field, 'https://');
    expect(field).not.toHaveAttribute('aria-invalid');
    await userEvent.tab();
    expect(field).toHaveAttribute('aria-invalid', 'true');
    expect(submitButton()).toBeDisabled();
  });
});

describe('ask_form: keys', () => {
  it('submits with Cmd+Enter from a text answer', async () => {
    show([NOTES]);
    await userEvent.type(fieldOf(NOTES), 'Ask first.');
    await userEvent.keyboard('{Meta>}{Enter}{/Meta}');
    answered({ notes: 'Ask first.' });
  });

  it('submits with Ctrl+Enter from a list field, the typed text as the last item', async () => {
    show([REPOS]);
    await userEvent.type(fieldOf(REPOS), 'org/repo-a');
    await userEvent.keyboard('{Control>}{Enter}{/Control}');
    answered({ repos: ['org/repo-a'] });
  });

  it('keeps Escape from stopping the waiting turn, wherever the focus is in the card', async () => {
    // The app-wide Escape (a document listener) stops a running turn; the
    // turn is running while it waits for this form.
    const onDocumentKey = vi.fn();
    document.addEventListener('keydown', onDocumentKey);
    try {
      show([CADENCE, REPOS, NOTES]);
      await userEvent.type(fieldOf(NOTES), 'Ask first.');
      await userEvent.keyboard('{Escape}');
      await userEvent.click(fieldOf(REPOS));
      await userEvent.keyboard('{Escape}');
      const other = group(CADENCE.question).getByRole('button', { name: /^Other…/ });
      await userEvent.click(other);
      // In the Other… field it goes back to Other…, the field kept.
      await userEvent.keyboard('{Escape}');
      expect(other).toHaveFocus();
      expect(group(CADENCE.question).getByRole('textbox', { name: 'Your answer' })).toBeInTheDocument();
      await userEvent.keyboard('{Escape}');

      expect(onDocumentKey.mock.calls.some(([e]) => (e as KeyboardEvent).key === 'Escape')).toBe(false);
      expect(fieldOf(NOTES)).toHaveValue('Ask first.');
      expect(ws.answerInteraction).not.toHaveBeenCalled();
    } finally {
      document.removeEventListener('keydown', onDocumentKey);
    }
  });
});

describe('ask_form: answered and read-only', () => {
  it('folds to one line per question as soon as it is answered here', async () => {
    show([REPOS, CADENCE, NOTES]);
    await userEvent.type(fieldOf(REPOS), 'org/repo-a{Enter}org/repo-b{Enter}');
    await userEvent.click(screen.getByRole('button', { name: /^Hourly/ }));
    await userEvent.click(submitButton());

    expect(screen.queryByRole('form')).not.toBeInTheDocument();
    const repos = within(answerTo('Repositories'));
    expect(repos.getByText('org/repo-a')).toBeInTheDocument();
    expect(repos.getByText('org/repo-b')).toBeInTheDocument();
    expect(answerTo('Cadence')).toHaveTextContent('Hourly');
    expect(answerTo('Notes')).toHaveTextContent('No answer');
    expect(screen.getByText('Answered')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Show questions' })).toHaveAttribute('aria-expanded', 'false');
  });

  it('shows the answers from the tool result after a reload, and the full card read-only on request', async () => {
    const result = resultOf(
      ['Repositories: org/repo-a, org/repo-b', 'Cadence: Every 30 minutes (recommended)', 'Notes: (skipped)'],
      { answers: { repos: ['org/repo-a', 'org/repo-b'], cadence: 'Every 30 minutes (recommended)' }, skipped: ['notes'] },
    );
    render(<FormBlock block={form([REPOS, CADENCE, NOTES], { status: 'complete', result })} />);
    expect(screen.queryByRole('form')).not.toBeInTheDocument();
    expect(within(answerTo('Repositories')).getByText('org/repo-b')).toBeInTheDocument();
    expect(answerTo('Cadence')).toHaveTextContent('Every 30 minutes (recommended)');
    expect(answerTo('Notes')).toHaveTextContent('No answer');

    await userEvent.click(screen.getByRole('button', { name: 'Show questions' }));
    expect(screen.getByText('A few facts')).toBeInTheDocument();
    const recommended = group(CADENCE.question).getByRole('button', { name: /^Every 30 minutes/ });
    expect(recommended).toHaveAttribute('aria-pressed', 'true');
    expect(recommended).toBeDisabled();
    const repos = group(REPOS.question);
    expect(repos.getByText('org/repo-a')).toBeInTheDocument();
    expect(repos.queryByRole('textbox')).not.toBeInTheDocument();
    expect(repos.queryByRole('button', { name: /^Remove/ })).not.toBeInTheDocument();
    expect(group(NOTES.question).getByText('No answer')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Submit' })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Hide' }));
    expect(screen.queryByRole('group', { name: CADENCE.question as string })).not.toBeInTheDocument();
    expect(answerTo('Cadence')).toBeInTheDocument();
  });

  it('reads the answers from the readable lines when the result has no JSON', () => {
    const result = resultOf(['Repositories: org/repo-a, org/repo-b', 'Notes: Never merge.', '  Ask first.', 'Cadence: Daily']);
    render(<FormBlock block={form([REPOS, NOTES, CADENCE], { status: 'complete', result })} />);
    expect(within(answerTo('Repositories')).getByText('org/repo-b')).toBeInTheDocument();
    expect(answerTo('Notes')).toHaveTextContent('Never merge. Ask first.');
    expect(answerTo('Cadence')).toHaveTextContent('Daily');
  });

  it('shows a card closed without answers for an error result', () => {
    const result = JSON.stringify([{ type: 'text', text: 'No answer within 1 hour: ask again when the person writes.' }]);
    render(<FormBlock block={form([REPOS, CADENCE], { status: 'complete', isError: true, result })} />);
    expect(screen.getByText('Closed')).toHaveAttribute('title', 'No answer within 1 hour: ask again when the person writes.');
    expect(answerTo('Repositories')).toHaveTextContent('No answer');
    expect(answerTo('Cadence')).toHaveTextContent('No answer');
  });

  it('locks when the wait ends elsewhere, and shows the answers once the result is in', async () => {
    const block = form([CADENCE]);
    pause([CADENCE]);
    const { rerender } = render(<FormBlock block={block} />);
    await userEvent.click(screen.getByRole('button', { name: /^Daily/ }));
    // Answered in another tab: the wait ends here, the result not in yet.
    act(() => useChatStore.setState({ pendingInteraction: null }));
    expect(screen.queryByRole('button', { name: 'Submit' })).not.toBeInTheDocument();

    const result = resultOf(['Cadence: Hourly'], { answers: { cadence: 'Hourly' }, skipped: [] });
    rerender(<FormBlock block={{ ...block, status: 'complete', result }} />);
    expect(answerTo('Cadence')).toHaveTextContent('Hourly');
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });

  it('does not answer a waiting form that asks other questions', async () => {
    // A newer form waits; this card is left over from a stopped turn.
    pause([REPOS]);
    render(<FormBlock block={form([CADENCE])} />);
    await userEvent.click(screen.getByRole('button', { name: /^Daily/ }));
    expect(submitButton()).toBeDisabled();
    await userEvent.keyboard('{Control>}{Enter}{/Control}');
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });

  it('answers with the server\'s copy of the questions when the running call itself does not parse', async () => {
    pause([CADENCE]);
    render(<FormBlock block={{ ...form([]), input: { arguments: 'not json' } }} />);
    await userEvent.click(screen.getByRole('button', { name: /^Daily/ }));
    await userEvent.click(submitButton());
    answered({ cadence: 'Daily' });
    // Answered, the card keeps those questions.
    expect(answerTo('Cadence')).toHaveTextContent('Daily');
  });
});

describe('ask_form in the chat', () => {
  it('shows the form card for mcp__nerve__ask_form, and a plain tool call when the call holds no question', () => {
    pause([CADENCE]);
    const { unmount } = render(<ToolCallBlock block={form([CADENCE])} />);
    expect(screen.getByRole('form', { name: 'A few facts' })).toBeInTheDocument();
    unmount();

    // Refused as invalid before any wait: nothing to answer, and a finished
    // call never takes another form's questions.
    const result = JSON.stringify([{ type: 'text', text: 'ask_form: the call is invalid: give 1 to 6 questions.' }]);
    render(<ToolCallBlock block={form([], { status: 'complete', isError: true, result })} />);
    expect(screen.getByText('mcp__nerve__ask_form')).toBeInTheDocument();
    expect(screen.queryByRole('form')).not.toBeInTheDocument();
  });
});
