import { fireEvent, render, screen, within } from '@testing-library/react';
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
const { QuestionBlock } = await import('./QuestionBlock');

const ACCOUNT = {
  question: 'Which account should the agent act as on the code host?',
  header: 'Account',
  multiSelect: false,
  options: [
    { label: 'alice (recommended)', description: 'Your own account' },
    { label: 'A bot account', description: 'Needs a token' },
  ],
};
const REPORT = {
  question: 'Where should results go?',
  header: 'Report',
  multiSelect: true,
  options: [
    { label: 'This chat', description: '' },
    { label: 'Notifications', description: '' },
  ],
};

function ask(questions: object[], overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: 'tu-ask', tool: 'AskUserQuestion', input: { questions }, status: 'running', ...overrides,
  };
}

/** The agent's turn waits on the question. */
function pause() {
  useChatStore.setState({
    pendingInteraction: { interactionId: 'i-ask', interactionType: 'question', toolName: 'AskUserQuestion', toolInput: {} },
  });
}

const group = (name: string) => within(screen.getByRole('group', { name }));

beforeEach(() => {
  vi.mocked(ws.answerInteraction).mockClear();
  useChatStore.setState({ activeSession: 'session-1', pendingInteraction: null });
});

describe('AskUserQuestion: Other…', () => {
  it('is offered under the options of every question', () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT, REPORT])} />);
    expect(group(ACCOUNT.question).getByRole('button', { name: /^Other…/ })).toHaveAttribute('aria-pressed', 'false');
    expect(group(REPORT.question).getByRole('button', { name: /^Other…/ })).toBeInTheDocument();
  });

  it('keeps a click on an option the answer to a single simple question', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT])} />);
    expect(screen.queryByRole('button', { name: 'Submit' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /^A bot account/ }));
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-ask', { [ACCOUNT.question]: 'A bot account' });
  });

  it('answers a single simple question in the person\'s own words, once they say something', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT])} />);
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    expect(screen.getByRole('button', { name: /^Other…/ })).toHaveAttribute('aria-pressed', 'true');

    const answer = screen.getByRole('textbox', { name: 'Your answer' });
    expect(answer).toHaveFocus();
    expect(answer).toBeRequired();
    const submit = screen.getByRole('button', { name: 'Submit' });
    expect(submit).toBeDisabled();
    await userEvent.type(answer, '   ');
    expect(submit).toBeDisabled();
    await userEvent.keyboard('{Enter}');
    expect(ws.answerInteraction).not.toHaveBeenCalled();

    await userEvent.clear(answer);
    await userEvent.type(answer, 'the review-bot account');
    expect(submit).toBeEnabled();
    await userEvent.click(submit);
    expect(ws.answerInteraction).toHaveBeenCalledWith(
      'session-1', 'i-ask', { [ACCOUNT.question]: 'the review-bot account' },
    );
    expect(screen.getByText('Answered')).toBeInTheDocument();
    expect(screen.getByText('the review-bot account')).toBeInTheDocument();
  });

  it('sends with Enter from the text field once every question has an answer', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT])} />);
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    await userEvent.type(screen.getByRole('textbox', { name: 'Your answer' }), 'bot{Enter}');
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-ask', { [ACCOUNT.question]: 'bot' });
  });

  it('replaces the option on a single-select question, and an option replaces it back', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT, REPORT])} />);
    const account = group(ACCOUNT.question);
    await userEvent.click(account.getByRole('button', { name: /^alice/ }));
    await userEvent.click(account.getByRole('button', { name: /^Other…/ }));
    expect(account.getByRole('button', { name: /^alice/ })).toHaveAttribute('aria-pressed', 'false');
    await userEvent.type(account.getByRole('textbox', { name: 'Your answer' }), 'the review-bot account');

    await userEvent.click(group(REPORT.question).getByRole('button', { name: /^This chat/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Submit' }));
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-ask', {
      [ACCOUNT.question]: 'the review-bot account',
      [REPORT.question]: 'This chat',
    });
  });

  it('drops the own words when an option is chosen instead on a single-select question', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT, REPORT])} />);
    const account = group(ACCOUNT.question);
    await userEvent.click(account.getByRole('button', { name: /^Other…/ }));
    await userEvent.type(account.getByRole('textbox', { name: 'Your answer' }), 'somebody else');
    await userEvent.click(account.getByRole('button', { name: /^alice/ }));
    expect(account.queryByRole('textbox')).not.toBeInTheDocument();
    expect(account.getByRole('button', { name: /^Other…/ })).toHaveAttribute('aria-pressed', 'false');

    await userEvent.click(group(REPORT.question).getByRole('button', { name: /^Notifications/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Submit' }));
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-ask', {
      [ACCOUNT.question]: 'alice (recommended)',
      [REPORT.question]: 'Notifications',
    });
  });

  it('appends the own words to the chosen options on a multi-select question', async () => {
    pause();
    render(<QuestionBlock block={ask([REPORT])} />);
    await userEvent.click(screen.getByRole('button', { name: /^This chat/ }));
    await userEvent.click(screen.getByRole('button', { name: /^Notifications/ }));
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    expect(screen.getByRole('button', { name: /^This chat/ })).toHaveAttribute('aria-pressed', 'true');

    // Chosen but empty: not an answer yet.
    const submit = screen.getByRole('button', { name: 'Submit' });
    expect(submit).toBeDisabled();
    await userEvent.type(screen.getByRole('textbox', { name: 'Your answer' }), 'a weekly email');
    await userEvent.click(submit);
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-ask', {
      [REPORT.question]: 'This chat, Notifications, a weekly email',
    });
  });

  it('turns the own words off again on a multi-select question', async () => {
    pause();
    render(<QuestionBlock block={ask([REPORT])} />);
    await userEvent.click(screen.getByRole('button', { name: /^This chat/ }));
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    expect(screen.getByRole('button', { name: 'Submit' })).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Submit' })).toBeEnabled();
  });

  it('keeps Submit disabled until every question has an answer', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT, REPORT])} />);
    const submit = screen.getByRole('button', { name: 'Submit' });
    await userEvent.click(group(ACCOUNT.question).getByRole('button', { name: /^Other…/ }));
    await userEvent.type(group(ACCOUNT.question).getByRole('textbox', { name: 'Your answer' }), 'bot');
    expect(submit).toBeDisabled();
    await userEvent.click(group(REPORT.question).getByRole('button', { name: /^This chat/ }));
    expect(submit).toBeEnabled();
  });

  it('shows the own words again from the tool result after a reload', () => {
    const result = `User has answered your questions: "${ACCOUNT.question}"="the review-bot account", `
      + `"${REPORT.question}"="This chat, a weekly email". You can now continue with the user's answers in mind.`;
    render(<QuestionBlock block={ask([ACCOUNT, REPORT], { status: 'complete', result })} />);

    const account = group(ACCOUNT.question);
    expect(account.getByRole('button', { name: /^Other…/ })).toHaveAttribute('aria-pressed', 'true');
    expect(account.getByText('the review-bot account')).toBeInTheDocument();
    expect(account.getByRole('button', { name: /^alice/ })).toHaveAttribute('aria-pressed', 'false');

    const report = group(REPORT.question);
    expect(report.getByRole('button', { name: /^This chat/ })).toHaveAttribute('aria-pressed', 'true');
    expect(report.getByText('a weekly email')).toBeInTheDocument();
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Submit' })).not.toBeInTheDocument();
  });

  it('re-highlights an option whose label holds a comma, without taking it for own words', () => {
    const question = { ...REPORT, options: [{ label: 'Chat, then email', description: '' }, { label: 'Slack', description: '' }] };
    const result = `User has answered your questions: "${REPORT.question}"="Chat, then email, Slack".`;
    render(<QuestionBlock block={ask([question], { status: 'complete', result })} />);
    expect(screen.getByRole('button', { name: /^Chat, then email/ })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: /^Slack/ })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: /^Other…/ })).toHaveAttribute('aria-pressed', 'false');
  });

  it('keeps Escape in the own-words field from stopping the waiting turn', async () => {
    // The app-wide Escape (a document listener) stops a running turn; the
    // turn is running while it waits for this answer.
    const onDocumentKey = vi.fn();
    document.addEventListener('keydown', onDocumentKey);
    try {
      pause();
      render(<QuestionBlock block={ask([ACCOUNT])} />);
      const other = screen.getByRole('button', { name: /^Other…/ });
      await userEvent.click(other);
      await userEvent.type(screen.getByRole('textbox', { name: 'Your answer' }), 'the review-bot account');
      await userEvent.keyboard('{Escape}');

      expect(onDocumentKey.mock.calls.some(([e]) => (e as KeyboardEvent).key === 'Escape')).toBe(false);
      // It only leaves the field: back on Other…, still chosen, text kept.
      expect(other).toHaveFocus();
      expect(other).toHaveAttribute('aria-pressed', 'true');
      expect(screen.getByRole('textbox', { name: 'Your answer' })).toHaveValue('the review-bot account');
      expect(ws.answerInteraction).not.toHaveBeenCalled();
    } finally {
      document.removeEventListener('keydown', onDocumentKey);
    }
  });

  it('does not submit on the Enter that confirms an input method composition', async () => {
    pause();
    render(<QuestionBlock block={ask([ACCOUNT])} />);
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    const answer = screen.getByRole('textbox', { name: 'Your answer' });
    await userEvent.type(answer, 'bot');
    fireEvent.keyDown(answer, { key: 'Enter', isComposing: true });
    expect(ws.answerInteraction).not.toHaveBeenCalled();
    fireEvent.keyDown(answer, { key: 'Enter' });
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-ask', { [ACCOUNT.question]: 'bot' });
  });

  it('renders markdown in the question and in option descriptions, with no link inside an option', () => {
    pause();
    const question = {
      question: 'Should `alice` post review comments? See [the rules](https://example.com/rules).',
      header: 'Posting',
      multiSelect: false,
      options: [
        { label: 'Drafts only (recommended)', description: 'You approve each one in **this chat**' },
        { label: 'Post directly', description: 'Needs a token, see [the docs](https://example.com/docs)' },
      ],
    };
    render(<QuestionBlock block={ask([question])} />);
    const posting = group('Should alice post review comments? See the rules.');
    expect(posting.getByText('alice').tagName).toBe('CODE');
    expect(posting.getByRole('link', { name: 'the rules' })).toHaveAttribute('href', 'https://example.com/rules');
    expect(posting.getByText('this chat').tagName).toBe('STRONG');
    const direct = posting.getByRole('button', { name: /^Post directly/ });
    expect(within(direct).queryByRole('link')).toBeNull();
    expect(direct).toHaveTextContent('Needs a token, see the docs');
  });

  it('answers a question that comes without options in the person\'s own words', async () => {
    pause();
    const free = { question: ACCOUNT.question, header: ACCOUNT.header, multiSelect: false };
    render(<QuestionBlock block={ask([free])} />);
    await userEvent.click(screen.getByRole('button', { name: /^Other…/ }));
    await userEvent.type(screen.getByRole('textbox', { name: 'Your answer' }), 'the review-bot account{Enter}');
    expect(ws.answerInteraction).toHaveBeenCalledWith(
      'session-1', 'i-ask', { [ACCOUNT.question]: 'the review-bot account' },
    );
  });
});
