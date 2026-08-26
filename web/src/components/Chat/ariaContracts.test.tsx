import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useChatStore } from '../../stores/chatStore';
import type { PanelTab } from '../../types/chat';
import { ChatInput } from './ChatInput';
import { InteractiveQuestionCard } from './InteractiveQuestionCard';
import { SidePanel } from './SidePanel';

vi.mock('../../api/client', () => ({
  api: {
    getPromptRewriteStatus: vi.fn(async () => ({ enabled: false })),
    getModels: vi.fn(async () => ({ models: [], default: null })),
    rewritePrompt: vi.fn(),
    uploadFiles: vi.fn(),
  },
}));

/**
 * Three controls that look like a richer keyboard pattern than they implement:
 * the answer pills, the run-later popup and the side-panel tab strip.
 *
 * `role="radio"`, `aria-selected` and `aria-haspopup="menu"` each commit the
 * author to a specific model — a radiogroup with one tab stop and arrow-key
 * movement, a tablist with tabpanels, a menu with `menuitem` children and focus
 * entry. None of the three is built here, so any of those claims would tell a
 * screen-reader user to press arrow keys that do nothing.
 *
 * These assert the *absence* of the claim as well as the presence of the honest
 * one, because the failure mode is adding the attribute back while everything
 * still looks and behaves correctly to a sighted mouse user.
 */

function askUserQuestion(multiSelect = false) {
  useChatStore.setState({
    pendingInteraction: {
      interactionId: 'i1',
      interactionType: 'question',
      toolName: 'AskUserQuestion',
      toolInput: {
        outOfBand: true,
        questions: [
          {
            id: 'q1',
            question: 'Which branch should this land on?',
            multiSelect,
            options: [{ label: 'main' }, { label: 'develop' }],
          },
        ],
      },
    },
  });
}

describe('InteractiveQuestionCard answer options', () => {
  beforeEach(() => {
    useChatStore.setState({ pendingInteraction: null });
  });

  it('are toggle buttons, not radios', () => {
    askUserQuestion();
    render(<InteractiveQuestionCard />);

    expect(screen.queryAllByRole('radio')).toHaveLength(0);
    expect(screen.queryByRole('radiogroup')).toBeNull();

    const main = screen.getByRole('button', { name: 'main' });
    expect(main).toHaveAttribute('aria-pressed', 'false');
    expect(main).not.toHaveAttribute('aria-checked');
  });

  it('move the pressed state from one answer to the other on single-select', async () => {
    askUserQuestion();
    render(<InteractiveQuestionCard />);
    const main = screen.getByRole('button', { name: 'main' });
    const develop = screen.getByRole('button', { name: 'develop' });

    await userEvent.click(main);
    expect(main).toHaveAttribute('aria-pressed', 'true');
    expect(develop).toHaveAttribute('aria-pressed', 'false');

    await userEvent.click(develop);
    // The single-select contract: choosing one releases the other, rather than
    // leaving two answers pressed for a question that takes one.
    expect(main).toHaveAttribute('aria-pressed', 'false');
    expect(develop).toHaveAttribute('aria-pressed', 'true');
  });

  it('hold several at once on multi-select, and are still not checkboxes', async () => {
    askUserQuestion(true);
    render(<InteractiveQuestionCard />);
    expect(screen.queryAllByRole('checkbox')).toHaveLength(0);

    await userEvent.click(screen.getByRole('button', { name: 'main' }));
    await userEvent.click(screen.getByRole('button', { name: 'develop' }));

    expect(screen.getByRole('button', { name: 'main' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: 'develop' })).toHaveAttribute('aria-pressed', 'true');
  });
});

describe("ChatInput's run-later popup", () => {
  it('is a disclosure, not a menu', async () => {
    render(<ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={false} />);

    const trigger = screen.getByRole('button', { name: 'More options' });
    expect(trigger).toHaveAttribute('aria-expanded', 'false');
    expect(trigger).not.toHaveAttribute('aria-haspopup');

    await userEvent.click(trigger);

    expect(trigger).toHaveAttribute('aria-expanded', 'true');
    // The popup's contents are ordinary buttons and announce as such.
    expect(screen.queryAllByRole('menu')).toHaveLength(0);
    expect(screen.queryAllByRole('menuitem')).toHaveLength(0);
    expect(screen.getByRole('button', { name: /run later/i })).toHaveAttribute(
      'aria-expanded',
      'false',
    );
  });
});

function panel(id: string, label: string): PanelTab {
  return {
    id, type: 'subagent', label, subagentType: 'general-purpose',
    description: '', content: null, prompt: '', streaming: false,
    status: 'complete', startedAt: 0, completedAt: 1000, blocks: [],
  };
}

describe("SidePanel's tab strip", () => {
  it('presses its current tab rather than claiming to select it', () => {
    useChatStore.setState({
      panels: [panel('p1', 'Explore'), panel('p2', 'Plan')],
      activePanelId: 'p1',
      panelVisible: true,
    });
    const { container } = render(<SidePanel />);

    const strip = within(container);
    expect(strip.queryByRole('tablist')).toBeNull();
    expect(strip.queryAllByRole('tab')).toHaveLength(0);

    const explore = strip.getByRole('button', { name: /Explore/ });
    expect(explore).toHaveAttribute('aria-pressed', 'true');
    expect(explore).not.toHaveAttribute('aria-selected');
    expect(strip.getByRole('button', { name: /^Plan/ })).toHaveAttribute(
      'aria-pressed',
      'false',
    );
  });
});
