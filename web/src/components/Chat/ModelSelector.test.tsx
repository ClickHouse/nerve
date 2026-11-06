import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ModelSelector } from './ModelSelector';
import { useStarredModels } from '../../stores/starredModelsStore';
import type { ModelEntry } from '../../utils/modelNames';

const MODELS: ModelEntry[] = [
  { id: 'claude-opus-5-5', provider: 'anthropic', backend: 'claude' },
  { id: 'claude-sonnet-5-5', provider: 'anthropic', backend: 'claude' },
  { id: 'claude-opus-4-8', provider: 'anthropic', backend: 'claude' },
  { id: 'gpt-5.6-sol', provider: 'openai', backend: 'codex' },
];
const BACKENDS = [
  { id: 'claude', label: 'Claude', available: true },
  { id: 'codex', label: 'Codex', available: true },
];

function renderSelector(props: Partial<Parameters<typeof ModelSelector>[0]> = {}) {
  const onPick = vi.fn();
  render(
    <ModelSelector
      models={MODELS}
      backends={BACKENDS}
      activeBackend="claude"
      currentModel="claude-opus-5-5"
      backendFixed={false}
      onPick={onPick}
      {...props}
    />,
  );
  return { onPick, user: userEvent.setup() };
}

const trigger = () => screen.getByRole('button', { name: 'Model: Opus 5.5' });
// A family row is named by its label, and by its note when it has one.
const family = (name: string) => screen.getByRole('button', { name: new RegExp(`^${name}( New chat| Unavailable)?$`) });
const model = (label: string) => new RegExp(`^${label.replace(/\./g, '\\.')}( New chat| Unavailable)?$`);

beforeEach(() => {
  localStorage.clear();
  useStarredModels.setState({ starred: [] });
});

describe('ModelSelector', () => {
  it('opens on the family of the current model, and other families open on demand', async () => {
    const { user } = renderSelector();
    await user.click(trigger());
    expect(trigger()).toHaveAttribute('aria-expanded', 'true');
    expect(family('Opus')).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('button', { name: model('Opus 5.5'), current: true })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: model('Sonnet 5.5') })).toBeNull();

    await user.click(family('Sonnet'));
    expect(screen.getByRole('button', { name: model('Sonnet 5.5') })).toBeInTheDocument();
    expect(family('Opus')).toHaveAttribute('aria-expanded', 'false');
  });

  it('keeps starred models above the families, in this browser', async () => {
    const { user, onPick } = renderSelector();
    await user.click(trigger());
    await user.click(family('GPT'));
    await user.click(screen.getByRole('button', { name: 'Star GPT-5.6 Sol' }));
    expect(JSON.parse(localStorage.getItem('nerve_starred_models') ?? '[]')).toEqual(['codex/gpt-5.6-sol']);

    // Close the family: the starred row stays.
    await user.click(family('GPT'));
    const unstar = screen.getByRole('button', { name: 'Unstar GPT-5.6 Sol' });
    expect(unstar).toHaveAttribute('aria-pressed', 'true');
    const starredRow = unstar.parentElement!;
    expect(starredRow.compareDocumentPosition(family('Opus')) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    // A new chat takes the model and its backend.
    await user.click(within(starredRow).getByRole('button', { name: model('GPT-5.6 Sol') }));
    expect(onPick).toHaveBeenCalledWith('codex', 'gpt-5.6-sol');
    expect(trigger()).toHaveAttribute('aria-expanded', 'false');
  });

  it('shows the other backend in a chat that exists, but does not pick it', async () => {
    const { user, onPick } = renderSelector({ backendFixed: true });
    await user.click(trigger());
    expect(family('GPT')).toHaveTextContent('New chat');
    await user.click(family('GPT'));
    const gpt = screen.getByRole('button', { name: model('GPT-5.6 Sol') });
    expect(gpt).toBeDisabled();

    // One family is open at a time.
    await user.click(family('Opus'));
    expect(screen.queryByRole('button', { name: model('GPT-5.6 Sol') })).toBeNull();
    await user.click(screen.getByRole('button', { name: model('Opus 4.8') }));
    expect(onPick).toHaveBeenCalledWith('claude', 'claude-opus-4-8');
  });

  it('says why a backend that lists no models is missing', async () => {
    const { user } = renderSelector({
      models: MODELS.filter(m => m.backend === 'claude'),
      backends: [BACKENDS[0], { id: 'codex', label: 'Codex', available: false, reason: 'codex binary not found' }],
    });
    await user.click(trigger());
    const codex = family('Codex');
    expect(codex).toHaveAttribute('aria-disabled', 'true');
    expect(codex).toHaveAttribute('title', 'Codex unavailable: codex binary not found');
    expect(codex).toHaveAccessibleDescription('Codex unavailable: codex binary not found');
    expect(screen.queryByRole('button', { name: /^GPT/ })).toBeNull();

    // A press opens nothing.
    await user.click(codex);
    expect(codex).not.toHaveAttribute('aria-expanded');
    expect(trigger()).toHaveAttribute('aria-expanded', 'true');
  });

  it('closes on Escape without letting it reach the page', async () => {
    const pageEscape = vi.fn();
    document.addEventListener('keydown', pageEscape);
    const { user } = renderSelector();
    await user.click(trigger());
    await user.keyboard('{Escape}');
    expect(trigger()).toHaveAttribute('aria-expanded', 'false');
    expect(document.activeElement).toBe(trigger());
    expect(pageEscape).not.toHaveBeenCalled();
    document.removeEventListener('keydown', pageEscape);
  });
});
