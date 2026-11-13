import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ModelSelector } from './ModelSelector';
import { useStarredModels } from '../../stores/starredModelsStore';
import type { ModelEntry } from '../../utils/modelNames';

const MODELS: ModelEntry[] = [
  { id: 'claude-fable-5-2', provider: 'anthropic', backend: 'claude' },
  { id: 'claude-opus-5-5', provider: 'anthropic', backend: 'claude' },
  { id: 'claude-sonnet-5-5', provider: 'anthropic', backend: 'claude' },
  { id: 'gpt-6-astra', provider: 'openai', backend: 'codex' },
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
// A provider row is named by its label, and by its note when it has one.
const provider = (name: string) => screen.getByRole('button', { name: new RegExp(`^${name}( New chat| Unavailable)?$`) });
const model = (label: string) => new RegExp(`^${label.replace(/\./g, '\\.')}( New chat| Unavailable)?$`);
// The model rows on screen, by label, in document order. Each row has one
// star button, named after the model.
const modelRows = () => screen.queryAllByRole('button', { name: /^(Star|Unstar) / })
  .map(b => b.getAttribute('aria-label')!.replace(/^(Star|Unstar) /, ''));

beforeEach(() => {
  localStorage.clear();
  useStarredModels.setState({ starred: [] });
});

describe('ModelSelector', () => {
  it('has one row per provider, and opens on the provider of the current model', async () => {
    const { user } = renderSelector();
    await user.click(trigger());
    expect(trigger()).toHaveAttribute('aria-expanded', 'true');
    // Nothing is starred: the top level is the providers only.
    expect(screen.queryByText('Starred')).toBeNull();
    expect(screen.queryByText(/Star a model/)).toBeNull();
    expect(provider('Claude')).toHaveAttribute('aria-expanded', 'true');
    expect(provider('ChatGPT')).toHaveAttribute('aria-expanded', 'false');
    // The models keep the configured order.
    expect(modelRows()).toEqual(['Fable 5.2', 'Opus 5.5', 'Sonnet 5.5']);
    expect(screen.getByRole('button', { name: model('Opus 5.5'), current: true })).toBeInTheDocument();

    await user.click(provider('ChatGPT'));
    expect(modelRows()).toEqual(['GPT-6 Astra', 'GPT-5.6 Sol']);
    expect(provider('Claude')).toHaveAttribute('aria-expanded', 'false');
  });

  it('keeps starred models above the providers, in this browser', async () => {
    const { user, onPick } = renderSelector();
    await user.click(trigger());
    await user.click(provider('ChatGPT'));
    await user.click(screen.getByRole('button', { name: 'Star GPT-5.6 Sol' }));
    expect(JSON.parse(localStorage.getItem('nerve_starred_models') ?? '[]')).toEqual(['codex/gpt-5.6-sol']);
    expect(screen.getByText('Starred')).toBeInTheDocument();

    // Close the provider: the starred row stays.
    await user.click(provider('ChatGPT'));
    const unstar = screen.getByRole('button', { name: 'Unstar GPT-5.6 Sol' });
    expect(unstar).toHaveAttribute('aria-pressed', 'true');
    const starredRow = unstar.parentElement!;
    expect(starredRow.compareDocumentPosition(provider('Claude')) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    // A new chat takes the model and its backend.
    await user.click(within(starredRow).getByRole('button', { name: model('GPT-5.6 Sol') }));
    expect(onPick).toHaveBeenCalledWith('codex', 'gpt-5.6-sol');
    expect(trigger()).toHaveAttribute('aria-expanded', 'false');
  });

  it('shows the other backend in a chat that exists, but does not pick it', async () => {
    const { user, onPick } = renderSelector({ backendFixed: true });
    await user.click(trigger());
    expect(provider('ChatGPT')).toHaveTextContent('New chat');
    expect(provider('ChatGPT')).toHaveAttribute(
      'title', 'ChatGPT models are for a new chat. The agent backend is fixed once the conversation starts.',
    );
    await user.click(provider('ChatGPT'));
    const gpt = screen.getByRole('button', { name: model('GPT-5.6 Sol') });
    expect(gpt).toBeDisabled();

    // One provider is open at a time.
    await user.click(provider('Claude'));
    expect(screen.queryByRole('button', { name: model('GPT-5.6 Sol') })).toBeNull();
    await user.click(screen.getByRole('button', { name: model('Sonnet 5.5') }));
    expect(onPick).toHaveBeenCalledWith('claude', 'claude-sonnet-5-5');
  });

  it('gives local Ollama models a provider row of their own', async () => {
    const { user, onPick } = renderSelector({
      models: [...MODELS, { id: 'llama3.1:8b', provider: 'ollama', backend: 'claude' }],
    });
    await user.click(trigger());
    await user.click(provider('Ollama \\(local\\)'));
    await user.click(screen.getByRole('button', { name: model('llama3.1:8b') }));
    expect(onPick).toHaveBeenCalledWith('claude', 'llama3.1:8b');
  });

  it('says why a backend that lists no models is missing', async () => {
    const { user } = renderSelector({
      models: MODELS.filter(m => m.backend === 'claude'),
      backends: [BACKENDS[0], { id: 'codex', label: 'Codex', available: false, reason: 'codex binary not found' }],
    });
    await user.click(trigger());
    const chatgpt = provider('ChatGPT');
    expect(chatgpt).toHaveAttribute('aria-disabled', 'true');
    expect(chatgpt).toHaveAttribute('title', 'ChatGPT unavailable: codex binary not found');
    expect(chatgpt).toHaveAccessibleDescription('ChatGPT unavailable: codex binary not found');
    expect(screen.queryByRole('button', { name: /^GPT/ })).toBeNull();

    // A press opens nothing.
    await user.click(chatgpt);
    expect(chatgpt).not.toHaveAttribute('aria-expanded');
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
