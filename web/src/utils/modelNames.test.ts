import { describe, expect, it } from 'vitest';
import { groupModels, modelLabel, providerLabel } from './modelNames';

describe('modelLabel', () => {
  it.each([
    ['claude-opus-5-5', 'Opus 5.5'],
    ['claude-opus-5', 'Opus 5'],
    ['claude-haiku-4-5-20251001', 'Haiku 4.5'],
    ['claude-sonnet-4-20250514', 'Sonnet 4'],
    ['gpt-5.6-sol', 'GPT-5.6 Sol'],
    ['gpt-5.5', 'GPT-5.5'],
    ['gpt-6-astra', 'GPT-6 Astra'],
    ['llama3.1:8b', 'llama3.1:8b'],
  ])('%s is %s', (id, label) => {
    expect(modelLabel(id)).toBe(label);
  });
});

describe('providerLabel', () => {
  it('names the providers of the model menu', () => {
    expect(providerLabel('anthropic')).toBe('Claude');
    expect(providerLabel('openai')).toBe('ChatGPT');
    expect(providerLabel('ollama')).toBe('Ollama (local)');
    expect(providerLabel('acme')).toBe('Acme');
  });
});

describe('groupModels', () => {
  it('groups by provider in backend order, and keeps the model order', () => {
    const groups = groupModels([
      { id: 'gpt-6-astra', provider: 'openai', backend: 'codex' },
      { id: 'claude-fable-5-2', provider: 'anthropic', backend: 'claude' },
      { id: 'claude-opus-5-5', provider: 'anthropic', backend: 'claude' },
      { id: 'llama3.1:8b', provider: 'ollama', backend: 'claude' },
      { id: 'gpt-5.6-sol', provider: 'openai', backend: 'codex' },
      { id: 'claude-haiku-5-5', provider: 'anthropic', backend: 'claude' },
    ], ['claude', 'codex']);
    expect(groups.map(g => [g.key, g.label])).toEqual([
      ['claude:anthropic', 'Claude'],
      ['claude:ollama', 'Ollama (local)'],
      ['codex:openai', 'ChatGPT'],
    ]);
    expect(groups[0].models.map(m => m.id)).toEqual(['claude-fable-5-2', 'claude-opus-5-5', 'claude-haiku-5-5']);
    expect(groups[2].models.map(m => m.id)).toEqual(['gpt-6-astra', 'gpt-5.6-sol']);
  });
});
