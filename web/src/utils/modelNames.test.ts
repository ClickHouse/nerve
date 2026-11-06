import { describe, expect, it } from 'vitest';
import { groupModelFamilies, modelFamily, modelLabel } from './modelNames';

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

describe('modelFamily', () => {
  it('takes the family from the id, or the provider for other ids', () => {
    expect(modelFamily({ id: 'claude-fable-5-1', provider: 'anthropic', backend: 'claude' })).toBe('Fable');
    expect(modelFamily({ id: 'gpt-5.6-luna', provider: 'openai', backend: 'codex' })).toBe('GPT');
    expect(modelFamily({ id: 'llama3.1:8b', provider: 'ollama', backend: 'claude' })).toBe('Ollama (local)');
  });
});

describe('groupModelFamilies', () => {
  it('orders by backend, then largest family first, and keeps the model order', () => {
    const families = groupModelFamilies([
      { id: 'gpt-5.6-sol', provider: 'openai', backend: 'codex' },
      { id: 'claude-fable-5-1', provider: 'anthropic', backend: 'claude' },
      { id: 'claude-haiku-5-5', provider: 'anthropic', backend: 'claude' },
      { id: 'claude-opus-5-5', provider: 'anthropic', backend: 'claude' },
      { id: 'claude-sonnet-5-5', provider: 'anthropic', backend: 'claude' },
      { id: 'claude-opus-4-8', provider: 'anthropic', backend: 'claude' },
    ], ['claude', 'codex']);
    expect(families.map(f => f.key)).toEqual([
      'claude:Opus', 'claude:Sonnet', 'claude:Haiku', 'claude:Fable', 'codex:GPT',
    ]);
    expect(families[0].models.map(m => m.id)).toEqual(['claude-opus-5-5', 'claude-opus-4-8']);
  });
});
