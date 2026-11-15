/**
 * Display names and menu groups for model ids.
 *
 * The names come from the id alone, so they work for every model that a
 * backend lists. An id that matches no pattern shows as the id itself.
 */

export interface ModelEntry {
  id: string;
  provider: string;
  backend: string;
}

/**
 * The models of one provider, from one backend: one top-level row of the
 * model menu. A group never spans backends.
 */
export interface ModelGroup {
  key: string;
  label: string;
  backend: string;
  models: ModelEntry[];
}

const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

/**
 * A short display name: `claude-opus-4-5-20251101` is "Opus 4.5",
 * `claude-opus-5` is "Opus 5", `gpt-5.6-sol` is "GPT-5.6 Sol".
 */
export function modelLabel(id: string): string {
  const claude = id.match(/^claude-([a-z]+)-(\d+)(?:-(\d{1,2}))?(?:-\d{8})?$/);
  if (claude) {
    const [, family, major, minor] = claude;
    return `${cap(family)} ${minor ? `${major}.${minor}` : major}`;
  }
  const gpt = id.match(/^gpt-(\d+(?:\.\d+)?)((?:-[a-z0-9]+)*)$/i);
  if (gpt) {
    const variant = gpt[2].split('-').filter(Boolean).map(cap).join(' ');
    return `GPT-${gpt[1]}${variant ? ` ${variant}` : ''}`;
  }
  return id;
}

const PROVIDER_LABELS: Record<string, string> = {
  anthropic: 'Claude',
  openai: 'ChatGPT',
  ollama: 'Ollama (local)',
};

/** The menu name of a provider: "Claude", "ChatGPT", "Ollama (local)". */
export function providerLabel(provider: string): string {
  return PROVIDER_LABELS[provider] ?? cap(provider);
}

/** The key of the menu group that a model belongs to. */
export const modelGroupKey = (model: ModelEntry) => `${model.backend}:${model.provider}`;

/**
 * The models grouped by provider. Groups are in `backendOrder`, then in the
 * order of the list. Models keep the order of the list, which is the
 * configured order.
 */
export function groupModels(models: ModelEntry[], backendOrder: string[]): ModelGroup[] {
  const groups = new Map<string, ModelGroup>();
  for (const model of models) {
    const key = modelGroupKey(model);
    let group = groups.get(key);
    if (!group) {
      group = { key, label: providerLabel(model.provider), backend: model.backend, models: [] };
      groups.set(key, group);
    }
    group.models.push(model);
  }
  const rank = (value: string) => {
    const i = backendOrder.indexOf(value);
    return i === -1 ? backendOrder.length : i;
  };
  const appearance = [...groups.keys()];
  return [...groups.values()].sort((a, b) =>
    rank(a.backend) - rank(b.backend)
    || appearance.indexOf(a.key) - appearance.indexOf(b.key));
}
