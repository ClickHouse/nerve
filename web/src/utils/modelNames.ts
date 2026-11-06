/**
 * Display names and families for model ids.
 *
 * The names come from the id alone, so they work for every model that a
 * backend lists. An id that matches no pattern shows as the id itself.
 */

export interface ModelEntry {
  id: string;
  provider: string;
  backend: string;
}

/** Models of one family, from one backend. A family never spans backends. */
export interface ModelFamily {
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

const PROVIDER_FAMILIES: Record<string, string> = {
  anthropic: 'Claude',
  openai: 'OpenAI',
  ollama: 'Ollama (local)',
};

/** The family name: "Opus", "Sonnet", "GPT", or the provider for other ids. */
export function modelFamily(model: ModelEntry): string {
  const claude = model.id.match(/^claude-([a-z]+)-/);
  if (claude) return cap(claude[1]);
  if (/^gpt-/i.test(model.id)) return 'GPT';
  return PROVIDER_FAMILIES[model.provider] ?? cap(model.provider);
}

// Largest first. Families not named here follow, in the order of the list.
const FAMILY_RANK = ['Opus', 'Sonnet', 'Haiku'];

/**
 * The models grouped by family. Families are in `backendOrder`, then in
 * FAMILY_RANK, then in the order of the list. Models keep the order of the
 * list, which the backends give newest first.
 */
export function groupModelFamilies(models: ModelEntry[], backendOrder: string[]): ModelFamily[] {
  const families = new Map<string, ModelFamily>();
  for (const model of models) {
    const label = modelFamily(model);
    const key = `${model.backend}:${label}`;
    let family = families.get(key);
    if (!family) {
      family = { key, label, backend: model.backend, models: [] };
      families.set(key, family);
    }
    family.models.push(model);
  }
  const rank = (list: string[], value: string) => {
    const i = list.indexOf(value);
    return i === -1 ? list.length : i;
  };
  const appearance = [...families.keys()];
  return [...families.values()].sort((a, b) =>
    rank(backendOrder, a.backend) - rank(backendOrder, b.backend)
    || rank(FAMILY_RANK, a.label) - rank(FAMILY_RANK, b.label)
    || appearance.indexOf(a.key) - appearance.indexOf(b.key));
}
