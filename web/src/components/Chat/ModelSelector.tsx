import { useEffect, useMemo, useRef, useState } from 'react';
import { Button, IconButton } from '../ui';
import { Check, ChevronDown, ChevronRight, Sparkle, SquareTerminal, Star } from '../ui/icons';
import { groupModels, modelGroupKey, modelLabel, providerLabel, type ModelEntry } from '../../utils/modelNames';
import { starKey, useStarredModels } from '../../stores/starredModelsStore';

/**
 * The composer's model menu. One control picks the model and, in a new chat,
 * the agent backend with it. The top level has one row per provider (Claude,
 * ChatGPT, Ollama), and each belongs to one backend. A provider opens a
 * submenu with its models, in the configured order. Starred models, when
 * there are any, come first.
 *
 * On a wide screen a provider opens to the right of the menu. On a phone it
 * opens in place, under its row, because two columns do not fit.
 *
 * The popup is a disclosure of ordinary buttons (no ARIA menu roles), the
 * same recipe as the composer's "More options".
 */

export interface BackendChoice {
  id: string;
  label: string;
  available?: boolean;
  reason?: string | null;
}

const BACKEND_ICONS: Record<string, { icon: typeof Sparkle; color: string }> = {
  claude: { icon: Sparkle, color: 'text-hue-orange' },
  codex: { icon: SquareTerminal, color: 'text-hue-teal' },
};

// The provider of a backend's own models. Its name is the backend's name in
// this menu: "ChatGPT" for the Codex backend.
const BACKEND_PROVIDERS: Record<string, string> = {
  claude: 'anthropic',
  codex: 'openai',
};

function BackendIcon({ backend }: { backend: string }) {
  const style = BACKEND_ICONS[backend] ?? BACKEND_ICONS.claude;
  const Icon = style.icon;
  return <Icon size={14} className={`shrink-0 ${style.color}`} aria-hidden="true" />;
}

// The popup sits on `bg-surface-raised`, where the ghost hover colour is that
// same surface and would not show.
const STAR_BUTTON = 'mr-1.5 hover:bg-surface-hover';

export function ModelSelector({
  models, backends, activeBackend, currentModel, backendFixed, disabled, title, onPick,
}: {
  /** Every model of every backend. */
  models: ModelEntry[];
  /** The agent backends, in display order. */
  backends: BackendChoice[];
  /** The backend of this chat, or the one a new chat will use. */
  activeBackend: string;
  currentModel: string | null;
  /**
   * The chat exists, so its backend cannot change. Models of the other
   * backends stay in the menu, but they cannot be picked.
   */
  backendFixed: boolean;
  disabled?: boolean;
  title?: string;
  onPick: (backend: string, id: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [openGroup, setOpenGroup] = useState<string | null>(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const starred = useStarredModels(s => s.starred);
  const toggleStar = useStarredModels(s => s.toggle);

  const groups = useMemo(
    () => groupModels(models, backends.map(b => b.id)),
    [models, backends],
  );
  // A backend that fails its preflight lists no models, so it has no
  // group. It gets a greyed-out row of its own that says why.
  const unavailable = useMemo(
    () => backends.filter(b => b.available === false && !models.some(m => m.backend === b.id)),
    [backends, models],
  );
  const starredModels = useMemo(
    () => starred
      .map(key => models.find(m => starKey(m.backend, m.id) === key))
      .filter((m): m is ModelEntry => m !== undefined),
    [starred, models],
  );

  const backendName = (backend: string) => {
    const provider = BACKEND_PROVIDERS[backend];
    return provider ? providerLabel(provider) : (backends.find(b => b.id === backend)?.label ?? backend);
  };

  /** Why a backend's models cannot be picked here, or null when they can. */
  const blocker = (backend: string): { note: string; why: string } | null => {
    if (backend === activeBackend) return null;
    const label = backendName(backend);
    if (backendFixed) {
      return { note: 'New chat', why: `${label} models are for a new chat. The agent backend is fixed once the conversation starts.` };
    }
    const option = backends.find(b => b.id === backend);
    if (!option || option.available === false) {
      return { note: 'Unavailable', why: `${label} unavailable: ${option?.reason ?? 'preflight failed'}` };
    }
    return null;
  };

  // Close on an outside press, and on Escape, which must not also reach the
  // global Escape that stops the turn.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      e.preventDefault();
      e.stopPropagation();
      setOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey, true);
    };
  }, [open]);

  const toggleMenu = () => {
    if (!open) {
      // Open on the provider of the current model, so its neighbours show.
      const current = models.find(m => m.id === currentModel && m.backend === activeBackend);
      setOpenGroup(current ? modelGroupKey(current) : null);
    }
    setOpen(v => !v);
  };

  const pick = (model: ModelEntry) => {
    setOpen(false);
    onPick(model.backend, model.id);
  };

  // Starred rows name their backend: they mix the models of all providers.
  const row = (model: ModelEntry, keyPrefix: string, showBackend = false) => {
    const label = modelLabel(model.id);
    const isStarred = starred.includes(starKey(model.backend, model.id));
    const isCurrent = model.id === currentModel && model.backend === activeBackend;
    const blocked = blocker(model.backend);
    return (
      <div key={`${keyPrefix}${model.backend}/${model.id}`} className="flex items-center">
        <Button
          variant="subtle"
          size="sm"
          fullWidth
          onClick={() => pick(model)}
          disabled={blocked !== null}
          aria-current={isCurrent ? 'true' : undefined}
          title={blocked ? blocked.why : model.id}
          className="flex-1 min-w-0 justify-start gap-2 px-3 py-1.5 rounded-none text-left"
        >
          <Check size={14} className={`shrink-0 ${isCurrent ? '' : 'invisible'}`} aria-hidden="true" />
          {showBackend && <BackendIcon backend={model.backend} />}
          <span className="truncate">{label}</span>
          {/* The space keeps the accessible name "Opus 5 New chat", not
              "Opus 5New chat". A flex container does not render it. */}
          {blocked && <>{' '}<span className="ml-auto shrink-0 text-2xs text-text-muted">{blocked.note}</span></>}
        </Button>
        <IconButton
          label={isStarred ? `Unstar ${label}` : `Star ${label}`}
          aria-pressed={isStarred}
          size="xs"
          onClick={() => toggleStar(starKey(model.backend, model.id))}
          className={isStarred ? `${STAR_BUTTON} text-hue-amber hover:text-hue-amber` : STAR_BUTTON}
        >
          <Star size={14} fill={isStarred ? 'currentColor' : 'none'} aria-hidden="true" />
        </IconButton>
      </div>
    );
  };

  const triggerLabel = currentModel ? modelLabel(currentModel) : 'Model';

  return (
    <div ref={wrapRef} className="relative min-w-0">
      <button
        ref={triggerRef}
        type="button"
        onClick={toggleMenu}
        disabled={disabled}
        aria-expanded={open}
        aria-label={`Model: ${triggerLabel}`}
        title={title}
        className="h-8 max-w-[220px] min-w-0 px-2 rounded-lg flex items-center gap-1.5 text-sm text-text-secondary hover:bg-surface-hover cursor-pointer transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      >
        <BackendIcon backend={activeBackend} />
        <span className="truncate">{triggerLabel}</span>
        <ChevronDown size={14} className="shrink-0 text-text-muted" aria-hidden="true" />
      </button>

      {open && (
        // On a phone the providers open in place, so this list can grow
        // past the screen and scrolls. On a wide screen it must not clip:
        // the providers open beside it.
        <div className="absolute left-0 bottom-full mb-1 z-50 w-64 max-h-[60vh] overflow-y-auto md:max-h-none md:overflow-visible bg-surface-raised border border-border-subtle rounded-lg shadow-xl py-1">
          {starredModels.length > 0 && (
            <>
              <div className="px-3 pt-1 pb-0.5 text-2xs font-medium uppercase tracking-wider text-text-muted">Starred</div>
              {starredModels.map(m => row(m, 'starred:', true))}
              <div className="my-1 border-t border-border" />
            </>
          )}

          {groups.map(group => {
            const blocked = blocker(group.backend);
            const isOpen = openGroup === group.key;
            return (
              <div key={group.key} className="relative">
                <Button
                  variant="subtle"
                  size="sm"
                  fullWidth
                  onClick={() => setOpenGroup(isOpen ? null : group.key)}
                  aria-expanded={isOpen}
                  title={blocked?.why}
                  className="justify-between gap-2.5 px-3 py-1.5 rounded-none text-left"
                >
                  <span className="flex items-center gap-2.5 min-w-0">
                    <BackendIcon backend={group.backend} />
                    <span className="truncate">{group.label}</span>
                  </span>
                  {' '}
                  <span className="flex items-center gap-1.5 shrink-0">
                    {blocked && <span className="text-2xs text-text-muted">{blocked.note}</span>}
                    <ChevronRight
                      size={14}
                      aria-hidden="true"
                      className={`transition-transform ${isOpen ? 'rotate-90 md:rotate-0' : ''}`}
                    />
                  </span>
                </Button>
                {isOpen && (
                  <div className="pl-4 md:pl-0 md:absolute md:left-full md:bottom-0 md:ml-1 md:w-60 md:max-h-[60vh] md:overflow-y-auto md:bg-surface-raised md:border md:border-border-subtle md:rounded-lg md:shadow-xl md:py-1">
                    {group.models.map(m => row(m, `${group.key}:`))}
                  </div>
                )}
              </div>
            );
          })}

          {/* Greyed out like a provider that cannot open; the tooltip says
              why. `aria-disabled` rather than `disabled`, so that the row
              still gets the pointer and focus that show the tooltip. */}
          {unavailable.map(backend => (
            <Button
              key={`unavailable:${backend.id}`}
              variant="subtle"
              size="sm"
              fullWidth
              aria-disabled="true"
              title={`${backendName(backend.id)} unavailable: ${backend.reason ?? 'preflight failed'}`}
              className="justify-between gap-2.5 px-3 py-1.5 rounded-none text-left opacity-50 cursor-not-allowed hover:text-text-secondary hover:bg-transparent"
            >
              <span className="flex items-center gap-2.5 min-w-0">
                <BackendIcon backend={backend.id} />
                <span className="truncate">{backendName(backend.id)}</span>
              </span>
              {' '}
              <span className="flex items-center gap-1.5 shrink-0">
                <span className="text-2xs">Unavailable</span>
                <ChevronRight size={14} aria-hidden="true" />
              </span>
            </Button>
          ))}
        </div>
      )}
    </div>
  );
}
