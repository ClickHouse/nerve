import {
  actorDiscriminators, actorName, isSystemActor, useActorStore,
} from '../../stores/actorStore';
import { useAuthStore } from '../../stores/authStore';
import { useEffect } from 'react';
import { Badge } from '../ui';
import { Bot, User } from '../ui/icons';

interface Attribution {
  name: string;
  system: boolean;
  anonymous: boolean;
  ambiguous: boolean;
}

/** Resolve an actor id to its current display label. */
function useAttribution(actorId: string | null | undefined): Attribution {
  const actors = useActorStore((s) => s.actors);
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => {
    if (actorId) resolve([actorId]);
  }, [actorId, resolve]);

  const actor = actorId ? actors[actorId] : undefined;
  const base = actorName(actor);
  const system = isSystemActor(actor);
  const discriminator = actorId ? actorDiscriminators(actors).get(actorId) : undefined;
  return {
    name: discriminator ? `${base} (${discriminator})` : base,
    system,
    anonymous: !actor?.display_name?.trim() && !system,
    ambiguous: !!discriminator,
  };
}

function tooltip(verb: string, actorId: string, attribution: Attribution): string {
  const { name, system, anonymous, ambiguous } = attribution;
  if (system) return `${verb} ${name} itself — scheduled or autonomous work`;
  if (anonymous) return `${verb} actor ${actorId}`;
  if (ambiguous) return `${verb} ${name} — actor ${actorId}`;
  return `${verb} ${name}`;
}

export function ActorLabel({ actorId }: { actorId: string | null | undefined }) {
  const attribution = useAttribution(actorId);
  if (!actorId) return null;
  return (
    <div
      data-attribution="message"
      title={tooltip('Sent by', actorId, attribution)}
      className="flex items-center gap-1 text-2xs text-text-faint leading-none mb-1.5"
    >
      {attribution.system && <Bot size={11} className="shrink-0" aria-hidden="true" />}
      <span className="truncate">{attribution.name}</span>
    </div>
  );
}

export function SessionCreator({ actorId }: { actorId: string | null | undefined }) {
  const viewerActorId = useAuthStore((s) => s.viewer?.id ?? null);
  const visibleActorId = actorId === viewerActorId ? null : actorId;
  const attribution = useAttribution(visibleActorId);
  if (!visibleActorId) return null;
  const label = tooltip('Started by', visibleActorId, attribution);
  return (
    <Badge
      data-attribution="session-header"
      tone="neutral"
      size="xs"
      title={label}
      className="shrink-0 max-w-[12rem] overflow-hidden"
    >
      {attribution.system
        ? <Bot size={11} className="shrink-0" aria-hidden="true" />
        : <User size={11} className="shrink-0" aria-hidden="true" />}
      <span className="truncate max-w-[5rem] md:max-w-none">
        <span className="sr-only md:not-sr-only">Started by </span>
        {attribution.name}
      </span>
    </Badge>
  );
}

export function SessionCreatorMarker({ actorId }: { actorId: string | null | undefined }) {
  const attribution = useAttribution(actorId);
  if (!actorId) return null;
  const label = tooltip('Started by', actorId, attribution);
  if (attribution.system) {
    return (
      <span
        data-attribution="session-row"
        role="img"
        aria-label={label}
        title={label}
        className="shrink-0 flex items-center text-text-dim"
      >
        <Bot size={11} />
      </span>
    );
  }
  return (
    <span
      data-attribution="session-row"
      title={label}
      className={`shrink-0 truncate text-2xs text-text-dim ${
        attribution.ambiguous ? 'max-w-[7.5rem]' : 'max-w-[4.5rem]'
      }`}
    >
      {attribution.name}
    </span>
  );
}
