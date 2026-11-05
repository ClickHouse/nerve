import { useEffect } from 'react';
import {
  actorDiscriminators, actorName, isSystemActor, useActorStore,
} from '../stores/actorStore';

export interface Attribution {
  name: string;
  system: boolean;
  anonymous: boolean;
  ambiguous: boolean;
}

/** Resolve an actor id to its current display label. */
export function useAttribution(actorId: string | null | undefined): Attribution {
  const actors = useActorStore((s) => s.actors);
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => {
    if (actorId) resolve([actorId]);
  }, [actorId, resolve]);

  const actor = actorId ? actors[actorId] : undefined;
  const base = actorName(actor, actorId ?? null);
  const system = isSystemActor(actor);
  const discriminator = actorId ? actorDiscriminators(actors).get(actorId) : undefined;
  return {
    name: discriminator ? `${base} (${discriminator})` : base,
    system,
    anonymous: !system && !actor?.display_name?.trim() && !actor?.username?.trim(),
    ambiguous: !!discriminator,
  };
}

/**
 * The actor, and the name of the person the actor worked for when that is
 * another person: the system actor (Nerve) doing work that a person started.
 * A cause that is the actor itself, or the system actor, adds nothing.
 * `label` is "Alice" or "Nerve for Alice".
 */
export function useActorAndCause(
  actorId: string | null | undefined,
  causeId: string | null | undefined,
): { actor: Attribution; forPerson: string | null; label: string } {
  const actor = useAttribution(actorId);
  const otherCauseId = actorId && causeId && causeId !== actorId ? causeId : null;
  const cause = useAttribution(otherCauseId);
  const forPerson = otherCauseId && !cause.system ? cause.name : null;
  const label = forPerson ? `${actor.name} for ${forPerson}` : actor.name;
  return { actor, forPerson, label };
}

/**
 * The text of `ActorAttribution` for a place that needs a string, such as a
 * tooltip. `null` without an actor id.
 */
export function useActorAttributionLabel(
  actorId: string | null | undefined,
  causeId: string | null | undefined,
): string | null {
  const { label } = useActorAndCause(actorId, causeId);
  return actorId ? label : null;
}
