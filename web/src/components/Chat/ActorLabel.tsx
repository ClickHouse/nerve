import { actorName, isSystemActor, useActorRef } from '../../stores/actorStore';
import { Badge } from '../ui';
import { Bot } from '../ui/icons';

/**
 * Attribution labels: who sent a message, who started a session.
 *
 * Three rules hold across all of them.
 *
 * **A null id renders nothing.** Not a placeholder, not an "unknown user", not
 * a greyed-out chip — no element at all. `null` is the common case (every
 * assistant and tool row, all history from before attribution existed, every
 * optimistic bubble you have just typed), and history has to keep looking the
 * way it looks today rather than turning into a wall of missing data.
 *
 * **The primary label is never a raw id.** An actor with no display name, and
 * an id this instance's actor map does not know, both read `Unnamed account`.
 * The id is in the tooltip, which is where somebody debugging can find it and
 * where nobody else has to read it.
 *
 * **The name is resolved at render time.** Nothing here stores a name; it comes
 * from the actor store's map, which is re-read rather than remembered. Renaming
 * somebody changes every one of these labels and rewrites no stored row.
 *
 * Every label carries `data-attribution`, which is how the specs assert that an
 * assistant row has no attribution markup anywhere inside it.
 */

/** What to say about an actor, given whatever the map currently knows. */
function useAttribution(actorId: string | null | undefined) {
  const actor = useActorRef(actorId);
  const name = actorName(actor);
  const system = isSystemActor(actor);
  return {
    name,
    system,
    /** True when `name` is the neutral fallback rather than a real name. */
    anonymous: !actor?.display_name?.trim() && !system,
  };
}

function tooltip(
  verb: string,
  actorId: string,
  { name, system, anonymous }: { name: string; system: boolean; anonymous: boolean },
): string {
  if (system) return `${verb} ${name} itself — scheduled or autonomous work`;
  // The id earns its place only when there is no name to show: it is the one
  // thing that tells "this account has no display name" apart from "this id is
  // from somewhere this instance has never heard of".
  if (anonymous) return `${verb} actor ${actorId}`;
  return `${verb} ${name}`;
}

/**
 * The sender's name above a user message.
 *
 * Deliberately quiet — one faint line at the transcript's small size, no chip
 * and no avatar, because it sits above every message a second person sends and
 * anything louder would compete with what they wrote. Whether it appears at all
 * is the caller's decision (`MessageList` applies the disambiguation rule), so
 * that a one-person transcript stays exactly as it is today.
 */
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

/**
 * Who started the open session, in the chat header.
 *
 * Always shown when the session has a creator, unlike the list labels: the
 * header describes one session, so this is an answer rather than a repetition,
 * and it is the only place the full answer exists. Hidden below `md` for the
 * same reason as the backend and model chips beside it — on a phone the title
 * needs the width more than the metadata does, and the session list still
 * marks the agent's own sessions there.
 */
export function SessionCreator({ actorId }: { actorId: string | null | undefined }) {
  const attribution = useAttribution(actorId);
  if (!actorId) return null;
  return (
    <Badge
      data-attribution="session-header"
      tone="neutral"
      size="xs"
      title={tooltip('Started by', actorId, attribution)}
      className="hidden md:inline-flex shrink-0 max-w-[12rem] overflow-hidden"
    >
      {attribution.system && <Bot size={11} className="shrink-0" aria-hidden="true" />}
      <span className="truncate">Started by {attribution.name}</span>
    </Badge>
  );
}

/**
 * Who started a session, on one row of the session list.
 *
 * The row is a single line of 12px text with a title, status and a menu in it,
 * so this is as small as the information gets: a glyph for the agent's own
 * sessions, which need no name because there is only ever one of it, and a
 * truncated name for a person, which is the whole point when two people share
 * a sidebar. As with messages, whether it appears is the caller's decision.
 *
 * `text-dim` rather than the `text-faint` the row's other trailing markers use,
 * because the *selected* row's background is `bg-accent/10` over the surface,
 * and the ramp in `index.css` is solved against the three flat backgrounds
 * rather than that tint. Measured with axe on the real page: `text-faint` lands
 * at 3.72:1 there, under AA; `text-dim` clears it in both themes.
 */
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
      className="shrink-0 max-w-[4.5rem] truncate text-2xs text-text-dim"
    >
      {attribution.name}
    </span>
  );
}
