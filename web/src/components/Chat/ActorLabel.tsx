import {
  actorDiscriminator, actorName, ambiguousActorIds, isSystemActor, useActorStore,
} from '../../stores/actorStore';
import { useEffect } from 'react';
import { Badge } from '../ui';
import { Bot, User } from '../ui/icons';

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

interface Attribution {
  /** What the label reads, discriminator included when one is needed. */
  name: string;
  system: boolean;
  /** True when `name` is the neutral fallback rather than a real name. */
  anonymous: boolean;
  /** True when another actor renders the same name and a suffix was added. */
  ambiguous: boolean;
}

/** What to say about an actor, given whatever the map currently knows. */
function useAttribution(actorId: string | null | undefined): Attribution {
  const actors = useActorStore((s) => s.actors);
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => {
    if (actorId) resolve([actorId]);
  }, [actorId, resolve]);

  const actor = actorId ? actors[actorId] : undefined;
  const base = actorName(actor);
  const system = isSystemActor(actor);
  // Display names are not identity and two of them can be equal, so a label
  // that is only a name can name two different people. When that happens both
  // get a stable slice of their own id appended — in the label, not just the
  // tooltip, because the tooltip is not there on a phone.
  const ambiguous = !!actorId && ambiguousActorIds(actors).has(actorId);
  return {
    name: ambiguous ? `${base} (${actorDiscriminator(actorId!)})` : base,
    system,
    anonymous: !actor?.display_name?.trim() && !system,
    ambiguous,
  };
}

function tooltip(verb: string, actorId: string, attribution: Attribution): string {
  const { name, system, anonymous, ambiguous } = attribution;
  if (system) return `${verb} ${name} itself — scheduled or autonomous work`;
  // The id earns its place when there is no name to show — it is the one thing
  // that tells "this account has no display name" apart from "this id is from
  // somewhere this instance has never heard of" — and again when the name is
  // shared, where the short suffix says there are two and the full id says
  // which.
  if (anonymous) return `${verb} actor ${actorId}`;
  if (ambiguous) return `${verb} ${name} — actor ${actorId}`;
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
 * and it is the only place the full answer exists.
 *
 * **It does not disappear on a phone.** It did, matching the backend and model
 * chips beside it, and that was wrong: those two are conveniences, this is the
 * only place a phone can learn who a shared session belongs to — the session
 * list marks a person only once two of them have started something, and on a
 * phone that list is a drawer you have to open. So the chip stays at every
 * width and sheds its *text* instead: below `md` the glyph carries it and the
 * name is `sr-only`, which keeps the full sentence for a screen reader and the
 * `title` for a long press, while costing the title bar about 22px.
 */
export function SessionCreator({ actorId }: { actorId: string | null | undefined }) {
  const attribution = useAttribution(actorId);
  if (!actorId) return null;
  const label = tooltip('Started by', actorId, attribution);
  return (
    <Badge
      data-attribution="session-header"
      tone="neutral"
      size="xs"
      title={label}
      className="shrink-0 max-w-[12rem] overflow-hidden"
    >
      {/* Decorative at `md` and up, where the text says the same thing; below
          it the text is still in the DOM for assistive technology, so the
          glyph never has to carry an accessible name of its own. */}
      {attribution.system
        ? <Bot size={11} className="shrink-0" aria-hidden="true" />
        : <User size={11} className="shrink-0" aria-hidden="true" />}
      <span className="sr-only md:not-sr-only md:truncate">Started by {attribution.name}</span>
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
      // Wider when a discriminator is in play: truncating "Alice (c3d4e5)" back
      // to "Alice (c3…" would drop the one part of it that disambiguates, which
      // is the only reason the marker is on the row at all.
      className={`shrink-0 truncate text-2xs text-text-dim ${
        attribution.ambiguous ? 'max-w-[7.5rem]' : 'max-w-[4.5rem]'
      }`}
    >
      {attribution.name}
    </span>
  );
}
