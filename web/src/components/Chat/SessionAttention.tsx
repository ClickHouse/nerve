import type { Session } from '../../types/chat';
import { ATTENTION_TONE, attentionReasons, needsYouLabel } from '../../utils/sessionAttention';

/** Second line of a session row: why the session needs the user. */
export function AttentionReason({ session }: { session: Session }) {
  const text = attentionReasons(session).join(' · ');
  if (!text) return null;
  return (
    <div className={`truncate text-2xs leading-tight mt-0.5 ${ATTENTION_TONE}`} title={text}>
      {text}
    </div>
  );
}

/** Status mark of a session that is blocked on the user: a red "!", no pulse. */
export function WaitingMark({ session }: { session: Session }) {
  return (
    <span
      role="img"
      aria-label="Waiting for you"
      title={attentionReasons(session).join(' · ')}
      className="inline-flex w-2 shrink-0 justify-center text-sm font-extrabold leading-none text-error"
    >
      !
    </span>
  );
}

/** One line above the session list: "2 need you · 3 running". */
export function SessionListSummary({ needsYou, running }: { needsYou: number; running: number }) {
  return (
    <div className="px-3 pt-2 pb-0.5 text-2xs text-text-faint tabular-nums">
      {needsYou === 0 && running === 0 ? 'Nothing needs you' : (
        <>
          {needsYou > 0 && <span className={`font-medium ${ATTENTION_TONE}`}>{needsYouLabel(needsYou)}</span>}
          {needsYou > 0 && running > 0 && ' · '}
          {running > 0 && <span>{running} running</span>}
        </>
      )}
    </div>
  );
}
