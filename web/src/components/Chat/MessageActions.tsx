import { useEffect, useRef, useState } from 'react';
import { Check, Copy, GitBranch } from '../ui/icons';
import { copyToClipboard } from '../../utils/clipboard';
import { messageText } from '../../utils/messageText';
import type { ChatMessage } from '../../types/chat';

/**
 * Hover-revealed per-message toolbar (Slack-style: floats at the message's
 * top-right, so it never shifts the transcript layout).
 *
 * Revealed by the `group/msg` on the message root — `opacity-0` at rest,
 * visible on row hover and while focused (keyboard users tab into it).
 * Hidden below `md`: there is no hover on touch, and a persistent toolbar
 * per message would be noise.
 */
export function MessageActions({ message, onFork }: {
  message: ChatMessage;
  /** Present only when this message is a valid fork anchor. */
  onFork?: () => void;
}) {
  const [copied, setCopied] = useState(false);
  const resetTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => {
    if (resetTimer.current) clearTimeout(resetTimer.current);
  }, []);

  const text = messageText(message);

  const handleCopy = async () => {
    if (!text) return;
    const ok = await copyToClipboard(text);
    if (!ok) return;
    setCopied(true);
    if (resetTimer.current) clearTimeout(resetTimer.current);
    resetTimer.current = setTimeout(() => setCopied(false), 1500);
  };

  if (!text && !onFork) return null;

  return (
    <div
      className="absolute -top-2.5 right-0 z-10 hidden md:flex items-center gap-0.5
                 rounded-lg border border-border-subtle bg-surface-raised shadow-md p-0.5
                 opacity-0 group-hover/msg:opacity-100 focus-within:opacity-100 transition-opacity"
    >
      {text && (
        <button
          type="button"
          onClick={handleCopy}
          title="Copy message"
          aria-label="Copy message"
          className="p-1.5 rounded-md text-text-faint hover:text-text-secondary hover:bg-surface-hover cursor-pointer transition-colors"
        >
          {copied ? <Check size={13} className="text-success" /> : <Copy size={13} />}
        </button>
      )}
      {onFork && (
        <button
          type="button"
          onClick={onFork}
          title="Fork chat from here — branch a new chat that remembers the conversation up to this point"
          aria-label="Fork chat from here"
          className="p-1.5 rounded-md text-text-faint hover:text-hue-violet hover:bg-surface-hover cursor-pointer transition-colors"
        >
          <GitBranch size={13} />
        </button>
      )}
    </div>
  );
}
