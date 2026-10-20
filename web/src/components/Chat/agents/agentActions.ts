// Actions the agent surfaces share: put text into the composer (Follow up…,
// Relaunch…), take the person back to the composer, and jump to an agent's
// spawn point in the transcript. On a phone the side panel covers the chat
// column (and makes it inert), so each of these first gets it out of the way.

import { useAgentStore } from '../../../stores/agentStore';
import { useChatStore } from '../../../stores/chatStore';
import { isMobileViewport } from '../../../hooks/useMediaQuery';

/** Hide the side panel when it covers the chat column (phones). */
function uncoverChat(): void {
  const chat = useChatStore.getState();
  if (chat.panelVisible && isMobileViewport()) chat.togglePanel();
}

/** Put `text` into the composer (ChatInput applies and focuses it). */
export function prefillComposer(text: string): void {
  uncoverChat();
  useAgentStore.getState().prefillComposer(text, 'append');
}

/** Move focus to the message box. */
export function focusComposer(): void {
  const covered = useChatStore.getState().panelVisible && isMobileViewport();
  uncoverChat();
  const focus = () => document.getElementById('nerve-chat-input')?.focus();
  // The column stops being inert on the next render.
  if (covered) setTimeout(focus, 0);
  else focus();
}

/**
 * Scroll the chat to the line of the spawn call `toolUseId` and give it focus.
 * False when that line is not on the page (a collapsed group, or history not
 * loaded).
 */
export function jumpToSpawn(toolUseId: string): boolean {
  const target = [...document.querySelectorAll<HTMLElement>('[data-agent-spawn]')]
    .find(el => el.dataset.agentSpawn === toolUseId);
  if (!target) return false;
  uncoverChat();
  const reduce = typeof window.matchMedia === 'function'
    && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  target.scrollIntoView?.({ block: 'center', behavior: reduce ? 'auto' : 'smooth' });
  if (!target.hasAttribute('tabindex')) target.setAttribute('tabindex', '-1');
  target.focus({ preventScroll: true });
  if (!reduce && typeof target.animate === 'function') {
    target.animate(
      [{ boxShadow: '0 0 0 2px var(--theme-accent)' }, { boxShadow: '0 0 0 2px transparent' }],
      { duration: 1400, easing: 'ease-out' },
    );
  }
  return true;
}
