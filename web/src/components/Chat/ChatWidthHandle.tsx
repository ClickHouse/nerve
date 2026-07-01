import { useCallback, useLayoutEffect, useState } from 'react';
import { useChatStore } from '../../stores/chatStore';

/**
 * Drag handle that sets the width of the centered conversation reading column.
 *
 * The column is centered, so we grow it symmetrically: moving the handle by
 * `dx` widens the column by `2 * dx`, which keeps it centered and makes the
 * dragged edge track the cursor 1:1. The chosen width drives the `--chat-width`
 * CSS variable (consumed by the message rows, todo panel, and composer via
 * `max-w-[var(--chat-width)]`) and is persisted in the store (localStorage
 * `nerve_chat_width`).
 *
 * Mirrors the side-panel resize interaction in SidePanel.tsx. Hidden below the
 * `md` breakpoint, where the column already fills the viewport.
 */
export function ChatWidthHandle() {
  const chatWidth = useChatStore((s) => s.chatWidth);
  const setChatWidth = useChatStore((s) => s.setChatWidth);
  const [isDragging, setIsDragging] = useState(false);

  // Drive the CSS variable from the stored width. Set on the document root so
  // it overrides the default declared in index.css (:root { --chat-width }) in
  // one place; useLayoutEffect applies the persisted width before first paint
  // to avoid a flash at the default width.
  useLayoutEffect(() => {
    document.documentElement.style.setProperty('--chat-width', `${chatWidth}px`);
  }, [chatWidth]);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = chatWidth;
    const prevCursor = document.body.style.cursor;
    const prevSelect = document.body.style.userSelect;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const handleMove = (moveEvent: MouseEvent) => {
      // Centered column: grow by 2 * dx so the dragged edge tracks the cursor.
      const dx = moveEvent.clientX - startX;
      setChatWidth(startWidth + dx * 2);
    };
    const handleUp = () => {
      setIsDragging(false);
      document.body.style.cursor = prevCursor;
      document.body.style.userSelect = prevSelect;
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleUp);
    };
    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
  }, [chatWidth, setChatWidth]);

  return (
    <div
      onMouseDown={handleResizeStart}
      className="group absolute top-0 bottom-0 z-20 hidden w-4 -translate-x-1/2 cursor-col-resize md:block"
      style={{ left: 'min(calc(50% + var(--chat-width) / 2), calc(100% - 8px))' }}
      title="Drag to resize the conversation width"
    >
      {/* Full-height guide line: appears on hover/drag to show the resize axis. */}
      <div
        className={`absolute inset-y-0 left-1/2 w-px -translate-x-1/2 transition-colors ${
          isDragging ? 'bg-accent/60' : 'bg-transparent group-hover:bg-accent/30'
        }`}
      />
      {/* Persistent grip so the resize affordance is discoverable at rest;
          brightens and grows on hover/drag. */}
      <div
        className={`absolute left-1/2 top-1/2 w-1 -translate-x-1/2 -translate-y-1/2 rounded-full transition-all duration-150 ${
          isDragging ? 'h-16 bg-accent' : 'h-10 bg-accent/40 group-hover:h-16 group-hover:bg-accent'
        }`}
      />
    </div>
  );
}
