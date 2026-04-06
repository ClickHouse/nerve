import type { ChatState } from '../chatStore';

/** Zustand getter — returns current state snapshot. */
export type Get = () => ChatState;

/** Zustand setter — accepts partial state or updater function. */
export type Set = (
  partial: Partial<ChatState> | ((state: ChatState) => Partial<ChatState>),
) => void;
