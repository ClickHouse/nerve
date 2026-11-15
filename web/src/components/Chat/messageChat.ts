import { createContext } from 'react';

/**
 * The chat that a piece of text or a tool call belongs to: the session of
 * the message it is part of. Null outside a chat (the text of a task, a
 * plan, a notification).
 *
 * A widget block reads it to know where what its widget saves is kept, and
 * the block of an MCP App to know whose tools its view calls. It comes down
 * the tree from the message itself (`MessageList`), which carries the
 * session the server handed it out for: what the chat's store calls the
 * chat on screen can be another one for a moment, while one chat's history
 * is still on its way and the person has opened the next. Around the
 * messages the chat page names its chat, for what is shown before the
 * server has it: the answer that is being written.
 *
 * It is a context so that showing text needs no chat store: a page that has
 * no chat need not load one.
 */
export const MessageChatContext = createContext<string | null>(null);
