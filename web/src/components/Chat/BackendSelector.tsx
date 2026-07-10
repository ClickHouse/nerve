import { Sparkle, SquareTerminal } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';

/**
 * Agent-backend selector for NEW chats: Claude (Claude Agent SDK) vs
 * Codex (OpenAI app-server, GPT-5.6).
 *
 * The choice binds when the session is created server-side (first
 * message / first upload) and is sticky for the session's lifetime
 * (`sessions.backend`), so the control only renders while the chat is
 * still virtual — after that the header's model badge shows what the
 * session runs on.
 */

const STYLES: Record<string, {
  icon: typeof Sparkle;
  active: string;
  dot: string;
}> = {
  claude: {
    icon: Sparkle,
    active: 'text-hue-orange bg-orange-500/10 shadow-[inset_0_0_0_1px_rgba(249,115,22,0.3)]',
    dot: 'bg-hue-orange',
  },
  codex: {
    icon: SquareTerminal,
    active: 'text-hue-teal bg-teal-400/10 shadow-[inset_0_0_0_1px_rgba(45,212,191,0.3)]',
    dot: 'bg-hue-teal',
  },
};

export function BackendSelector({ disabled }: { disabled?: boolean }) {
  const backendOptions = useChatStore(s => s.backendOptions);
  const backendDefault = useChatStore(s => s.backendDefault);
  const newChatBackend = useChatStore(s => s.newChatBackend);
  const setNewChatBackend = useChatStore(s => s.setNewChatBackend);

  if (backendOptions.length < 2) return null;

  const selected = newChatBackend ?? backendDefault ?? backendOptions[0].id;

  return (
    <div
      className="h-10 flex items-center gap-0.5 p-1 bg-surface-raised border border-border rounded-xl shrink-0"
      role="radiogroup"
      aria-label="Agent backend for this chat"
    >
      {backendOptions.map((opt) => {
        const style = STYLES[opt.id] ?? STYLES.claude;
        const Icon = style.icon;
        const isActive = selected === opt.id;
        return (
          <button
            key={opt.id}
            role="radio"
            aria-checked={isActive}
            disabled={disabled}
            onClick={() => setNewChatBackend(opt.id === backendDefault ? null : opt.id)}
            title={`${opt.label} — ${opt.model}${opt.id === backendDefault ? ' (default)' : ''}. Applies to this new chat; the choice is fixed once the conversation starts.`}
            className={`h-8 px-2.5 rounded-lg flex items-center gap-1.5 text-[13px] font-medium transition-all cursor-pointer disabled:opacity-30 disabled:cursor-default ${
              isActive
                ? style.active
                : 'text-text-muted hover:text-text-secondary hover:bg-surface'
            }`}
          >
            <Icon size={14} />
            <span>{opt.label}</span>
            {isActive && (
              <span className={`w-1 h-1 rounded-full ${style.dot}`} />
            )}
          </button>
        );
      })}
    </div>
  );
}
