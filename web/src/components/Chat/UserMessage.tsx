import type { ChatMessage } from '../../types/chat';

export function UserMessage({ message }: { message: ChatMessage }) {
  const text = message.blocks.find(b => b.type === 'text')?.content || '';

  return (
    <div className="py-4 px-5">
      <div className="max-w-3xl mx-auto">
        <div className="flex gap-3">
          <div className="w-7 h-7 rounded-full bg-surface-raised flex items-center justify-center text-xs font-medium text-text-muted shrink-0 mt-0.5">
            U
          </div>
          <div className="whitespace-pre-wrap text-[15px] leading-relaxed pt-0.5">{text}</div>
        </div>
      </div>
    </div>
  );
}
