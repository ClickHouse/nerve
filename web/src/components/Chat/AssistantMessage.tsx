import { Repeat } from 'lucide-react';
import type { ChatMessage } from '../../types/chat';
import { BlockRenderer } from './BlockRenderer';

export function AssistantMessage({ message }: { message: ChatMessage }) {
  // Review-loop milestones are controller-written system entries, not the
  // session's assistant speaking — render them as compact tagged cards.
  if (message.channel === 'review-loop') {
    return (
      <div className="py-1.5 px-5" data-role="review-loop-milestone">
        <div className="max-w-[var(--chat-width)] mx-auto">
          <div className="flex gap-3 rounded-lg border-l-2 border-emerald-400/40 bg-emerald-400/[0.04] pl-3 pr-3 py-2">
            <Repeat size={14} className="text-emerald-400/80 shrink-0 mt-1" />
            <div className="min-w-0 flex-1 text-[13.5px] [&_p]:my-0.5">
              <BlockRenderer blocks={message.blocks} />
            </div>
          </div>
        </div>
      </div>
    );
  }
  return (
    <div className="py-4 px-5 msg-assistant" data-role="assistant">
      <div className="max-w-[var(--chat-width)] mx-auto">
        <div className="flex gap-3">
          <div className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-medium shrink-0 mt-0.5 bg-accent/20 text-accent">
            N
          </div>
          <div className="min-w-0 flex-1">
            <BlockRenderer blocks={message.blocks} />
          </div>
        </div>
      </div>
    </div>
  );
}
