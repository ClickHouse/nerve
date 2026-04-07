import type { ChatMessage, ImageBlockData, FileBlockData } from '../../types/chat';
import { Download, FileText } from 'lucide-react';
import { getToken } from '../../api/client';

export function UserMessage({ message }: { message: ChatMessage }) {
  const text = message.blocks.find(b => b.type === 'text')?.content || '';
  const images = message.blocks.filter(b => b.type === 'image') as ImageBlockData[];
  const files = message.blocks.filter(b => b.type === 'file') as FileBlockData[];

  return (
    <div className="py-4 px-5">
      <div className="max-w-3xl mx-auto">
        <div className="flex gap-3">
          <div className="w-7 h-7 rounded-full bg-surface-raised flex items-center justify-center text-xs font-medium text-text-muted shrink-0 mt-0.5">
            U
          </div>
          <div className="min-w-0 flex-1">
            {text && <div className="whitespace-pre-wrap text-[15px] leading-relaxed pt-0.5">{text}</div>}

            {/* Attached images */}
            {images.length > 0 && (
              <div className="flex gap-2 flex-wrap mt-2">
                {images.map((img, idx) => (
                  <a
                    key={idx}
                    href={img.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="block rounded-lg overflow-hidden border border-border hover:border-accent/50 transition-colors"
                  >
                    <img
                      src={img.url}
                      alt={img.filename}
                      className="max-w-[200px] max-h-[200px] object-cover"
                    />
                  </a>
                ))}
              </div>
            )}

            {/* Attached non-image files */}
            {files.length > 0 && (
              <div className="flex gap-2 flex-wrap mt-2">
                {files.map((file, idx) => (
                  <a
                    key={idx}
                    href={`${file.url}${file.url.includes('?') ? '&' : '?'}token=${getToken()}`}
                    download={file.filename}
                    className="flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-surface hover:bg-surface-hover transition-colors text-sm text-text-secondary"
                  >
                    <FileText size={14} />
                    <span className="truncate max-w-[150px]">{file.filename}</span>
                    <Download size={12} className="text-text-muted" />
                  </a>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
