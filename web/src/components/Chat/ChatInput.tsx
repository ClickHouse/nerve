import { useState, useRef, useEffect, useCallback, type KeyboardEvent, type ClipboardEvent, type DragEvent } from 'react';
import { Send, Square, X, Plus, Trash2, Sparkles, HelpCircle, StickyNote, Paperclip, FileText, Loader2 } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import type { QuoteAction, QuoteEntry } from '../../stores/chatStore';
import { api } from '../../api/client';

const ACTION_CONFIG: Record<QuoteAction, { icon: typeof Plus; label: string; color: string; placeholder: string }> = {
  add:      { icon: Plus,       label: 'Add',     color: 'var(--theme-accent)', placeholder: 'Instructions...' },
  remove:   { icon: Trash2,     label: 'Remove',  color: '#ef4444', placeholder: 'Instructions...' },
  improve:  { icon: Sparkles,   label: 'Improve', color: '#a855f7', placeholder: 'Instructions...' },
  question: { icon: HelpCircle, label: 'Ask',     color: '#f59e0b', placeholder: 'What do you want to know?' },
  note:     { icon: StickyNote, label: 'Note',    color: '#6b7280', placeholder: 'Your note...' },
};

// Actions that auto-focus the instruction input (need user input)
const FOCUS_ACTIONS = new Set<QuoteAction>(['add', 'question', 'note']);

interface AttachmentFile {
  id: string;
  file: File;
  preview?: string;
  uploading: boolean;
  uploadedId?: string;
  uploadedMeta?: { filename: string; media_type: string; file_type: string };
  error?: string;
}

export function ChatInput({ onSend, onStop, isStreaming, disabled }: {
  onSend: (message: string, fileIds?: string[], imageBlocks?: Array<{ url: string; filename: string; media_type: string }>) => void;
  onStop: () => void;
  isStreaming: boolean;
  disabled?: boolean;
}) {
  const [input, setInput] = useState('');
  const [attachments, setAttachments] = useState<AttachmentFile[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const lastInstructionRef = useRef<HTMLInputElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const dragCountRef = useRef(0);

  const quotes = useChatStore(s => s.quotes);
  const removeQuote = useChatStore(s => s.removeQuote);
  const updateQuoteInstruction = useChatStore(s => s.updateQuoteInstruction);
  const clearQuotes = useChatStore(s => s.clearQuotes);
  const activeSession = useChatStore(s => s.activeSession);

  const [prevQuoteCount, setPrevQuoteCount] = useState(0);

  // Auto-focus instruction input when a new quote is added
  useEffect(() => {
    if (quotes.length > prevQuoteCount && quotes.length > 0) {
      const last = quotes[quotes.length - 1];
      if (FOCUS_ACTIONS.has(last.action)) {
        setTimeout(() => lastInstructionRef.current?.focus(), 0);
      }
    }
    setPrevQuoteCount(quotes.length);
  }, [quotes.length, prevQuoteCount, quotes]);

  // Cleanup object URLs on unmount
  useEffect(() => {
    return () => {
      attachments.forEach(a => { if (a.preview) URL.revokeObjectURL(a.preview); });
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const addFiles = useCallback(async (files: File[]) => {
    const newAttachments: AttachmentFile[] = files.map(file => ({
      id: crypto.randomUUID(),
      file,
      preview: file.type.startsWith('image/') ? URL.createObjectURL(file) : undefined,
      uploading: true,
    }));

    setAttachments(prev => [...prev, ...newAttachments]);

    // Upload all files
    try {
      const result = await api.uploadFiles(files, activeSession);
      setAttachments(prev => prev.map(a => {
        const idx = newAttachments.findIndex(n => n.id === a.id);
        if (idx >= 0 && result.files[idx]) {
          const meta = result.files[idx];
          return {
            ...a,
            uploading: false,
            uploadedId: meta.id,
            uploadedMeta: { filename: meta.filename, media_type: meta.media_type, file_type: meta.file_type },
          };
        }
        return a;
      }));
    } catch (err) {
      setAttachments(prev => prev.map(a => {
        if (newAttachments.some(n => n.id === a.id)) {
          return { ...a, uploading: false, error: String(err) };
        }
        return a;
      }));
    }
  }, [activeSession]);

  const removeAttachment = useCallback((id: string) => {
    setAttachments(prev => {
      const removed = prev.find(a => a.id === id);
      if (removed?.preview) URL.revokeObjectURL(removed.preview);
      return prev.filter(a => a.id !== id);
    });
  }, []);

  const composeMessage = (): string => {
    const parts: string[] = [];
    const ACTION_LABELS: Record<QuoteAction, string> = {
      add: 'Add', remove: 'Remove', improve: 'Improve', question: 'Question', note: 'Note',
    };

    for (const q of quotes) {
      const blockquote = q.text.split('\n').map(l => `> ${l}`).join('\n');
      const instr = q.instruction.trim();
      const label = ACTION_LABELS[q.action];
      parts.push(instr ? `${blockquote}\n${label}: ${instr}` : blockquote);
    }

    if (input.trim()) {
      parts.push(input.trim());
    }

    return parts.join('\n\n');
  };

  const allUploaded = attachments.length === 0 || attachments.every(a => !a.uploading);
  const hasContent = input.trim() || quotes.length > 0 || attachments.some(a => a.uploadedId);
  const canSend = !disabled && !isStreaming && hasContent && allUploaded;

  const handleSend = () => {
    const message = composeMessage();
    if (!message && attachments.length === 0) return;

    const fileIds = attachments.filter(a => a.uploadedId).map(a => a.uploadedId!);
    const imageBlocks = attachments
      .filter(a => a.uploadedId && a.uploadedMeta?.file_type === 'image')
      .map(a => ({
        url: `/api/files/uploads/${a.uploadedId}`,
        filename: a.uploadedMeta!.filename,
        media_type: a.uploadedMeta!.media_type,
      }));

    onSend(message, fileIds.length > 0 ? fileIds : undefined, imageBlocks.length > 0 ? imageBlocks : undefined);
    setInput('');
    clearQuotes();
    // Clean up previews
    attachments.forEach(a => { if (a.preview) URL.revokeObjectURL(a.preview); });
    setAttachments([]);
    if (textareaRef.current) textareaRef.current.style.height = 'auto';
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (canSend) handleSend();
    }
  };

  const handlePaste = (e: ClipboardEvent) => {
    const items = e.clipboardData?.items;
    if (!items) return;

    const files: File[] = [];
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.kind === 'file') {
        const file = item.getAsFile();
        if (file) files.push(file);
      }
    }

    if (files.length > 0) {
      e.preventDefault();
      addFiles(files);
    }
  };

  const handleDragEnter = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCountRef.current++;
    if (e.dataTransfer.types.includes('Files')) {
      setIsDragging(true);
    }
  };

  const handleDragLeave = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCountRef.current--;
    if (dragCountRef.current === 0) {
      setIsDragging(false);
    }
  };

  const handleDragOver = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCountRef.current = 0;
    setIsDragging(false);

    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      addFiles(files);
    }
  };

  const handleInput = () => {
    const el = textareaRef.current;
    if (el) {
      el.style.height = 'auto';
      el.style.height = Math.min(el.scrollHeight, 200) + 'px';
    }
  };

  return (
    <div
      className="border-t border-border-subtle bg-bg shrink-0 relative"
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {/* Drag overlay */}
      {isDragging && (
        <div className="absolute inset-0 z-50 bg-accent/10 border-2 border-dashed border-accent rounded-lg flex items-center justify-center">
          <span className="text-accent font-medium text-sm">Drop files here</span>
        </div>
      )}

      {/* Quote cards */}
      {quotes.length > 0 && (
        <div className="px-4 pt-3 pb-1">
          <div className="max-w-3xl mx-auto space-y-2">
            {quotes.map((quote, idx) => (
              <QuoteCard
                key={quote.id}
                quote={quote}
                instructionRef={idx === quotes.length - 1 ? lastInstructionRef : undefined}
                onRemove={() => removeQuote(quote.id)}
                onUpdateInstruction={(v) => updateQuoteInstruction(quote.id, v)}
                onSend={canSend ? handleSend : undefined}
              />
            ))}
          </div>
        </div>
      )}

      {/* Attachment previews */}
      {attachments.length > 0 && (
        <div className="px-4 pt-3 pb-1">
          <div className="max-w-3xl mx-auto flex gap-2 flex-wrap">
            {attachments.map(a => (
              <AttachmentPreview key={a.id} attachment={a} onRemove={() => removeAttachment(a.id)} />
            ))}
          </div>
        </div>
      )}

      {/* Main input */}
      <div className="px-4 py-3">
        <div className="max-w-3xl mx-auto flex gap-3 items-end">
          {/* File attach button */}
          <button
            onClick={() => fileInputRef.current?.click()}
            disabled={disabled || isStreaming}
            className="w-10 h-10 text-text-muted hover:text-text-secondary rounded-xl flex items-center justify-center cursor-pointer transition-colors shrink-0 disabled:opacity-30"
            title="Attach files"
          >
            <Paperclip size={18} />
          </button>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            onChange={(e) => {
              const files = Array.from(e.target.files || []);
              if (files.length > 0) addFiles(files);
              e.target.value = '';
            }}
          />

          <textarea
            ref={textareaRef}
            value={input}
            onChange={(e) => { setInput(e.target.value); handleInput(); }}
            onKeyDown={handleKeyDown}
            onPaste={handlePaste}
            placeholder={quotes.length > 0 ? 'Add context (optional)...' : attachments.length > 0 ? 'Add a message (optional)...' : 'Send a message...'}
            rows={1}
            disabled={disabled}
            className="flex-1 px-4 py-3 bg-surface-raised border border-border rounded-xl text-[15px] text-text outline-none focus:border-accent/50 resize-none disabled:opacity-50 placeholder:text-text-faint"
          />
          {isStreaming ? (
            <button
              onClick={onStop}
              className="w-10 h-10 bg-red-500/80 hover:bg-red-500 text-white rounded-xl flex items-center justify-center cursor-pointer transition-colors shrink-0"
              title="Stop generation"
            >
              <Square size={16} />
            </button>
          ) : (
            <button
              onClick={handleSend}
              disabled={!canSend}
              className="w-10 h-10 bg-accent hover:bg-accent-hover text-white rounded-xl flex items-center justify-center disabled:opacity-30 cursor-pointer transition-colors shrink-0"
            >
              <Send size={18} />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}


function AttachmentPreview({ attachment, onRemove }: { attachment: AttachmentFile; onRemove: () => void }) {
  const isImage = attachment.file.type.startsWith('image/');

  return (
    <div className="relative group rounded-lg border border-border bg-surface overflow-hidden flex items-center gap-2">
      {isImage && attachment.preview ? (
        <img src={attachment.preview} alt={attachment.file.name} className="w-16 h-16 object-cover" />
      ) : (
        <div className="w-16 h-16 flex items-center justify-center bg-surface-raised">
          <FileText size={20} className="text-text-muted" />
        </div>
      )}
      <div className="pr-7 py-1.5 min-w-0">
        <div className="text-[12px] text-text-secondary truncate max-w-[120px]">{attachment.file.name}</div>
        <div className="text-[11px] text-text-muted">
          {attachment.uploading ? (
            <span className="flex items-center gap-1"><Loader2 size={10} className="animate-spin" /> Uploading...</span>
          ) : attachment.error ? (
            <span className="text-error">Failed</span>
          ) : (
            <span className="text-success">Ready</span>
          )}
        </div>
      </div>
      <button
        onClick={onRemove}
        className="absolute top-1 right-1 w-5 h-5 rounded-full bg-bg/80 text-text-muted hover:text-text flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer"
      >
        <X size={12} />
      </button>
    </div>
  );
}


function QuoteCard({ quote, instructionRef, onRemove, onUpdateInstruction, onSend }: {
  quote: QuoteEntry;
  instructionRef?: React.RefObject<HTMLInputElement | null>;
  onRemove: () => void;
  onUpdateInstruction: (v: string) => void;
  onSend?: () => void;
}) {
  const config = ACTION_CONFIG[quote.action];
  const Icon = config.icon;
  const truncated = quote.text.length > 120 ? quote.text.slice(0, 120) + '…' : quote.text;

  return (
    <div
      className="quote-card rounded-lg bg-surface border border-border overflow-hidden"
      style={{ borderLeftColor: config.color, borderLeftWidth: '3px' }}
    >
      <div className="flex items-start gap-2 px-3 py-2">
        {/* Icon + label */}
        <div className="flex items-center gap-1.5 shrink-0 pt-0.5">
          <Icon size={13} style={{ color: config.color }} />
          <span className="text-[11px] font-medium uppercase tracking-wider" style={{ color: config.color }}>
            {config.label}
          </span>
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0">
          <div className="text-[12px] text-text-muted leading-relaxed line-clamp-2">{truncated}</div>
          <input
            ref={instructionRef}
            type="text"
            value={quote.instruction}
            onChange={(e) => onUpdateInstruction(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter' && onSend) { e.preventDefault(); onSend(); } }}
            placeholder={config.placeholder}
            className="w-full mt-1.5 px-0 py-0.5 bg-transparent text-[13px] text-text-secondary outline-none placeholder:text-text-faint border-b border-border focus:border-border transition-colors"
          />
        </div>

        {/* Remove */}
        <button
          onClick={onRemove}
          className="text-text-faint hover:text-text-muted cursor-pointer transition-colors shrink-0 pt-0.5"
        >
          <X size={14} />
        </button>
      </div>
    </div>
  );
}
