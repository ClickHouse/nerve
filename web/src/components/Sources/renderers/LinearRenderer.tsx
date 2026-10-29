import { Badge } from '../../ui';
import { ListTodo, ExternalLink } from '../../ui/icons';
import { MarkdownRenderer } from './MarkdownRenderer';

interface Props {
  content: string;
  metadata?: Record<string, unknown>;
  summary: string;
}

export function LinearRenderer({ content, metadata, summary: _summary }: Props) {
  void _summary; // Part of common renderer interface
  const identifier = String(metadata?.identifier ?? '');
  const teamKey = String(metadata?.team_key ?? '');
  const event = String(metadata?.event ?? '');
  const state = String(metadata?.state ?? '');
  const subjectUrl = String(metadata?.subject_url ?? '');

  return (
    <div>
      {/* Linear header card */}
      {(identifier || subjectUrl) && (
        <div className="mb-4 p-3 bg-surface border border-border-subtle rounded-lg">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <ListTodo size={14} className="text-hue-violet shrink-0" />
            <span className="text-sm text-text-secondary font-medium">{identifier}</span>
            {/* `info` names the source, as on the Sources page; the state is
                Linear's own label, not an outcome of this run. */}
            {teamKey && <Badge tone="info">{teamKey}</Badge>}
            {event && <Badge>{event}</Badge>}
            {state && <Badge>{state}</Badge>}
          </div>
          {subjectUrl && (
            <a href={subjectUrl} target="_blank" rel="noopener noreferrer"
              className="flex items-center gap-1 text-xs text-accent hover:text-link transition-colors">
              <ExternalLink size={11} /> View in Linear
            </a>
          )}
        </div>
      )}

      <MarkdownRenderer content={content} />
    </div>
  );
}
