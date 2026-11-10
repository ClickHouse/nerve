import { Badge } from '../../ui';
import { ListTodo, ExternalLink } from '../../ui/icons';
import { MarkdownRenderer } from './MarkdownRenderer';
import { RecordData } from './RecordData';

interface Props {
  content: string;
  metadata?: Record<string, unknown>;
  summary: string;
}

/**
 * A member of a JSON object, or `undefined`.
 *
 * The metadata is the data of a `linear.issue_event` record as the gateway
 * sent it. A member can be missing, or of another type in a later version, so
 * every read is tolerant.
 */
function member(value: unknown, key: string): unknown {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)[key]
    : undefined;
}

/** A string or a number as text; an object such as a person or a state by its `name`. */
function text(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return String(value);
  const name = member(value, 'name');
  return typeof name === 'string' ? name : '';
}

/** An http(s) URL, or nothing: the data is untrusted, so no other scheme becomes a link. */
function webUrl(value: unknown): string {
  const url = text(value);
  return /^https?:\/\//i.test(url) ? url : '';
}

export function LinearRenderer({ content, metadata, summary: _summary }: Props) {
  void _summary; // Part of common renderer interface
  const issue = member(metadata, 'issue');
  const identifier = text(member(issue, 'identifier'));
  const title = text(member(issue, 'title'));
  const event = text(member(metadata, 'event')).replace(/_/g, ' ');
  const state = text(member(issue, 'state'));
  const actor = text(member(metadata, 'actor'));
  // A comment event links to the comment, every other event to the issue.
  const link = webUrl(member(member(metadata, 'comment'), 'url')) || webUrl(member(issue, 'url'));

  return (
    <div>
      {/* Linear issue card */}
      {(identifier || title || link) && (
        <div className="mb-4 p-3 bg-surface border border-border-subtle rounded-lg">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <ListTodo size={14} className="text-hue-violet shrink-0" />
            {identifier && <span className="text-sm text-text-secondary font-medium">{identifier}</span>}
            {/* The event and the state are Linear's own labels, not an
                outcome of this run, so they stay neutral. */}
            {event && <Badge>{event}</Badge>}
            {state && <Badge>{state}</Badge>}
          </div>
          {title && <div className="text-sm text-text-secondary break-words">{title}</div>}
          {actor && <div className="mt-0.5 text-xs text-text-dim break-words">by {actor}</div>}
          {link && (
            <a href={link} target="_blank" rel="noopener noreferrer"
              className="mt-1 inline-flex items-center gap-1 text-xs text-accent hover:text-link transition-colors">
              <ExternalLink size={11} /> View in Linear
            </a>
          )}
        </div>
      )}

      <MarkdownRenderer content={content} />
      {metadata && <RecordData data={metadata} />}
    </div>
  );
}
