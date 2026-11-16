import { MarkdownRenderer } from './MarkdownRenderer';
import { RecordData } from './RecordData';
import { recordData } from './storedRecord';

interface Props {
  content: string;
  metadata?: Record<string, unknown>;
  summary: string;
}

/**
 * A record of a generic hosted source that has no renderer of its own: the
 * content, then the record's data as JSON, open, since there is no card.
 */
export function DataRenderer({ content, metadata, summary: _summary }: Props) {
  void _summary; // Part of common renderer interface
  const record = recordData(metadata);

  return (
    <div>
      <MarkdownRenderer content={content} />
      {record && <RecordData schema={record.schema} data={record.data} defaultOpen />}
    </div>
  );
}
