import { useState } from 'react';
import { Button } from '../../ui';
import { ChevronDown, ChevronRight } from '../../ui/icons';

interface Props {
  /** The record schema that the gateway named, such as `linear.issue_event/1`. */
  schema: string;
  data: Record<string, unknown>;
  /** Show the JSON at once, as for a record without a card of its own. */
  defaultOpen?: boolean;
}

/**
 * The data of a generic hosted record, such as a Linear issue event, as
 * indented JSON behind a toggle that names its record schema.
 *
 * Nerve stores the data that the gateway sent with every member, also the
 * ones that no renderer knows. This view shows all of them; the metadata
 * chips of the detail header cannot show nested values.
 */
export function RecordData({ schema, data, defaultOpen = false }: Props) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="mt-4">
      <Button variant="ghost" size="xs" className="gap-1.5 mb-2" aria-expanded={open}
        onClick={() => setOpen(!open)}>
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        Record data
        {schema && <span className="font-mono text-text-dim">{schema}</span>}
      </Button>
      {open && (
        <pre className="p-3 bg-surface border border-border-subtle rounded-lg text-xs leading-5 text-text-muted whitespace-pre-wrap break-words max-h-[32rem] overflow-auto">
          {JSON.stringify(data, null, 2)}
        </pre>
      )}
    </div>
  );
}
