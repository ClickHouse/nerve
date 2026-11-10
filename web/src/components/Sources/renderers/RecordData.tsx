import { useState } from 'react';
import { Button } from '../../ui';
import { ChevronDown, ChevronRight } from '../../ui/icons';

interface Props {
  data: Record<string, unknown>;
  /** Show the JSON at once, as for a record without a card of its own. */
  defaultOpen?: boolean;
}

/**
 * The data of a generic hosted record, such as a Linear issue event, as
 * indented JSON behind a toggle.
 *
 * Nerve stores the data that the gateway sent as the record's metadata, with
 * every member, also the ones that no renderer knows. This view shows all of
 * them; the metadata chips of the detail header cannot show nested values.
 */
export function RecordData({ data, defaultOpen = false }: Props) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="mt-4">
      <Button variant="ghost" size="xs" className="gap-1.5 mb-2" aria-expanded={open}
        onClick={() => setOpen(!open)}>
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        Record data
      </Button>
      {open && (
        <pre className="p-3 bg-surface border border-border-subtle rounded-lg text-xs leading-5 text-text-muted whitespace-pre-wrap break-words max-h-[32rem] overflow-auto">
          {JSON.stringify(data, null, 2)}
        </pre>
      )}
    </div>
  );
}
