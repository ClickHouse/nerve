import { recordData } from './storedRecord';

export { MarkdownRenderer } from './MarkdownRenderer';
export { EmailRenderer } from './EmailRenderer';
export { GitHubRenderer } from './GitHubRenderer';
export { LinearRenderer } from './LinearRenderer';
export { DataRenderer } from './DataRenderer';
export { recordData, type StoredRecordData } from './storedRecord';

export type RendererType = 'email' | 'github' | 'linear' | 'data' | 'default';

/** The record schema of the Linear records that the Linear card knows. */
export const LINEAR_ISSUE_EVENT_SCHEMA = 'linear.issue_event/1';

/**
 * Map a record to the appropriate renderer type.
 *
 * A record of a generic hosted source (see recordData) gets its renderer from
 * its record schema: the Linear card for the schema it knows, the record data
 * view for any other. Any other record gets it from its source name, which
 * follows the pattern "type" or "type:account" (e.g., "gmail:user@example.com"):
 * the prefix before ":" determines the renderer.
 *
 * To add a new renderer:
 * 1. Create a component in this directory (see EmailRenderer/GitHubRenderer for examples)
 * 2. Add the source type mapping here
 * 3. Handle the new type in MessageContent.tsx's switch statement
 * 4. Export the component from this file
 */
export function getRendererType(source: string, metadata?: Record<string, unknown> | null): RendererType {
  const record = recordData(metadata);
  if (record) return record.schema === LINEAR_ISSUE_EVENT_SCHEMA ? 'linear' : 'data';
  const type = source.split(':')[0];
  switch (type) {
    case 'gmail': return 'email';
    case 'github': return 'github';
    case 'github_repos': return 'github';
    default: return 'default';
  }
}

/** Whether the renderer shows the record data, so the detail header leaves out its metadata chips. */
export function showsRecordData(type: RendererType): boolean {
  return type === 'linear' || type === 'data';
}
