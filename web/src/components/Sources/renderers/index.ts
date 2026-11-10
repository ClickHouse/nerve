export { MarkdownRenderer } from './MarkdownRenderer';
export { EmailRenderer } from './EmailRenderer';
export { GitHubRenderer } from './GitHubRenderer';
export { LinearRenderer } from './LinearRenderer';
export { DataRenderer } from './DataRenderer';

export type RendererType = 'email' | 'github' | 'linear' | 'data' | 'default';

/**
 * Whether *metadata* holds nested JSON: an object, or an array with an object
 * or an array in it.
 *
 * That is the data of a generic hosted source, which Nerve stores as the
 * record's metadata. The metadata of every other source is flat: strings,
 * numbers, booleans, and lists of strings.
 */
export function hasRecordData(metadata?: Record<string, unknown> | null): boolean {
  const nested = (value: unknown) => value !== null && typeof value === 'object';
  return Object.values(metadata ?? {}).some(
    (value) => nested(value) && (!Array.isArray(value) || value.some(nested)),
  );
}

/**
 * Map a source name to the appropriate renderer type.
 *
 * Source names follow the pattern "type" or "type:account" (e.g., "gmail:user@example.com").
 * The prefix before ":" determines the renderer. A source without a renderer
 * of its own gets the record data view when its metadata holds nested JSON
 * (see hasRecordData), and plain markdown otherwise.
 *
 * To add a new renderer:
 * 1. Create a component in this directory (see EmailRenderer/GitHubRenderer for examples)
 * 2. Add the source type mapping here
 * 3. Handle the new type in MessageContent.tsx's switch statement
 * 4. Export the component from this file
 */
export function getRendererType(source: string, metadata?: Record<string, unknown> | null): RendererType {
  const type = source.split(':')[0];
  switch (type) {
    case 'gmail': return 'email';
    case 'github': return 'github';
    case 'github_repos': return 'github';
    case 'linear': return 'linear';
    default: return hasRecordData(metadata) ? 'data' : 'default';
  }
}

/** Whether the renderer shows the record data, so the detail header leaves out its metadata chips. */
export function showsRecordData(type: RendererType): boolean {
  return type === 'linear' || type === 'data';
}
