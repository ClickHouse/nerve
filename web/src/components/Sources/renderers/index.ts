export { MarkdownRenderer } from './MarkdownRenderer';
export { EmailRenderer } from './EmailRenderer';
export { GitHubRenderer } from './GitHubRenderer';

export type RendererType = 'email' | 'github' | 'default';

/**
 * Map a source name to the appropriate renderer type.
 *
 * Source names follow the pattern "type" or "type:account" (e.g., "gmail:user@example.com").
 * The prefix before ":" determines the renderer.
 *
 * To add a new renderer:
 * 1. Create a component in this directory (see EmailRenderer/GitHubRenderer for examples)
 * 2. Add the source type mapping here
 * 3. Handle the new type in MessageContent.tsx's switch statement
 * 4. Export the component from this file
 */
export function getRendererType(source: string): RendererType {
  const type = source.split(':')[0];
  switch (type) {
    case 'gmail': return 'email';
    case 'github': return 'github';
    case 'github_repos': return 'github';
    default: return 'default';
  }
}
