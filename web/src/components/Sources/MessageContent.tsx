import { getRendererType, EmailRenderer, GitHubRenderer, LinearRenderer, MarkdownRenderer } from './renderers';

interface Props {
  source: string;
  content: string;
  rawContent?: string | null;
  metadata?: Record<string, any>;
  summary: string;
}

/**
 * Source-aware message content renderer.
 *
 * Selects the appropriate renderer based on the source type:
 * - gmail → EmailRenderer (sandboxed iframe for HTML, text toggle)
 * - github → GitHubRenderer (repo card header + markdown)
 * - linear → LinearRenderer (issue card header + markdown)
 * - default → MarkdownRenderer
 *
 * To add a new source renderer, see renderers/index.ts for instructions.
 */
export function MessageContent({ source, content, rawContent, metadata, summary }: Props) {
  const rendererType = getRendererType(source);

  switch (rendererType) {
    case 'email':
      return <EmailRenderer content={content} rawContent={rawContent || null} summary={summary} />;
    case 'github':
      return <GitHubRenderer content={content} metadata={metadata} summary={summary} />;
    case 'linear':
      return <LinearRenderer content={content} metadata={metadata} summary={summary} />;
    default:
      return <MarkdownRenderer content={content} />;
  }
}
