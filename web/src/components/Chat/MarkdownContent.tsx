import { memo, type ReactNode } from 'react';
import ReactMarkdown from 'react-markdown';
import { Link, useInRouterContext } from 'react-router-dom';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import { CodeBlock } from './CodeBlock';

/** A path in this app, such as `/plans/plan-1`. `//host` is another site. */
function isAppPath(href: string | undefined): href is string {
  return !!href && href.startsWith('/') && !href.startsWith('//');
}

/**
 * Links to a path in the app use the router, so they open in place. Other
 * links open in a new tab. react-markdown already removes unsafe URLs, such
 * as `javascript:`, and renders no raw HTML.
 */
function MarkdownLink({ href, children }: { href?: string; children?: ReactNode }) {
  const inRouter = useInRouterContext();
  if (inRouter && isAppPath(href)) return <Link to={href}>{children}</Link>;
  return <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>;
}

// Memoized: react-markdown parses and highlights the full string on each
// render, and the chat renders again for each streamed token batch.
export const MarkdownContent = memo(function MarkdownContent({ content }: { content: string }) {
  return (
    <div className="markdown-content">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeHighlight]}
        components={{
          pre({ children }) {
            return <>{children}</>;
          },
          code({ className, children, ...props }) {
            const isBlock = className?.includes('language-') || className?.includes('hljs') || (typeof children === 'string' && children.includes('\n'));
            if (isBlock) {
              return <CodeBlock className={className}>{children}</CodeBlock>;
            }
            return <code className={className} {...props}>{children}</code>;
          },
          a({ href, children }) {
            return <MarkdownLink href={href}>{children}</MarkdownLink>;
          },
        }}
      >{content}</ReactMarkdown>
    </div>
  );
});
