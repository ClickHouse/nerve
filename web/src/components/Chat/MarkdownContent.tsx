import { memo, type ComponentProps, type ReactNode } from 'react';
import ReactMarkdown, { type ExtraProps, type Options } from 'react-markdown';
import { Link, useInRouterContext } from 'react-router-dom';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import { CodeBlock } from './CodeBlock';
import { RICH_LANGUAGES, richKindFor } from './rich/kinds';
import { RichCodeBlock } from './rich/RichCodeBlock';
import { MarkdownStreamingContext } from './streamingContext';

/**
 * A page of this app, such as `/plans/plan-1`. `//host` is another site, and
 * `/api/...` is a server URL (a download, an upload), not a page.
 */
function isAppPath(href: string | undefined): href is string {
  return !!href && href.startsWith('/') && !href.startsWith('//') && !href.startsWith('/api/');
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

/** What inline markdown keeps; anything else (a heading, a list) keeps its text without the block. */
const INLINE_ELEMENTS = ['p', 'code', 'strong', 'em', 'del', 'a', 'br'];
const INLINE_WITHOUT_LINKS = INLINE_ELEMENTS.filter(name => name !== 'a');

/**
 * Markdown inside a line of other text — a question, an option's
 * description: code, emphasis and links, with no paragraph or other block
 * around them. `links={false}` keeps a link's text only, for text that sits
 * inside a button (a link there would be a control inside a control).
 */
export const InlineMarkdown = memo(function InlineMarkdown({ content, links = true }: {
  content: string;
  links?: boolean;
}) {
  return (
    <span className="markdown-content">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        allowedElements={links ? INLINE_ELEMENTS : INLINE_WITHOUT_LINKS}
        unwrapDisallowed
        components={{
          p({ children }) {
            return <>{children}</>;
          },
          a({ href, children }) {
            return <MarkdownLink href={href}>{children}</MarkdownLink>;
          },
        }}
      >{content}</ReactMarkdown>
    </span>
  );
});

const REMARK_PLUGINS: NonNullable<Options['remarkPlugins']> = [remarkGfm];
// The rendered languages keep their source as one text node (see RICH_LANGUAGES).
const REHYPE_PLUGINS: NonNullable<Options['rehypePlugins']> = [[rehypeHighlight, { plainText: RICH_LANGUAGES }]];

interface HastNode {
  type: string;
  value?: string;
  children?: HastNode[];
}

/** The text of a hast node — a code block's source, however highlighting split it. */
function hastText(node: HastNode | undefined): string {
  if (!node) return '';
  if (node.type === 'text') return node.value ?? '';
  return (node.children ?? []).map(hastText).join('');
}

function languageOf(className: string | undefined): string | undefined {
  return className?.match(/language-([\w+-]+)/)?.[1];
}

function Pre({ children }: { children?: ReactNode }) {
  return <>{children}</>;
}

function Code({ className, children, node, ...props }: ComponentProps<'code'> & ExtraProps) {
  const kind = richKindFor(languageOf(className));
  if (kind) {
    // mdast-util-to-hast ends a code block's text with a newline that is not part of the source.
    return <RichCodeBlock kind={kind} source={hastText(node).replace(/\n$/, '')} />;
  }
  const isBlock = className?.includes('language-') || className?.includes('hljs') || (typeof children === 'string' && children.includes('\n'));
  if (isBlock) {
    return <CodeBlock className={className}>{children}</CodeBlock>;
  }
  return <code className={className} {...props}>{children}</code>;
}

function Anchor({ href, children }: { href?: string; children?: ReactNode }) {
  return <MarkdownLink href={href}>{children}</MarkdownLink>;
}

// One components object for every render: react-markdown treats a new function
// as a new element type, which would remount every code block — and a
// rendering one would start over — on each streamed token batch.
const COMPONENTS: Options['components'] = { pre: Pre, code: Code, a: Anchor };

// Memoized: react-markdown parses and highlights the full string on each
// render, and the chat renders again for each streamed token batch.
// `streaming` says the text is still arriving; the blocks that render their
// source (diagrams, charts) wait for it to settle.
export const MarkdownContent = memo(function MarkdownContent({ content, streaming = false }: {
  content: string;
  streaming?: boolean;
}) {
  return (
    <MarkdownStreamingContext.Provider value={streaming}>
      <div className="markdown-content">
        <ReactMarkdown
          remarkPlugins={REMARK_PLUGINS}
          rehypePlugins={REHYPE_PLUGINS}
          components={COMPONENTS}
        >{content}</ReactMarkdown>
      </div>
    </MarkdownStreamingContext.Provider>
  );
});
