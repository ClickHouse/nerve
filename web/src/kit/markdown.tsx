import { Children, isValidElement, type ReactNode } from 'react';
import ReactMarkdown, { type Options } from 'react-markdown';
import rehypeHighlight from 'rehype-highlight';
import remarkGfm from 'remark-gfm';
import { postToHost } from './channel';
import { textIsAddress } from './links';
import { sourceTooLarge, type PageSource } from './source';

/**
 * The Markdown page: the Markdown renderer of the chat (react-markdown with
 * GitHub tables, task lists and code colors), without the blocks that the
 * chat draws as diagrams, charts or widgets: such a block is shown as code.
 *
 * react-markdown renders no raw HTML of the source, and removes unsafe
 * addresses such as `javascript:`. A frame of the kit cannot open another
 * page, so a link is plain text: its text and then its address in
 * parentheses, or the address once when the text is the address. A person
 * can select and copy it. A link to `#id` stays a link: it moves in the
 * page. An image shows its alt text: a page loads no file.
 *
 * The download button sends `download` to the host, which downloads the
 * source file of the bundle (`source.md`). It floats at the top right of
 * the frame and shows while the pointer is over the page, while it has the
 * keyboard, and always on a touch screen (`kit.css`).
 */

const REMARK_PLUGINS: NonNullable<Options['remarkPlugins']> = [remarkGfm];
const REHYPE_PLUGINS: NonNullable<Options['rehypePlugins']> = [rehypeHighlight];

/** The text of React children, for the comparison with an address. */
function textOf(children: ReactNode): string {
  return Children.toArray(children).map(child => {
    if (typeof child === 'string' || typeof child === 'number') return String(child);
    if (isValidElement<{ children?: ReactNode }>(child)) return textOf(child.props.children);
    return '';
  }).join('');
}

function Anchor({ href, children }: { href?: string; children?: ReactNode }) {
  if (href?.startsWith('#')) return <a href={href}>{children}</a>;
  if (!href) return <span className="nk-md-link">{children}</span>;
  if (textIsAddress(textOf(children), href)) return <span className="nk-md-link">{children}</span>;
  return (
    <span className="nk-md-link">
      {children}
      {' '}
      <span className="nk-md-address">({href})</span>
    </span>
  );
}

function Picture({ alt }: { alt?: string }) {
  return <span className="nk-md-image">{alt ? `[${alt}]` : '[image]'}</span>;
}

const COMPONENTS: Options['components'] = { a: Anchor, img: Picture };

export function MarkdownPage({ source }: { source: PageSource | null }) {
  if (!source) return <p className="nk-empty">This page has no text.</p>;
  // Checked before the parse: the page does not rely on the runtime.
  const tooLarge = sourceTooLarge(source);
  if (tooLarge) return <p className="nk-empty">{tooLarge}</p>;
  return (
    <div className="nk-md-page">
      {/* Floats at the top right of the frame; shown while the pointer is over the page (kit.css). */}
      <button
        type="button"
        className="nk-button nk-md-download"
        title="Download the Markdown file"
        onClick={() => postToHost({ type: 'download' })}
      >
        Download
      </button>
      <article className="nk-md">
        <ReactMarkdown remarkPlugins={REMARK_PLUGINS} rehypePlugins={REHYPE_PLUGINS} components={COMPONENTS}>
          {source.text}
        </ReactMarkdown>
      </article>
    </div>
  );
}
