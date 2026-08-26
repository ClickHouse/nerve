import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface Props {
  content: string;
  muted?: boolean;
}

/**
 * Styled by `.markdown-content` (index.css), the app's own markdown
 * stylesheet, which the chat transcript uses too — so a GitHub notification
 * and an agent message render the same way. `prose*` classes are not an
 * option here: `@tailwindcss/typography` is not installed, so they generate no
 * CSS at all.
 *
 * The base text colour stays on this element: `.markdown-content` colours
 * links, blockquotes and code, but inherits the body colour, which is what
 * `muted` is for.
 */
export function MarkdownRenderer({ content, muted = false }: Props) {
  return (
    <div className={`markdown-content ${muted ? 'text-text-muted' : 'text-text-secondary'}`}>
      <ReactMarkdown remarkPlugins={[remarkGfm]}>
        {content || '*(empty)*'}
      </ReactMarkdown>
    </div>
  );
}
