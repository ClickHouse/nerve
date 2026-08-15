import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface Props {
  content: string;
  muted?: boolean;
}

/**
 * The eight `prose*` classes this used to carry — `prose prose-invert
 * prose-sm`, plus five `prose-<element>:` modifiers — generated no CSS at all:
 * `@tailwindcss/typography` is not installed, so every heading, link, table and
 * code block in a source message rendered as bare browser defaults. They are
 * replaced by `.markdown-content` (index.css), which is the app's own markdown
 * stylesheet and is what the chat transcript already uses — so a GitHub
 * notification and an agent message now render the same way.
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
