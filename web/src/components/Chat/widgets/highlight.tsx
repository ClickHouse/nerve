import type { ReactNode } from 'react';

/**
 * The source of a widget, highlighted as HTML, for the code block that
 * shows it (`WidgetSource.tsx`).
 *
 * The highlighter's result is HTML text. It never reaches the page as HTML:
 * it is parsed into a document of its own, which runs and loads nothing, and
 * only the `<span>` elements with the highlighter's own class names and
 * their text are rebuilt, as elements of this page. A result that does not
 * read as the source, character for character, is not used at all.
 */

/** Above this many characters the source is shown plain: highlighting it would stall the page. */
const MAX_HIGHLIGHT_CHARS = 60_000;

type Highlight = (source: string) => string;

// The highlighter loads the first time a widget's source is shown.
let highlighter: Promise<Highlight> | null = null;
function loadHighlighter(): Promise<Highlight> {
  highlighter ??= Promise.all([
    import('highlight.js/lib/core'),
    import('highlight.js/lib/languages/xml'),
    import('highlight.js/lib/languages/css'),
    import('highlight.js/lib/languages/javascript'),
  ]).then(([core, xml, css, javascript]) => {
    // An instance of its own: the languages it knows are nobody else's business.
    const hljs = core.default.newInstance();
    hljs.registerLanguage('xml', xml.default);
    hljs.registerLanguage('css', css.default);
    hljs.registerLanguage('javascript', javascript.default);
    return (source: string) => hljs.highlight(source, { language: 'xml', ignoreIllegals: true }).value;
  }).catch((error: unknown) => {
    highlighter = null;
    throw error;
  });
  return highlighter;
}

const TEXT_NODE = 3;
const ELEMENT_NODE = 1;
/** The class names the highlighter gives: `hljs-tag`, `language-javascript`. */
const HIGHLIGHT_CLASS = /^(hljs|language)-[\w-]+$/;

/** The children of `parent` as nodes of this page: spans of the highlighter and text. Anything else is its text. */
function rebuild(parent: Node, path: string): ReactNode[] {
  return Array.from(parent.childNodes, (node, index) => {
    if (node.nodeType === TEXT_NODE) return node.nodeValue;
    if (node.nodeType === ELEMENT_NODE && (node as Element).localName === 'span') {
      const key = `${path}${index}`;
      const classes = ((node as Element).getAttribute('class') ?? '').split(/\s+/).filter(name => HIGHLIGHT_CLASS.test(name));
      return <span key={key} className={classes.join(' ')}>{rebuild(node, `${key}.`)}</span>;
    }
    return node.textContent;
  });
}

/** `source` highlighted as HTML, as nodes of this page; null when it cannot be (then it is shown plain). */
export async function highlightWidgetSource(source: string): Promise<ReactNode[] | null> {
  if (source.length > MAX_HIGHLIGHT_CHARS || typeof DOMParser === 'undefined') return null;
  try {
    const highlight = await loadHighlighter();
    const parsed = new DOMParser().parseFromString(`<body><div>${highlight(source)}</div></body>`, 'text/html');
    const holder = parsed.body.firstElementChild;
    // The highlighter adds no text and takes none away (the parser reads
    // every line end as a line feed).
    return holder && holder.textContent === source.replace(/\r\n?/g, '\n') ? rebuild(holder, '') : null;
  } catch {
    return null;
  }
}
