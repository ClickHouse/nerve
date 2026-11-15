import { createContext } from 'react';

/**
 * True inside markdown whose text is still streaming in. Read by the blocks
 * that render their source (diagrams, charts) to wait for the text to settle
 * and to keep quiet about a parse error that is only the stream being
 * mid-sentence. Its own module so that those blocks need not import the
 * markdown component that renders them.
 */
export const MarkdownStreamingContext = createContext(false);
