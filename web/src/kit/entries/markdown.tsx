import { createRoot } from 'react-dom/client';
import { connectPage } from '../channel';
import { MarkdownPage } from '../markdown';
import { pageRoot, readSource, whenParsed } from '../source';

whenParsed(() => {
  const source = readSource();
  if (source) document.title = source.name;
  createRoot(pageRoot()).render(<MarkdownPage source={source} />);
  connectPage(() => ({ type: 'ready', kind: 'markdown', canPrint: true }));
});
