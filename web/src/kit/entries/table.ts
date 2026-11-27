import { startTable } from '../table';
import { pageRoot, readSource, whenParsed } from '../source';

whenParsed(() => {
  startTable(pageRoot(), readSource());
});
