import { startDeck } from '../deck';
import { whenParsed } from '../source';

whenParsed(() => {
  startDeck();
});
