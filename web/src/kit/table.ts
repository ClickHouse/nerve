import { connectPage, postToHost } from './channel';
import { parseCsv, TableTooLarge } from './csv';
import { sourceTooLarge, type PageSource } from './source';

/**
 * The table page: the rows of a CSV source, with a search, a sort by each
 * column and the count of rows. Each value goes into the page as text
 * (`textContent`), never as markup.
 *
 * The download button sends `download` to the host: the page is in a frame
 * that cannot start a download, so the host downloads the source file of
 * the bundle (`data.csv`), the whole file.
 */

type Order = 'ascending' | 'descending' | 'none';

/**
 * The most rows that the page draws at one time. Search and sort work on all
 * rows of the data; "Show more rows" draws the next ones. A table at the
 * cell limit drawn in full costs seconds and gigabytes in the browser.
 */
export const DRAWN_ROWS = 500;

/**
 * A number as a table shows it: digits with an optional sign, currency
 * sign, thousands commas, decimals, exponent and a percent sign.
 */
const NUMBER = /^[+-]?[$€£]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?%?$/;

/** The number of a value, or null for a value that is not a number. */
export function numberOf(value: string): number | null {
  const text = value.trim();
  if (!NUMBER.test(text)) return null;
  const n = Number(text.replace(/[$€£,%]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function plural(n: number, word: string): string {
  return `${n.toLocaleString('en-US')} ${word}${n === 1 ? '' : 's'}`;
}

const number = (n: number) => n.toLocaleString('en-US');

/**
 * The count text: "214 rows"; "8 of 214 rows" while a search hides rows;
 * with rows that are not drawn yet, how many are shown of how many match:
 * "500 of 9,999 rows shown", "500 of 1,234 matching rows shown (9,999 rows)".
 */
export function countText(drawn: number, matching: number, total: number): string {
  if (drawn >= matching) return matching === total ? plural(total, 'row') : `${number(matching)} of ${plural(total, 'row')}`;
  if (matching === total) return `${number(drawn)} of ${plural(total, 'row')} shown`;
  return `${number(drawn)} of ${number(matching)} matching rows shown (${plural(total, 'row')})`;
}

function element<K extends keyof HTMLElementTagNameMap>(
  doc: Document, tag: K, className?: string, text?: string,
): HTMLElementTagNameMap[K] {
  const node = doc.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

export interface TablePage {
  /** The rows that match the search, in the order of the sort (drawn or not). */
  shownRows(): string[][];
  /** The rows that are drawn, in order. */
  drawnRows(): string[][];
  close(): void;
}

/** Draw the table of `source` into `root`, and connect the page to its host. */
export function startTable(root: HTMLElement, source: PageSource | null, win: Window = window): TablePage {
  const doc = root.ownerDocument;
  const connection = connectPage(() => ({ type: 'ready', kind: 'table', canPrint: false }), undefined, win);
  if (source) doc.title = source.name;
  root.replaceChildren();
  const tooLarge = source ? sourceTooLarge(source) : null;
  if (tooLarge) {
    root.append(element(doc, 'p', 'nk-empty', tooLarge));
    return { shownRows: () => [], drawnRows: () => [], close: () => connection.close() };
  }
  let all: string[][];
  try {
    all = source ? parseCsv(source.text) : [];
  } catch (e) {
    if (!(e instanceof TableTooLarge)) throw e;
    root.append(element(doc, 'p', 'nk-empty', e.message));
    return { shownRows: () => [], drawnRows: () => [], close: () => connection.close() };
  }
  if (all.length === 0) {
    root.append(element(doc, 'p', 'nk-empty', source ? 'The CSV file has no rows.' : 'This page has no table.'));
    return { shownRows: () => [], drawnRows: () => [], close: () => connection.close() };
  }

  const header = all[0];
  const body = all.slice(1);
  const width = Math.max(...all.map(row => row.length));
  const names = Array.from({ length: width }, (_, i) => header[i]?.trim() ? header[i] : `Column ${i + 1}`);
  const cells = body.map(row => Array.from({ length: width }, (_, i) => row[i] ?? ''));
  const numeric = names.map((_, i) => {
    const values = cells.map(row => row[i]).filter(v => v.trim() !== '');
    return values.length > 0 && values.every(v => numberOf(v) !== null);
  });
  const lower = cells.map(row => row.map(v => v.toLowerCase()));

  let query = '';
  let sortColumn = -1;
  let order: Order = 'none';
  let shown: number[] = [];
  let drawn = 0;
  // The numbers of a numeric column, made once when the column is sorted.
  const numbers = new Map<number, Array<number | null>>();
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

  const page = element(doc, 'div', 'nk-table');
  const bar = element(doc, 'div', 'nk-toolbar');
  const search = element(doc, 'input', 'nk-search');
  search.type = 'search';
  search.placeholder = `Search in ${plural(cells.length, 'row')}`;
  search.setAttribute('aria-label', 'Search the table');
  const count = element(doc, 'span', 'nk-count');
  count.setAttribute('aria-live', 'polite');
  const download = element(doc, 'button', 'nk-button', 'Download CSV');
  download.type = 'button';
  download.title = 'Download the whole CSV file';
  download.addEventListener('click', () => postToHost({ type: 'download' }, win));
  bar.append(search, count, download);

  const scroll = element(doc, 'div', 'nk-scroll');
  const table = element(doc, 'table');
  if (source) table.setAttribute('aria-label', source.name);
  const head = element(doc, 'thead');
  const headRow = element(doc, 'tr');
  const headers = names.map((name, i) => {
    const th = element(doc, 'th');
    th.scope = 'col';
    th.setAttribute('aria-sort', 'none');
    if (numeric[i]) th.classList.add('nk-number');
    const button = element(doc, 'button', 'nk-sort', name);
    // The sort arrow is a picture of aria-sort: not part of the name.
    const mark = element(doc, 'span', 'nk-sort-mark');
    mark.setAttribute('aria-hidden', 'true');
    button.append(mark);
    button.type = 'button';
    button.title = `Sort by ${name}`;
    button.addEventListener('click', () => sortBy(i));
    th.append(button);
    headRow.append(th);
    return th;
  });
  head.append(headRow);
  const tbody = element(doc, 'tbody');
  table.append(head, tbody);
  scroll.append(table);
  const more = element(doc, 'button', 'nk-button nk-more');
  more.type = 'button';
  more.addEventListener('click', () => drawMore());
  page.append(bar, scroll, more);
  root.append(page);

  function compare(a: number, b: number): number {
    const x = cells[a][sortColumn];
    const y = cells[b][sortColumn];
    let result: number;
    if (numeric[sortColumn]) {
      // Empty values go last in both orders.
      const values = numbers.get(sortColumn)!;
      const nx = values[a];
      const ny = values[b];
      if (nx === null || ny === null) return nx === ny ? a - b : nx === null ? 1 : -1;
      result = nx - ny;
    } else {
      result = collator.compare(x, y);
    }
    if (order === 'descending') result = -result;
    return result || a - b;
  }

  function draw(): void {
    const words = query.toLowerCase().split(/\s+/).filter(Boolean);
    shown = [];
    for (let i = 0; i < cells.length; i++) {
      if (words.every(word => lower[i].some(value => value.includes(word)))) shown.push(i);
    }
    if (order !== 'none' && sortColumn >= 0) {
      if (numeric[sortColumn] && !numbers.has(sortColumn)) numbers.set(sortColumn, cells.map(row => numberOf(row[sortColumn])));
      shown.sort(compare);
    }
    tbody.replaceChildren();
    drawn = 0;
    drawMore();
    headers.forEach((th, i) => th.setAttribute('aria-sort', i === sortColumn ? order : 'none'));
  }

  /** Draw the next DRAWN_ROWS rows of the matching rows. */
  function drawMore(): void {
    const rows = doc.createDocumentFragment();
    const end = Math.min(drawn + DRAWN_ROWS, shown.length);
    for (let k = drawn; k < end; k++) {
      const tr = element(doc, 'tr');
      cells[shown[k]].forEach((value, i) => {
        tr.append(element(doc, 'td', numeric[i] ? 'nk-number' : undefined, value));
      });
      rows.append(tr);
    }
    tbody.append(rows);
    drawn = end;
    count.textContent = countText(drawn, shown.length, cells.length);
    const left = shown.length - drawn;
    more.hidden = left <= 0;
    more.textContent = `Show ${number(Math.min(DRAWN_ROWS, left))} more rows`;
  }

  function sortBy(column: number): void {
    if (column !== sortColumn) {
      sortColumn = column;
      order = 'ascending';
    } else {
      order = order === 'ascending' ? 'descending' : order === 'descending' ? 'none' : 'ascending';
    }
    draw();
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  search.addEventListener('input', () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      query = search.value;
      draw();
    }, cells.length > 2000 ? 150 : 0);
  });

  draw();
  return {
    shownRows: () => shown.map(i => cells[i]),
    drawnRows: () => shown.slice(0, drawn).map(i => cells[i]),
    close: () => {
      clearTimeout(timer);
      connection.close();
    },
  };
}
