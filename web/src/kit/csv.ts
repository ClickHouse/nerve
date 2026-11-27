/**
 * The limits of a table page. The runtime checks the same before it makes
 * the page (`nerve/artifacts/bundle.py`): rows after the header row,
 * columns of the widest row, and cells (all rows by the widest row).
 */
export const MAX_TABLE_ROWS = 10_000;
export const MAX_TABLE_COLUMNS = 200;
export const MAX_TABLE_CELLS = 200_000;

/** A CSV text that is larger than a table page shows. */
export class TableTooLarge extends Error {}

const LIMITS_TEXT = `A table page shows at most ${MAX_TABLE_ROWS.toLocaleString('en-US')} rows, `
  + `${MAX_TABLE_COLUMNS} columns and ${MAX_TABLE_CELLS.toLocaleString('en-US')} cells.`;

/**
 * Rows of a CSV text (RFC 4180): fields between commas, a field in double
 * quotes can hold commas, line breaks and `""` for one quote. A line break
 * is `\n`, `\r\n` or `\r`. An empty line is no row, as for the row limit of
 * the runtime (`nerve/artifacts/bundle.py`). A byte order mark at the start
 * is left out. The parse stops with `TableTooLarge` at the first row wider
 * than `MAX_TABLE_COLUMNS` or past `MAX_TABLE_ROWS` rows after the header,
 * and the rows by the widest row must be at most `MAX_TABLE_CELLS`.
 */
export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = '';
  let quoted = false;
  // The current line has a character: an empty line is no row.
  let started = false;
  let i = text.charCodeAt(0) === 0xfeff ? 1 : 0;
  let width = 0;
  const endRow = () => {
    if (started) {
      row.push(field);
      rows.push(row);
      width = Math.max(width, row.length);
      if (rows.length - 1 > MAX_TABLE_ROWS) {
        throw new TableTooLarge(`The CSV file has more than ${MAX_TABLE_ROWS.toLocaleString('en-US')} rows. ${LIMITS_TEXT}`);
      }
    }
    row = [];
    field = '';
    started = false;
  };
  for (; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          quoted = false;
        }
      } else {
        field += c;
      }
      continue;
    }
    if (c === '"' && field === '') {
      quoted = true;
      started = true;
    } else if (c === ',') {
      row.push(field);
      field = '';
      started = true;
      if (row.length + 1 > MAX_TABLE_COLUMNS) {
        throw new TableTooLarge(`A row of the CSV file has more than ${MAX_TABLE_COLUMNS} columns. ${LIMITS_TEXT}`);
      }
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      endRow();
    } else {
      field += c;
      started = true;
    }
  }
  endRow();
  if (rows.length * width > MAX_TABLE_CELLS) {
    throw new TableTooLarge(`The CSV file has ${(rows.length * width).toLocaleString('en-US')} cells. ${LIMITS_TEXT}`);
  }
  return rows;
}
