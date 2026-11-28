import { describe, expect, it } from 'vitest';
import cases from './csv-cases.json';
import { MAX_TABLE_CELLS, MAX_TABLE_COLUMNS, MAX_TABLE_ROWS, parseCsv, TableTooLarge } from './csv';

describe('parseCsv', () => {
  it('reads fields, quotes, line breaks in quotes and doubled quotes', () => {
    expect(parseCsv('a,b,c\n1,"x, y","say ""hi"""\r\n2,"two\nlines",\n')).toEqual([
      ['a', 'b', 'c'],
      ['1', 'x, y', 'say "hi"'],
      ['2', 'two\nlines', ''],
    ]);
  });

  it('leaves out a byte order mark and empty lines', () => {
    expect(parseCsv('﻿name\n\nalice\r\rbob')).toEqual([['name'], ['alice'], ['bob']]);
  });

  it('keeps a row of one empty quoted field', () => {
    expect(parseCsv('a\n""\n')).toEqual([['a'], ['']]);
  });

  it('has no rows for an empty text', () => {
    expect(parseCsv('')).toEqual([]);
  });
});

describe('the counts of the runtime', () => {
  // The same cases as tests/test_artifact_kit.py: the runtime counts rows
  // and columns with its own scanner, and both must agree.
  it.each(cases)('$name: $rows rows, $width columns', ({ text, rows, width }) => {
    const parsed = parseCsv(text);
    expect(parsed.length).toBe(rows);
    expect(Math.max(0, ...parsed.map(row => row.length))).toBe(width);
  });
});

describe('the limits of a table page', () => {
  it('refuses a row wider than the column limit before it reads on', () => {
    const header = Array.from({ length: 5000 }, (_, i) => `c${i}`).join(',');
    const text = `${header}\n${'x\n'.repeat(9999)}`;
    expect(() => parseCsv(text)).toThrow(TableTooLarge);
    expect(() => parseCsv(text)).toThrow(`more than ${MAX_TABLE_COLUMNS} columns`);
  });

  it('refuses more rows than the row limit', () => {
    expect(() => parseCsv(`h\n${'1\n'.repeat(MAX_TABLE_ROWS + 1)}`)).toThrow(TableTooLarge);
    expect(parseCsv(`h\n${'1\n'.repeat(MAX_TABLE_ROWS)}`)).toHaveLength(MAX_TABLE_ROWS + 1);
  });

  it('refuses more cells than the cell limit', () => {
    const width = 100;
    const line = Array(width).fill('1').join(',');
    const rows = MAX_TABLE_CELLS / width;
    expect(parseCsv(Array(rows).fill(line).join('\n'))).toHaveLength(rows);
    expect(() => parseCsv(Array(rows + 1).fill(line).join('\n'))).toThrow(/cells/);
  });
});
