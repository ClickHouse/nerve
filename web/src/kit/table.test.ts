import { afterEach, describe, expect, it, vi } from 'vitest';
import { countText, DRAWN_ROWS, numberOf, startTable } from './table';
import { MAX_SOURCE_BYTES } from './source';
import { testWindow } from './testWindow';

const CSV = [
  'Service,Region,Cost USD',
  'ingest-prod-eu,eu-west-1,"148,310"',
  'query-prod-us,us-east-2,108940',
  'sandbox,us-west-2,1580',
  '<img src=x onerror=alert(1)>,<b>bold</b>,12',
].join('\n');

function setup(text = CSV) {
  const t = testWindow();
  const root = document.createElement('main');
  document.body.append(root);
  const page = startTable(root, { name: 'cost.csv', text }, t.win);
  const rows = () => Array.from(root.querySelectorAll('tbody tr')).map(tr => Array.from(tr.children).map(td => td.textContent));
  const header = (name: string) => root.querySelector<HTMLButtonElement>(`th button[title="Sort by ${name}"]`)!;
  const search = root.querySelector<HTMLInputElement>('input[type="search"]')!;
  const count = () => root.querySelector('[aria-live="polite"]')!.textContent;
  return { t, root, page, rows, header, search, count };
}

async function type(input: HTMLInputElement, value: string) {
  input.value = value;
  input.dispatchEvent(new Event('input'));
  await vi.advanceTimersByTimeAsync(200);
}

afterEach(() => {
  vi.useRealTimers();
  document.body.innerHTML = '';
});

describe('the table page', () => {
  it('shows each row, the header and the count, and says ready', () => {
    const { t, root, rows, count } = setup();
    expect(Array.from(root.querySelectorAll('th')).map(th => th.textContent)).toEqual(['Service', 'Region', 'Cost USD']);
    expect(rows()).toHaveLength(4);
    expect(count()).toBe('4 rows');
    expect(t.of('ready')).toEqual([{ channel: 'nerve-artifact', v: 1, type: 'ready', kind: 'table', canPrint: false }]);
    expect(root.querySelector('input')!.placeholder).toBe('Search in 4 rows');
  });

  it('shows values as text, never as markup', () => {
    const { root, rows } = setup();
    expect(root.querySelector('tbody img, tbody b')).toBeNull();
    expect(rows()[3]).toEqual(['<img src=x onerror=alert(1)>', '<b>bold</b>', '12']);
  });

  it('searches in all columns and says how many rows it shows', async () => {
    vi.useFakeTimers();
    const { rows, search, count } = setup();
    await type(search, 'US-');
    expect(rows().map(r => r[0])).toEqual(['query-prod-us', 'sandbox']);
    expect(count()).toBe('2 of 4 rows');
    await type(search, 'nothing matches');
    expect(rows()).toEqual([]);
    expect(count()).toBe('0 of 4 rows');
    await type(search, '');
    expect(count()).toBe('4 rows');
  });

  it('sorts a number column by value, then the other way, then as in the file', () => {
    const { root, rows, header } = setup();
    const th = () => header('Cost USD').closest('th')!;
    header('Cost USD').click();
    expect(rows().map(r => r[2])).toEqual(['12', '1580', '108940', '148,310']);
    expect(th()).toHaveAttribute('aria-sort', 'ascending');
    header('Cost USD').click();
    expect(rows().map(r => r[2])).toEqual(['148,310', '108940', '1580', '12']);
    expect(th()).toHaveAttribute('aria-sort', 'descending');
    header('Cost USD').click();
    expect(rows().map(r => r[0])[0]).toBe('ingest-prod-eu');
    expect(th()).toHaveAttribute('aria-sort', 'none');
    expect(root.querySelector('td.nk-number')).not.toBeNull();
  });

  it('sorts a text column as text', () => {
    const { rows, header } = setup();
    header('Service').click();
    expect(rows().map(r => r[0])).toEqual(['<img src=x onerror=alert(1)>', 'ingest-prod-eu', 'query-prod-us', 'sandbox']);
  });

  it('asks the host for the download and starts none itself', () => {
    const createObjectURL = vi.fn();
    Object.defineProperty(URL, 'createObjectURL', { value: createObjectURL, configurable: true });
    const { t, root } = setup();
    const button = Array.from(root.querySelectorAll('button')).find(b => b.textContent === 'Download CSV')!;
    expect(button.title).toBe('Download the whole CSV file');
    const clicks = vi.fn();
    document.addEventListener('click', event => {
      if ((event.target as Element).closest('a')) clicks();
    });
    button.click();
    expect(t.of('download')).toEqual([{ channel: 'nerve-artifact', v: 1, type: 'download' }]);
    expect(createObjectURL).not.toHaveBeenCalled();
    expect(clicks).not.toHaveBeenCalled();
    expect(document.querySelector('a[download]')).toBeNull();
  });

  it('says so for a table larger than the page shows, and draws no cell', () => {
    const header = Array.from({ length: 5000 }, (_, i) => `c${i}`).join(',');
    const { root } = setup(`${header}\n${'x\n'.repeat(9999)}`);
    expect(root.querySelector('table, td')).toBeNull();
    expect(root.textContent).toContain('more than 200 columns');
  });

  it('takes a source of exactly 5 MiB and refuses one byte more, before it parses', () => {
    const exact = `a\n${'€'.repeat((MAX_SOURCE_BYTES - 2) / 3)}`;
    expect(setup(exact).rows()).toHaveLength(1);
    document.body.innerHTML = '';
    const { root } = setup(`${exact}x`);
    expect(root.querySelector('table')).toBeNull();
    expect(root.textContent).toBe('The file is larger than 5 MiB. A page shows at most that.');
  });

  it('draws at most DRAWN_ROWS rows, and the next ones on request', async () => {
    vi.useFakeTimers();
    const rows = Array.from({ length: 1200 }, (_, i) => `row-${i},${(i * 7919) % 1200}`);
    const { root, rows: drawn, search, count, header, page } = setup(`name,value\n${rows.join('\n')}`);
    const more = () => Array.from(root.querySelectorAll('button')).find(b => b.textContent?.startsWith('Show') && !b.hidden);
    expect(drawn()).toHaveLength(DRAWN_ROWS);
    expect(count()).toBe('500 of 1,200 rows shown');
    expect(more()!.textContent).toBe('Show 500 more rows');
    more()!.click();
    expect(drawn()).toHaveLength(1000);
    more()!.click();
    expect(drawn()).toHaveLength(1200);
    expect(count()).toBe('1,200 rows');
    expect(more()).toBeUndefined();

    // The search looks at all rows, also the ones that were never drawn.
    await type(search, 'row-1199');
    expect(drawn().map(r => r[0])).toEqual(['row-1199']);
    expect(count()).toBe('1 of 1,200 rows');
    await type(search, '1');
    const matching = rows.filter(r => r.includes('1')).length;
    expect(matching).toBeGreaterThan(DRAWN_ROWS);
    expect(matching).toBeLessThan(1200);
    expect(drawn()).toHaveLength(DRAWN_ROWS);
    expect(count()).toBe(`500 of ${matching.toLocaleString('en-US')} matching rows shown (1,200 rows)`);
    await type(search, '');

    // The sort orders all rows: the first drawn row is the smallest of all.
    header('value').click();
    expect(page.shownRows().map(r => Number(r[1]))).toEqual([...Array(1200).keys()]);
    expect(drawn()[0][1]).toBe('0');
    expect(drawn()).toHaveLength(DRAWN_ROWS);
    header('value').click();
    expect(drawn()[0][1]).toBe('1199');
  });

  it('says so for a file without rows', () => {
    const { root } = setup('');
    expect(root.textContent).toBe('The CSV file has no rows.');
  });

  it('pads short rows and names columns without a header', () => {
    const { root, rows } = setup('a\n1,2\n3');
    expect(Array.from(root.querySelectorAll('th')).map(th => th.textContent)).toEqual(['a', 'Column 2']);
    expect(rows()).toEqual([['1', '2'], ['3', '']]);
  });
});

describe('helpers', () => {
  it.each([
    ['41,280', 41280], ['-12.4%', -12.4], ['+3.1%', 3.1], ['$1,580', 1580], ['1e3', 1000], ['0.5', 0.5],
    ['eu-west-1', null], ['12,34', null], ['', null], ['1.2.3', null],
  ])('numberOf(%j) is %j', (value, n) => {
    expect(numberOf(value)).toBe(n);
  });

  it('counts rows', () => {
    expect(countText(1, 1, 1)).toBe('1 row');
    expect(countText(8, 8, 214)).toBe('8 of 214 rows');
    expect(countText(9000, 9000, 9000)).toBe('9,000 rows');
    expect(countText(500, 9999, 9999)).toBe('500 of 9,999 rows shown');
    expect(countText(500, 1234, 9999)).toBe('500 of 1,234 matching rows shown (9,999 rows)');
  });
});
