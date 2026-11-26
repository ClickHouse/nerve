// page.js (shareable-html skill): fills the page from the JSON data island
// (the script element with id "data"). Vanilla JS, no network.
(() => {
  'use strict';
  const D = JSON.parse(document.getElementById('data').textContent);
  const meta = D.meta || {};
  const lang = document.documentElement.lang || 'en';
  const nf = new Intl.NumberFormat(lang, { maximumFractionDigits: 1 });
  const pf = new Intl.NumberFormat(lang, { style: 'percent', maximumFractionDigits: 1 });
  const df = new Intl.DateTimeFormat(lang, { year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC' });
  const fixed = {}; // "digits" in a column or a key figure: always that many decimals
  const nfd = (d, style) => (fixed[style + d] ||= new Intl.NumberFormat(lang,
    { style, minimumFractionDigits: d, maximumFractionDigits: d }));
  const fmt = (v, type, digits) => {
    if (v === null || v === undefined || v === '') return '';
    if (type === 'number') return (digits === undefined ? nf : nfd(digits, 'decimal')).format(v);
    if (type === 'percent') return (digits === undefined ? pf : nfd(digits, 'percent')).format(v);
    if (type === 'date') return df.format(new Date(String(v).slice(0, 10) + 'T00:00:00Z'));
    return String(v);
  };
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  };

  // Header: date and source come from the data, so they always match it.
  for (const n of document.querySelectorAll('[data-meta]')) {
    const key = n.dataset.meta;
    if (key === 'source' && meta.source_url) {
      const a = el('a', '', meta.source || meta.source_url);
      a.href = meta.source_url;
      n.replaceChildren(a);
    } else if (meta[key] !== undefined) {
      n.textContent = key === 'as_of' ? fmt(meta[key], 'date') : meta[key];
    }
  }
  if (meta.example) {
    document.querySelector('header').after(el('p', 'pg-warn',
      'EXAMPLE DATA: replace page-data.json with real data before you share this page.'));
  }

  // Key figures: value with its unit, the period, and the change with a sign.
  const kpis = document.getElementById('kpis');
  for (const k of D.kpis || []) {
    const box = el('div', 'pg-kpi');
    box.append(el('div', 'pg-kpi-label', k.label));
    const val = el('div', 'pg-kpi-value', fmt(k.value, k.type || 'number', k.digits));
    if (k.unit) val.append(el('span', 'pg-kpi-unit', k.unit));
    box.append(val);
    const note = el('div', 'pg-kpi-note', k.period || '');
    if (typeof k.change === 'number') {
      const good = (k.change >= 0) === (k.better !== 'down');
      const sign = k.change > 0 ? '+' : k.change < 0 ? '−' : '';
      const size = k.change_unit ? `${nf.format(Math.abs(k.change))} ${k.change_unit}` : pf.format(Math.abs(k.change));
      const ch = el('span', k.change === 0 ? '' : good ? 'pg-good' : 'pg-bad',
        `${sign}${size} ${k.change_label || ''}`.trim());
      if (note.textContent) note.append(document.createTextNode(' · '));
      note.append(ch);
    }
    box.append(note);
    kpis.append(box);
  }

  // Table: click a header to sort, type in the filter to narrow the rows.
  const table = document.getElementById('table');
  if (!table || !D.columns) return;
  const cols = D.columns;
  const rows = (D.rows || []).slice();
  const numeric = (c) => c.type === 'number' || c.type === 'percent';
  const headRow = el('tr');
  cols.forEach((c, i) => {
    const th = el('th', numeric(c) ? 'num' : '');
    th.setAttribute('aria-sort', 'none');
    const b = el('button', '', c.label + (c.unit ? ` (${c.unit})` : ''));
    b.type = 'button';
    b.addEventListener('click', () => sortBy(i, th));
    th.append(b);
    headRow.append(th);
  });
  table.tHead.append(headRow);
  const body = table.tBodies[0];
  const filter = document.getElementById('filter');
  const count = document.getElementById('rowcount');
  let sortCol = -1, dir = 1;

  function draw() {
    const q = (filter ? filter.value : '').trim().toLowerCase();
    const shown = rows.filter((r) => !q || cols.some((c) => fmt(r[c.key], c.type, c.digits).toLowerCase().includes(q)));
    body.replaceChildren(...shown.map((r) => {
      const tr = el('tr');
      for (const c of cols) tr.append(el('td', numeric(c) ? 'num' : '', fmt(r[c.key], c.type, c.digits)));
      return tr;
    }));
    if (count) count.textContent = `${shown.length} of ${rows.length} rows`;
  }
  function sortBy(i, th) {
    dir = sortCol === i ? -dir : (numeric(cols[i]) ? -1 : 1);
    sortCol = i;
    const k = cols[i].key;
    rows.sort((a, b) => {
      const x = a[k], y = b[k];
      if (x === y) return 0;
      if (x === null || x === undefined) return 1;
      if (y === null || y === undefined) return -1;
      return (typeof x === 'number' ? x - y : String(x).localeCompare(String(y), lang)) * dir;
    });
    for (const h of table.tHead.rows[0].cells) h.setAttribute('aria-sort', 'none');
    th.setAttribute('aria-sort', dir > 0 ? 'ascending' : 'descending');
    draw();
  }
  if (filter) filter.addEventListener('input', draw);
  draw();
})();
