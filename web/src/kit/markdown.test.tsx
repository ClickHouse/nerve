import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { textIsAddress } from './links';
import { MarkdownPage } from './markdown';
import { MAX_SOURCE_BYTES } from './source';
import css from './kit.css?raw';

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

function show(text: string) {
  return render(<MarkdownPage source={{ name: 'report.md', text }} />);
}

describe('the Markdown page', () => {
  it('draws Markdown with GitHub tables and code colors', () => {
    const { container } = show('# Report\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n```js\nconst x = 1;\n```\n\n- [x] done');
    expect(screen.getByRole('heading', { level: 1, name: 'Report' })).toBeInTheDocument();
    expect(screen.getByRole('table')).toBeInTheDocument();
    expect(container.querySelector('code.hljs .hljs-keyword')).toHaveTextContent('const');
    expect(screen.getByRole('checkbox')).toBeChecked();
  });

  it('runs no raw HTML of the source and shows no image', () => {
    const { container } = show('Hi <script>window.bad = 1</script> <img src=x onerror="window.bad=2"> <iframe src="https://example.com"></iframe>\n\n![chart](https://example.com/c.png)');
    expect(container.querySelector('article script, article img, article iframe')).toBeNull();
    expect(screen.getByText('[chart]')).toBeInTheDocument();
    expect((window as unknown as { bad?: number }).bad).toBeUndefined();
  });

  it('shows a diagram or chart block as code', () => {
    const { container } = show('```mermaid\ngraph TD; A-->B\n```\n\n```vega-lite\n{"mark": "bar"}\n```');
    const blocks = Array.from(container.querySelectorAll('pre code')).map(c => c.textContent);
    expect(blocks).toEqual(['graph TD; A-->B\n', '{"mark": "bar"}\n']);
    expect(container.querySelector('svg')).toBeNull();
  });

  it('shows a link as its text and its address, as plain text', () => {
    const { container } = show(
      '[the report](https://example.org/page) and https://example.org/plain and [bad](javascript:alert(1))'
      + ' and [**bold** words](https://example.org/b) and [part](#notes) and <https://example.org/x>',
    );
    const article = container.querySelector('article')!;
    expect(article.textContent).toContain('the report (https://example.org/page)');
    expect(article.textContent).toContain('bold words (https://example.org/b)');
    expect(article.textContent).toContain('and https://example.org/plain and');
    expect(article.textContent).not.toContain('https://example.org/plain (');
    expect(article.textContent).not.toContain('https://example.org/x (');
    expect(article.textContent).not.toMatch(/javascript/i);
    expect(Array.from(article.querySelectorAll('a')).map(a => a.getAttribute('href'))).toEqual(['#notes']);
  });

  it('knows when the text of a link is its address', () => {
    expect(textIsAddress('www.example.org', 'http://www.example.org')).toBe(true);
    expect(textIsAddress('alice@example.org', 'mailto:alice@example.org')).toBe(true);
    expect(textIsAddress('site', 'https://example.org')).toBe(false);
  });

  it('refuses a source one byte over 5 MiB, also of characters of several bytes, before it parses', () => {
    for (const text of ['#'.repeat(MAX_SOURCE_BYTES + 1), `${'€'.repeat((MAX_SOURCE_BYTES - 2) / 3)}xyz`]) {
      const { container, unmount } = show(text);
      expect(container.querySelector('article')).toBeNull();
      expect(container.textContent).toBe('The file is larger than 5 MiB. A page shows at most that.');
      unmount();
    }
  });

  it('asks the host for the download and starts none itself', async () => {
    const post = vi.fn();
    const parent = { postMessage: post };
    vi.spyOn(window, 'parent', 'get').mockReturnValue(parent as unknown as Window);
    show('text');
    const button = screen.getByRole('button', { name: 'Download' });
    expect(button).toHaveAttribute('title', 'Download the Markdown file');
    await userEvent.click(button);
    expect(post).toHaveBeenCalledWith({ channel: 'nerve-artifact', v: 1, type: 'download' }, '*');
    expect(document.querySelector('a[download]')).toBeNull();
  });

  it('floats the download button over the page, shown on hover, on focus and on a touch screen', () => {
    show('# Report');
    const button = screen.getByRole('button', { name: 'Download' });
    expect(button).toHaveClass('nk-md-download');
    // No row of its own above the text.
    expect(button.closest('.nk-toolbar')).toBeNull();
    expect(button.nextElementSibling).toHaveClass('nk-md');
    const rule = /\.nk-md-download \{([^}]*)\}/.exec(css)?.[1] ?? '';
    expect(rule).toMatch(/position: fixed;/);
    expect(rule).toMatch(/opacity: 0;/);
    expect(css).toContain(':root:hover .nk-md-download, .nk-md-download:focus-visible { opacity: 1; }');
    expect(css).toContain('@media (hover: none) { .nk-md-download { opacity: 1; } }');
    expect(css).toMatch(/@media print \{[^@]*\.nk-md-download[^}]*display: none;/);
  });
});
