import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { InlineMarkdown, MarkdownContent } from './MarkdownContent';

// The real diagram renderer measures text in a browser and is a few
// megabytes; the routing under test only needs to see which block got which
// source.
const { mountMermaid } = vi.hoisted(() => ({
  mountMermaid: vi.fn<(stage: HTMLElement, source: string, theme: string) => Promise<() => void>>(),
}));
vi.mock('./rich/mermaid', () => ({ mountMermaid }));

beforeEach(() => {
  mountMermaid.mockReset();
  mountMermaid.mockImplementation(async (stage, source) => {
    stage.innerHTML = `<svg><text>${source}</text></svg>`;
    return () => { stage.innerHTML = ''; };
  });
});

const NOTE = '- Plan proposed: [plan-1](/plans/plan-1) (v1)\n- See [the docs](https://example.com/docs)';

describe('MarkdownContent links', () => {
  it('keeps app links in the tab and opens other links in a new one', () => {
    render(<MemoryRouter><MarkdownContent content={NOTE} /></MemoryRouter>);
    const plan = screen.getByRole('link', { name: 'plan-1' });
    expect(plan).toHaveAttribute('href', '/plans/plan-1');
    expect(plan).not.toHaveAttribute('target');
    expect(screen.getByRole('link', { name: 'the docs' })).toHaveAttribute('target', '_blank');
  });

  it('renders app links as plain links outside a router', () => {
    render(<MarkdownContent content={NOTE} />);
    expect(screen.getByRole('link', { name: 'plan-1' })).toHaveAttribute('href', '/plans/plan-1');
  });

  it('opens a server URL in a new tab', () => {
    render(<MemoryRouter><MarkdownContent content="[log](/api/files/download?path=a.log)" /></MemoryRouter>);
    expect(screen.getByRole('link', { name: 'log' })).toHaveAttribute('target', '_blank');
  });

  it('does not keep a protocol-relative link in the tab', () => {
    render(<MemoryRouter><MarkdownContent content="[x](//example.com/x)" /></MemoryRouter>);
    expect(screen.getByRole('link', { name: 'x' })).toHaveAttribute('target', '_blank');
  });

  it('drops a script link', () => {
    render(<MemoryRouter><MarkdownContent content="[run](javascript:alert(1))" /></MemoryRouter>);
    expect(screen.getByText('run').closest('a')).not.toHaveAttribute('href', expect.stringContaining('javascript'));
  });
});

describe('InlineMarkdown', () => {
  it('renders code, emphasis and links with no block around them', () => {
    const { container } = render(
      <p>Should <InlineMarkdown content="`alice` post **drafts** to [org/repo-a](https://example.com/org/repo-a)?" /></p>,
    );
    expect(screen.getByText('alice').tagName).toBe('CODE');
    expect(screen.getByText('drafts').tagName).toBe('STRONG');
    expect(screen.getByRole('link', { name: 'org/repo-a' })).toHaveAttribute('target', '_blank');
    expect(container.querySelector('p p, p div')).toBeNull();
  });

  it('keeps the text of a block without the block', () => {
    const { container } = render(<InlineMarkdown content={'# Heading\n\n- one item'} />);
    expect(container.querySelector('h1, ul, li')).toBeNull();
    expect(container).toHaveTextContent('Heading');
    expect(container).toHaveTextContent('one item');
  });

  it('keeps only a link\'s text where links are off', () => {
    render(<InlineMarkdown content="See [the docs](https://example.com/docs)" links={false} />);
    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.getByText(/the docs/)).toBeInTheDocument();
  });
});

const fence = (lang: string, body: string) => `Intro\n\n\`\`\`${lang}\n${body}\n\`\`\`\n\nOutro`;

describe('MarkdownContent rendered code blocks', () => {
  it('renders a mermaid fence as a diagram with its source one click away', async () => {
    render(<MarkdownContent content={fence('mermaid', 'graph TD\n  A-->B')} />);
    const figure = await screen.findByRole('img', { name: 'Rendered mermaid' });
    expect(figure.querySelector('svg')).not.toBeNull();
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    // The fence's text, without the newline markdown ends a code block with.
    expect(mountMermaid.mock.calls[0][1]).toBe('graph TD\n  A-->B');
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.getByText('Intro')).toBeInTheDocument();
    expect(screen.getByText('Outro')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Source' }));
    expect(figure.hidden).toBe(true);
    expect(document.querySelector('pre code')?.textContent).toBe('graph TD\n  A-->B');

    await userEvent.click(screen.getByRole('button', { name: 'Rendered' }));
    expect(figure.hidden).toBe(false);
    expect(document.querySelector('pre')).toBeNull();
  });

  it('keeps an ordinary fence as a code block', () => {
    render(<MarkdownContent content={fence('python', 'print(1)')} />);
    expect(screen.getByText('python')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Copy' })).toBeInTheDocument();
    expect(document.querySelector('pre code')?.textContent).toContain('print(1)');
    expect(screen.queryByRole('button', { name: 'Source' })).toBeNull();
    expect(mountMermaid).not.toHaveBeenCalled();
  });

  it('leaves inline code alone', () => {
    render(<MarkdownContent content="Run `npm test` now" />);
    expect(screen.getByText('npm test').tagName).toBe('CODE');
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('draws an svg fence after sanitizing it', async () => {
    const svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10">'
      + '<script>alert(1)</script><circle cx="5" cy="5" r="4" onclick="alert(1)"/></svg>';
    render(<MarkdownContent content={fence('svg', svg)} />);
    const figure = await screen.findByRole('img', { name: 'Rendered svg' });
    const circle = figure.querySelector('circle');
    expect(circle).not.toBeNull();
    expect(circle?.getAttribute('r')).toBe('4');
    expect(circle?.getAttribute('onclick')).toBeNull();
    expect(figure.querySelector('script')).toBeNull();
    expect(screen.getByRole('button', { name: 'Download SVG' })).toBeInTheDocument();
  });

  it('shows the source and the reason when a chart points outside itself', async () => {
    const spec = JSON.stringify({ data: { url: 'https://example.invalid/d.json' }, mark: 'bar' });
    render(<MarkdownContent content={fence('vega-lite', spec)} />);
    const note = await screen.findByRole('status');
    expect(note.textContent).toContain("Couldn't render this vega-lite: spec.data.url points outside the spec");
    expect(screen.getByText("couldn't render")).toBeInTheDocument();
    expect(document.querySelector('pre code')?.textContent).toContain('example.invalid');
    expect(screen.queryByRole('button', { name: 'Source' })).toBeNull();
  });
});
