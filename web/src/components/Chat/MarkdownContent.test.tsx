import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';

import { InlineMarkdown, MarkdownContent } from './MarkdownContent';

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
