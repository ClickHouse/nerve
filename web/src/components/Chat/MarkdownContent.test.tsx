import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';

import { MarkdownContent } from './MarkdownContent';

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

  it('does not keep a protocol-relative link in the tab', () => {
    render(<MemoryRouter><MarkdownContent content="[x](//example.com/x)" /></MemoryRouter>);
    expect(screen.getByRole('link', { name: 'x' })).toHaveAttribute('target', '_blank');
  });

  it('drops a script link', () => {
    render(<MemoryRouter><MarkdownContent content="[run](javascript:alert(1))" /></MemoryRouter>);
    expect(screen.getByText('run').closest('a')).not.toHaveAttribute('href', expect.stringContaining('javascript'));
  });
});
