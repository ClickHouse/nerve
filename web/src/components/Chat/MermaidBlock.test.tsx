import { render, screen, waitFor, cleanup } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MermaidBlock } from './MermaidBlock';

const renderer = vi.hoisted(() => ({ initialize: vi.fn(), render: vi.fn() }));
vi.mock('mermaid', () => ({ default: renderer }));
afterEach(() => { cleanup(); vi.clearAllMocks(); });

describe('MermaidBlock', () => {
  it('renders sanitized SVG and keeps copyable source', async () => {
    renderer.render.mockResolvedValue({ svg: '<svg><text>before</text><script>alert(1)</script><foreignObject>unsafe</foreignObject></svg>' });
    const { container } = render(<MermaidBlock source={'sequenceDiagram\nA->>B: new'} />);
    await screen.findByRole('img', { name: 'Mermaid diagram' });
    expect(container.querySelector('svg text')).toHaveTextContent('before');
    expect(container.querySelector('script,foreignObject')).toBeNull();
    expect(screen.getByText('Diagram source')).toBeInTheDocument();
    expect(renderer.initialize).toHaveBeenCalledWith(expect.objectContaining({ securityLevel: 'strict', startOnLoad: false }));
  });
  it('falls back to source for invalid syntax', async () => {
    renderer.render.mockRejectedValue(new Error('Invalid syntax'));
    render(<MermaidBlock source={'sequenceDiagram\ninvalid'} />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Invalid syntax');
    expect(screen.getByText('Diagram source').closest('details')).toHaveAttribute('open');
  });
  it('rejects source configuration before rendering', async () => {
    render(<MermaidBlock source={'%%{init: {securityLevel: loose}}%%\nsequenceDiagram'} />);
    await screen.findByRole('alert');
    expect(renderer.render).not.toHaveBeenCalled();
  });
  it('does not keep the previous SVG when the source changes', async () => {
    renderer.render.mockResolvedValue({ svg: '<svg><text>old</text></svg>' });
    const { rerender } = render(<MermaidBlock source={'sequenceDiagram\nA->>B: old'} />);
    await screen.findByRole('img');
    renderer.render.mockRejectedValue(new Error('Changed diagram invalid'));
    rerender(<MermaidBlock source={'sequenceDiagram\nbroken'} />);
    await waitFor(() => expect(screen.queryByRole('img')).toBeNull());
    await screen.findByRole('alert');
  });
});
