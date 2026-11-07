import { render, screen, waitFor, cleanup } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MermaidBlock } from './MermaidBlock';
import { useThemeStore } from '../../stores/themeStore';

const renderer = vi.hoisted(() => ({ initialize: vi.fn(), render: vi.fn() }));
vi.mock('mermaid', () => ({ default: renderer }));
afterEach(() => { cleanup(); vi.resetAllMocks(); useThemeStore.setState({ resolved: 'light' }); });

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

it('retries invalid sequence notes with literal semicolons and preserves source', async () => {
  renderer.render.mockRejectedValueOnce(new Error('Parse error'))
    .mockResolvedValueOnce({ svg: '<svg><text>ready; then done</text></svg>' });
  const source = 'sequenceDiagram\nNote over A,B: ready; then done #59; unchanged';
  render(<MermaidBlock source={source} />);
  await screen.findByRole('img');
  expect(renderer.render.mock.calls[1][1]).toBe('sequenceDiagram\nNote over A,B: ready#59; then done #59; unchanged');
  expect(screen.getByText(/original source is preserved/)).toBeInTheDocument();
  expect(screen.getByText(/ready; then done #59; unchanged/)).toBeInTheDocument();
});

it('does not reinterpret valid semicolon-separated diagrams', async () => {
  renderer.render.mockResolvedValue({ svg: '<svg><text>valid</text></svg>' });
  const source = 'sequenceDiagram\nNote over A,B: ready; A->>B: next';
  render(<MermaidBlock source={source} />);
  await screen.findByRole('img');
  expect(renderer.render).toHaveBeenCalledTimes(1);
  expect(renderer.render.mock.calls[0][1]).toBe(source);
});

it.each(['->>', '-->>', '-)', '--)', '-x', '--x', '<<->>', '-|/', '-|\\', '/|-'])('preserves inline %s commands while retrying a different malformed note', async (arrow) => {
  renderer.render.mockRejectedValueOnce(new Error('Parse error'))
    .mockResolvedValueOnce({ svg: '<svg><text>ready</text></svg>' });
  const source = `sequenceDiagram\nNote over A,B: start; A${arrow}B: next\nNote over A,B: ready; finished`;
  render(<MermaidBlock source={source} />);
  await screen.findByRole('img');
  expect(renderer.render.mock.calls[1][1]).toBe(`sequenceDiagram\nNote over A,B: start; A${arrow}B: next\nNote over A,B: ready#59; finished`);
});

it('keeps pale sequence highlights readable with light text in dark mode', async () => {
  useThemeStore.setState({ resolved: 'dark' });
  renderer.render.mockResolvedValue({ svg: '<svg><text>new behavior</text></svg>' });
  render(<MermaidBlock source={'sequenceDiagram\nA->>B: new'} />);
  await screen.findByRole('img');
  expect(renderer.initialize).toHaveBeenCalledWith(expect.objectContaining({
    theme: 'dark', themeCSS: 'rect.rect { fill-opacity: 0.16; }',
  }));
});
