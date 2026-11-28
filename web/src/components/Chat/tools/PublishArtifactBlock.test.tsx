import { act, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useHostedStore } from '../../../api/hosted';
import { resetArtifactConfig, useArtifactStore } from '../../../stores/artifactStore';
import type { ToolCallBlockData } from '../../../types/chat';
import { isPinnedCall } from '../toolBursts';
import { PublishArtifactBlock, VIEWER_ALLOW, VIEWER_FRAME_HEIGHT, VIEWER_SANDBOX } from './PublishArtifactBlock';
import { isPublishArtifactTool, readPublished, viewerFrameUrl } from './publishedArtifact';

const VIEWER = 'https://artifacts.example.test';
const ADDRESS = `${VIEWER}/t/0b9d6c1e-5c3f-4e43-9f0a-1d2c3b4a5f60/a/7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d`;

/** The result text of the tool, as `result_text` writes it. */
function resultText(address = ADDRESS, version = 2): string {
  return [
    `Published "Q3 review" as version ${version}.`,
    `Viewer address: ${address}`,
    'Who can see it: the members of this agent. A member can share this version with everyone in the tenant with the Share button of the viewer.',
    'Artifact ID: 7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d (give it as artifact_id to publish a new version of this artifact)',
    'Expires: 2026-12-25T10:00:00Z',
  ].join('\n');
}

function block(result: string, extra: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: 't-publish', tool: 'mcp__nerve__publish_artifact',
    input: { path: 'decks/q3', title: 'Q3 review' }, status: 'complete',
    // Claude gives the content blocks as a JSON list.
    result: JSON.stringify([{ type: 'text', text: result }]), ...extra,
  } as ToolCallBlockData;
}

function hosted(viewerOrigin: string | null = VIEWER) {
  useHostedStore.setState({ mode: 'external' });
  useArtifactStore.setState({ publish: true, viewerOrigin });
}

afterEach(() => {
  resetArtifactConfig();
  useHostedStore.setState({ mode: 'local' });
});

describe('the viewer address of a result', () => {
  it('is read from its line, with the version', () => {
    expect(readPublished(resultText())).toEqual({ address: ADDRESS, version: 2 });
    expect(readPublished('Not published: something')).toBeNull();
  });

  it('is framed only for exactly the viewer origin, with embed=1 and the version', () => {
    const at = (address: string) => viewerFrameUrl({ address, version: 3 }, VIEWER);
    expect(at(ADDRESS)).toBe(`${ADDRESS}?embed=1&v=3`);
    expect(at(`${VIEWER.toUpperCase().replace('HTTPS', 'https')}/t/x/a/y`)).toBe(`${VIEWER}/t/x/a/y?embed=1&v=3`);
    // Origins, not prefixes of text.
    expect(at('https://artifacts.example.test.other.test/t/x/a/y')).toBeNull();
    expect(at('https://artifacts.example.test@other.test/t/x/a/y')).toBeNull();
    expect(at('https://user@artifacts.example.test/t/x/a/y')).toBeNull();
    expect(at('http://artifacts.example.test/t/x/a/y')).toBeNull();
    expect(at('https://artifacts.example.test:8443/t/x/a/y')).toBeNull();
    expect(at('javascript:alert(1)')).toBeNull();
    expect(viewerFrameUrl({ address: ADDRESS, version: 3 }, null)).toBeNull();
  });
});

describe('a publish_artifact call in the chat', () => {
  it('is the viewer in a frame in hosted mode, for the viewer origin', () => {
    hosted();
    render(<PublishArtifactBlock block={block(resultText())} />);
    const frame = screen.getByTitle('Artifact: Q3 review') as HTMLIFrameElement;
    expect(frame.tagName).toBe('IFRAME');
    expect(frame.getAttribute('src')).toBe(`${ADDRESS}?embed=1&v=2`);
    expect(frame.getAttribute('allow')).toBe('fullscreen; clipboard-write');
    expect(VIEWER_ALLOW).toBe('fullscreen; clipboard-write');
    expect(frame.getAttribute('height')).toBe(String(VIEWER_FRAME_HEIGHT));
    // Its own origin, scripts and tabs for the sign-in; no move of this page.
    const sandbox = frame.getAttribute('sandbox')?.split(' ') ?? [];
    expect(frame.getAttribute('sandbox')).toBe(VIEWER_SANDBOX);
    expect(sandbox).toEqual(expect.arrayContaining([
      'allow-scripts', 'allow-same-origin', 'allow-popups', 'allow-popups-to-escape-sandbox', 'allow-modals',
    ]));
    expect(sandbox.some(flag => flag.startsWith('allow-top-navigation'))).toBe(false);
    expect(frame.getAttribute('referrerpolicy')).toBe('no-referrer');
  });

  it.each([
    ['another origin', 'https://other.example.test/t/x/a/y', VIEWER],
    ['no viewer origin', ADDRESS, null],
  ])('is a link card for %s', (_name, address, origin) => {
    hosted(origin);
    render(<PublishArtifactBlock block={block(resultText(address))} />);
    expect(document.querySelector('iframe')).toBeNull();
    const card = screen.getByRole('group', { name: 'Published artifact: Q3 review' });
    expect(card).toHaveTextContent('Q3 review');
    expect(card).toHaveTextContent(address);
    const link = screen.getByRole('link', { name: address });
    expect(link).toHaveAttribute('href', address);
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(link).toHaveAttribute('target', '_blank');
  });

  it('is a link card in local mode, also for the viewer origin', () => {
    useArtifactStore.setState({ publish: false, viewerOrigin: VIEWER });
    render(<PublishArtifactBlock block={block(resultText())} />);
    expect(document.querySelector('iframe')).toBeNull();
    expect(screen.getByRole('link', { name: ADDRESS })).toBeInTheDocument();
  });

  it('shows the address of another scheme as text, not as a link', () => {
    render(<PublishArtifactBlock block={block(resultText('ftp://files.example.test/x'))} />);
    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.getByText('ftp://files.example.test/x')).toBeInTheDocument();
  });

  it('says why a publish failed', () => {
    hosted();
    render(<PublishArtifactBlock block={block(
      'Not published: Agents with restricted internet access cannot publish artifacts.', { isError: true },
    )} />);
    expect(screen.getByRole('status')).toHaveTextContent(
      'Could not publish Q3 review: Agents with restricted internet access cannot publish artifacts.',
    );
    expect(document.querySelector('iframe')).toBeNull();
  });

  it('says that it runs', () => {
    render(<PublishArtifactBlock block={block('', { status: 'running', result: undefined })} />);
    expect(screen.getByText(/Publishing Q3 review/)).toBeInTheDocument();
  });

  it('does not listen to the frame: its messages are data of an unknown sender', () => {
    hosted();
    const listen = vi.spyOn(window, 'addEventListener');
    const { container } = render(<PublishArtifactBlock block={block(resultText())} />);
    expect(listen.mock.calls.some(([type]) => type === 'message')).toBe(false);
    listen.mockRestore();
    const frame = container.querySelector('iframe') as HTMLIFrameElement;
    const before = container.innerHTML;
    const href = window.location.href;
    act(() => {
      for (const data of [
        { channel: 'nerve-artifact', v: 1, type: 'download' },
        { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-refused', params: { reason: 'x' } },
        { type: 'navigate', to: 'https://other.example.test' },
      ]) {
        window.dispatchEvent(new MessageEvent('message', { data, origin: VIEWER, source: frame.contentWindow }));
      }
    });
    expect(container.innerHTML).toBe(before);
    expect(window.location.href).toBe(href);
  });

  it('stays out of a tool burst', () => {
    expect(isPinnedCall(block(resultText()), new Set())).toBe(true);
  });

  it('is only Nerve\'s own tool, not a tool of that name of another server', () => {
    expect(isPublishArtifactTool('mcp__nerve__publish_artifact')).toBe(true);
    expect(isPublishArtifactTool('publish_artifact')).toBe(true);
    expect(isPublishArtifactTool('mcp__other__publish_artifact')).toBe(false);
    expect(isPinnedCall(block(resultText(), { tool: 'mcp__other__publish_artifact' }), new Set())).toBe(false);
  });

  it('routes a call of another server to the generic block', async () => {
    hosted();
    const { ToolCallBlock } = await import('../ToolCallBlock');
    render(<ToolCallBlock block={block(resultText(), { tool: 'mcp__other__publish_artifact' })} />);
    expect(document.querySelector('iframe')).toBeNull();
    expect(screen.queryByRole('group', { name: /Published artifact/ })).toBeNull();
  });
});
