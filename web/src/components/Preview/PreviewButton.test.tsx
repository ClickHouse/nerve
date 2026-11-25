import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ToolCallBlockData } from '../../types/chat';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));

const { useChatStore } = await import('../../stores/chatStore');
const { usePreviewStore, resetPreviewConfig } = await import('../../stores/previewStore');
const { EditToolBlock } = await import('../Chat/tools/EditToolBlock');
const { FileToolBlock } = await import('../Chat/tools/FileToolBlock');
const { SendFileBlock } = await import('../Chat/tools/SendFileBlock');
const { FileEditor } = await import('../Files/FileEditor');
const { MemoryRouter, Route, Routes } = await import('react-router-dom');

function block(tool: string, input: Record<string, unknown>, extra: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return { type: 'tool_call', toolUseId: `t-${tool}`, tool, input, status: 'complete', result: 'ok', ...extra } as ToolCallBlockData;
}

beforeEach(() => {
  useChatStore.setState({ panels: [], activePanelId: null, panelVisible: false });
});
afterEach(() => resetPreviewConfig());

describe('the preview action of a file', () => {
  it('is absent while the Preview tab is off', () => {
    render(<EditToolBlock block={block('Edit', { file_path: '/ws/deck/index.html' })} />);
    expect(screen.queryByRole('button', { name: /^Preview / })).toBeNull();
  });

  it('opens the preview of an edited HTML file in the side panel', async () => {
    usePreviewStore.setState({ enabled: true });
    render(<EditToolBlock block={block('Edit', { file_path: '/ws/deck/index.html' })} />);
    await userEvent.click(screen.getByRole('button', { name: 'Preview index.html' }));
    const { panels, panelVisible } = useChatStore.getState();
    expect(panels).toEqual([expect.objectContaining({ type: 'preview', previewPath: '/ws/deck/index.html' })]);
    expect(panelVisible).toBe(true);
  });

  it('is on a written image and a sent page, and not on a read or a failed write', () => {
    usePreviewStore.setState({ enabled: true });
    render(<>
      <FileToolBlock block={block('Write', { file_path: '/ws/logo.svg' })} />
      <FileToolBlock block={block('Read', { file_path: '/ws/read.html' })} />
      <FileToolBlock block={block('Write', { file_path: '/ws/failed.html' }, { isError: true })} />
      <SendFileBlock block={block('mcp__nerve__send_file', { file_path: '/ws/report.html' })} />
      <EditToolBlock block={block('Edit', { file_path: '/ws/style.css' })} />
    </>);
    expect(screen.getByRole('button', { name: 'Preview logo.svg' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Preview report.html' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Preview read.html' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Preview failed.html' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Preview style.css' })).toBeNull();
  });

  it('on the Files page, opens the tab in the open chat and goes there', async () => {
    usePreviewStore.setState({ enabled: true });
    useChatStore.setState({ activeSession: 'session-1' });
    const editor = (path: string) => (
      <FileEditor path={path} content="" modified={false} saving={false} onContentChange={() => {}} onSave={() => {}} />
    );
    render(
      <MemoryRouter initialEntries={['/files']}>
        <Routes>
          <Route path="/files" element={<>{editor('site/index.html')}{editor('memory/notes.md')}</>} />
          <Route path="/chat/:sessionId" element={<p>chat page</p>} />
        </Routes>
      </MemoryRouter>,
    );
    expect(screen.queryByRole('button', { name: 'Preview notes.md' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Preview index.html' }));
    expect(screen.getByText('chat page')).toBeInTheDocument();
    expect(useChatStore.getState().panels).toEqual([expect.objectContaining({ previewPath: 'site/index.html' })]);
  });
});
