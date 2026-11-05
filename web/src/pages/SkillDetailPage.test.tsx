import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SkillDetail } from '../stores/skillsStore';

vi.mock('../api/client', () => ({
  api: {
    getSkill: vi.fn(),
    listSkills: vi.fn(),
    updateSkill: vi.fn(),
    deleteSkill: vi.fn(),
    toggleSkill: vi.fn(),
  },
}));

const { api } = await import('../api/client');
const { useSkillsStore } = await import('../stores/skillsStore');
const { SkillDetailPage } = await import('./SkillDetailPage');

const getSkill = vi.mocked(api.getSkill);
const toggleSkill = vi.mocked(api.toggleSkill);

const RAW = '---\nname: beta\ndescription: A test skill.\n---\n\nDo the thing.\n';

function detail(overrides: Partial<SkillDetail>): SkillDetail {
  return {
    id: 'beta',
    name: 'beta',
    description: 'A test skill.',
    version: '1.0.0',
    enabled: true,
    source: 'workspace',
    user_invocable: true,
    model_invocable: true,
    allowed_tools: null,
    total_invocations: 0,
    success_count: 0,
    avg_duration_ms: null,
    last_used: null,
    created_at: '2026-01-01T00:00:00+00:00',
    updated_at: '2026-01-01T00:00:00+00:00',
    content: 'Do the thing.',
    raw: RAW,
    has_references: false,
    has_scripts: false,
    has_assets: false,
    references: [],
    stats: { total_invocations: 0, success_count: 0, avg_duration_ms: null, last_used: null },
    recent_usage: [],
    ...overrides,
  };
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/skills/beta']}>
      <Routes>
        <Route path="/skills/:skillId" element={<SkillDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  useSkillsStore.setState({ selectedSkill: null, detailLoading: false, actionLoading: false });
});

describe('SkillDetailPage', () => {
  it('shows a platform skill read only, without edit or delete controls', async () => {
    getSkill.mockResolvedValue(detail({ source: 'platform' }));
    toggleSkill.mockResolvedValue({ id: 'beta', enabled: false });
    renderPage();

    expect(await screen.findByText('Platform')).toBeInTheDocument();
    expect(screen.getByText('The platform provides this skill. You cannot change it here.'))
      .toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete skill' })).not.toBeInTheDocument();

    const editor = screen.getByRole('textbox');
    expect(editor).toHaveAttribute('readonly');
    await userEvent.type(editor, 'more');
    expect(editor).toHaveValue(RAW);
    expect(screen.queryByRole('button', { name: /Save/ })).not.toBeInTheDocument();

    // Enable and disable still work.
    await userEvent.click(screen.getByRole('button', { name: 'Enabled' }));
    expect(toggleSkill).toHaveBeenCalledWith('beta', false);
  });

  it('keeps the edit and delete controls for a workspace skill', async () => {
    getSkill.mockResolvedValue(detail({ source: 'workspace' }));
    renderPage();

    expect(await screen.findByRole('button', { name: 'Delete skill' })).toBeInTheDocument();
    expect(screen.queryByText('Platform')).not.toBeInTheDocument();

    const editor = screen.getByRole('textbox');
    expect(editor).not.toHaveAttribute('readonly');
    await userEvent.type(editor, 'more');
    expect(screen.getByRole('button', { name: /Save/ })).toBeInTheDocument();
  });
});
