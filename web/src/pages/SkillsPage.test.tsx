import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Skill } from '../stores/skillsStore';

vi.mock('../api/client', () => ({
  api: {
    listSkills: vi.fn(),
    toggleSkill: vi.fn(),
    syncSkills: vi.fn(),
    createSkill: vi.fn(),
  },
}));

const { api } = await import('../api/client');
const { useSkillsStore } = await import('../stores/skillsStore');
const { SkillsPage } = await import('./SkillsPage');

const listSkills = vi.mocked(api.listSkills);

function skill(overrides: Partial<Skill>): Skill {
  return {
    id: 'alpha',
    name: 'alpha',
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
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  useSkillsStore.setState({ skills: [], loading: true, showCreateDialog: false });
});

describe('SkillsPage', () => {
  it('shows a Platform badge on platform skills only', async () => {
    listSkills.mockResolvedValue({
      skills: [
        skill({ id: 'alpha', name: 'alpha' }),
        skill({ id: 'beta', name: 'beta', source: 'platform' }),
      ],
    });

    render(<MemoryRouter><SkillsPage /></MemoryRouter>);

    const beta = (await screen.findByRole('heading', { name: 'beta' })).parentElement!;
    const alpha = screen.getByRole('heading', { name: 'alpha' }).parentElement!;
    expect(within(beta).getByText('Platform')).toBeInTheDocument();
    expect(within(alpha).queryByText('Platform')).not.toBeInTheDocument();
  });
});
