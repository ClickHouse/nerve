import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { GitHubRenderer } from './GitHubRenderer';

describe('GitHubRenderer', () => {
  it('shows the repository, the subject type and the link of a record', () => {
    render(
      <GitHubRenderer
        summary="[acme/widgets] New PR #12: Add a widget cache"
        content="Repository: acme/widgets"
        metadata={{
          repo_name: 'acme/widgets',
          subject_type: 'PullRequest',
          subject_url: 'https://github.com/acme/widgets/pull/12',
        }}
      />,
    );

    expect(screen.getByText('acme/widgets')).toBeInTheDocument();
    expect(screen.getByText('PullRequest')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /View on GitHub/ }))
      .toHaveAttribute('href', 'https://github.com/acme/widgets/pull/12');
  });

  it('shows the branch of new commits, and each commit as a list item', () => {
    render(
      <GitHubRenderer
        summary="[acme/widgets] 2 new commits on main"
        content={[
          'Repository: acme/widgets',
          'Type: Commit',
          'Branch: main',
          '',
          '--- Commits ---',
          '- 5c4d3e2f1a0b99887766554433221100ffeeddcc Cache rendered widgets',
          '- 1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d Add the widget cache interface',
        ].join('\n')}
        metadata={{
          repo_name: 'acme/widgets',
          subject_type: 'Commit',
          branch: 'main',
          subject_url: 'https://github.com/acme/widgets/compare/89abcdef...5c4d3e2f',
        }}
      />,
    );

    expect(screen.getByText('Commit')).toBeInTheDocument();
    expect(screen.getByText('main')).toBeInTheDocument();
    const commits = screen.getAllByRole('listitem').map((item) => item.textContent);
    expect(commits).toEqual([
      '5c4d3e2f1a0b99887766554433221100ffeeddcc Cache rendered widgets',
      '1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d Add the widget cache interface',
    ]);
  });
});
