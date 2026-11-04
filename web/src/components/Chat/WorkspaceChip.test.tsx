import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { WorkspaceChip } from './WorkspaceChip';

const linked = { path: '/r/app-wt-login', branch: 'login-form', current_branch: 'login-form', exists: true };
const main = { path: '/r/app', branch: 'main', current_branch: 'release', exists: true };

describe('WorkspaceChip', () => {
  it('shows the latest checkout with a VS Code link and lists every checkout on hover', () => {
    render(<WorkspaceChip workspaces={[linked, main]} />);

    expect(screen.getByText('app-wt-login')).toBeInTheDocument();
    expect(screen.getByText('login-form')).not.toHaveClass('text-warning');
    expect(screen.getByText('+1')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open app-wt-login in VS Code' })).toHaveAttribute(
      'href', 'vscode://file/r/app-wt-login',
    );
    expect(screen.getByRole('button')).toHaveAttribute(
      'title', '/r/app-wt-login (login-form)\n/r/app (main, now on release)\nClick to copy the path',
    );
  });

  it('marks a checkout that moved to another branch, and drops the link once it is removed', () => {
    const { rerender } = render(<WorkspaceChip workspaces={[main]} />);
    expect(screen.getByText('main')).toHaveClass('text-warning');

    rerender(<WorkspaceChip workspaces={[{ ...linked, current_branch: null, exists: false }]} />);
    expect(screen.getByText('login-form').parentElement).toHaveClass('line-through');
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });
});
