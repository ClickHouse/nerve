import { act, render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { useUIStore } from '../stores/uiStore';
import { ShortcutsModal } from './ShortcutsModal';

describe('ShortcutsModal', () => {
  it('documents the agents shortcuts', () => {
    render(<ShortcutsModal />);
    act(() => { useUIStore.getState().toggleShortcutsModal(); });
    const dialog = screen.getByRole('dialog', { name: 'Keyboard shortcuts' });
    expect(within(dialog).getByText('Toggle the agents panel')).toBeInTheDocument();
    expect(within(dialog).getByText('Open agents (empty message box)')).toBeInTheDocument();
    expect(within(dialog).getByText('Agents panel')).toBeInTheDocument();
    expect(within(dialog).getByText('x x')).toBeInTheDocument();
    expect(within(dialog).getByText('Back to the message box')).toBeInTheDocument();
  });
});
