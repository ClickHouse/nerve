import { fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { OptionList } from './OptionList';
import { questionOptions } from './askOptions';

const OPTIONS = questionOptions(['Keep them', 'Delete all (recommended)', 'Ask me later']);

function renderList(props: Partial<Parameters<typeof OptionList>[0]> = {}) {
  const onChoose = vi.fn();
  render(<OptionList options={OPTIONS} label="Old caches" onChoose={onChoose} allowOther {...props} />);
  return { onChoose, list: within(screen.getByRole('group', { name: 'Old caches' })) };
}

describe('OptionList', () => {
  it('shows one row per option, the recommended one first with a tag', () => {
    const { list } = renderList();
    const rows = list.getAllByRole('button');
    expect(rows.map(r => r.textContent)).toEqual(['ADelete allRecommended', 'BKeep them', 'CAsk me later', 'DOther…']);
    expect(rows[0]).toHaveAttribute('aria-keyshortcuts', 'A 1');
  });

  it('sends the option as the agent wrote it', async () => {
    const { onChoose, list } = renderList();
    await userEvent.click(list.getByRole('button', { name: /Delete all/ }));
    expect(onChoose).toHaveBeenCalledWith('Delete all (recommended)');
  });

  it('chooses a row by its letter or its digit while focus is in the list', () => {
    const { onChoose, list } = renderList();
    const first = list.getAllByRole('button')[0];
    fireEvent.keyDown(first, { key: 'b' });
    expect(onChoose).toHaveBeenLastCalledWith('Keep them');
    fireEvent.keyDown(first, { key: '3' });
    expect(onChoose).toHaveBeenLastCalledWith('Ask me later');
    fireEvent.keyDown(first, { key: 'C', shiftKey: true });
    expect(onChoose).toHaveBeenLastCalledWith('Ask me later');
  });

  it('ignores keys with no row or with a modifier', () => {
    const { onChoose, list } = renderList();
    const first = list.getAllByRole('button')[0];
    fireEvent.keyDown(first, { key: 'z' });
    fireEvent.keyDown(first, { key: '9' });
    fireEvent.keyDown(first, { key: 'a', ctrlKey: true });
    fireEvent.keyDown(first, { key: 'a', metaKey: true });
    expect(onChoose).not.toHaveBeenCalled();
  });

  it('ignores keys while disabled', () => {
    const { onChoose, list } = renderList({ disabled: true });
    fireEvent.keyDown(list.getAllByRole('button')[0], { key: 'a' });
    expect(onChoose).not.toHaveBeenCalled();
  });

  it('opens Other… by its letter and sends the typed answer, not the keys', async () => {
    const { onChoose, list } = renderList();
    fireEvent.keyDown(list.getAllByRole('button')[0], { key: 'd' });
    const field = list.getByRole('textbox', { name: 'Your answer' });
    expect(field).toHaveFocus();
    await userEvent.type(field, 'Only the ones from last year{Enter}');
    expect(onChoose).toHaveBeenCalledTimes(1);
    expect(onChoose).toHaveBeenCalledWith('Only the ones from last year');
  });

  it('closes Other… with Escape and keeps the Escape from the page', async () => {
    const onPageKey = vi.fn();
    document.addEventListener('keydown', onPageKey);
    const { list } = renderList();
    await userEvent.click(list.getByRole('button', { name: /Other/ }));
    await userEvent.keyboard('{Escape}');
    document.removeEventListener('keydown', onPageKey);
    expect(list.queryByRole('textbox')).not.toBeInTheDocument();
    expect(list.getByRole('button', { name: /Other/ })).toHaveFocus();
    expect(onPageKey).not.toHaveBeenCalled();
  });

  it('sends nothing for an empty answer', async () => {
    const { onChoose, list } = renderList();
    await userEvent.click(list.getByRole('button', { name: /Other/ }));
    await userEvent.type(list.getByRole('textbox'), '   {Enter}');
    expect(onChoose).not.toHaveBeenCalled();
    expect(list.getByRole('button', { name: 'Send answer' })).toBeDisabled();
  });

  it('has no Other… row when it is off', () => {
    const { list } = renderList({ allowOther: false });
    expect(list.queryByRole('button', { name: /Other/ })).not.toBeInTheDocument();
  });

  it('offers an answer in own words for a question without options', async () => {
    const { onChoose, list } = renderList({ options: [] });
    fireEvent.keyDown(list.getByRole('button', { name: 'Answer in your own words…' }), { key: 'a' });
    await userEvent.type(list.getByRole('textbox', { name: 'Your answer' }), 'Tomorrow{Enter}');
    expect(onChoose).toHaveBeenCalledWith('Tomorrow');
  });

  it('read-only, lists the options and marks the answer', () => {
    render(<OptionList options={OPTIONS} label="Old caches" chosen="Keep them" />);
    const items = within(screen.getByRole('list', { name: 'Old caches' })).getAllByRole('listitem');
    expect(items).toHaveLength(3);
    expect(items[1]).toHaveTextContent('Keep them(your answer)');
    expect(items[0]).not.toHaveTextContent('your answer');
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });
});
