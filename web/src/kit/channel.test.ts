import { describe, expect, it } from 'vitest';
import { connectPage, readHostMessage } from './channel';
import { testWindow } from './testWindow';

const base = { channel: 'nerve-artifact', v: 1 };

describe('readHostMessage', () => {
  it.each([
    [{ ...base, type: 'hello' }, { type: 'hello' }],
    [{ ...base, type: 'print' }, { type: 'print' }],
    [{ ...base, type: 'present', on: true }, { type: 'present', on: true }],
    [{ ...base, type: 'slide', to: 'next' }, { type: 'slide', to: 'next' }],
    [{ ...base, type: 'slide', to: 4 }, { type: 'slide', to: 4 }],
  ])('reads %j', (data, read) => {
    expect(readHostMessage(data)).toEqual(read);
  });

  it.each([
    ['another channel', { channel: 'other', v: 1, type: 'hello' }],
    ['another version', { ...base, v: 2, type: 'hello' }],
    ['an unknown type', { ...base, type: 'navigate', to: 'https://example.com' }],
    ['a step that is not known', { ...base, type: 'slide', to: 'random' }],
    ['slide 0', { ...base, type: 'slide', to: 0 }],
    ['a slide that is not an integer', { ...base, type: 'slide', to: 1.5 }],
    ['present that is not a boolean', { ...base, type: 'present', on: 'yes' }],
    ['no object', 'hello'],
  ])('refuses %s', (_name, data) => {
    expect(readHostMessage(data)).toBeNull();
  });
});

describe('connectPage', () => {
  const ready = () => ({ type: 'ready' as const, kind: 'markdown' as const, canPrint: true });

  it('says ready at once, to the target *, and again for each hello of the parent', () => {
    const t = testWindow();
    connectPage(ready, undefined, t.win);
    expect(t.posted).toEqual([{ message: { ...base, type: 'ready', kind: 'markdown', canPrint: true }, target: '*' }]);
    t.say({ ...base, type: 'hello' });
    expect(t.of('ready')).toHaveLength(2);
  });

  it('takes no message of another window', () => {
    const t = testWindow();
    connectPage(ready, undefined, t.win);
    t.say({ ...base, type: 'hello' }, {});
    t.say({ ...base, type: 'print' }, null);
    expect(t.of('ready')).toHaveLength(1);
    expect(t.print).not.toHaveBeenCalled();
  });

  it('prints only a page that can print', () => {
    const t = testWindow();
    connectPage(ready, undefined, t.win);
    t.say({ ...base, type: 'print' });
    expect(t.print).toHaveBeenCalledTimes(1);

    const u = testWindow();
    connectPage(() => ({ type: 'ready', kind: 'table', canPrint: false }), undefined, u.win);
    u.say({ ...base, type: 'print' });
    expect(u.print).not.toHaveBeenCalled();
  });

  it('posts nothing at the top level', () => {
    const t = testWindow();
    Object.assign(t.win, { parent: t.win });
    connectPage(ready, undefined, t.win);
    expect(t.posted).toEqual([]);
  });
});
