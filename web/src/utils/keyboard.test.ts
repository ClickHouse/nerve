import { describe, expect, it } from 'vitest';
import { FOCUS_COMPOSER_COMBO, isMac, matchesCombo } from './keyboard';

const press = (init: KeyboardEventInit) => new KeyboardEvent('keydown', {
  ...init,
  metaKey: isMac,
  ctrlKey: !isMac,
});

describe('matchesCombo', () => {
  it('matches the focus shortcut when Shift turns ";" into ":"', () => {
    expect(matchesCombo(press({ key: ':', code: 'Semicolon', shiftKey: true }), FOCUS_COMPOSER_COMBO)).toBe(true);
    expect(matchesCombo(press({ key: ';', code: 'Semicolon', shiftKey: true }), FOCUS_COMPOSER_COMBO)).toBe(true);
  });

  it('still needs the modifiers', () => {
    const noShift = press({ key: ';', code: 'Semicolon' });
    expect(matchesCombo(noShift, FOCUS_COMPOSER_COMBO)).toBe(false);
    const noMod = new KeyboardEvent('keydown', { key: ':', code: 'Semicolon', shiftKey: true });
    expect(matchesCombo(noMod, FOCUS_COMPOSER_COMBO)).toBe(false);
  });
});
