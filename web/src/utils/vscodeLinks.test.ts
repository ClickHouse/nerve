import { describe, expect, it } from 'vitest';
import { vscodeFolderUrl } from './vscodeLinks';

describe('vscodeFolderUrl', () => {
  it('opens a local folder when the UI is served from this machine', () => {
    expect(vscodeFolderUrl('/Users/alice/dev/app-wt-login', 'localhost')).toBe('vscode://file/Users/alice/dev/app-wt-login');
    expect(vscodeFolderUrl('/Users/alice/dev/app', '127.0.0.1')).toBe('vscode://file/Users/alice/dev/app');
    expect(vscodeFolderUrl('/Users/alice/dev/app', '[::1]')).toBe('vscode://file/Users/alice/dev/app');
  });

  it('opens the folder over Remote-SSH on the host the UI is served from', () => {
    expect(vscodeFolderUrl('/home/alice/dev/my app', 'dev-box')).toBe('vscode://vscode-remote/ssh-remote+dev-box/home/alice/dev/my%20app');
  });

  it('keeps characters that end a URL path inside the path', () => {
    const url = new URL(vscodeFolderUrl('/home/alice/c#/what?/100%', 'localhost'));
    expect(url.hash).toBe('');
    expect(url.search).toBe('');
    expect(decodeURIComponent(url.pathname)).toBe('/home/alice/c#/what?/100%');
  });
});
