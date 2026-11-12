const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1', '[::1]']);

/** A VS Code link to a folder on the Nerve host: local, or through Remote-SSH to the host the UI is served from. */
export function vscodeFolderUrl(path: string, host: string = window.location.hostname): string {
  const encoded = path.split('/').map(encodeURIComponent).join('/');
  return LOCAL_HOSTS.has(host)
    ? `vscode://file${encoded}`
    : `vscode://vscode-remote/ssh-remote+${host}${encoded}`;
}
