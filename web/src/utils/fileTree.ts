export interface FileNode {
  name: string;
  path: string;
  type: 'file' | 'directory';
  size?: number;
  modified?: string;
  // The server's answer, not the client's guess: on a locked instance the
  // reviewed files (SOUL.md, AGENTS.md, ...) are readable and not writable.
  readOnly?: boolean;
  children?: FileNode[];
}

export function buildFileTree(files: { path: string; name: string; size: number; modified?: string; read_only?: boolean }[]): FileNode[] {
  const root: FileNode = { name: '', path: '', type: 'directory', children: [] };

  for (const file of files) {
    const parts = file.path.split('/');
    let current = root;

    for (let i = 0; i < parts.length - 1; i++) {
      const dirName = parts[i];
      const dirPath = parts.slice(0, i + 1).join('/');
      let child = current.children!.find(c => c.name === dirName && c.type === 'directory');
      if (!child) {
        child = { name: dirName, path: dirPath, type: 'directory', children: [] };
        current.children!.push(child);
      }
      current = child;
    }

    current.children!.push({
      name: file.name,
      path: file.path,
      type: 'file',
      size: file.size,
      modified: file.modified,
      readOnly: file.read_only,
    });
  }

  const sortTree = (node: FileNode) => {
    if (node.children) {
      node.children.sort((a, b) => {
        if (a.type !== b.type) return a.type === 'directory' ? -1 : 1;
        return a.name.localeCompare(b.name);
      });
      node.children.forEach(sortTree);
    }
  };
  sortTree(root);

  return root.children || [];
}
