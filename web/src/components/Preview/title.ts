/** A title to start from: the name of the folder or file of the path, without its extension. */
export function defaultTitle(path: string): string {
  const names = path.split('/').filter(Boolean);
  let name = names.pop() ?? '';
  if (name === 'index.html' && names.length) name = names.pop() ?? name;
  return name.replace(/\.[A-Za-z0-9]+$/, '') || name;
}
