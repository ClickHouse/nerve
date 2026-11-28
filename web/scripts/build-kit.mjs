// The second build of the web UI: the files of the Nerve kit, with fixed
// names, in dist/nerve-kit/ (see src/kit/files.json and
// nerve/artifacts/bundle.py). Each script is one file that runs as a
// classic script and imports nothing; each stylesheet is copied as it is.
// The build fails when a kit file names an address of the network.
import { copyFile, mkdir, readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import react from '@vitejs/plugin-react';
import { build } from 'vite';

const web = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const out = path.join(web, 'dist', 'nerve-kit');
const files = JSON.parse(await readFile(path.join(web, 'src', 'kit', 'files.json'), 'utf8'));

// Address strings that a kit script may hold because they load nothing:
// XML namespace names (React creates SVG and MathML elements with them), and
// pages that a library names in the text of an error or a warning.
const ALLOWED_ADDRESSES = [
  'http://www.w3.org/1999/xhtml',
  'http://www.w3.org/2000/svg',
  'http://www.w3.org/1998/Math/MathML',
  'http://www.w3.org/1999/xlink',
  'http://www.w3.org/XML/1998/namespace',
  'http://www.w3.org/2000/xmlns/',
  'https://react.dev/errors/',
  'https://github.com/syntax-tree/hast-util-to-jsx-runtime',
  'https://github.com/highlightjs/highlight.js/issues/',
  'https://github.com/highlightjs/highlight.js/wiki/',
];
// Calls that load from the network. No kit script makes one.
const NETWORK_CALLS = /\b(?:fetch|sendBeacon|importScripts)\s*\(|\bnew\s+(?:XMLHttpRequest|WebSocket|EventSource|Worker|SharedWorker)\b|\bimport\s*\(/;

await mkdir(out, { recursive: true });

for (const [name, entry] of Object.entries(files.scripts)) {
  await build({
    configFile: false,
    root: web,
    logLevel: 'warn',
    plugins: [react()],
    define: { 'process.env.NODE_ENV': JSON.stringify('production') },
    build: {
      outDir: out,
      emptyOutDir: false,
      copyPublicDir: false,
      sourcemap: false,
      lib: {
        entry: path.join(web, entry),
        formats: ['iife'],
        name: 'NerveKit',
        fileName: () => name,
      },
    },
  });
}
for (const [name, source] of Object.entries(files.styles)) {
  await copyFile(path.join(web, source), path.join(out, name));
}

const expected = new Set([...Object.keys(files.scripts), ...Object.keys(files.styles)]);
const written = new Set(await readdir(out));
const problems = [];
for (const name of expected) {
  if (!written.has(name)) problems.push(`${name} was not written`);
}
for (const name of written) {
  if (!expected.has(name)) problems.push(`${name} is not a kit file`);
}
for (const name of expected) {
  if (!written.has(name)) continue;
  const text = await readFile(path.join(out, name), 'utf8');
  for (const match of text.matchAll(/(?:https?:)?\/\/[a-z0-9.-]+\.[a-z]{2,}[^\s"'`)]*/gi)) {
    if (!ALLOWED_ADDRESSES.some(allowed => match[0].startsWith(allowed))) {
      problems.push(`${name} names the address ${match[0]}`);
    }
  }
  if (name.endsWith('.css') && /@import|url\((?!\s*["']?data:)/i.test(text)) {
    problems.push(`${name} loads another file`);
  }
  if (name.endsWith('.js') && NETWORK_CALLS.test(text)) {
    problems.push(`${name} has a call that loads from the network: ${text.match(NETWORK_CALLS)[0]}`);
  }
}
if (problems.length > 0) {
  console.error(`The kit build failed:\n${problems.map(p => `- ${p}`).join('\n')}`);
  process.exit(1);
}
console.log(`kit: ${[...expected].sort().join(', ')} in dist/nerve-kit/`);
