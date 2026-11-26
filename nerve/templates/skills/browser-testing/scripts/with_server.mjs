#!/usr/bin/env node
// Run an app's server only for as long as you need it, and stop only that server.
//
//   node with_server.mjs --cmd "<server command>" --url <url> [--cwd <dir>]
//        [--out <file>] [--timeout <s>] -- <test command> [args...]
//     Start the server, wait until <url> answers, run the test command, stop the
//     server. Exit code: the test command's exit code; 2 when the server fails.
//
//   node with_server.mjs start --cmd "<server command>" --url <url> --pidfile <file>
//        [--cwd <dir>] [--out <file>] [--timeout <s>]
//     Start the server, wait until <url> answers, write its process group id to
//     <pidfile> and exit. The server keeps running.
//
//   node with_server.mjs stop --pidfile <file>
//     Stop the process group written by "start". Nothing else is stopped.
//
// The server runs in its own process group, so a stop also ends the children that
// npm or a shell start. The command must stay in the foreground (no "-d", no
// daemon mode). Before the start, the port of <url> must be free.
import { spawn } from 'node:child_process';
import { openSync, readFileSync, writeFileSync, unlinkSync, existsSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';

const argv = process.argv.slice(2);
const sep = argv.indexOf('--');
const own = sep === -1 ? argv : argv.slice(0, sep);
const testCmd = sep === -1 ? [] : argv.slice(sep + 1);
const mode = ['start', 'stop'].includes(own[0]) ? own.shift() : 'run';
const opt = (name, def) => {
  const i = own.indexOf(`--${name}`);
  return i > -1 && own[i + 1] !== undefined ? own[i + 1] : def;
};
const die = (msg, code = 2) => { console.error(`with_server: ${msg}`); process.exit(code); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// True while a process of the group still runs. On Linux, read /proc and ignore
// zombies: in a container without an init process nobody reaps them, and
// kill(-pgid, 0) still succeeds for a zombie.
function groupAlive(pgid) {
  try { process.kill(-pgid, 0); } catch (e) { if (e.code !== 'EPERM') return false; }
  if (!existsSync('/proc/self/stat')) return true;
  for (const pid of readdirSync('/proc')) {
    if (!/^\d+$/.test(pid)) continue;
    let stat;
    try { stat = readFileSync(`/proc/${pid}/stat`, 'utf8'); } catch { continue; }
    const [state, , pgrp] = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    if (Number(pgrp) === pgid && state !== 'Z') return true;
  }
  return false;
}

function portOpen(host, port) {
  return new Promise((resolve) => {
    const s = net.connect({ host, port });
    s.setTimeout(1000);
    s.once('connect', () => { s.destroy(); resolve(true); });
    s.once('timeout', () => { s.destroy(); resolve(false); });
    s.once('error', () => resolve(false));
  });
}

async function stopGroup(pgid, label) {
  if (!groupAlive(pgid)) { console.error(`with_server: ${label} ${pgid} is not running`); return; }
  try { process.kill(-pgid, 'SIGTERM'); } catch { /* already gone */ }
  for (let i = 0; i < 50 && groupAlive(pgid); i++) await sleep(100);
  if (groupAlive(pgid)) {
    try { process.kill(-pgid, 'SIGKILL'); } catch { /* already gone */ }
    console.error(`with_server: ${label} ${pgid} did not stop in 5 s; sent SIGKILL`);
  } else {
    console.error(`with_server: stopped ${label} ${pgid}`);
  }
}

if (mode === 'stop') {
  const pidfile = opt('pidfile') || die('stop needs --pidfile <file>');
  if (!existsSync(pidfile)) die(`no pidfile at ${pidfile}`);
  const pgid = Number(readFileSync(pidfile, 'utf8').trim());
  if (!Number.isInteger(pgid) || pgid <= 1) die(`bad process group id in ${pidfile}`);
  await stopGroup(pgid, 'server process group');
  unlinkSync(pidfile);
  process.exit(0);
}

const cmd = opt('cmd') || die('--cmd "<server command>" is required');
const urlText = opt('url') || die('--url <url> is required');
const url = new URL(urlText);
const host = url.hostname.replace(/^\[|\]$/g, '');
const port = Number(url.port || (url.protocol === 'https:' ? 443 : 80));
const timeoutS = Number(opt('timeout', '60'));
const out = opt('out', join(tmpdir(), `with-server-${port}.out`));
const pidfile = opt('pidfile');
if (mode === 'start' && !pidfile) die('start needs --pidfile <file>');
if (mode === 'run' && testCmd.length === 0) die('give the test command after "--", or use "start"');

if (await portOpen(host, port)) {
  die(`port ${port} on ${host} already answers. Another process uses it. Pick a free port.`);
}

const fd = openSync(out, 'a');
const child = spawn(cmd, {
  shell: true, cwd: opt('cwd', process.cwd()), detached: true, stdio: ['ignore', fd, fd],
});
let exited = null;
child.on('exit', (code, signal) => { exited = { code, signal }; });
console.error(`with_server: started process group ${child.pid}, output in ${out}`);

const tail = () => {
  try { return readFileSync(out, 'utf8').split('\n').slice(-20).join('\n'); } catch { return ''; }
};

const t0 = Date.now();
let status = null;
while (Date.now() - t0 < timeoutS * 1000) {
  if (exited) {
    die(`server command exited (code ${exited.code}, signal ${exited.signal}) before ${urlText} answered.\n${tail()}`);
  }
  try {
    const res = await fetch(urlText, { signal: AbortSignal.timeout(2000), redirect: 'manual' });
    status = res.status;
    break;
  } catch { await sleep(500); }
}
if (status === null) {
  await stopGroup(child.pid, 'server process group');
  die(`${urlText} did not answer in ${timeoutS} s.\n${tail()}`);
}
console.error(`with_server: ${urlText} answered HTTP ${status} after ${((Date.now() - t0) / 1000).toFixed(1)} s`);

if (mode === 'start') {
  writeFileSync(pidfile, `${child.pid}\n`);
  console.error(`with_server: process group id written to ${pidfile}; stop it with: node with_server.mjs stop --pidfile ${pidfile}`);
  child.unref();
  process.exit(0);
}

let stopping = false;
const finish = async (code) => {
  if (stopping) return;
  stopping = true;
  await stopGroup(child.pid, 'server process group');
  process.exit(code);
};
process.on('SIGINT', () => finish(130));
process.on('SIGTERM', () => finish(143));

const test = spawn(testCmd[0], testCmd.slice(1), { stdio: 'inherit' });
test.on('error', (e) => { console.error(`with_server: cannot run ${testCmd[0]}: ${e.message}`); finish(2); });
test.on('exit', (code, signal) => finish(code ?? (signal ? 1 : 0)));
