// Screenshots the running dashboard with headless Chromium over the DevTools protocol, once the
// page has data: until then a screenshot would show the empty state. Node 22+ (global WebSocket).
//
//   node scripts/screenshot.mjs --url 'http://localhost:8088/#RDSA.NL' --out figure.png [--width 1600 --height 900]
//
// CHROME names the browser (default chromium). It waits until the signals panel has a row, the
// price chart has drawn, and nothing has changed for a second.

import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const args = Object.fromEntries(process.argv.slice(2).reduce((out, a, i, all) => {
  if (a.startsWith('--')) out.push([a.slice(2), all[i + 1]]);
  return out;
}, []));
const width = Number(args.width ?? 1600);
const height = Number(args.height ?? 900);
const scale = Number(args.scale ?? 2);
const port = 9300 + Math.floor(Math.random() * 500);
const profile = mkdtempSync(join(tmpdir(), 'viz-shot-'));

const chrome = spawn(process.env.CHROME ?? 'chromium', [
  '--headless=new', '--disable-gpu', '--hide-scrollbars', '--no-first-run', `--user-data-dir=${profile}`,
  `--remote-debugging-port=${port}`, `--window-size=${width},${height}`, 'about:blank',
], { stdio: 'ignore' });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function pageSocket() {
  for (let i = 0; i < 100; i++) {
    try {
      const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
      const page = targets.find((t) => t.type === 'page');
      if (page) return page.webSocketDebuggerUrl;
    } catch { /* not up yet */ }
    await sleep(100);
  }
  throw new Error('Chromium did not start');
}

let failed = false;
try {
  const ws = new WebSocket(await pageSocket());
  await new Promise((r) => { ws.onopen = r; });
  let id = 0;
  const waiting = new Map();
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.id && waiting.has(msg.id)) {
      waiting.get(msg.id)(msg);
      waiting.delete(msg.id);
    }
  };
  const send = (method, params = {}) => new Promise((resolve) => {
    id += 1;
    waiting.set(id, resolve);
    ws.send(JSON.stringify({ id, method, params }));
  });
  const evaluate = async (expression) => (await send('Runtime.evaluate', { expression, returnByValue: true })).result?.result?.value;

  await send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: scale, mobile: false });
  await send('Page.navigate', { url: args.url });
  const ready = `(() => document.querySelectorAll('.sg-row').length > 0
    && document.querySelector('.pane-price canvas') !== null) ? document.body.innerText.length : -1`;
  let last = null;
  let stableSince = 0;
  for (let waited = 0; waited < 30000; waited += 250) {
    const v = await evaluate(ready);
    if (v > 0 && v === last) {
      if (stableSince === 0) stableSince = Date.now();
      if (Date.now() - stableSince >= 1000) break;
    } else {
      stableSince = 0;
    }
    last = v;
    await sleep(250);
  }
  if (stableSince === 0) throw new Error('the page did not get data within 30 s');
  const shot = await send('Page.captureScreenshot', { format: 'png' });
  writeFileSync(args.out, Buffer.from(shot.result.data, 'base64'));
  console.log(args.out);
  ws.close();
} catch (e) {
  console.error(e.message);
  failed = true;
} finally {
  chrome.kill();
  await sleep(200);
  rmSync(profile, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);
