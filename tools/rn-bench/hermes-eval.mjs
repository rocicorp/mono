/* oxlint-disable no-console -- a CLI whose output is the point */
/**
 * Evaluates a JavaScript file in a running React Native app's Hermes runtime
 * and prints what it returns.
 *
 * A micro-benchmark loop that needs a rebuild, reinstall and relaunch per
 * variant is too slow to think with. This reuses the CDP connection the
 * profiler uses, so a candidate implementation can be measured on the real
 * engine in seconds. It is how the compare-utf8 first-code-unit probe was
 * found to be a pessimization on Hermes (rocicorp/compare-utf8#11): four
 * alternatives were measured this way, and the one that looked most promising
 * on paper was three times slower.
 *
 * Needs Metro running and the app open, e.g.
 *
 *   cd packages/zql-benchmarks/rn && npx expo start --port 8082
 *   adb reverse tcp:8082 tcp:8082
 *   adb shell am start -n dev.rocicorp.zqlperfrn/.MainActivity
 *
 * then, from this directory (so `ws` resolves):
 *
 *   node hermes-eval.mjs /tmp/probe.js [device-title-substring]
 *
 * The file is evaluated as a single expression, so wrap it in an IIFE and
 * return a string — CDP returns values by value, so keep the result small.
 */
import {readFileSync} from 'node:fs';
import WebSocket from 'ws';

const HOST = '127.0.0.1';
const PORT = Number(process.env.METRO_PORT ?? 8082);
const deviceMatch = process.argv[3] ?? 'sdk_gphone';

const list = await fetch(`http://${HOST}:${PORT}/json/list`).then(r =>
  r.json(),
);
const target = list.find(t => (t.title ?? '').includes(deviceMatch));
if (!target) {
  throw new Error(
    `No CDP target matching ${deviceMatch}. Targets: ${list
      .map(t => t.title)
      .join(' | ')}`,
  );
}

// The proxy checks Origin, and rejects `localhost` in favour of its own host.
const ws = new WebSocket(
  target.webSocketDebuggerUrl.replace('localhost', HOST),
  {
    headers: {Origin: `http://${HOST}:${PORT}`},
    perMessageDeflate: false,
  },
);
await new Promise((resolve, reject) => {
  ws.once('open', resolve);
  ws.once('error', reject);
});

let id = 0;
const pending = new Map();
ws.on('message', data => {
  const msg = JSON.parse(String(data));
  if (msg.id && pending.has(msg.id)) {
    const {resolve, reject} = pending.get(msg.id);
    pending.delete(msg.id);
    msg.error
      ? reject(new Error(JSON.stringify(msg.error)))
      : resolve(msg.result);
  }
});

function send(method, params = {}) {
  const myId = ++id;
  ws.send(JSON.stringify({id: myId, method, params}));
  return new Promise((resolve, reject) => {
    pending.set(myId, {resolve, reject});
    setTimeout(() => {
      if (pending.delete(myId)) {
        reject(new Error(`${method} timed out`));
      }
    }, 120_000);
  });
}

const result = await send('Runtime.evaluate', {
  expression: readFileSync(process.argv[2], 'utf-8'),
  returnByValue: true,
});
if (result.exceptionDetails) {
  console.error(JSON.stringify(result.exceptionDetails).slice(0, 800));
  process.exit(1);
}
console.log(
  typeof result.result.value === 'string'
    ? result.result.value
    : JSON.stringify(result.result.value, null, 1),
);
process.exit(0);
