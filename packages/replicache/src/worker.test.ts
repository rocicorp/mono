import {expect, test} from 'vitest';
import {sleep} from '../../shared/src/sleep.ts';

test('worker test', async () => {
  // Need to have the 'new URL' call inside `new Worker` for vite to
  // correctly bundle the worker file.
  const w = new Worker(new URL('./worker-test.ts', import.meta.url), {
    type: 'module',
  });
  const name = 'worker-test';

  // The worker itself closes its Replicache instance and drops its database
  // (including the replicache-dbs-v0 registry record). Cleanup must happen in
  // the worker: workers use the real origin-wide IndexedDB, bypassing vitest
  // browser mode's per-file storage isolation, so anything left behind leaks
  // into every other browser test file.
  try {
    const data = await send(w, {name});
    if (data !== undefined) {
      throw data;
    }
    expect(data).toBeUndefined();
  } finally {
    // The worker posts its message only after cleanup has finished, so it is
    // safe to terminate here. This closes any IndexedDB connections still
    // open in the worker which could otherwise block other test files from
    // deleting databases.
    w.terminate();
  }
});

function send(w: Worker, data: {name: string}): Promise<unknown> {
  const p = new Promise((resolve, reject) => {
    w.onmessage = e => resolve(e.data);
    w.onerror = reject;
    w.onmessageerror = reject;
  });
  w.postMessage(data);
  return withTimeout(p);
}

function withTimeout<T>(p: Promise<T>): Promise<T> {
  return Promise.race([
    p,
    sleep(9000).then(() => Promise.reject(new Error('Timed out'))),
  ]);
}
