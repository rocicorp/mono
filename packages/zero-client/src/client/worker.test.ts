import {sleep} from 'shared/src/sleep.js';
import {expect, test} from 'vitest';

test('worker test', async () => {
  const url = new URL('./worker-test.ts', import.meta.url);
  const w = new Worker(url, {type: 'module'});
  // Wait for the worker "test harness" to be ready since it may have async
  // modules.
  await waitForReady(w);
  const userID = 'worker-test-user-id';
  const data = await send(w, {userID});
  if (data !== undefined) {
    throw data;
  }
  expect(data).to.be.undefined;
});

function waitForReady(w: Worker): Promise<void> {
  const p = new Promise<void>((resolve, reject) => {
    w.onmessage = e => {
      if (e.data === 'ready') {
        resolve();
      } else {
        reject(new Error('Unexpected message: ' + e.data));
      }
    };
  });
  return withTimeout<void>(p);
}

function send(w: Worker, data: {userID: string}): Promise<unknown> {
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
    sleep(6000).then(() => Promise.reject(new Error('Timed out'))),
  ]);
}
