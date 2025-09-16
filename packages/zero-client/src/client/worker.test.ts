/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
import {expect, test} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';

test('worker test', async () => {
  // Need to have the 'new URL' call inside `new Worker` for vite to
  // correctly bundle the worker file.
  const w = new Worker(new URL('./worker-test.ts', import.meta.url), {
    type: 'module',
  });
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
    sleep(9000).then(() => Promise.reject(new Error('Timed out'))),
  ]);
}
