import {afterEach, expect, test} from 'vitest';
import {sleep} from '../../shared/src/sleep.ts';
import {closeAllReps, dbsToDrop, deleteAllDatabases} from './test-util.ts';

afterEach(async () => {
  await closeAllReps();
  await deleteAllDatabases();
});

test('worker test', async () => {
  // Need to have the 'new URL' call inside `new Worker` for vite to
  // correctly bundle the worker file.
  const w = new Worker(new URL('./worker-test.ts', import.meta.url), {
    type: 'module',
  });
  const name = 'worker-test';
  dbsToDrop.add(name);

  const data = await send(w, {name});
  if (data !== undefined) {
    throw data;
  }
  expect(data).to.be.undefined;
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
