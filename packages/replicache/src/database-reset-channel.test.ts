import {resolver} from '@rocicorp/resolver';
import {afterEach, expect, test, vi} from 'vitest';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';
import {
  listenForDatabaseReset,
  makeDatabaseResetChannelNameForTesting,
  notifyDatabaseReset,
} from './database-reset-channel.ts';

const controllers: AbortController[] = [];
afterEach(() => {
  for (const c of controllers) {
    c.abort();
  }
  controllers.length = 0;
});

function listen(idbName: string, onReset: (dropped: boolean) => void) {
  const controller = new AbortController();
  controllers.push(controller);
  listenForDatabaseReset(idbName, controller.signal, onReset);
  return controller;
}

test('notifies listeners on the same database, with whether it was dropped', async () => {
  const {promise, resolve} = resolver();
  const onReset = vi.fn((_dropped: boolean) => resolve());
  const onResetOther = vi.fn();
  listen('db-a', onReset);
  listen('db-b', onResetOther);

  notifyDatabaseReset('db-a', true);
  await promise;

  expect(onReset).toHaveBeenCalledExactlyOnceWith(true);
  expect(onResetOther).not.toHaveBeenCalled();
});

test('passes along a failed drop', async () => {
  const {promise, resolve} = resolver();
  const onReset = vi.fn((_dropped: boolean) => resolve());
  listen('db-a', onReset);

  notifyDatabaseReset('db-a', false);
  await promise;

  expect(onReset).toHaveBeenCalledExactlyOnceWith(false);
});

test('ignores messages for a different database on the same channel', async () => {
  const onReset = vi.fn();
  listen('db-a', onReset);
  const channel = new BroadcastChannel(
    makeDatabaseResetChannelNameForTesting('db-a'),
  );
  channel.postMessage({idbName: 'db-other', dropped: true});
  await new Promise(r => setTimeout(r, 10));
  channel.close();
  expect(onReset).not.toHaveBeenCalled();
});

test('stops listening once aborted', async () => {
  const onReset = vi.fn();
  const controller = listen('db-a', onReset);
  controller.abort();

  notifyDatabaseReset('db-a', true);
  await new Promise(r => setTimeout(r, 10));
  expect(onReset).not.toHaveBeenCalled();
});
