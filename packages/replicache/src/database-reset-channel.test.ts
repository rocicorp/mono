import {resolver} from '@rocicorp/resolver';
import {afterEach, expect, test, vi} from 'vitest';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';
import {
  initDatabaseResetChannel,
  makeDatabaseResetChannelNameForTesting,
} from './database-reset-channel.ts';

const controllers: AbortController[] = [];
afterEach(() => {
  for (const c of controllers) {
    c.abort();
  }
  controllers.length = 0;
});

function init(idbName: string, onReset: () => void) {
  const controller = new AbortController();
  controllers.push(controller);
  return initDatabaseResetChannel(idbName, controller.signal, onReset);
}

test('notifies other instances on the same database but not the sender', async () => {
  const {promise, resolve} = resolver();
  const onReset1 = vi.fn();
  const onReset2 = vi.fn(resolve);
  const onResetOther = vi.fn();
  const notify1 = init('db-a', onReset1);
  init('db-a', onReset2);
  init('db-b', onResetOther);

  notify1();
  await promise;

  expect(onReset2).toHaveBeenCalledTimes(1);
  // BroadcastChannel does not deliver to the posting channel.
  expect(onReset1).not.toHaveBeenCalled();
  expect(onResetOther).not.toHaveBeenCalled();
});

test('ignores messages for a different database on the same channel', async () => {
  const onReset = vi.fn();
  init('db-a', onReset);
  const channel = new BroadcastChannel(
    makeDatabaseResetChannelNameForTesting('db-a'),
  );
  channel.postMessage({idbName: 'db-other'});
  await new Promise(r => setTimeout(r, 10));
  channel.close();
  expect(onReset).not.toHaveBeenCalled();
});

test('does nothing once aborted', async () => {
  const controller = new AbortController();
  const onReset = vi.fn();
  const notify = initDatabaseResetChannel('db-a', controller.signal, onReset);
  const {promise, resolve} = resolver();
  const onOther = vi.fn(resolve);
  init('db-a', onOther);

  controller.abort();
  notify();
  // Give a message time to arrive if one was (wrongly) sent.
  await Promise.race([promise, new Promise(r => setTimeout(r, 10))]);
  expect(onOther).not.toHaveBeenCalled();
});
