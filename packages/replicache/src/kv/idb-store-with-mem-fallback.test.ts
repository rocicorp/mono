import {LogContext} from '@rocicorp/logger';
import {afterEach, expect, test, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.ts';
import {
  IDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './idb-store-with-mem-fallback.ts';
import {IDBStore} from './idb-store.ts';

afterEach(() => {
  vi.restoreAllMocks();
});

test('Firefox private browsing', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Firefox def',
  );

  const name = `ff-${Math.random()}`;

  const store = storeThatErrorsInOpen(new LogContext(), name);
  expect(store).instanceOf(IDBStoreWithMemFallback);

  await withWrite(store, async tx => {
    await tx.put('foo', 'bar');
  });
  await withRead(store, async tx => {
    expect(await tx.get('foo')).to.equal('bar');
  });
});

test('No wrapper if not Firefox', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Safari def',
  );
  const name = `not-ff-${Math.random()}`;
  const store = newIDBStoreWithMemFallback(new LogContext(), name);
  expect(store).not.instanceOf(IDBStoreWithMemFallback);
  expect(store).instanceOf(IDBStore);
  await store.close();
});

test('race condition', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Firefox def',
  );
  const logFake = vi.fn();

  const name = `ff-race-${Math.random()}`;
  const store = storeThatErrorsInOpen(
    new LogContext('debug', {my: 'context'}, {log: logFake}),
    name,
  );

  const p1 = withWriteNoImplicitCommit(store, () => undefined);
  const p2 = withWriteNoImplicitCommit(store, () => undefined);
  await p1;
  await p2;

  expect(logFake).toBeCalledTimes(1);
  expect(logFake.mock.calls[0]).to.deep.equal([
    'info',
    {my: 'context'},
    'Switching to MemStore because of Firefox private browsing error',
  ]);
});

function storeThatErrorsInOpen(lc: LogContext, name: string) {
  const openRequest = {
    error: new DOMException(
      'A mutation operation was attempted on a database that did not allow mutations.',
      'InvalidStateError',
    ),
  } as IDBOpenDBRequest;
  vi.spyOn(indexedDB, 'open').mockImplementation(() => openRequest);

  const store = newIDBStoreWithMemFallback(lc, name);
  expect(store).instanceOf(IDBStoreWithMemFallback);

  assert(openRequest.onerror);
  openRequest.onerror(new Event('error'));
  return store;
}
