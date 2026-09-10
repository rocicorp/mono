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

afterEach(() => {
  vi.restoreAllMocks();
});

const firefoxPrivateBrowsingError = () =>
  new DOMException(
    'A mutation operation was attempted on a database that did not allow mutations.',
    'InvalidStateError',
  );

test('Firefox private browsing', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Firefox def',
  );

  const name = `ff-${Math.random()}`;

  const store = storeThatErrorsInOpen(
    new LogContext(),
    name,
    firefoxPrivateBrowsingError(),
  );

  await withWrite(store, async tx => {
    await tx.put('foo', 'bar');
  });
  await withRead(store, async tx => {
    expect(await tx.get('foo')).toBe('bar');
  });
});

test('Wrapper on every browser', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Safari def',
  );
  const name = `not-ff-${Math.random()}`;
  const store = newIDBStoreWithMemFallback(new LogContext(), name);
  expect(store).toBeInstanceOf(IDBStoreWithMemFallback);
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
    firefoxPrivateBrowsingError(),
  );

  const p1 = withWriteNoImplicitCommit(store, () => undefined);
  const p2 = withWriteNoImplicitCommit(store, () => undefined);
  await p1;
  await p2;

  expect(logFake).toBeCalledTimes(1);
  expect(logFake.mock.calls[0]).toEqual([
    'info',
    {my: 'context'},
    'Switching to MemStore because of Firefox private browsing error',
  ]);
});

test.each([
  'Unable to open database file on disk',
  'Error creating Records table (13) - database or disk is full',
])('IndexedDB open failure: %s', async message => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Safari def',
  );
  const logFake = vi.fn();
  const error = new DOMException(message, 'UnknownError');

  const name = `open-failure-${Math.random()}`;
  const store = storeThatErrorsInOpen(
    new LogContext('debug', {my: 'context'}, {log: logFake}),
    name,
    error,
  );

  await withWrite(store, async tx => {
    await tx.put('foo', 'bar');
  });
  await withRead(store, async tx => {
    expect(await tx.get('foo')).toBe('bar');
  });

  expect(logFake).toBeCalledTimes(1);
  expect(logFake.mock.calls[0]).toEqual([
    'info',
    {my: 'context'},
    'Switching to MemStore because IndexedDB failed to open',
    error,
  ]);
});

test('IndexedDB open failure with concurrent first calls', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Safari def',
  );
  const logFake = vi.fn();
  const error = new DOMException(
    'Unable to open database file on disk',
    'UnknownError',
  );

  const name = `open-failure-race-${Math.random()}`;
  const store = storeThatErrorsInOpen(
    new LogContext('debug', {my: 'context'}, {log: logFake}),
    name,
    error,
  );

  const p1 = withWrite(store, async tx => {
    await tx.put('a', 1);
  });
  const p2 = withWrite(store, async tx => {
    await tx.put('b', 2);
  });
  await p1;
  await p2;

  await withRead(store, async tx => {
    expect(await tx.get('a')).toBe(1);
    expect(await tx.get('b')).toBe(2);
  });

  expect(logFake).toBeCalledTimes(1);
  expect(logFake.mock.calls[0]).toEqual([
    'info',
    {my: 'context'},
    'Switching to MemStore because IndexedDB failed to open',
    error,
  ]);
});

test('Transaction error after a successful open is rethrown', async () => {
  vi.spyOn(navigator, 'userAgent', 'get').mockImplementation(
    () => 'abc Safari def',
  );
  const logFake = vi.fn();

  const name = `tx-error-${Math.random()}`;
  const store = newIDBStoreWithMemFallback(
    new LogContext('debug', {my: 'context'}, {log: logFake}),
    name,
  );

  await withWrite(store, async tx => {
    await tx.put('foo', 'bar');
  });

  const error = new DOMException('Connection is closing.', 'UnknownError');
  const transactionSpy = vi
    .spyOn(IDBDatabase.prototype, 'transaction')
    .mockImplementation(() => {
      throw error;
    });
  await expect(withRead(store, () => undefined)).rejects.toBe(error);
  transactionSpy.mockRestore();

  await withRead(store, async tx => {
    expect(await tx.get('foo')).toBe('bar');
  });

  expect(logFake).not.toBeCalled();
  await store.close();
});

function storeThatErrorsInOpen(
  lc: LogContext,
  name: string,
  error: DOMException,
) {
  const openRequest = {error} as IDBOpenDBRequest;
  vi.spyOn(indexedDB, 'open').mockImplementation(() => openRequest);

  const store = newIDBStoreWithMemFallback(lc, name);
  expect(store).toBeInstanceOf(IDBStoreWithMemFallback);

  assert(openRequest.onerror, 'Expected openRequest.onerror to be defined');
  openRequest.onerror(new Event('error'));
  return store;
}
