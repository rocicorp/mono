import {LogContext} from '@rocicorp/logger';
import * as sinon from 'sinon';
import {afterEach, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.js';
import {
  IDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './idb-store-with-mem-fallback.js';
import {IDBStore} from './idb-store.js';

afterEach(() => {
  sinon.restore();
});

test('Firefox private browsing', async () => {
  sinon.stub(navigator, 'userAgent').get(() => 'abc Firefox def');

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
  sinon.stub(navigator, 'userAgent').get(() => 'abc Safari def');
  const name = `not-ff-${Math.random()}`;
  const store = newIDBStoreWithMemFallback(new LogContext(), name);
  expect(store).not.instanceOf(IDBStoreWithMemFallback);
  expect(store).instanceOf(IDBStore);
  await store.close();
});

test('race condition', async () => {
  sinon.stub(navigator, 'userAgent').get(() => 'abc Firefox def');
  const logFake = sinon.fake();

  const name = `ff-race-${Math.random()}`;
  const store = storeThatErrorsInOpen(
    new LogContext('debug', {my: 'context'}, {log: logFake}),
    name,
  );

  const p1 = withWriteNoImplicitCommit(store, () => undefined);
  const p2 = withWriteNoImplicitCommit(store, () => undefined);
  await p1;
  await p2;

  expect(logFake.callCount).to.equal(1);
  expect(logFake.firstCall.args).to.deep.equal([
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
  sinon.replace(indexedDB, 'open', () => openRequest);

  const store = newIDBStoreWithMemFallback(lc, name);
  expect(store).instanceOf(IDBStoreWithMemFallback);

  assert(openRequest.onerror);
  openRequest.onerror(new Event('error'));
  return store;
}
