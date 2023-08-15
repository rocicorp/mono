import {
  Timestamp,
  type DocumentReference,
  DocumentSnapshot,
} from '@google-cloud/firestore';
import {
  describe,
  expect,
  test,
  jest,
  beforeEach,
  afterEach,
} from '@jest/globals';
import {LEASE_BUFFER_MS, LEASE_INTERVAL_MS, Lock} from './lock.js';
import {resolver} from '@rocicorp/resolver';
import type {LockDoc} from 'mirror-schema/src/lock.js';
import {Queue} from 'shared/src/queue.js';
import {HttpsError} from 'firebase-functions/v2/https';

beforeEach(() => {
  jest.useFakeTimers();
});

afterEach(() => {
  jest.resetAllMocks();
});

function makeRunner<T>(retVal: T) {
  const started = resolver<void>();
  const finish = resolver<void>();

  const run = async () => {
    started.resolve();
    await finish.promise;
    return retVal;
  };

  return {
    running: started.promise,
    finish: finish.resolve,
    run,
  };
}

function makeEmptySnapshot() {
  return {exists: false} as unknown as DocumentSnapshot<LockDoc>;
}

function makeSnapshot(
  lockDoc: Pick<LockDoc, 'expiration'>,
  createTime: number,
  updateTime: number,
) {
  return {
    exists: true,
    createTime: Timestamp.fromMillis(createTime),
    updateTime: Timestamp.fromMillis(updateTime),
    data: () => ({...lockDoc, holder: 'existing lock holder'}),
  } as unknown as DocumentSnapshot<LockDoc>;
}

type SnapshotReceiver = (s: DocumentSnapshot<LockDoc>) => void;

function mockDoc() {
  function nextWriteResult(results: Queue<number>) {
    return async () => {
      const timestamp = await results.dequeue();
      return {writeTime: Timestamp.fromMillis(timestamp)};
    };
  }
  const creates = new Queue<number>();
  const updates = new Queue<number>();
  const deletes = new Queue<number>();

  const mock = {
    doc: {
      path: 'my/lock',
      onSnapshot: jest.fn().mockImplementation(next => {
        mock.nextSnapshot = next as unknown as SnapshotReceiver;
        return () => {
          /* empty */
        };
      }),
      create: jest.fn().mockImplementation(nextWriteResult(creates)),
      update: jest.fn().mockImplementation(nextWriteResult(updates)),
      delete: jest.fn().mockImplementation(nextWriteResult(deletes)),
    },
    nextSnapshot: undefined as unknown as SnapshotReceiver,
    creates,
    updates,
    deletes,
  };
  return mock;
}

const LEASE_DURATION_MS = LEASE_INTERVAL_MS + LEASE_BUFFER_MS;

describe('firestore lock', () => {
  test('acquires free lock', async () => {
    const now = 987;
    const newLockCreateTime = now + 123;
    const newLockDeleteTime = now + 234;

    const runner = makeRunner('acquired!');
    const mock = mockDoc();

    jest.setSystemTime(now);

    const lock = new Lock(mock.doc as unknown as DocumentReference<LockDoc>);
    const running = lock.withLock('acquire test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    mock.nextSnapshot(makeEmptySnapshot());

    await mock.creates.enqueue(newLockCreateTime);
    await runner.running;

    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      expiration: Timestamp.fromMillis(now + LEASE_DURATION_MS),
      holder: 'acquire test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    runner.finish();
    await mock.deletes.enqueue(newLockDeleteTime);
    expect(await running).toBe('acquired!');

    expect(mock.doc.delete).toBeCalledTimes(1);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockCreateTime),
    });
  });

  test('retries acquire after lock contention', async () => {
    const now = 987;
    const newLockCreateTime = now + 123;
    const newLockDeleteTime = now + 234;

    const runner = makeRunner('acquired after contention');
    const mock = mockDoc();

    jest.setSystemTime(now);

    const lock = new Lock(mock.doc as unknown as DocumentReference<LockDoc>);
    const running = lock.withLock('acquire after contention test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    mock.nextSnapshot(makeEmptySnapshot());

    // First create() fails with lock contention.
    await mock.creates.enqueueRejection(
      new HttpsError('already-exists', 'you lost bro'),
    );

    // Allow the snapshot loop run.
    await jest.advanceTimersByTimeAsync(1000);
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      expiration: Timestamp.fromMillis(now + LEASE_DURATION_MS),
      holder: 'acquire after contention test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    // The 'already-exists' error is followed by the snapshot of the lock winner.
    mock.nextSnapshot(
      makeSnapshot(
        {expiration: Timestamp.fromMillis(now + 5000)},
        now - 100,
        now - 100,
      ),
    );

    await jest.advanceTimersByTimeAsync(1000);
    expect(mock.doc.delete).not.toBeCalled;

    // Once the lock is released, the acquisition should be attempted again.
    mock.nextSnapshot(makeEmptySnapshot());

    // Second create() succeeds.
    await mock.creates.enqueue(newLockCreateTime);
    await runner.running;

    expect(mock.doc.create).toBeCalledTimes(2);
    expect(mock.doc.create.mock.calls[1][0]).toEqual({
      expiration: Timestamp.fromMillis(now + 2000 + LEASE_DURATION_MS),
      holder: 'acquire after contention test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    runner.finish();

    await mock.deletes.enqueue(newLockDeleteTime);
    expect(await running).toBe('acquired after contention');

    expect(mock.doc.delete).toBeCalledTimes(1);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockCreateTime),
    });
  });

  test('waits for held lock', async () => {
    const now = 987;
    const newLockCreateTime = now + 123;
    const newLockDeleteTime = now + 234;

    const runner = makeRunner('waited!');
    const mock = mockDoc();

    jest.setSystemTime(now);

    const lock = new Lock(mock.doc as unknown as DocumentReference<LockDoc>);
    const running = lock.withLock('held lock test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    mock.nextSnapshot(
      makeSnapshot(
        {expiration: Timestamp.fromMillis(now + 2000)},
        now - 100,
        now - 100,
      ),
    );

    await jest.advanceTimersByTimeAsync(1000);
    expect(mock.doc.create).not.toBeCalled;
    expect(mock.doc.delete).not.toBeCalled;

    // Another update. Lease is extended.
    mock.nextSnapshot(
      makeSnapshot(
        {expiration: Timestamp.fromMillis(now + LEASE_DURATION_MS)},
        now - 100,
        now - 100,
      ),
    );

    await jest.advanceTimersByTimeAsync(10000);
    expect(mock.doc.create).not.toBeCalled;
    expect(mock.doc.delete).not.toBeCalled;

    // Lock is released.
    mock.nextSnapshot(makeEmptySnapshot());

    await mock.creates.enqueue(newLockCreateTime);
    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      expiration: Timestamp.fromMillis(Date.now() + LEASE_DURATION_MS),
      holder: 'held lock test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    runner.finish();
    await mock.deletes.enqueue(newLockDeleteTime);
    expect(await running).toBe('waited!');
    expect(mock.doc.delete).toBeCalledTimes(1);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockCreateTime),
    });
  });

  test('deletes expired lock', async () => {
    const now = 987;
    const newLockCreateTime = now + 500;
    const expiredLockDeleteTime = now + 400;
    const newLockDeleteTime = now + 800;

    const runner = makeRunner('expired!');
    const mock = mockDoc();

    jest.setSystemTime(now);

    const lock = new Lock(mock.doc as unknown as DocumentReference<LockDoc>);
    const running = lock.withLock('expire test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    const expiredLockCreateTime = now - 500;
    const expiredLockUpdateTime = now - 300;

    mock.nextSnapshot(
      makeSnapshot(
        {expiration: Timestamp.fromMillis(now + 100)},
        expiredLockCreateTime,
        expiredLockUpdateTime,
      ),
    );

    // Expiration timer should fire.
    await jest.advanceTimersByTimeAsync(101);
    await mock.deletes.enqueue(expiredLockDeleteTime);

    expect(mock.doc.create).not.toBeCalled;
    expect(mock.doc.delete).toBeCalledTimes(1);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(expiredLockUpdateTime),
    });

    // Lock is released.
    mock.nextSnapshot(makeEmptySnapshot());

    await mock.creates.enqueue(newLockCreateTime);
    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      expiration: Timestamp.fromMillis(Date.now() + LEASE_DURATION_MS),
      holder: 'expire test',
    });

    runner.finish();
    await mock.deletes.enqueue(newLockDeleteTime);
    expect(await running).toBe('expired!');
    expect(mock.doc.delete).toBeCalledTimes(2);
    expect(mock.doc.delete.mock.calls[1][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockCreateTime),
    });
  });

  test('extends lock lease', async () => {
    const now = 987;
    const newLockCreateTime = now + 123;
    const newLockUpdateTime1 = newLockCreateTime + LEASE_INTERVAL_MS;
    const newLockUpdateTime2 = newLockCreateTime + LEASE_INTERVAL_MS * 2;
    const newLockDeleteTime = newLockUpdateTime2 + 234;

    const runner = makeRunner('extended!');
    const mock = mockDoc();

    jest.setSystemTime(now);

    const lock = new Lock(mock.doc as unknown as DocumentReference<LockDoc>);
    const running = lock.withLock('extend test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    mock.nextSnapshot(makeEmptySnapshot());

    await mock.creates.enqueue(newLockCreateTime);
    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      expiration: Timestamp.fromMillis(now + LEASE_DURATION_MS),
      holder: 'extend test',
    });
    expect(mock.doc.update).not.toBeCalled;
    expect(mock.doc.delete).not.toBeCalled;

    await jest.advanceTimersByTimeAsync(LEASE_INTERVAL_MS + 1000);
    await mock.updates.enqueue(newLockUpdateTime1);
    expect(mock.doc.update).toBeCalledTimes(1);
    expect(mock.doc.update.mock.calls[0][1]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockCreateTime),
    });

    await jest.advanceTimersByTimeAsync(LEASE_INTERVAL_MS + 1000);
    await mock.updates.enqueue(newLockUpdateTime2);
    expect(mock.doc.update).toBeCalledTimes(2);
    expect(mock.doc.update.mock.calls[1][1]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockUpdateTime1),
    });

    runner.finish();
    await mock.deletes.enqueue(newLockDeleteTime);
    expect(await running).toBe('extended!');
    expect(mock.doc.delete).toBeCalledTimes(1);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockUpdateTime2),
    });
  });
});
