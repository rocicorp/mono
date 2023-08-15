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
import {Lock} from './lock.js';
import {resolver} from '@rocicorp/resolver';
import type {LockDoc} from 'mirror-schema/src/lock.js';
import {must} from 'shared/src/must.js';

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

function mockDoc(
  createTimestamps: number[],
  updateTimestamps: number[],
  deleteTimestamps: number[],
) {
  const mock = {
    doc: {
      path: 'my/lock',
      onSnapshot: jest.fn().mockImplementation(next => {
        mock.nextSnapshot = next as unknown as SnapshotReceiver;
        return () => {
          /* empty */
        };
      }),
      create: jest.fn().mockImplementation(() => ({
        writeTime: Timestamp.fromMillis(must(createTimestamps.shift())),
      })),
      update: jest.fn().mockImplementation(() => ({
        writeTime: Timestamp.fromMillis(must(updateTimestamps.shift())),
      })),
      delete: jest.fn().mockImplementation(() => ({
        writeTime: Timestamp.fromMillis(must(deleteTimestamps.shift())),
      })),
    },
    nextSnapshot: undefined as unknown as SnapshotReceiver,
  };
  return mock;
}

describe('firestore lock', () => {
  test('acquires free lock', async () => {
    const now = 987;
    const newLockCreateTime = now + 123;
    const newLockDeleteTime = now + 234;

    const runner = makeRunner('acquired!');
    const mock = mockDoc([newLockCreateTime], [], [newLockDeleteTime]);

    jest.setSystemTime(now);

    const lock = new Lock(mock.doc as unknown as DocumentReference<LockDoc>);
    const running = lock.withLock('acquire test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    mock.nextSnapshot(makeEmptySnapshot());

    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      // Expiration should be lease duration + buffer == 12 seconds.
      expiration: Timestamp.fromMillis(now + 12000),
      holder: 'acquire test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    runner.finish();
    expect(await running).toBe('acquired!');
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
    const mock = mockDoc([newLockCreateTime], [], [newLockDeleteTime]);

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
        {expiration: Timestamp.fromMillis(now + 12000)},
        now - 100,
        now - 100,
      ),
    );

    await jest.advanceTimersByTimeAsync(10000);
    expect(mock.doc.create).not.toBeCalled;
    expect(mock.doc.delete).not.toBeCalled;

    // Lock is released.
    mock.nextSnapshot(makeEmptySnapshot());

    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      // Expiration should be lease duration + buffer == 12 seconds.
      expiration: Timestamp.fromMillis(Date.now() + 12000),
      holder: 'held lock test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    runner.finish();
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
    const mock = mockDoc(
      [newLockCreateTime],
      [],
      [expiredLockDeleteTime, newLockDeleteTime],
    );

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

    expect(mock.doc.create).not.toBeCalled;
    expect(mock.doc.delete).toBeCalledTimes(1);

    // Lock is released.
    mock.nextSnapshot(makeEmptySnapshot());

    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      // Expiration should be lease duration + buffer == 12 seconds.
      expiration: Timestamp.fromMillis(Date.now() + 12000),
      holder: 'expire test',
    });
    expect(mock.doc.delete).not.toBeCalled;

    runner.finish();
    expect(await running).toBe('expired!');
    expect(mock.doc.delete).toBeCalledTimes(2);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(expiredLockUpdateTime),
    });
    expect(mock.doc.delete.mock.calls[1][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockCreateTime),
    });
  });

  test('extends lock lease', async () => {
    const leaseInterval = 10000;
    const now = 987;
    const newLockCreateTime = now + 123;
    const newLockUpdateTime1 = newLockCreateTime + leaseInterval;
    const newLockUpdateTime2 = newLockCreateTime + leaseInterval * 2;
    const newLockDeleteTime = newLockUpdateTime2 + 234;

    const runner = makeRunner('extended!');
    const mock = mockDoc(
      [newLockCreateTime],
      [newLockUpdateTime1, newLockUpdateTime2],
      [newLockDeleteTime],
    );

    jest.setSystemTime(now);

    const lock = new Lock(
      mock.doc as unknown as DocumentReference<LockDoc>,
      leaseInterval,
    );
    const running = lock.withLock('extend test', runner.run);

    expect(mock.doc.onSnapshot).toBeCalledTimes(1);
    expect(mock.doc.create).not.toBeCalled;

    mock.nextSnapshot(makeEmptySnapshot());

    await runner.running;
    expect(mock.doc.create).toBeCalledTimes(1);
    expect(mock.doc.create.mock.calls[0][0]).toEqual({
      // Expiration should be lease duration + buffer == 12 seconds.
      expiration: Timestamp.fromMillis(now + 12000),
      holder: 'extend test',
    });
    expect(mock.doc.update).not.toBeCalled;
    expect(mock.doc.delete).not.toBeCalled;

    await jest.advanceTimersByTimeAsync(leaseInterval + 1000);
    expect(mock.doc.update).toBeCalledTimes(1);
    await jest.advanceTimersByTimeAsync(leaseInterval + 1000);
    expect(mock.doc.update).toBeCalledTimes(2);

    runner.finish();
    expect(await running).toBe('extended!');
    expect(mock.doc.delete).toBeCalledTimes(1);
    expect(mock.doc.delete.mock.calls[0][0]).toEqual({
      lastUpdateTime: Timestamp.fromMillis(newLockUpdateTime2),
    });
  });
});
