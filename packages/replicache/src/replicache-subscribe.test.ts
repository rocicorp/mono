import {resolver} from '@rocicorp/resolver';
import {describe, expect, test, vi} from 'vitest';
import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';
import {TestLogSink} from '../../shared/src/logging-test-utils.ts';
import {sleep} from '../../shared/src/sleep.ts';
import type {IndexKey} from './db/index.ts';
import type {IndexDefinitions} from './index-defs.ts';
import type {PatchOperation} from './patch-operation.ts';
import type {ScanOptions} from './scan-options.ts';
import {
  disableAllBackgroundProcesses,
  fetchMocker,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  tickAFewTimes,
  tickUntil,
} from './test-util.ts';
import type {ReadTransaction, WriteTransaction} from './transactions.ts';

initReplicacheTesting();

async function addData(tx: WriteTransaction, data: {[key: string]: JSONValue}) {
  for (const [key, value] of Object.entries(data)) {
    await tx.set(key, value);
  }
}

test('subscribe', async () => {
  const log: (readonly [string, ReadonlyJSONValue])[] = [];

  const rep = await replicacheForTesting('subscribe', {
    mutators: {
      addData,
    },
  });
  let queryCallCount = 0;
  const cancel = rep.subscribe(
    (tx: ReadTransaction) => {
      queryCallCount++;
      return tx.scan({prefix: 'a/'}).entries().toArray();
    },
    {
      onData: (values: Iterable<readonly [string, ReadonlyJSONValue]>) => {
        for (const entry of values) {
          log.push(entry);
        }
      },
    },
  );

  expect(log).toHaveLength(0);
  expect(queryCallCount).toBe(0);

  const add = rep.mutate.addData;
  await add({'a/0': 0});
  expect(log).toEqual([['a/0', 0]]);
  expect(queryCallCount).toBe(2); // One for initial subscribe and one for the add.

  // Changing a entry to the same value no longer triggers the subscription to
  // fire.
  log.length = 0;
  await add({'a/0': 0});
  expect(log).toEqual([]);
  expect(queryCallCount).toBe(2);

  log.length = 0;
  await add({'a/1': 1});
  expect(log).toEqual([
    ['a/0', 0],
    ['a/1', 1],
  ]);
  expect(queryCallCount).toBe(3);

  log.length = 0;
  log.length = 0;
  await add({'a/1': 11});
  expect(log).toEqual([
    ['a/0', 0],
    ['a/1', 11],
  ]);
  expect(queryCallCount).toBe(4);

  log.length = 0;
  cancel();
  await add({'a/1': 12});
  await Promise.resolve();
  expect(log).toHaveLength(0);
  expect(queryCallCount).toBe(4);
});

describe('subscribe', () => {
  async function make(
    indexes: IndexDefinitions | undefined,
    scanOptions: ScanOptions,
  ) {
    const log: (readonly [string | IndexKey, ReadonlyJSONValue])[] = [];

    const rep = await replicacheForTesting('subscribe-with-index-no-prefix', {
      mutators: {
        addData,
      },
      indexes,
    });

    const onErrorFake = vi.fn();

    let queryCallCount = 0;
    let onDataCallCount = 0;

    const cancel = rep.subscribe(
      (tx: ReadTransaction) => {
        queryCallCount++;
        return tx.scan(scanOptions).entries().toArray();
      },
      {
        onData: values => {
          onDataCallCount++;
          for (const entry of values) {
            log.push(entry);
          }
        },
        onError: onErrorFake,
      },
    );

    expect(queryCallCount).toBe(0);
    expect(onDataCallCount).toBe(0);
    expect(onErrorFake).not.toHaveBeenCalled();

    await tickUntil(vi, () => queryCallCount > 0);

    return {
      log,
      rep,
      onErrorFake,
      cancel,
      queryCallCount: () => queryCallCount,
      onDataCallCount: () => onDataCallCount,
    };
  }

  test('index with a prefix', async () => {
    const {log, rep, onErrorFake, cancel, queryCallCount, onDataCallCount} =
      await make(
        {
          i1: {
            jsonPointer: '/id',
            prefix: 'a',
          },
        },
        {indexName: 'i1'},
      );

    await tickUntil(vi, () => queryCallCount() > 0);

    await rep.mutate.addData({
      a1: {id: 'a-1', x: 1},
      a2: {id: 'a-2', x: 2},
      b: {id: 'bx'},
    });

    expect(log).toEqual([
      [
        ['a-1', 'a1'],
        {
          id: 'a-1',
          x: 1,
        },
      ],
      [
        ['a-2', 'a2'],
        {
          id: 'a-2',
          x: 2,
        },
      ],
    ]);
    expect(queryCallCount()).toBe(2); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(2);
    expect(onErrorFake).not.toHaveBeenCalled();

    log.length = 0;
    await rep.mutate.addData({a3: {id: 'a-3', x: 3}});

    expect(queryCallCount()).toBe(3);
    expect(onDataCallCount()).toBe(3);
    expect(onErrorFake).not.toHaveBeenCalled();
    expect(log).toEqual([
      [
        ['a-1', 'a1'],
        {
          id: 'a-1',
          x: 1,
        },
      ],
      [
        ['a-2', 'a2'],
        {
          id: 'a-2',
          x: 2,
        },
      ],
      [
        ['a-3', 'a3'],
        {
          id: 'a-3',
          x: 3,
        },
      ],
    ]);

    cancel();
  });

  test('missing index', async () => {
    const {rep, onErrorFake, cancel} = await make(undefined, {indexName: 'i1'});

    await rep.mutate.addData({
      a1: {id: 'a-1', x: 1},
      a2: {id: 'a-2', x: 2},
      b: {id: 'bx'},
    });

    expect(onErrorFake).toHaveBeenCalledOnce();
    const error = onErrorFake.mock.calls[0][0];
    expect(error).toBeInstanceOf(Error);
    expect(error).toHaveProperty('message', 'Unknown index name: i1');

    cancel();
  });

  test('no prefix', async () => {
    const {log, rep, cancel} = await make(
      {
        i1: {
          jsonPointer: '/id',
        },
      },
      {indexName: 'i1'},
    );

    await rep.mutate.addData({
      a1: {id: 'a-1', x: 1},
      a2: {id: 'a-2', x: 2},
      b: {id: 'bx'},
    });

    expect(log).toEqual([
      [
        ['a-1', 'a1'],
        {
          id: 'a-1',
          x: 1,
        },
      ],
      [
        ['a-2', 'a2'],
        {
          id: 'a-2',
          x: 2,
        },
      ],
      [
        ['bx', 'b'],
        {
          id: 'bx',
        },
      ],
    ]);

    cancel();
  });

  test('index and start', async () => {
    const {log, rep, cancel, queryCallCount, onDataCallCount} = await make(
      {
        i1: {
          jsonPointer: '/id',
        },
      },
      {indexName: 'i1', start: {key: 'a-2'}},
    );

    await rep.mutate.addData({
      a1: {id: 'a-1', x: 1},
      a2: {id: 'a-2', x: 2},
      b: {id: 'bx'},
    });

    expect(log).toEqual([
      [
        ['a-2', 'a2'],
        {
          id: 'a-2',
          x: 2,
        },
      ],
      [
        ['bx', 'b'],
        {
          id: 'bx',
        },
      ],
    ]);
    expect(queryCallCount()).toBe(2); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(2);

    log.length = 0;
    await rep.mutate.addData({
      b: {id: 'bx2'},
    });
    expect(log).toEqual([
      [
        ['a-2', 'a2'],
        {
          id: 'a-2',
          x: 2,
        },
      ],
      [
        ['bx2', 'b'],
        {
          id: 'bx2',
        },
      ],
    ]);
    expect(queryCallCount()).toBe(3); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(3);

    // Adding a entry that does not match the index... no id property
    await rep.mutate.addData({
      c: {noid: 'c-3'},
    });
    expect(queryCallCount()).toBe(3); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(3);

    // Changing a entry before the start key
    await rep.mutate.addData({
      a1: {id: 'a-1', x: 2},
    });
    expect(queryCallCount()).toBe(3); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(3);

    // Changing a entry to the same value we do not fire the subscription any more.
    await rep.mutate.addData({
      a2: {id: 'a-2', x: 2},
    });
    expect(queryCallCount()).toBe(3); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(3);

    cancel();
  });

  test('index and prefix', async () => {
    const {log, rep, cancel, queryCallCount, onDataCallCount} = await make(
      {
        i1: {
          jsonPointer: '/id',
        },
      },
      {indexName: 'i1', prefix: 'b'},
    );

    await rep.mutate.addData({
      a1: {id: 'a-1', x: 1},
      a2: {id: 'a-2', x: 2},
      b: {id: 'bx'},
    });

    expect(log).toEqual([
      [
        ['bx', 'b'],
        {
          id: 'bx',
        },
      ],
    ]);
    expect(queryCallCount()).toBe(2); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(2);

    log.length = 0;
    await rep.mutate.addData({
      b: {id: 'bx2'},
    });
    expect(log).toEqual([
      [
        ['bx2', 'b'],
        {
          id: 'bx2',
        },
      ],
    ]);
    expect(queryCallCount()).toBe(3); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(3);

    // Adding a entry that does not match the index... no id property
    await rep.mutate.addData({
      c: {noid: 'c-3'},
    });
    expect(queryCallCount()).toBe(3); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(3);

    // Changing a entry but still matching prefix
    await rep.mutate.addData({
      b: {id: 'bx3', x: 3},
    });
    expect(queryCallCount()).toBe(4);
    expect(onDataCallCount()).toBe(4);

    // Changing a entry to the same value will not trigger the subscription.
    await rep.mutate.addData({
      b: {id: 'bx3', x: 3},
    });
    expect(queryCallCount()).toBe(4); // One for initial subscribe and one for the add.
    expect(onDataCallCount()).toBe(4);

    cancel();
  });
});

test('subscribe with isEmpty and prefix', async () => {
  const log: boolean[] = [];

  const rep = await replicacheForTesting('subscribe-with-is-empty', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, k: string) => tx.del(k),
    },
  });

  let queryCallCount = 0;
  let onDataCallCount = 0;
  const cancel = rep.subscribe(
    (tx: ReadTransaction) => {
      queryCallCount++;
      return tx.isEmpty();
    },
    {
      onData: (empty: boolean) => {
        onDataCallCount++;
        log.push(empty);
      },
    },
  );

  expect(log).toEqual([]);
  expect(queryCallCount).toBe(0);
  expect(onDataCallCount).toBe(0);

  await tickAFewTimes(vi);

  expect(log).toEqual([true]);
  expect(queryCallCount).toBe(1);
  expect(onDataCallCount).toBe(1);

  await rep.mutate.addData({
    a: 1,
  });

  expect(log).toEqual([true, false]);
  expect(queryCallCount).toBe(2);
  expect(onDataCallCount).toBe(2);

  await rep.mutate.addData({
    b: 2,
  });

  expect(log).toEqual([true, false]);
  expect(queryCallCount).toBe(3);
  expect(onDataCallCount).toBe(2);

  await rep.mutate.del('a');

  expect(log).toEqual([true, false]);
  expect(queryCallCount).toBe(4);
  expect(onDataCallCount).toBe(2);

  await rep.mutate.del('b');

  expect(log).toEqual([true, false, true]);
  expect(queryCallCount).toBe(5);
  expect(onDataCallCount).toBe(3);

  cancel();
});

test('subscribe change keys', async () => {
  const log: ReadonlyJSONValue[][] = [];

  const rep = await replicacheForTesting('subscribe-change-keys', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, k: string) => tx.del(k),
    },
  });

  let queryCallCount = 0;
  let onDataCallCount = 0;
  const cancel = rep.subscribe(
    async (tx: ReadTransaction) => {
      queryCallCount++;
      const a = await tx.get('a');
      const rv = [a ?? 'no-a'];
      if (a === 1) {
        rv.push((await tx.get('b')) ?? 'no b');
      }
      await tx.has('c');
      return rv;
    },
    {
      onData: (values: ReadonlyJSONValue[]) => {
        onDataCallCount++;
        log.push(values);
      },
    },
  );

  expect(log).toHaveLength(0);
  expect(queryCallCount).toBe(0);
  expect(onDataCallCount).toBe(0);

  await rep.mutate.addData({
    a: 0,
  });

  expect(log).toEqual([['no-a'], [0]]);
  expect(queryCallCount).toBe(2); // One for initial subscribe and one for the add.
  expect(onDataCallCount).toBe(2);

  await rep.mutate.addData({
    b: 2,
  });
  expect(queryCallCount).toBe(2);
  expect(onDataCallCount).toBe(2);

  log.length = 0;
  await rep.mutate.addData({
    a: 1,
  });
  expect(queryCallCount).toBe(3);
  expect(onDataCallCount).toBe(3);
  expect(log).toEqual([[1, 2]]);

  log.length = 0;
  await rep.mutate.addData({
    b: 3,
  });
  expect(queryCallCount).toBe(4);
  expect(onDataCallCount).toBe(4);
  expect(log).toEqual([[1, 3]]);

  log.length = 0;
  await rep.mutate.addData({
    a: 4,
  });
  expect(queryCallCount).toBe(5);
  expect(onDataCallCount).toBe(5);
  expect(log).toEqual([[4]]);

  await rep.mutate.addData({
    b: 5,
  });
  expect(queryCallCount).toBe(5);
  expect(onDataCallCount).toBe(5);

  await rep.mutate.addData({
    c: 6,
  });
  expect(queryCallCount).toBe(6);
  expect(onDataCallCount).toBe(5);

  await rep.mutate.del('c');
  expect(queryCallCount).toBe(7);
  expect(onDataCallCount).toBe(5);

  cancel();
});

test('subscribe close', async () => {
  const rep = await replicacheForTesting('subscribe-close', {
    mutators: {addData},
  });

  const log: (ReadonlyJSONValue | undefined)[] = [];

  const cancel = rep.subscribe((tx: ReadTransaction) => tx.get('k'), {
    onData: value => log.push(value),
    onDone: () => (done = true),
  });

  expect(log).toHaveLength(0);

  const add = rep.mutate.addData;
  await add({k: 0});
  await Promise.resolve();
  expect(log).toEqual([undefined, 0]);

  let done = false;

  await rep.close();
  expect(done).toBe(true);
  cancel();
});

test('subscribe with error', async () => {
  const rep = await replicacheForTesting('suberr', {mutators: {addData}});

  const add = rep.mutate.addData;

  let gottenValue = 0;
  let error;

  const cancel = rep.subscribe(
    async tx => {
      const v = await tx.get('k');
      if (v !== undefined && v !== null) {
        throw v;
      }
      return null;
    },
    {
      onData: () => {
        gottenValue++;
      },
      onError: e => {
        error = e;
      },
    },
  );
  await Promise.resolve();

  expect(error).toBe(undefined);
  expect(gottenValue).toBe(0);

  await add({k: 'throw'});
  expect(gottenValue).toBe(1);
  await Promise.resolve();
  expect(error).toBe('throw');

  cancel();
});

test('subscribe pull and index update', async () => {
  const pullURL = 'https://pull.com/rep';
  const indexName = 'idx1';
  const rep = await replicacheForTesting(
    'subscribe-pull-and-index-update',
    {
      pullURL,
      mutators: {addData},
      indexes: {[indexName]: {jsonPointer: '/id'}},
    },
    disableAllBackgroundProcesses,
  );

  const log: ReadonlyJSONValue[] = [];
  let queryCallCount = 0;

  const cancel = rep.subscribe(
    tx => {
      queryCallCount++;
      return tx.scan({indexName}).entries().toArray();
    },
    {
      onData(res) {
        log.push(res);
      },
    },
  );

  let lastMutationID = 0;
  let cookie = 0;

  let expectedQueryCallCount = 1;

  async function testPull(opt: {
    patch: PatchOperation[];
    expectedLog: JSONValue[];
    expectChange: boolean;
  }) {
    if (opt.expectChange) {
      expectedQueryCallCount++;
    }
    log.length = 0;
    const {clientID} = rep;
    fetchMocker.post(
      pullURL,
      makePullResponseV1(clientID, lastMutationID++, opt.patch, cookie++),
    );

    rep.pullIgnorePromise();
    await tickUntil(vi, () => log.length >= opt.expectedLog.length);
    expect(queryCallCount).toBe(expectedQueryCallCount);
    expect(log).toEqual(opt.expectedLog);
  }

  await testPull({patch: [], expectedLog: [[]], expectChange: false});

  await testPull({
    patch: [
      {
        op: 'put',
        key: 'a1',
        value: {id: 'a-1', x: 1},
      },
    ],
    expectedLog: [
      [
        [
          ['a-1', 'a1'],
          {
            id: 'a-1',
            x: 1,
          },
        ],
      ],
    ],
    expectChange: true,
  });

  // Same value
  await testPull({
    patch: [
      {
        op: 'put',
        key: 'a1',
        value: {id: 'a-1', x: 1},
      },
    ],
    expectedLog: [],
    expectChange: false,
  });

  // Change value
  await testPull({
    patch: [
      {
        op: 'put',
        key: 'a1',
        value: {id: 'a-1', x: 2},
      },
    ],
    expectedLog: [
      [
        [
          ['a-1', 'a1'],
          {
            id: 'a-1',
            x: 2,
          },
        ],
      ],
    ],
    expectChange: true,
  });

  // Not matching index json patch
  await testPull({
    patch: [
      {
        op: 'put',
        key: 'b1',
        value: {notAnId: 'b-1', x: 1},
      },
    ],
    expectedLog: [],
    expectChange: false,
  });

  // Clear
  await testPull({
    patch: [
      {
        op: 'clear',
      },
    ],
    expectedLog: [[]],
    expectChange: true,
  });

  // Add again so we can test del...
  await testPull({
    patch: [
      {
        op: 'put',
        key: 'a2',
        value: {id: 'a-2', x: 2},
      },
    ],
    expectedLog: [
      [
        [
          ['a-2', 'a2'],
          {
            id: 'a-2',
            x: 2,
          },
        ],
      ],
    ],
    expectChange: true,
  });
  // .. and del
  await testPull({
    patch: [
      {
        op: 'del',
        key: 'a2',
      },
    ],
    expectedLog: [[]],
    expectChange: true,
  });

  cancel();
});

test('subscription coalescing', async () => {
  const rep = await replicacheForTesting(
    'subscription-coalescing',
    {
      mutators: {addData},
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  const store = rep.memdag;
  const readSpy = vi.spyOn(store, 'read');
  const writeSpy = vi.spyOn(store, 'write');
  const closeSpy = vi.spyOn(store, 'close');

  const resetCounters = () => {
    readSpy.mockClear();
    writeSpy.mockClear();
    closeSpy.mockClear();
  };

  expect(readSpy).not.toHaveBeenCalled();
  expect(writeSpy).not.toHaveBeenCalled();
  expect(closeSpy).not.toHaveBeenCalled();
  resetCounters();

  const resolverA = resolver<void>();
  const resolverB = resolver<void>();
  const resolverC = resolver<void>();

  const log: string[] = [];
  const ca = rep.subscribe(tx => tx.has('a'), {
    onData() {
      log.push('a');
      resolverA.resolve();
    },
  });
  const cb = rep.subscribe(tx => tx.has('b'), {
    onData() {
      log.push('b');
      resolverB.resolve();
    },
  });
  const cc = rep.subscribe(tx => tx.has('c'), {
    onData() {
      log.push('c');
      resolverC.resolve();
    },
  });

  await Promise.all([resolverA.promise, resolverB.promise, resolverC.promise]);

  expect(log).toEqual(['a', 'b', 'c']);

  expect(readSpy).toHaveBeenCalledTimes(1);
  expect(writeSpy).toHaveBeenCalledTimes(0);
  expect(closeSpy).toHaveBeenCalledTimes(0);
  resetCounters();

  ca();
  cb();
  cc();
  log.length = 0;
  rep.subscribe(tx => tx.has('d'), {
    onData() {
      log.push('d');
    },
  });
  rep.subscribe(tx => tx.has('e'), {
    onData() {
      log.push('e');
    },
  });

  expect(readSpy).toHaveBeenCalledTimes(0);
  expect(writeSpy).toHaveBeenCalledTimes(0);
  expect(closeSpy).toHaveBeenCalledTimes(0);
  resetCounters();

  await rep.mutate.addData({a: 1});

  expect(readSpy).toHaveBeenCalledTimes(1);
  expect(writeSpy).toHaveBeenCalledTimes(1);
  expect(closeSpy).toHaveBeenCalledTimes(0);
  resetCounters();

  expect(log).toEqual(['d', 'e']);
});

test('subscribe perf test regression', async () => {
  const count = 100;
  const maxCount = 1000;
  const minCount = 10;
  const key = (k: number) => `key${k}`;
  const rep = await replicacheForTesting('subscribe-perf-test-regression', {
    mutators: {
      async init(tx: WriteTransaction) {
        await Promise.all(
          Array.from({length: maxCount}, (_, i) => tx.set(key(i), i)),
        );
      },
      async put(tx: WriteTransaction, options: {key: string; val: JSONValue}) {
        await tx.set(options.key, options.val);
      },
    },
  });
  vi.useRealTimers();

  await rep.mutate.init();
  const data = Array.from({length: count}).fill(0);
  let onDataCallCount = 0;
  const subs = Array.from({length: count}, (_, i) =>
    rep.subscribe(tx => tx.get(key(i)), {
      onData(v) {
        onDataCallCount++;
        data[i] = v;
      },
    }),
  );

  // We need to wait until all the initial async onData have been called.
  while (onDataCallCount !== count) {
    await sleep(10);
  }

  // The number of mutations to do. These should each trigger one
  // subscription. The goal of this test is to ensure that we are only
  // paying the runtime cost of subscriptions that are affected by the
  // changes.
  const mut = 10;
  if (mut < minCount) {
    throw new Error('Please decrease minCount');
  }
  const rand = Math.random();

  for (let i = 0; i < mut; i++) {
    await rep.mutate.put({key: key(i), val: i ** 2 + rand});
  }

  subs.forEach(c => c());

  await sleep(100);

  expect(onDataCallCount).toBe(count + mut);
  for (let i = 0; i < count; i++) {
    expect(data[i]).toBe(i < mut ? i ** 2 + rand : i);
  }
});

test('subscription with error in body', async () => {
  const rep = await replicacheForTesting('subscription-with-error-in-body', {
    mutators: {
      addData,
    },
  });

  let bodyCallCounter = 0;
  let errorCounter = 0;
  const letters = 'abc';

  rep.subscribe(
    async tx => {
      bodyCallCounter++;
      const a = await tx.get('a');
      if (a === undefined) {
        throw new Error('a is undefined');
      }
      const b = await tx.get('b');
      if (b === undefined) {
        throw new Error('b is undefined');
      }
      const c = await tx.get('c');
      if (c === undefined) {
        throw new Error('c is undefined');
      }
      return {a, b, c};
    },
    {
      onData(data) {
        expect(data).toEqual({a: 1, b: 2, c: 3});
      },
      onError(err) {
        expect(err).toBeInstanceOf(Error);
        expect(err, `Error for ${errorCounter} is incorrect`).toHaveProperty(
          'message',
          letters[errorCounter++] + ' is undefined',
        );
      },
    },
  );

  await tickUntil(vi, () => bodyCallCounter === 1);

  await rep.mutate.addData({a: 1});
  expect(bodyCallCounter).toBe(2);

  await rep.mutate.addData({b: 2});
  expect(bodyCallCounter).toBe(3);

  await rep.mutate.addData({c: 3});
  expect(bodyCallCounter).toBe(4);
});

test('Errors in subscriptions are logged if no onError', async () => {
  const t = async (
    onError?: (err: unknown) => void,
    err: unknown = new Error('a'),
  ) => {
    let called = false;
    const testLogSink = new TestLogSink();
    const rep = await replicacheForTesting('subscription-with-exception', {
      logSinks: [testLogSink],
      logLevel: 'error',
    });

    rep.subscribe(
      () => {
        called = true;
        return Promise.reject(err);
      },
      {
        onData: () => {
          throw new Error('Should not be called');
        },
        onError,
      },
    );

    await tickUntil(vi, () => called);
    if (onError) {
      expect(testLogSink.messages.length).toBe(0);
    } else {
      expect(testLogSink.messages.length).toBe(1);
      expect(testLogSink.messages[0]).toEqual([
        'error',
        {name: rep.name},
        ['Error in subscription body:', err],
      ]);
    }

    await rep.close();
  };

  await t();

  const f = vi.fn();
  const err = new Error('b');
  await t(f, err);
  expect(f).toHaveBeenCalledOnce();
  expect(f).toHaveBeenCalledWith(err);
});

test('subscribe using a function', async () => {
  const log: (readonly [string, ReadonlyJSONValue])[] = [];

  const rep = await replicacheForTesting('subscribe', {
    mutators: {
      addData,
    },
  });
  let queryCallCount = 0;
  const cancel = rep.subscribe(
    tx => {
      queryCallCount++;
      return tx.scan({prefix: 'a/'}).entries().toArray();
    },
    values => {
      for (const entry of values) {
        log.push(entry);
      }
    },
  );

  expect(log).toHaveLength(0);
  expect(queryCallCount).toBe(0);

  const add = rep.mutate.addData;
  await add({'a/0': 0});
  expect(log).toEqual([['a/0', 0]]);
  expect(queryCallCount).toBe(2); // One for initial subscribe and one for the add.

  cancel();
});

test('subscribe where body returns non json', async () => {
  const log: unknown[] = [];

  const rep = await replicacheForTesting('subscribe-non-json-result', {
    mutators: {
      addData,
    },
  });
  const cancel = rep.subscribe(
    async tx => {
      const entries = await tx.scan().entries().toArray();
      return new Map(entries.map(([k, v]) => [k, BigInt(v as number)]));
    },
    {
      onData(values) {
        expect(values).toBeInstanceOf(Map);
        for (const entry of values) {
          log.push(entry);
        }
      },
      isEqual(a, b) {
        if (!(a instanceof Map) || !(b instanceof Map) || a.size !== b.size) {
          return false;
        }
        for (const [k, v] of a) {
          if (b.get(k) !== v) {
            return false;
          }
        }
        return true;
      },
    },
  );

  await rep.mutate.addData({a: 0, b: 1});
  expect(log).toEqual([
    ['a', 0n],
    ['b', 1n],
  ]);

  cancel();
});
