import type {Context, LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {
  clearBrowserOverrides,
  overrideBrowserGlobal,
} from '../../shared/src/browser-env.ts';
import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';
import {promiseVoid} from '../../shared/src/resolved-promises.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {asyncIterableToArray} from './async-iterable-to-array.ts';
import {Write} from './db/write.ts';
import {httpStatusUnauthorized} from './http-status-unauthorized.ts';
import {TestMemStore} from './kv/test-mem-store.ts';
import type {PatchOperation} from './patch-operation.ts';
import {getClientGroup} from './persist/client-groups.ts';
import {deleteClientForTesting} from './persist/clients-test-helpers.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import {Replicache} from './replicache.ts';
import type {ScanOptions} from './scan-options.ts';
import type {ClientID} from './sync/ids.ts';
import {
  MemStoreWithCounters,
  ReplicacheTest,
  addData,
  disableAllBackgroundProcesses,
  expectAsyncFuncToThrow,
  expectConsoleLogContextStub,
  expectLogContext,
  fetchMocker,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  requestIDLogContextRegex,
  tickAFewTimes,
  tickUntil,
} from './test-util.ts';
import {TransactionClosedError} from './transaction-closed-error.ts';
import type {ReadTransaction, WriteTransaction} from './transactions.ts';
import type {MutatorDefs, Poke} from './types.ts';
import {withRead} from './with-transactions.ts';

initReplicacheTesting();

test('name is required', () => {
  expect(
    () => new Replicache({} as ReplicacheOptions<Record<string, never>>),
  ).toThrow(/name.*required/);
});

test('name cannot be empty', () => {
  expect(() => new Replicache({name: ''})).toThrow(/name.*must be non-empty/);
});

test('cookie', async () => {
  const pullURL = 'https://pull.com/rep';
  const rep = await replicacheForTesting('test2', {
    pullURL,
  });
  expect(await rep.impl.cookie).toBe(null);

  let pullDone = false;
  fetchMocker.post(pullURL, () => {
    pullDone = true;
    return makePullResponseV1(rep.clientID, 0, [], 'newCookie');
  });

  await rep.pull();

  await tickUntil(vi, () => pullDone);
  await tickAFewTimes(vi);

  expect(await rep.impl.cookie).toBe('newCookie');
  fetchMocker.reset();
});

test('get, has, scan on empty db', async () => {
  const rep = await replicacheForTesting('test2');
  async function t(tx: ReadTransaction) {
    expect(await tx.get('key')).toBe(undefined);
    expect(await tx.has('key')).toBe(false);

    const scanItems = await asyncIterableToArray(tx.scan());
    expect(scanItems).toHaveLength(0);
  }

  await rep.query(t);
});

test('put, get, has, del inside tx', async () => {
  const rep = await replicacheForTesting('test3', {
    mutators: {
      testMut: async (
        tx: WriteTransaction,
        args: {key: string; value: JSONValue},
      ) => {
        const {key, value} = args;
        await tx.set(key, value);
        expect(await tx.has(key)).toBe(true);
        const v = await tx.get(key);
        expect(v).toEqual(value);

        expect(await tx.del(key)).toBe(true);
        expect(await tx.has(key)).toBe(false);
      },
    },
  });

  const {testMut} = rep.mutate;

  for (const [key, value] of Object.entries({
    a: true,
    b: false,
    c: null,
    d: 'string',
    e: 12,
    f: {},
    g: [],
    h: {h1: true},
    i: [0, 1],
  })) {
    await testMut({key, value: value as JSONValue});
  }
});

async function testScanResult<K, V>(
  rep: Replicache,
  options: ScanOptions | undefined,
  entries: [K, V][],
) {
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options).entries())).toEqual(
      entries,
    );
  });
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options))).toEqual(
      entries.map(([, v]) => v),
    );
  });
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options).values())).toEqual(
      entries.map(([, v]) => v),
    );
  });
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options).keys())).toEqual(
      entries.map(([k]) => k),
    );
  });

  await rep.query(async tx => {
    expect(await tx.scan(options).toArray()).toEqual(entries.map(([, v]) => v));
  });
  // scan().xxx().toArray()
  await rep.query(async tx => {
    expect(await tx.scan(options).entries().toArray()).toEqual(entries);
  });
  await rep.query(async tx => {
    expect(await tx.scan(options).values().toArray()).toEqual(
      entries.map(([, v]) => v),
    );
  });
  await rep.query(async tx => {
    expect(await tx.scan(options).keys().toArray()).toEqual(
      entries.map(([k]) => k),
    );
  });
}

test('scan', async () => {
  const rep = await replicacheForTesting('test4', {
    mutators: {
      addData,
    },
  });
  const add = rep.mutate.addData;
  await add({
    'a/0': 0,
    'a/1': 1,
    'a/2': 2,
    'a/3': 3,
    'a/4': 4,
    'b/0': 5,
    'b/1': 6,
    'b/2': 7,
    'c/0': 8,
  });

  await testScanResult(rep, undefined, [
    ['a/0', 0],
    ['a/1', 1],
    ['a/2', 2],
    ['a/3', 3],
    ['a/4', 4],
    ['b/0', 5],
    ['b/1', 6],
    ['b/2', 7],
    ['c/0', 8],
  ]);

  await testScanResult(rep, {prefix: 'a'}, [
    ['a/0', 0],
    ['a/1', 1],
    ['a/2', 2],
    ['a/3', 3],
    ['a/4', 4],
  ]);

  await testScanResult(rep, {prefix: 'b'}, [
    ['b/0', 5],
    ['b/1', 6],
    ['b/2', 7],
  ]);

  await testScanResult(rep, {prefix: 'c/'}, [['c/0', 8]]);

  await testScanResult(
    rep,
    {
      start: {key: 'b/1', exclusive: false},
    },
    [
      ['b/1', 6],
      ['b/2', 7],
      ['c/0', 8],
    ],
  );

  await testScanResult(
    rep,
    {
      start: {key: 'b/1'},
    },
    [
      ['b/1', 6],
      ['b/2', 7],
      ['c/0', 8],
    ],
  );

  await testScanResult(
    rep,
    {
      start: {key: 'b/1', exclusive: true},
    },
    [
      ['b/2', 7],
      ['c/0', 8],
    ],
  );

  await testScanResult(
    rep,
    {
      limit: 3,
    },
    [
      ['a/0', 0],
      ['a/1', 1],
      ['a/2', 2],
    ],
  );

  await testScanResult(
    rep,
    {
      limit: 10,
      prefix: 'a/',
    },
    [
      ['a/0', 0],
      ['a/1', 1],
      ['a/2', 2],
      ['a/3', 3],
      ['a/4', 4],
    ],
  );

  await testScanResult(
    rep,
    {
      limit: 1,
      prefix: 'b/',
    },
    [['b/0', 5]],
  );
});

test('name', async () => {
  const repA = await replicacheForTesting('a', {mutators: {addData}});
  const repB = await replicacheForTesting('b', {mutators: {addData}});

  const addA = repA.mutate.addData;
  const addB = repB.mutate.addData;

  await addA({key: 'A'});
  await addB({key: 'B'});

  expect(await repA.query(tx => tx.get('key'))).toBe('A');
  expect(await repB.query(tx => tx.get('key'))).toBe('B');

  await repA.close();
  await repB.close();

  indexedDB.deleteDatabase(repA.idbName);
  indexedDB.deleteDatabase(repB.idbName);
});

test('register with error', async () => {
  const rep = await replicacheForTesting('regerr', {
    mutators: {
      // oxlint-disable-next-line require-await
      err: async (_: WriteTransaction, args: number) => {
        throw args;
      },
    },
  });

  const doErr = rep.mutate.err;

  try {
    await doErr(42);
    unreachable();
  } catch (ex) {
    expect(ex).toBe(42);
  }
});

test('overlapping writes', async () => {
  async function dbWait(tx: ReadTransaction, dur: number) {
    // Try to take setTimeout away from me???
    const t0 = Date.now();
    while (Date.now() - t0 > dur) {
      await tx.get('foo');
    }
  }

  const pushURL = 'https://push.com';
  // writes wait on writes
  const rep = await replicacheForTesting('conflict', {
    pushURL,
    mutators: {
      'wait-then-return': async <T extends JSONValue>(
        tx: ReadTransaction,
        {duration, ret}: {duration: number; ret: T},
      ) => {
        await dbWait(tx, duration);
        return ret;
      },
    },
  });
  fetchMocker.post(pushURL, {});

  const mut = rep.mutate['wait-then-return'];

  let resA = mut({duration: 250, ret: 'a'});
  // create a gap to make sure resA starts first (our rwlock isn't fair).
  await vi.advanceTimersByTimeAsync(100);
  let resB = mut({duration: 0, ret: 'b'});
  // race them, a should complete first, indicating that b waited
  expect(await Promise.race([resA, resB])).toBe('a');
  // wait for the other to finish so that we're starting from null state for next one.
  await Promise.all([resA, resB]);

  // reads wait on writes
  resA = mut({duration: 250, ret: 'a'});
  await vi.advanceTimersByTimeAsync(100);
  resB = rep.query(() => 'b');
  await tickAFewTimes(vi);
  expect(await Promise.race([resA, resB])).toBe('a');

  await tickAFewTimes(vi);
  await resA;
  await tickAFewTimes(vi);
  await resB;
});

test('push delay', async () => {
  const pushURL = 'https://push.com';

  const rep = await replicacheForTesting('push', {
    auth: '1',
    pushURL,
    pushDelay: 1,
    mutators: {
      createTodo: async <A extends {id: number}>(
        tx: WriteTransaction,
        args: A,
      ) => {
        await tx.set(`/todo/${args.id}`, args);
      },
    },
  });

  const {createTodo} = rep.mutate;

  const id1 = 14323534;

  await tickAFewTimes(vi);
  fetchMocker.reset();

  fetchMocker.postOnce(pushURL, {
    mutationInfos: [],
  });

  expect(fetchMocker.spy).not.toHaveBeenCalled();

  await createTodo({id: id1});

  expect(fetchMocker.spy).not.toHaveBeenCalled();

  await tickAFewTimes(vi);

  expect(fetchMocker.spy).toHaveBeenCalledOnce();
});

test('reauth push', async () => {
  const pushURL = 'https://diff.com/push';

  const rep = await replicacheForTesting('reauth', {
    pushURL,
    pushDelay: 0,
    mutators: {
      noop() {
        // no op
      },
    },
  });

  const consoleErrorStub = vi.spyOn(console, 'error');
  const getAuthFake = vi.fn().mockReturnValue(null);
  rep.getAuth = getAuthFake;

  await tickAFewTimes(vi);

  fetchMocker.post(pushURL, {body: 'xxx', status: httpStatusUnauthorized});

  await rep.mutate.noop();
  await tickUntil(vi, () => getAuthFake.mock.calls.length > 0, 1);

  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[0],
    'Got a non 200 response doing push: 401: xxx',
    ['push', requestIDLogContextRegex],
  );

  {
    await tickAFewTimes(vi);

    const consoleInfoStub = vi.spyOn(console, 'info');
    const getAuthFake = vi.fn(() => 'boo');
    rep.getAuth = getAuthFake;

    await rep.mutate.noop();
    await tickUntil(vi, () => consoleInfoStub.mock.calls.length > 0, 1);

    expectConsoleLogContextStub(
      rep.name,
      consoleInfoStub.mock.calls[0],
      'Tried to reauthenticate too many times',
      ['push'],
    );
  }
});

test('HTTP status pull', async () => {
  const pullURL = 'https://diff.com/pull';

  const rep = await replicacheForTesting('http-status-pull', {
    pullURL,
  });

  const {clientID} = rep;
  let okCalled = false;
  let i = 0;
  fetchMocker.post(pullURL, () => {
    switch (i++) {
      case 0:
        return {body: 'internal error', status: 500};
      case 1:
        return {body: 'not found', status: 404};
      case 2:
        return {body: 'created', status: 201};
      case 3:
        return {status: 204};
      default: {
        okCalled = true;
        return {body: makePullResponseV1(clientID, undefined), status: 200};
      }
    }
  });

  const consoleErrorStub = vi.spyOn(console, 'error');

  rep.pullIgnorePromise({now: true});

  await tickAFewTimes(vi, 60, 10);

  expect(consoleErrorStub).toHaveBeenCalledTimes(4);
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[0],
    'Got a non 200 response doing pull: 500: internal error',
    ['pull', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[1],
    'Got a non 200 response doing pull: 404: not found',
    ['pull', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[2],
    'Got a non 200 response doing pull: 201: created',
    ['pull', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.lastCall!,
    'Got a non 200 response doing pull: 204',
    ['pull', requestIDLogContextRegex],
  );

  expect(okCalled).toBe(true);
});

test('HTTP status push', async () => {
  const pushURL = 'https://diff.com/push';

  const rep = await replicacheForTesting('http-status-push', {
    pushURL,
    pushDelay: 1,
    mutators: {addData},
  });
  const add = rep.mutate.addData;

  let okCalled = false;
  let i = 0;
  fetchMocker.post(pushURL, () => {
    switch (i++) {
      case 0:
        return {body: 'internal error', status: 500};
      case 1:
        return {body: 'not found', status: 404};
      case 2:
        return {body: 'created', status: 201};
      case 3:
        return {status: 204};
      default:
        okCalled = true;
        return {body: {}, status: 200};
    }
  });

  const consoleErrorStub = vi.spyOn(console, 'error');

  await add({
    a: 0,
  });

  await tickAFewTimes(vi, 60, 10);

  expect(consoleErrorStub).toHaveBeenCalledTimes(4);
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[0],
    'Got a non 200 response doing push: 500: internal error',
    ['push', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[1],
    'Got a non 200 response doing push: 404: not found',
    ['push', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[2],
    'Got a non 200 response doing push: 201: created',
    ['push', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.lastCall!,
    'Got a non 200 response doing push: 204',
    ['push', requestIDLogContextRegex],
  );

  expect(okCalled).toBe(true);
});

test('closed tx', async () => {
  const rep = await replicacheForTesting('reauth', {
    mutators: {
      mut: (tx: WriteTransaction) => {
        wtx = tx;
      },
    },
  });

  let rtx: ReadTransaction;
  await rep.query(tx => (rtx = tx));

  await expectAsyncFuncToThrow(() => rtx.get('x'), TransactionClosedError);
  await expectAsyncFuncToThrow(() => rtx.has('y'), TransactionClosedError);
  await expectAsyncFuncToThrow(
    () => rtx.scan().values().next(),
    TransactionClosedError,
  );

  let wtx: WriteTransaction | undefined;

  await rep.mutate.mut();
  expect(wtx).not.toBeUndefined();
  await expectAsyncFuncToThrow(() => wtx?.set('z', 1), TransactionClosedError);
  await expectAsyncFuncToThrow(() => wtx?.del('w'), TransactionClosedError);
});

test('pullInterval in constructor', async () => {
  const rep = await replicacheForTesting('pullInterval', {
    pullInterval: 12.34,
  });
  expect(rep.pullInterval).toBe(12.34);
  await rep.close();
});

test('index in options', async () => {
  const rep = await replicacheForTesting('test-index-in-options', {
    mutators: {addData},
    indexes: {
      aIndex: {jsonPointer: '/a'},
      bc: {prefix: 'c/', jsonPointer: '/bc'},
      dIndex: {jsonPointer: '/d/e/f'},
      emptyKeyIndex: {jsonPointer: '/'},
    },
  });

  await testScanResult(rep, {indexName: 'aIndex'}, []);

  const add = rep.mutate.addData;
  await add({
    'a/0': {a: '0'},
    'a/1': {a: '1'},
    'a/2': {a: '2'},
    'a/3': {a: '3'},
    'a/4': {a: '4'},
    'b/0': {bc: '5'},
    'b/1': {bc: '6'},
    'b/2': {bc: '7'},
    'c/0': {bc: '8'},
    'd/0': {d: {e: {f: '9'}}},
  });

  await testScanResult(rep, {indexName: 'aIndex'}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'a/1'], {a: '1'}],
    [['2', 'a/2'], {a: '2'}],
    [['3', 'a/3'], {a: '3'}],
    [['4', 'a/4'], {a: '4'}],
  ]);

  await testScanResult(rep, {indexName: 'aIndex', limit: 3}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'a/1'], {a: '1'}],
    [['2', 'a/2'], {a: '2'}],
  ]);

  await testScanResult(rep, {indexName: 'bc'}, [[['8', 'c/0'], {bc: '8'}]]);
  await add({
    'c/1': {bc: '88'},
  });
  await testScanResult(rep, {indexName: 'bc'}, [
    [['8', 'c/0'], {bc: '8'}],
    [['88', 'c/1'], {bc: '88'}],
  ]);

  await testScanResult(rep, {indexName: 'dIndex'}, [
    [['9', 'd/0'], {d: {e: {f: '9'}}}],
  ]);

  await add({
    'e/0': {'': ''},
  });

  await testScanResult(rep, {indexName: 'emptyKeyIndex'}, [
    [['', 'e/0'], {'': ''}],
  ]);
});

test('allow redefinition of indexes', async () => {
  const rep = await replicacheForTesting(
    'index-redefinition',
    {
      mutators: {addData},
      indexes: {
        aIndex: {jsonPointer: '/a'},
      },
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  await populateDataUsingPull(rep, {
    'a/0': {a: '0'},
    'a/1': {a: '1'},
    'b/2': {a: '2'},
    'b/3': {a: '3'},
  });

  await testScanResult(rep, {indexName: 'aIndex'}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'a/1'], {a: '1'}],
    [['2', 'b/2'], {a: '2'}],
    [['3', 'b/3'], {a: '3'}],
  ]);

  await rep.close();

  const rep2 = await replicacheForTesting(
    rep.name,
    {
      mutators: {addData},
      indexes: {
        aIndex: {jsonPointer: '/a', prefix: 'b'},
      },
    },
    {
      enablePullAndPushInOpen: false,
    },
    {useUniqueName: false},
  );

  await testScanResult(rep2, {indexName: 'aIndex'}, [
    [['2', 'b/2'], {a: '2'}],
    [['3', 'b/3'], {a: '3'}],
  ]);

  await rep2.close();
});

test('add more indexes', async () => {
  const rep = await replicacheForTesting(
    'index-add-more',
    {
      mutators: {addData},
      indexes: {
        aIndex: {jsonPointer: '/a'},
      },
    },
    disableAllBackgroundProcesses,
  );

  await populateDataUsingPull(rep, {
    'a/0': {a: '0'},
    'b/1': {a: '1'},
    'b/2': {a: '2'},
    'b/3': {b: '3'},
  });

  await testScanResult(rep, {indexName: 'aIndex'}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'b/1'], {a: '1'}],
    [['2', 'b/2'], {a: '2'}],
  ]);

  await rep.close();

  const rep2 = await replicacheForTesting(
    rep.name,
    {
      mutators: {addData},
      indexes: {
        aIndex: {jsonPointer: '/a'},
        bIndex: {jsonPointer: '/b'},
      },
    },
    undefined,
    {useUniqueName: false},
  );

  await testScanResult(rep, {indexName: 'aIndex'}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'b/1'], {a: '1'}],
    [['2', 'b/2'], {a: '2'}],
  ]);

  await testScanResult(rep2, {indexName: 'bIndex'}, [[['3', 'b/3'], {b: '3'}]]);

  await rep2.close();
});

test('add index definition with prefix', async () => {
  const rep = await replicacheForTesting(
    'index-add-more',
    {
      mutators: {addData},
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  await populateDataUsingPull(rep, {
    'a/0': {a: '0'},
    'a/1': {b: '1'},
    'b/2': {a: '2'},
    'b/3': {b: '3'},
  });

  await rep.close();

  const rep2 = await replicacheForTesting(
    rep.name,
    {
      mutators: {addData},
      indexes: {
        aIndex: {jsonPointer: '/a', prefix: 'a'},
      },
    },
    {
      enablePullAndPushInOpen: false,
    },
    {useUniqueName: false},
  );

  await testScanResult(rep2, {indexName: 'aIndex'}, [[['0', 'a/0'], {a: '0'}]]);

  await rep2.close();
});

test('rename indexes', async () => {
  const rep = await replicacheForTesting(
    'index-add-more',
    {
      mutators: {addData},
      indexes: {
        aIndex: {jsonPointer: '/a'},
        bIndex: {jsonPointer: '/b'},
      },
    },
    disableAllBackgroundProcesses,
  );
  await populateDataUsingPull(rep, {
    'a/0': {a: '0'},
    'b/1': {a: '1'},
    'b/2': {a: '2'},
    'b/3': {b: '3'},
  });

  await testScanResult(rep, {indexName: 'aIndex'}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'b/1'], {a: '1'}],
    [['2', 'b/2'], {a: '2'}],
  ]);

  await testScanResult(rep, {indexName: 'bIndex'}, [[['3', 'b/3'], {b: '3'}]]);

  await rep.close();

  const rep2 = await replicacheForTesting(
    rep.name,
    {
      mutators: {addData},
      indexes: {
        bIndex: {jsonPointer: '/a'},
        aIndex: {jsonPointer: '/b'},
      },
    },
    undefined,
    {useUniqueName: false},
  );

  await testScanResult(rep2, {indexName: 'bIndex'}, [
    [['0', 'a/0'], {a: '0'}],
    [['1', 'b/1'], {a: '1'}],
    [['2', 'b/2'], {a: '2'}],
  ]);

  await testScanResult(rep2, {indexName: 'aIndex'}, [[['3', 'b/3'], {b: '3'}]]);

  await rep2.close();
});

test('index array', async () => {
  const rep = await replicacheForTesting('test-index', {
    mutators: {addData},
    indexes: {aIndex: {jsonPointer: '/a'}},
  });

  const add = rep.mutate.addData;
  await add({
    'a/0': {a: []},
    'a/1': {a: ['0']},
    'a/2': {a: ['1', '2']},
    'a/3': {a: '3'},
    'a/4': {a: ['4']},
    'b/0': {bc: '5'},
    'b/1': {bc: '6'},
    'b/2': {bc: '7'},
    'c/0': {bc: '8'},
  });

  await testScanResult(rep, {indexName: 'aIndex'}, [
    [['0', 'a/1'], {a: ['0']}],
    [['1', 'a/2'], {a: ['1', '2']}],
    [['2', 'a/2'], {a: ['1', '2']}],
    [['3', 'a/3'], {a: '3'}],
    [['4', 'a/4'], {a: ['4']}],
  ]);
});

test('index scan start', async () => {
  const rep = await replicacheForTesting('test-index-scan', {
    mutators: {addData},
    indexes: {
      bIndex: {
        jsonPointer: '/b',
      },
    },
  });

  const add = rep.mutate.addData;
  await add({
    'a/1': {a: '0'},
    'b/0': {b: 'a5'},
    'b/1': {b: 'a6'},
    'b/2': {b: 'b7'},
    'b/3': {b: 'b8'},
  });

  for (const key of ['a6', ['a6'], ['a6', undefined], ['a6', '']] as (
    | string
    | [string, string?]
  )[]) {
    await testScanResult(rep, {indexName: 'bIndex', start: {key}}, [
      [['a6', 'b/1'], {b: 'a6'}],
      [['b7', 'b/2'], {b: 'b7'}],
      [['b8', 'b/3'], {b: 'b8'}],
    ]);
    await testScanResult(
      rep,
      {indexName: 'bIndex', start: {key, exclusive: false}},
      [
        [['a6', 'b/1'], {b: 'a6'}],
        [['b7', 'b/2'], {b: 'b7'}],
        [['b8', 'b/3'], {b: 'b8'}],
      ],
    );
  }

  for (const key of ['a6', ['a6'], ['a6', undefined]] as (
    | string
    | [string, string?]
  )[]) {
    await testScanResult(
      rep,
      {indexName: 'bIndex', start: {key, exclusive: false}},
      [
        [['a6', 'b/1'], {b: 'a6'}],
        [['b7', 'b/2'], {b: 'b7'}],
        [['b8', 'b/3'], {b: 'b8'}],
      ],
    );
    await testScanResult(
      rep,
      {indexName: 'bIndex', start: {key: ['a6', ''], exclusive: true}},
      [
        [['a6', 'b/1'], {b: 'a6'}],
        [['b7', 'b/2'], {b: 'b7'}],
        [['b8', 'b/3'], {b: 'b8'}],
      ],
    );
  }

  for (const key of ['a6', ['a6'], ['a6', undefined]] as (
    | string
    | [string, string?]
  )[]) {
    await testScanResult(
      rep,
      {indexName: 'bIndex', start: {key, exclusive: true}},
      [
        [['b7', 'b/2'], {b: 'b7'}],
        [['b8', 'b/3'], {b: 'b8'}],
      ],
    );
  }

  await testScanResult(
    rep,
    {indexName: 'bIndex', start: {key: ['b7', 'b/2']}},
    [
      [['b7', 'b/2'], {b: 'b7'}],
      [['b8', 'b/3'], {b: 'b8'}],
    ],
  );
  await testScanResult(
    rep,
    {indexName: 'bIndex', start: {key: ['b7', 'b/2'], exclusive: false}},
    [
      [['b7', 'b/2'], {b: 'b7'}],
      [['b8', 'b/3'], {b: 'b8'}],
    ],
  );
  await testScanResult(
    rep,
    {indexName: 'bIndex', start: {key: ['b7', 'b/2'], exclusive: true}},
    [[['b8', 'b/3'], {b: 'b8'}]],
  );

  await testScanResult(
    rep,
    {indexName: 'bIndex', start: {key: ['a6', 'b/2']}},
    [
      [['b7', 'b/2'], {b: 'b7'}],
      [['b8', 'b/3'], {b: 'b8'}],
    ],
  );
  await testScanResult(
    rep,
    {indexName: 'bIndex', start: {key: ['a6', 'b/2'], exclusive: false}},
    [
      [['b7', 'b/2'], {b: 'b7'}],
      [['b8', 'b/3'], {b: 'b8'}],
    ],
  );
  await testScanResult(
    rep,
    {indexName: 'bIndex', start: {key: ['a6', 'b/2'], exclusive: true}},
    [
      [['b7', 'b/2'], {b: 'b7'}],
      [['b8', 'b/3'], {b: 'b8'}],
    ],
  );
});

test('logLevel', async () => {
  const info = vi.spyOn(console, 'info');
  const debug = vi.spyOn(console, 'debug');

  // Just testing that we get some output
  let rep = await replicacheForTesting('log-level', {logLevel: 'error'});
  await rep.query(() => 42);
  expect(info).toHaveBeenCalledTimes(0);
  await rep.close();

  info.mockClear();
  debug.mockClear();
  await tickAFewTimes(vi, 10, 100);

  rep = await replicacheForTesting('log-level', {logLevel: 'info'});
  await rep.query(() => 42);
  expect(info).toHaveBeenCalledTimes(0);
  expect(debug).toHaveBeenCalledTimes(0);
  await rep.close();

  info.mockClear();
  debug.mockClear();
  await tickAFewTimes(vi, 10, 100);

  rep = await replicacheForTesting('log-level', {logLevel: 'debug'});

  await rep.query(() => 42);
  expect(info).toHaveBeenCalledTimes(0);
  expect(debug.mock.calls.length).toBeGreaterThan(0);

  expect(
    debug.mock.calls.some(args => args[0].startsWith(`name=${rep.name}`)),
  ).toBe(true);
  expect(
    debug.mock.calls.some(args => args.length > 0 && args[1].endsWith('PULL')),
  ).toBe(true);
  expect(
    debug.mock.calls.some(args => args.length > 0 && args[1].endsWith('PUSH')),
  ).toBe(true);

  await rep.close();
});

test('logSinks length 0', async () => {
  const infoStub = vi.spyOn(console, 'info');
  const debugStub = vi.spyOn(console, 'debug');
  const expectNoLogsToConsole = () => {
    expect(infoStub).toHaveBeenCalledTimes(0);
    expect(debugStub).toHaveBeenCalledTimes(0);
  };

  const clearLogCounts = () => {
    infoStub.mockClear();
    debugStub.mockClear();
  };

  clearLogCounts();
  let rep = await replicacheForTesting('logSinks-0', {
    logLevel: 'info',
    logSinks: [],
  });
  await rep.query(() => 42);
  expectNoLogsToConsole();
  await rep.close();
  rep = await replicacheForTesting('logSinks-0', {
    logLevel: 'debug',
    logSinks: [],
  });
  await rep.query(() => 42);
  expectNoLogsToConsole();
  await rep.close();
});

test('logSinks length 1', async () => {
  const infoStub = vi.spyOn(console, 'info');
  const debugStub = vi.spyOn(console, 'debug');
  const expectNoLogsToConsole = () => {
    expect(infoStub).toHaveBeenCalledTimes(0);
    expect(debugStub).toHaveBeenCalledTimes(0);
  };

  const initLogCounts = () => ({
    info: 0,
    debug: 0,
    warn: 0,
    error: 0,
  });
  let logCounts: Record<LogLevel, number> = initLogCounts();
  const clearLogCounts = () => {
    logCounts = initLogCounts();
    infoStub.mockClear();
    debugStub.mockClear();
  };

  const logSink = {
    log: (level: LogLevel, _ctx: Context | undefined, ..._args: unknown[]) => {
      logCounts[level]++;
    },
  };
  clearLogCounts();
  let rep = await replicacheForTesting('logSinks-1', {
    logLevel: 'info',
    logSinks: [logSink],
  });
  await rep.query(() => 42);
  expect(logCounts.info).toBe(0);
  expect(logCounts.debug).toBe(0);
  expectNoLogsToConsole();
  await rep.close();

  logCounts = initLogCounts();
  rep = await replicacheForTesting('logSinks-1', {
    logLevel: 'debug',
    logSinks: [logSink],
  });
  await rep.query(() => 42);
  expect(logCounts.info).toBe(0);
  expect(logCounts.debug).toBeGreaterThan(0);
  expectNoLogsToConsole();
  await rep.close();
});

test('logSinks length 3', async () => {
  const infoStub = vi.spyOn(console, 'info');
  const debugStub = vi.spyOn(console, 'debug');
  const expectNoLogsToConsole = () => {
    expect(infoStub).toHaveBeenCalledTimes(0);
    expect(debugStub).toHaveBeenCalledTimes(0);
  };

  const initLogCounts = () =>
    Array.from({length: 3}, () => ({
      info: 0,
      debug: 0,
      warn: 0,
      error: 0,
    }));
  let logCounts: Record<LogLevel, number>[] = initLogCounts();
  const clearLogCounts = () => {
    logCounts = initLogCounts();
    infoStub.mockClear();
    debugStub.mockClear();
  };

  const logSinks = Array.from({length: 3}, (_, i) => ({
    log: (level: LogLevel, _ctx: Context | undefined, ..._args: unknown[]) => {
      logCounts[i][level]++;
    },
  }));
  clearLogCounts();
  let rep = await replicacheForTesting('log-level', {
    logLevel: 'info',
    logSinks,
  });
  await rep.query(() => 42);
  for (const counts of logCounts) {
    expect(counts.info).toBe(0);
    expect(counts.debug).toBe(0);
  }
  expectNoLogsToConsole();
  await rep.close();

  logCounts = initLogCounts();
  rep = await replicacheForTesting('log-level', {
    logLevel: 'debug',
    logSinks,
  });
  await rep.query(() => 42);
  for (const counts of logCounts) {
    expect(counts.info).toBe(0);
    expect(counts.info).toBe(0);
  }
  expectNoLogsToConsole();
  await rep.close();
});

test('mem store', async () => {
  let rep = await replicacheForTesting('mem', {
    mutators: {addData},
  });
  const add = rep.mutate.addData;
  await add({a: 42});
  expect(await rep.query(tx => tx.get('a'))).toBe(42);
  await rep.close();

  // Open again and test that we lost the data
  rep = await replicacheForTesting('mem');
  expect(await rep.query(tx => tx.get('a'))).toBe(undefined);
});

test('isEmpty', async () => {
  const rep = await replicacheForTesting('test-is-empty', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
      mut: async (tx: WriteTransaction) => {
        expect(await tx.isEmpty()).toBe(false);

        await tx.del('c');
        expect(await tx.isEmpty()).toBe(false);

        await tx.del('a');
        expect(await tx.isEmpty()).toBe(true);

        await tx.set('d', 4);
        expect(await tx.isEmpty()).toBe(false);
      },
    },
  });
  const {addData: add, del, mut} = rep.mutate;

  async function t(expected: boolean) {
    expect(await rep.query(tx => tx.isEmpty())).toBe(expected);
  }

  await t(true);

  await add({a: 1});
  await t(false);

  await add({b: 2, c: 3});
  await t(false);

  await del('b');
  await t(false);

  await mut();

  await t(false);
});

test('mutationID on transaction', async () => {
  let expectedMutationID = 1;
  const rep = await replicacheForTesting('test-is-empty', {
    mutators: {
      addData: async (
        tx: WriteTransaction,
        data: {[key: string]: JSONValue},
      ) => {
        for (const [key, value] of Object.entries(data)) {
          await tx.set(key, value);
        }
        expect(tx.mutationID).toBe(expectedMutationID++);
      },
    },
  });
  const {addData} = rep.mutate;
  await addData({foo: 'bar'});
  await addData({fuzzy: 'wuzzy'});
  await addData({fizz: 'bang'});
  expect(expectedMutationID).equals(4);
});

test('onSync', async () => {
  const pullURL = 'https://pull.com/pull';
  const pushURL = 'https://push.com/push';

  const rep = await replicacheForTesting(
    'onSync',
    {
      pullURL,
      pushURL,
      pushDelay: 5,
      mutators: {addData},
    },
    {
      enablePullAndPushInOpen: false,
    },
  );
  const add = rep.mutate.addData;

  const onSync = vi.fn();
  rep.onSync = onSync;

  expect(onSync).toHaveBeenCalledTimes(0);

  const {clientID} = rep;
  fetchMocker.postOnce(pullURL, makePullResponseV1(clientID, 2, undefined, 1));
  await rep.pull();
  await tickAFewTimes(vi, 15);

  expect(onSync).toHaveBeenCalledTimes(2);
  expect(onSync.mock.calls[0][0]).toBe(true);
  expect(onSync.mock.calls[1][0]).toBe(false);

  onSync.mockClear();
  fetchMocker.postOnce(pushURL, {});
  await add({a: 'a'});
  await tickAFewTimes(vi);

  expect(onSync).toHaveBeenCalledTimes(2);
  expect(onSync.mock.calls[0][0]).toBe(true);
  expect(onSync.mock.calls[1][0]).toBe(false);

  fetchMocker.postOnce(pushURL, {});
  onSync.mockClear();
  await add({b: 'b'});
  await tickAFewTimes(vi);
  expect(onSync).toHaveBeenCalledTimes(2);
  expect(onSync.mock.calls[0][0]).toBe(true);
  expect(onSync.mock.calls[1][0]).toBe(false);

  {
    // Try with reauth
    const consoleErrorStub = vi.spyOn(console, 'error');
    fetchMocker.postOnce(pushURL, {
      body: 'xxx',
      status: httpStatusUnauthorized,
    });
    onSync.mockClear();
    rep.getAuth = () => {
      // Next time it is going to be fine
      fetchMocker.postOnce(pushURL, {});
      return 'ok';
    };

    await add({c: 'c'});

    await tickUntil(vi, () => onSync.mock.calls.length >= 4);

    expectConsoleLogContextStub(
      rep.name,
      consoleErrorStub.mock.calls[0],
      'Got a non 200 response doing push: 401: xxx',
      ['push', requestIDLogContextRegex],
    );

    expect(onSync).toHaveBeenCalledTimes(4);
    expect(onSync.mock.calls[0][0]).toBe(true);
    expect(onSync.mock.calls[1][0]).toBe(false);
    expect(onSync.mock.calls[2][0]).toBe(true);
    expect(onSync.mock.calls[3][0]).toBe(false);
  }

  rep.onSync = null;
  onSync.mockClear();
  fetchMocker.postOnce(pushURL, {});
  expect(onSync).toHaveBeenCalledTimes(0);
});

test('push timing', async () => {
  const pushURL = 'https://push.com/push';
  const pushDelay = 5;

  const rep = await replicacheForTesting('push-timing', {
    pushURL,
    pushDelay,
    mutators: {addData},
  });

  const onInvokePush = (rep.onPushInvoked = vi.fn());

  const add = rep.mutate.addData;

  fetchMocker.post(pushURL, {});
  await add({a: 0});
  await tickAFewTimes(vi);

  const pushCallCount = () => {
    const rv = onInvokePush.mock.calls.length;
    onInvokePush.mockClear();
    return rv;
  };

  expect(pushCallCount()).toBe(1);

  // This will schedule push in pushDelay ms
  await add({a: 1});
  await add({b: 2});
  await add({c: 3});
  await add({d: 4});

  expect(pushCallCount()).toBe(0);

  await vi.advanceTimersByTimeAsync(pushDelay + 10);

  expect(pushCallCount()).toBe(1);

  const p1 = add({e: 5});
  const p2 = add({f: 6});
  const p3 = add({g: 7});

  expect(pushCallCount()).toBe(0);

  await tickAFewTimes(vi);
  await p1;
  expect(pushCallCount()).toBe(1);
  await tickAFewTimes(vi);
  await p2;
  expect(pushCallCount()).toBe(0);
  await tickAFewTimes(vi);
  await p3;
  expect(pushCallCount()).toBe(0);
});

test('push and pull concurrently', async () => {
  const pushURL = 'https://push.com/push';
  const pullURL = 'https://pull.com/pull';

  const rep = await replicacheForTesting(
    'concurrently',
    {
      pullURL,
      pushURL,
      pushDelay: 10,
      mutators: {addData},
    },
    {
      enablePullAndPushInOpen: false,
    },
  );

  const onBeginPull = (rep.onBeginPull = vi.fn());
  const commitSpy = vi.spyOn(Write.prototype, 'commitWithDiffs');
  const onPushInvoked = (rep.onPushInvoked = vi.fn());

  function resetSpies() {
    onBeginPull.mockClear();
    commitSpy.mockClear();
    onPushInvoked.mockClear();
  }

  const callCounts = () => {
    const rv = {
      beginPull: onBeginPull.mock.calls.length,
      commit: commitSpy.mock.calls.length,
      invokePush: onPushInvoked.mock.calls.length,
    };
    resetSpies();
    return rv;
  };

  const add = rep.mutate.addData;

  const requests: string[] = [];

  const {clientID} = rep;
  fetchMocker.post(pushURL, () => {
    requests.push(pushURL);
    return {};
  });
  fetchMocker.post(pullURL, () => {
    requests.push(pullURL);
    return makePullResponseV1(clientID, 0, [], 1);
  });

  await add({a: 0});
  resetSpies();

  await add({b: 1});
  await rep.pull();

  await vi.advanceTimersByTimeAsync(10);

  // Only one push at a time but we want push and pull to be concurrent.
  expect(callCounts()).toEqual({
    beginPull: 1,
    commit: 1,
    invokePush: 1,
  });

  await tickAFewTimes(vi);

  expect(requests).toEqual([pullURL, pushURL]);

  await tickAFewTimes(vi);

  expect(requests).toEqual([pullURL, pushURL]);

  expect(callCounts()).toEqual({
    beginPull: 0,
    commit: 0,
    invokePush: 0,
  });
});

test('schemaVersion pull', async () => {
  const schemaVersion = 'testing-pull';

  const rep = await replicacheForTesting('schema-version-pull', {
    schemaVersion,
  });

  await rep.pull();
  await tickAFewTimes(vi);

  const req = fetchMocker.lastBody() as {schemaVersion: string};
  expect(req.schemaVersion).toEqual(schemaVersion);
});

test('schemaVersion push', async () => {
  const pushURL = 'https://push.com/push';
  const schemaVersion = 'testing-push';

  const rep = await replicacheForTesting('schema-version-push', {
    pushURL,
    schemaVersion,
    pushDelay: 1,
    mutators: {addData},
  });

  const add = rep.mutate.addData;
  await add({a: 1});

  fetchMocker.post(pushURL, {});
  await tickAFewTimes(vi);

  const req = fetchMocker.lastBody() as {schemaVersion: string};
  expect(req.schemaVersion).toEqual(schemaVersion);
});

test('clientID', async () => {
  const re = /^[0-9a-v]{18}$/;

  let rep = await replicacheForTesting('clientID');
  const {clientID} = rep;
  expect(clientID).toMatch(re);
  await rep.close();

  const rep2 = await replicacheForTesting('clientID2');
  const clientID2 = rep2.clientID;
  expect(clientID2).toMatch(re);
  expect(clientID2).not.toBe(clientID);

  rep = await replicacheForTesting('clientID');
  const clientID3 = rep.clientID;
  expect(clientID3).toMatch(re);
  // With SDD we never reuse client IDs.
  expect(clientID3).not.toBe(clientID);

  const rep4 = new Replicache({
    name: 'clientID4',
    pullInterval: null,
  });
  const clientID4 = rep4.clientID;
  expect(clientID4).toMatch(re);
  await rep4.close();
});

test('profileID', async () => {
  const re = /^p.+/; // More specific re tested in IdbDatabase.test.ts.

  const rep = await replicacheForTesting('clientID');
  const profileID = await rep.profileID;
  expect(profileID).not.toBe(rep.clientID);
  expect(profileID).toMatch(re);
  await rep.close();

  const rep2 = await replicacheForTesting('clientID2');
  const profileID2 = await rep2.profileID;
  expect(profileID2).toBe(profileID);

  const rep3 = new Replicache({
    name: 'clientID3',
  });
  const profileID3 = await rep3.profileID;
  expect(profileID3).toBe(profileID);
  await rep3.close();
});

test('pull and index update', async () => {
  const pullURL = 'https://pull.com/rep';
  const indexName = 'idx1';
  const rep = await replicacheForTesting('pull-and-index-update', {
    pullURL,
    indexes: {[indexName]: {jsonPointer: '/id'}},
  });
  const {clientID} = rep;

  let lastMutationID = 0;
  let lastCookie = 0;
  async function testPull(opt: {
    patch: PatchOperation[];
    expectedResult: JSONValue;
  }) {
    let pullDone = false;
    fetchMocker.post(pullURL, () => {
      pullDone = true;
      return makePullResponseV1(
        clientID,
        lastMutationID++,
        opt.patch,
        lastCookie++,
      );
    });

    await rep.pull();

    await tickUntil(vi, () => pullDone);
    await tickAFewTimes(vi);

    const actualResult = await rep.query(tx =>
      tx.scan({indexName}).entries().toArray(),
    );
    expect(actualResult).toEqual(opt.expectedResult);
  }

  await testPull({patch: [], expectedResult: []});

  await testPull({
    patch: [
      {
        op: 'put',
        key: 'a1',
        value: {id: 'a-1', x: 1},
      },
    ],
    expectedResult: [
      [
        ['a-1', 'a1'],
        {
          id: 'a-1',
          x: 1,
        },
      ],
    ],
  });

  // Change value for existing key
  await testPull({
    patch: [
      {
        op: 'put',
        key: 'a1',
        value: {id: 'a-1', x: 2},
      },
    ],
    expectedResult: [
      [
        ['a-1', 'a1'],
        {
          id: 'a-1',
          x: 2,
        },
      ],
    ],
  });

  // Del
  await testPull({
    patch: [
      {
        op: 'del',
        key: 'a1',
      },
    ],
    expectedResult: [],
  });
});

async function populateDataUsingPull<MD extends MutatorDefs = {}>(
  rep: ReplicacheTest<MD>,
  data: Record<string, ReadonlyJSONValue>,
) {
  const {clientID} = rep;
  fetchMocker.postOnce(rep.pullURL, {
    cookie: '',
    lastMutationIDChanges: {[clientID]: 2},
    patch: Object.entries(data).map(([key, value]) => ({
      op: 'put',
      key,
      value,
    })),
  });

  await rep.pull();

  // Allow pull to finish (larger than PERSIST_TIMEOUT)
  await vi.advanceTimersByTimeAsync(22 * 1000);
  await tickAFewTimes(vi, 20, 100);

  await rep.persist();
}

async function tickUntilTimeIs(time: number, tick = 10) {
  while (Date.now() < time) {
    await vi.advanceTimersByTimeAsync(tick);
  }
}

test('pull mutate options', async () => {
  const pullURL = 'https://diff.com/pull';
  const rep = await replicacheForTesting(
    'pull-mutate-options',
    {
      pullURL,
      pushURL: '',
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );
  const {clientID} = rep;
  const log: number[] = [];

  fetchMocker.post(pullURL, () => {
    log.push(Date.now());
    return makePullResponseV1(clientID, undefined, [], '');
  });

  await tickUntilTimeIs(1000);

  // Phase 1: default minDelayMs (30ms)
  while (Date.now() < 1300) {
    rep.pullIgnorePromise();
    await vi.advanceTimersByTimeAsync(30);
  }

  const phase1End = log.length;

  rep.requestOptions.minDelayMs = 500;

  // Phase 2: minDelayMs = 500ms
  while (Date.now() < 2800) {
    rep.pullIgnorePromise();
    await vi.advanceTimersByTimeAsync(500);
  }

  const phase2End = log.length;

  rep.requestOptions.minDelayMs = 25;

  // Phase 3: minDelayMs = 25ms
  while (Date.now() < 3300) {
    rep.pullIgnorePromise();
    await vi.advanceTimersByTimeAsync(25);
  }

  // Helper to check deltas in a range, skipping entries at phase boundaries
  // where timing can be unstable due to async microtask scheduling differences
  // across browsers.
  const checkDeltas = (
    start: number,
    end: number,
    minDelay: number,
    maxDelay: number,
    phase: string,
  ) => {
    let validCount = 0;
    for (let i = start; i < end; i++) {
      const delta = log[i] - log[i - 1];
      // Skip entries with 0 delta (same timestamp due to microtask batching)
      if (delta === 0) continue;
      expect(delta, `${phase} delta at index ${i}`).toBeGreaterThanOrEqual(
        minDelay,
      );
      expect(delta, `${phase} delta at index ${i}`).toBeLessThanOrEqual(
        maxDelay,
      );
      validCount++;
    }
    return validCount;
  };

  // Verify phase 1: pulls should respect minDelayMs of 30ms
  // Allow some tolerance for debounce and internal timing variations
  expect(phase1End, 'phase1 pull count').toBeGreaterThanOrEqual(3);
  const phase1Valid = checkDeltas(1, phase1End, 25, 60, 'phase1');
  expect(phase1Valid, 'phase1 valid deltas').toBeGreaterThanOrEqual(2);

  // Verify phase 2: pulls should respect minDelayMs of 500ms
  // Allow ~10% tolerance for timing variations
  expect(phase2End - phase1End, 'phase2 pull count').toBeGreaterThanOrEqual(2);
  const phase2Valid = checkDeltas(
    phase1End + 1,
    phase2End,
    450,
    1000,
    'phase2',
  );
  expect(phase2Valid, 'phase2 valid deltas').toBeGreaterThanOrEqual(1);

  // Verify phase 3: pulls should respect minDelayMs of 25ms
  expect(log.length - phase2End, 'phase3 pull count').toBeGreaterThanOrEqual(5);
  const phase3Valid = checkDeltas(phase2End + 1, log.length, 20, 75, 'phase3');
  expect(phase3Valid, 'phase3 valid deltas').toBeGreaterThanOrEqual(3);
});

test('online', async () => {
  const pushURL = 'https://diff.com/push';
  const rep = await replicacheForTesting('online', {
    pushURL,
    pushDelay: 0,
    mutators: {addData},
    logLevel: 'debug',
  });

  const log: boolean[] = [];
  rep.onOnlineChange = b => {
    log.push(b);
  };

  const consoleDebugStub = vi.spyOn(console, 'debug');

  fetchMocker.post(pushURL, async () => {
    await sleep(10);
    return {throws: new Error('Simulate fetch error in push')};
  });

  expect(rep.online).toBe(true);
  expect(log).toEqual([]);

  await rep.mutate.addData({a: 0});

  await tickAFewTimes(vi);

  expect(rep.online).toBe(false);
  expect(
    consoleDebugStub.mock.calls.some(
      args => args.join('\n').indexOf('Push threw') > -1,
    ),
  );
  expect(log).toEqual([false]);

  consoleDebugStub.mockClear();

  fetchMocker.post(pushURL, 'ok');
  await rep.mutate.addData({a: 1});

  await tickAFewTimes(vi, 20);

  expect(
    !consoleDebugStub.mock.calls.some(
      args => args.join('\n').indexOf('Push threw') > -1,
    ),
  );
  expect(rep.online).toBe(true);
  expect(log).toEqual([false, true]);
});

test('overlapping open/close', async () => {
  const pullInterval = 60_000;
  const name = 'overlapping-open-close';

  const rep = new Replicache({
    name,
    pullInterval,
  });
  const p = rep.close();

  const rep2 = new Replicache({
    name,
    pullInterval,
  });
  const p2 = rep2.close();

  const rep3 = new Replicache({
    name,
    pullInterval,
  });
  const p3 = rep3.close();

  await p;
  await p2;
  await p3;

  {
    const rep = new Replicache({
      name,
      pullInterval,
    });
    await rep.clientGroupID;
    const p = rep.close();
    const rep2 = new Replicache({
      name,
      pullInterval,
    });
    await rep2.clientGroupID;
    const p2 = rep2.close();
    await p;
    await p2;
  }
});

async function testMemStoreWithCounters<MD extends MutatorDefs>(
  rep: ReplicacheTest<MD>,
  store: MemStoreWithCounters,
) {
  // Safari does not have requestIdleTimeout so it delays 1 second for persist
  // and 1 second for refresh. We need to wait to have all browsers have a
  // chance to run persist and the refresh triggered by persist before we continue.
  await vi.advanceTimersByTimeAsync(2000);

  expect(store.readCount, 'readCount').toBeGreaterThan(0);
  expect(store.writeCount, 'writeCount').toBeGreaterThan(0);
  expect(store.closeCount, 'closeCount').toBe(0);
  store.resetCounters();

  const b = await rep.query(tx => tx.has('foo'));
  expect(b).toBe(false);
  // When DD31 refresh has pulled enough data into the lazy store
  // to not have to read from the experiment-kv-store
  expect(store.readCount, 'readCount').toBe(1);
  expect(store.writeCount, 'writeCount').toBe(0);
  expect(store.closeCount, 'closeCount').toBe(0);
  store.resetCounters();

  await rep.mutate.addData({foo: 'bar'});
  expect(store.readCount, 'readCount').toBe(0);
  expect(store.writeCount, 'writeCount').toBe(0);
  expect(store.closeCount, 'closeCount').toBe(0);
  store.resetCounters();

  await rep.persist();

  expect(store.readCount, 'readCount').toBe(1);
  expect(store.writeCount, 'writeCount').toBe(1);
  expect(store.closeCount, 'closeCount').toBe(0);
  store.resetCounters();

  await rep.close();
  expect(store.readCount, 'readCount').toBe(0);
  expect(store.writeCount, 'writeCount').toBe(0);
  expect(store.closeCount, 'closeCount').toBe(1);
}

test('Create KV Store', async () => {
  let store: MemStoreWithCounters | undefined;

  const rep = await replicacheForTesting(
    'kv-store',
    {
      kvStore: {
        create: name => {
          if (!store && name.includes('kv-store')) {
            store = new MemStoreWithCounters(name);
            return store;
          }

          return new MemStoreWithCounters(name);
        },
        drop: (_name: string) => {
          if (store) {
            store = undefined;
            return promiseVoid;
          }
          return promiseVoid;
        },
      },
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );

  expect(store).toBeInstanceOf(MemStoreWithCounters);
  assert(store);

  await testMemStoreWithCounters(rep, store);
});

function findPropertyValue(
  obj: unknown,
  propertyName: string,
  propertyValue: unknown,
): unknown | undefined {
  if (typeof obj === 'object' && obj !== null) {
    const rec = obj as Record<string, unknown>;
    if (rec[propertyName] === propertyValue) {
      return rec;
    }

    let values: Iterable<unknown>;
    if (obj instanceof Set || obj instanceof Map || obj instanceof Array) {
      values = obj.values();
    } else {
      values = Object.values(rec);
    }
    for (const v of values) {
      const r = findPropertyValue(v, propertyName, propertyValue);
      if (r) {
        return r;
      }
    }
  }
  return undefined;
}

test('mutate args in mutation throws due to frozen', async () => {
  // This tests that mutating the args in a mutation does not mutate the args we
  // store in the kv.Store.
  const store = new TestMemStore();
  const rep = await replicacheForTesting('mutate-args-in-mutation', {
    kvStore: {create: () => store, drop: () => promiseVoid},
    mutators: {
      async mutArgs(tx: WriteTransaction, args: {v: number}) {
        args.v = 42;
        await tx.set('v', args.v);
      },
    },
  });

  let err;
  try {
    await rep.mutate.mutArgs({v: 1});
  } catch (e) {
    err = e;
  }
  expect(err).toBeInstanceOf(Error);

  // Safari does not have requestIdleTimeout so it waits for a second.
  await vi.advanceTimersByTimeAsync(1000);

  const o = findPropertyValue(store.map(), 'mutatorName', 'mutArgs');
  expect(o).toBeUndefined();
});

test('client ID is set correctly on transactions', async () => {
  const rep = await replicacheForTesting(
    'client-id-is-set-correctly-on-transactions',
    {
      mutators: {
        expectClientID(tx: WriteTransaction, expectedClientID: ClientID) {
          expect(tx.clientID).toBe(expectedClientID);
        },
      },
    },
  );

  const {clientID} = rep;

  await rep.query(tx => {
    expect(tx.clientID).toBe(clientID);
  });

  await rep.mutate.expectClientID(clientID);
});

test('mutation timestamps are immutable', async () => {
  let pending: unknown;
  const rep = await replicacheForTesting('mutation-timestamps-are-immutable', {
    mutators: {
      foo: async (tx: WriteTransaction, _: JSONValue) => {
        await tx.set('foo', 'bar');
      },
    },
    // oxlint-disable-next-line require-await
    pusher: async req => {
      pending = req.mutations;
      return {
        httpRequestInfo: {
          errorMessage: '',
          httpStatusCode: 200,
        },
      };
    },
  });

  // Create a mutation and verify it has been assigned current time.
  await rep.mutate.foo(null);
  await rep.push({now: true});
  expect(pending).toEqual([
    {
      clientID: rep.clientID,
      id: 1,
      name: 'foo',
      args: null,
      timestamp: 100,
    },
  ]);

  // Move clock forward, then cause a rebase, the pending mutation will
  // replay internally.
  pending = [];
  await tickAFewTimes(vi);

  const {clientID} = rep;
  const poke: Poke = {
    baseCookie: null,
    pullResponse: makePullResponseV1(
      clientID,
      0,
      [
        {
          op: 'put',
          key: 'hot',
          value: 'dog',
        },
      ],
      '',
    ),
  };
  await rep.poke(poke);

  // Verify rebase did occur by checking for the new value.
  const val = await rep.query(tx => tx.get('hot'));
  expect(val).toBe('dog');

  // Check that mutation timestamp did not change
  await rep.push({now: true});
  expect(pending).toEqual([
    {
      clientID: rep.clientID,
      id: 1,
      name: 'foo',
      args: null,
      timestamp: 100,
    },
  ]);
});

// Define this here to prevent issues with building docs
type DocumentVisibilityState = 'hidden' | 'visible';

describe('check for client not found in visibilitychange', () => {
  let document: Document;

  beforeEach(() => {
    document = new (class extends EventTarget {
      //  #visibilityState = 'visible';
      get visibilityState() {
        return 'visible';
      }
    })() as Document;

    overrideBrowserGlobal('document', document);

    return () => {
      clearBrowserOverrides();
      vi.restoreAllMocks();
    };
  });

  const t = (
    visibilityState: DocumentVisibilityState,
    shouldBeCalled: boolean,
  ) => {
    test('visibilityState: ' + visibilityState, async () => {
      const consoleErrorStub = vi.spyOn(console, 'error');
      const visibilityStateResolver = resolver<void>();
      const spy = vi
        .spyOn(document, 'visibilityState', 'get')
        .mockImplementation(() => {
          visibilityStateResolver.resolve();
          return visibilityState;
        });

      const rep = await replicacheForTesting(
        `check-for-client-not-found-in-visibilitychange-${visibilityState}`,
      );

      const onClientStateNotFound = () => {
        onClientStateNotFound.resolver.resolve();
        onClientStateNotFound.called = true;
      };
      onClientStateNotFound.resolver = resolver<void>();
      onClientStateNotFound.called = false;
      rep.onClientStateNotFound = onClientStateNotFound;

      const {clientID} = rep;
      await deleteClientForTesting(clientID, rep.perdag);

      consoleErrorStub.mockClear();

      document.dispatchEvent(new Event('visibilitychange'));
      await visibilityStateResolver.promise;

      if (shouldBeCalled) {
        await onClientStateNotFound.resolver.promise;
        expect(onClientStateNotFound.called).toBe(true);
        expectLogContext(
          consoleErrorStub,
          0,
          rep,
          `Client state not found on client, clientID: ${clientID}`,
        );
      } else {
        expect(onClientStateNotFound.called).toBe(false);
      }

      await rep.close();

      spy.mockClear();
    });
  };

  t('hidden', false);
  t('visible', true);
});

test('disableClientGroup', async () => {
  const rep = await replicacheForTesting(
    'disable-client-group',
    {
      mutators: {
        noop: () => undefined,
      },
    },
    disableAllBackgroundProcesses,
  );
  const clientGroupID = await rep.clientGroupID;

  expect(rep.isClientGroupDisabled).toBe(false);
  expect(
    (
      await withRead(rep.perdag, dagRead =>
        getClientGroup(clientGroupID, dagRead),
      )
    )?.disabled,
  ).toBe(false);

  await rep.impl.disableClientGroup();

  expect(rep.isClientGroupDisabled).toBe(true);
  expect(
    (
      await withRead(rep.perdag, dagRead =>
        getClientGroup(clientGroupID, dagRead),
      )
    )?.disabled,
  ).toBe(true);
});

test('scan in write transaction', async () => {
  let x = 0;
  const rep = await replicacheForTesting('scan-before-commit', {
    mutators: {
      async test(tx: WriteTransaction, v: number) {
        await tx.set('a', v);
        expect(await tx.scan().toArray()).toEqual([v]);
        x++;
      },
    },
  });

  await rep.mutate.test(42);

  expect(x).toBe(1);
});

test('scan mutate', async () => {
  const log: unknown[] = [];
  const rep = await replicacheForTesting('scan-mutate', {
    mutators: {
      addData,
      async test(tx: WriteTransaction) {
        for await (const entry of tx.scan().entries()) {
          log.push(entry);
          switch (entry[0]) {
            case 'a':
              // put upcoming entry
              await tx.set('e', 4);
              break;
            case 'b':
              // delete upcoming entry
              await tx.del('c');
              break;
            case 'f':
              // delete already visited
              await tx.del('a');
              break;
            case 'g':
              // set existing key to new value
              await tx.set('h', 77);
              break;
            case 'h':
              // set already visited key to new value
              await tx.set('b', 11);
              break;
          }
        }
      },
    },
  });

  await rep.mutate.addData({
    a: 0,
    b: 1,
    c: 2,
    d: 3,

    f: 5,
    g: 6,
    h: 7,
  });

  await rep.mutate.test();
  expect(log).toEqual([
    ['a', 0],
    ['b', 1],
    ['d', 3],
    ['e', 4],
    ['f', 5],
    ['g', 6],
    ['h', 77],
  ]);
});

test('index scan mutate', async () => {
  const log: unknown[] = [];
  const rep = await replicacheForTesting('index-scan-mutate', {
    mutators: {
      addData,
      async test(tx: WriteTransaction) {
        for await (const entry of tx.scan({indexName: 'i'}).entries()) {
          log.push(entry);

          switch (entry[0][1]) {
            case 'a':
              // put upcoming entry
              await tx.set('e', {a: '4'});
              break;
            case 'b':
              // delete upcoming entry
              await tx.del('c');
              break;
            case 'f':
              // delete already visited
              await tx.del('a');
              break;
            case 'g':
              // set existing key to new value
              await tx.set('h', {a: '77'});
              break;
            case 'h':
              // set already visited key to new value
              await tx.set('b', {a: '11'});
              break;
          }
        }
      },
    },
    indexes: {i: {jsonPointer: '/a'}},
  });

  await rep.mutate.addData({
    a: {a: '0'},
    b: {a: '1'},
    c: {a: '2'},
    d: {a: '3'},

    f: {a: '5'},
    g: {a: '6'},
    h: {a: '7'},
  });

  await rep.mutate.test();
  expect(log).toEqual([
    [['0', 'a'], {a: '0'}],
    [['1', 'b'], {a: '1'}],
    [['3', 'd'], {a: '3'}],
    [['4', 'e'], {a: '4'}],
    [['5', 'f'], {a: '5'}],
    [['6', 'g'], {a: '6'}],
    [['77', 'h'], {a: '77'}],
  ]);
});

test('concurrent puts and gets', async () => {
  const rep = await replicacheForTesting('concurrent-puts', {
    mutators: {
      async insert(tx: WriteTransaction, args: Record<string, number>) {
        const ps = Object.entries(args).map(([k, v]) => tx.set(k, v));
        await Promise.all(ps);
      },
      async race(tx: WriteTransaction) {
        // Conceptually the put could finish first but in practice that does not
        // happen.
        const p1 = tx.set('a', 4);
        const p2 = tx.get('a');
        await Promise.all([p1, p2]);
        const v = await p2;
        await tx.set('d', v ?? null);
      },
    },
  });

  await rep.mutate.insert({a: 1, b: 2, c: 3});

  const keys = ['a', 'b', 'c'];
  const values = await rep.query(tx => {
    const ps = keys.map(k => tx.get(k));
    return Promise.all(ps);
  });
  expect(values).toEqual([1, 2, 3]);

  await rep.mutate.race();
  const v = await rep.query(tx => tx.get('d'));
  expect(v === 1 || v === 4).toBe(true);

  const v2 = await rep.query(tx => tx.get('a'));
  expect(v2).toBe(4);
});

test('Invalid name', () => {
  expect(() => new ReplicacheTest({name: ''})).toThrow(
    'name is required and must be non-empty',
  );
  expect(
    () =>
      new Replicache({
        name: 1 as unknown as string,
      }),
  ).toThrow('name is required and must be non-empty');

  expect(
    () =>
      new ReplicacheTest({
        name: true as unknown as string,
      }),
  ).toThrow(TypeError);
});

test('set with undefined key', async () => {
  // We use a local variable instead of a mutator argument because the args gets
  // frozen and we do not want to test the freezing of the args but the behavior
  // of undefined passed into set.
  let value: unknown;
  const rep = await replicacheForTesting('set-with-undefined-key', {
    mutators: {
      async set(tx: WriteTransaction) {
        // @ts-expect-error unknown is not a valid key
        await tx.set('key', value);
      },
    },
  });

  const set = async (v: unknown) => {
    try {
      value = v;
      await rep.mutate.set();
    } catch (e) {
      return e;
    }
    return undefined;
  };

  expect(await set(undefined)).toBeInstanceOf(TypeError);

  // no error
  expect(await set({a: undefined})).toBe(undefined);

  expect(await set([1, undefined, 2])).toBeInstanceOf(TypeError);

  // oxlint-disable-next-line no-sparse-arrays
  expect(await set([1, , 2])).toBeInstanceOf(TypeError);
});

test('subscribe while closing', async () => {
  // This tests that we do not try to open an IndexedDB transaction after the
  // database has been closed.
  const rep = await replicacheForTesting('subscribe-while-closing', {
    mutators: {addData},
  });
  await rep.mutate.addData({a: 1});
  const p = rep.close();
  const query = vi.fn();
  const onData = vi.fn();
  const watchCallback = vi.fn();
  const unsubscribe = rep.subscribe(query, onData);
  const unwatch = rep.experimentalWatch(watchCallback);

  await vi.advanceTimersByTimeAsync(10);

  await p;
  unsubscribe();
  unwatch();

  expect(query).toHaveBeenCalledTimes(0);
  expect(onData).toHaveBeenCalledTimes(0);
  expect(watchCallback).toHaveBeenCalledTimes(0);
});
