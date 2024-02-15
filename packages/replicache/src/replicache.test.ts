import {
  LicenseStatus,
  PROD_LICENSE_SERVER_URL,
  TEST_LICENSE_KEY,
} from '@rocicorp/licensing/src/client';
import {
  LICENSE_ACTIVE_PATH,
  LICENSE_STATUS_PATH,
} from '@rocicorp/licensing/src/server/api-types.js';
import type {Context, LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert as chaiAssert, expect} from 'chai';
import {assert} from 'shared/src/asserts.js';
import type {JSONValue, ReadonlyJSONValue} from 'shared/src/json.js';
import {sleep} from 'shared/src/sleep.js';
import * as sinon from 'sinon';
import {asyncIterableToArray} from './async-iterable-to-array.js';
import {Write} from './db/write.js';
import {TestMemStore} from './kv/test-mem-store.js';
import type {PatchOperation} from './patch-operation.js';
import {deleteClientForTesting} from './persist/clients-test-helpers.js';
import type {ReplicacheOptions} from './replicache-options.js';
import {
  MutatorDefs,
  Poke,
  Replicache,
  getTestInstance,
  httpStatusUnauthorized,
} from './replicache.js';
import type {ScanOptions} from './scan-options.js';
import type {ClientID} from './sync/ids.js';
import {
  MemStoreWithCounters,
  ReplicacheTest,
  addData,
  clock,
  disableAllBackgroundProcesses,
  expectAsyncFuncToThrow,
  expectConsoleLogContextStub,
  expectLogContext,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  requestIDLogContextRegex,
  tickAFewTimes,
  tickUntil,
} from './test-util.js';
import {TransactionClosedError} from './transaction-closed-error.js';
import type {ReadTransaction, WriteTransaction} from './transactions.js';

// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';

const {fail} = chaiAssert;

initReplicacheTesting();

test('name is required', () => {
  expect(
    () =>
      new Replicache({
        licenseKey: TEST_LICENSE_KEY,
      } as ReplicacheOptions<Record<string, never>>),
  ).to.throw(/name.*required/);
});

test('name cannot be empty', () => {
  expect(
    () => new Replicache({licenseKey: TEST_LICENSE_KEY, name: ''}),
  ).to.throw(/name.*must be non-empty/);
});

test('get, has, scan on empty db', async () => {
  const rep = await replicacheForTesting('test2');
  async function t(tx: ReadTransaction) {
    expect(await tx.get('key')).to.equal(undefined);
    expect(await tx.has('key')).to.be.false;

    const scanItems = await asyncIterableToArray(tx.scan());
    expect(scanItems).to.have.length(0);
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
        expect(await tx.has(key)).to.equal(true);
        const v = await tx.get(key);
        expect(v).to.deep.equal(value);

        expect(await tx.del(key)).to.equal(true);
        expect(await tx.has(key)).to.be.false;
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
    expect(
      await asyncIterableToArray(tx.scan(options).entries()),
    ).to.deep.equal(entries);
  });
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options))).to.deep.equal(
      entries.map(([, v]) => v),
    );
  });
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options).values())).to.deep.equal(
      entries.map(([, v]) => v),
    );
  });
  await rep.query(async tx => {
    expect(await asyncIterableToArray(tx.scan(options).keys())).to.deep.equal(
      entries.map(([k]) => k),
    );
  });

  await rep.query(async tx => {
    expect(await tx.scan(options).toArray()).to.deep.equal(
      entries.map(([, v]) => v),
    );
  });
  // scan().xxx().toArray()
  await rep.query(async tx => {
    expect(await tx.scan(options).entries().toArray()).to.deep.equal(entries);
  });
  await rep.query(async tx => {
    expect(await tx.scan(options).values().toArray()).to.deep.equal(
      entries.map(([, v]) => v),
    );
  });
  await rep.query(async tx => {
    expect(await tx.scan(options).keys().toArray()).to.deep.equal(
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

  expect(await repA.query(tx => tx.get('key'))).to.equal('A');
  expect(await repB.query(tx => tx.get('key'))).to.equal('B');

  await repA.close();
  await repB.close();

  indexedDB.deleteDatabase(repA.idbName);
  indexedDB.deleteDatabase(repB.idbName);
});

test('register with error', async () => {
  const rep = await replicacheForTesting('regerr', {
    mutators: {
      // eslint-disable-next-line require-await
      err: async (_: WriteTransaction, args: number) => {
        throw args;
      },
    },
  });

  const doErr = rep.mutate.err;

  try {
    await doErr(42);
    fail('Should have thrown');
  } catch (ex) {
    expect(ex).to.equal(42);
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
  fetchMock.post(pushURL, {});

  const mut = rep.mutate['wait-then-return'];

  let resA = mut({duration: 250, ret: 'a'});
  // create a gap to make sure resA starts first (our rwlock isn't fair).
  await clock.tickAsync(100);
  let resB = mut({duration: 0, ret: 'b'});
  // race them, a should complete first, indicating that b waited
  expect(await Promise.race([resA, resB])).to.equal('a');
  // wait for the other to finish so that we're starting from null state for next one.
  await Promise.all([resA, resB]);

  // reads wait on writes
  resA = mut({duration: 250, ret: 'a'});
  await clock.tickAsync(100);
  resB = rep.query(() => 'b');
  await tickAFewTimes();
  expect(await Promise.race([resA, resB])).to.equal('a');

  await tickAFewTimes();
  await resA;
  await tickAFewTimes();
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

  await tickAFewTimes();
  fetchMock.reset();

  fetchMock.postOnce(pushURL, {
    mutationInfos: [],
  });

  expect(fetchMock.calls()).to.have.length(0);

  await createTodo({id: id1});

  expect(fetchMock.calls()).to.have.length(0);

  await tickAFewTimes();

  expect(fetchMock.calls()).to.have.length(1);
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

  const consoleErrorStub = sinon.stub(console, 'error');
  const getAuthFake = sinon.fake.returns(null);
  rep.getAuth = getAuthFake;

  await tickAFewTimes();

  fetchMock.post(pushURL, {body: 'xxx', status: httpStatusUnauthorized});

  await rep.mutate.noop();
  await tickUntil(() => getAuthFake.callCount > 0, 1);

  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.firstCall,
    'Got error response doing push: 401: xxx',
    ['push', requestIDLogContextRegex],
  );

  {
    await tickAFewTimes();

    const consoleInfoStub = sinon.stub(console, 'info');
    const getAuthFake = sinon.fake(() => 'boo');
    rep.getAuth = getAuthFake;

    await rep.mutate.noop();
    await tickUntil(() => consoleInfoStub.callCount > 0, 1);

    expectConsoleLogContextStub(
      rep.name,
      consoleInfoStub.firstCall,
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
  fetchMock.post(pullURL, () => {
    switch (i++) {
      case 0:
        return {body: 'internal error', status: 500};
      case 1:
        return {body: 'not found', status: 404};
      default: {
        okCalled = true;
        return {body: makePullResponseV1(clientID, 0), status: 200};
      }
    }
  });

  const consoleErrorStub = sinon.stub(console, 'error');

  rep.pullIgnorePromise({now: true});

  await tickAFewTimes(20, 10);

  expect(consoleErrorStub.callCount).to.equal(2);
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.firstCall,
    'Got error response doing pull: 500: internal error',
    ['pull', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.lastCall,
    'Got error response doing pull: 404: not found',
    ['pull', requestIDLogContextRegex],
  );

  expect(okCalled).to.equal(true);
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
  fetchMock.post(pushURL, () => {
    switch (i++) {
      case 0:
        return {body: 'internal error', status: 500};
      case 1:
        return {body: 'not found', status: 404};
      default:
        okCalled = true;
        return {body: {}, status: 200};
    }
  });

  const consoleErrorStub = sinon.stub(console, 'error');

  await add({
    a: 0,
  });

  await tickAFewTimes(20, 10);

  expect(consoleErrorStub.callCount).to.equal(2);
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.firstCall,
    'Got error response doing push: 500: internal error',
    ['push', requestIDLogContextRegex],
  );
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.lastCall,
    'Got error response doing push: 404: not found',
    ['push', requestIDLogContextRegex],
  );

  expect(okCalled).to.equal(true);
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
  expect(wtx).to.not.be.undefined;
  await expectAsyncFuncToThrow(() => wtx?.set('z', 1), TransactionClosedError);
  await expectAsyncFuncToThrow(() => wtx?.del('w'), TransactionClosedError);
});

test('pullInterval in constructor', async () => {
  const rep = await replicacheForTesting('pullInterval', {
    pullInterval: 12.34,
  });
  expect(rep.pullInterval).to.equal(12.34);
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
  const rep = await replicacheForTesting('index-redefinition', {
    mutators: {addData},
    indexes: {
      aIndex: {jsonPointer: '/a'},
    },
    ...disableAllBackgroundProcesses,
    enablePullAndPushInOpen: false,
  });

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
  const rep = await replicacheForTesting('index-add-more', {
    mutators: {addData},
    indexes: {
      aIndex: {jsonPointer: '/a'},
    },
    ...disableAllBackgroundProcesses,
  });

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
  const rep = await replicacheForTesting('index-add-more', {
    mutators: {addData},
    ...disableAllBackgroundProcesses,
    enablePullAndPushInOpen: false,
  });

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
      enablePullAndPushInOpen: false,
    },
    {useUniqueName: false},
  );

  await testScanResult(rep2, {indexName: 'aIndex'}, [[['0', 'a/0'], {a: '0'}]]);

  await rep2.close();
});

test('rename indexes', async () => {
  const rep = await replicacheForTesting('index-add-more', {
    mutators: {addData},
    indexes: {
      aIndex: {jsonPointer: '/a'},
      bIndex: {jsonPointer: '/b'},
    },
    ...disableAllBackgroundProcesses,
  });
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
  const info = sinon.stub(console, 'info');
  const debug = sinon.stub(console, 'debug');

  // Just testing that we get some output
  let rep = await replicacheForTesting('log-level', {logLevel: 'error'});
  await rep.query(() => 42);
  expect(info.callCount).to.equal(0);
  await rep.close();

  info.reset();
  debug.reset();
  await tickAFewTimes(10, 100);

  rep = await replicacheForTesting('log-level', {logLevel: 'info'});
  await rep.query(() => 42);
  expect(info.callCount).to.be.greaterThan(0);
  expect(debug.callCount).to.equal(0);
  await rep.close();

  info.reset();
  debug.reset();
  await tickAFewTimes(10, 100);

  rep = await replicacheForTesting('log-level', {logLevel: 'debug'});

  await rep.query(() => 42);
  expect(info.callCount).to.be.greaterThan(0);
  expect(debug.callCount).to.be.greaterThan(0);

  expect(
    debug.getCalls().some(call => call.firstArg.startsWith(`name=${rep.name}`)),
  ).to.equal(true);
  expect(
    debug
      .getCalls()
      .some(call => call.args.length > 0 && call.args[1].endsWith('PULL')),
  ).to.equal(true);
  expect(
    debug
      .getCalls()
      .some(call => call.args.length > 0 && call.args[1].endsWith('PUSH')),
  ).to.equal(true);

  await rep.close();
});

test('logSinks length 0', async () => {
  const infoStub = sinon.stub(console, 'info');
  const debugStub = sinon.stub(console, 'debug');
  const expectNoLogsToConsole = () => {
    expect(infoStub.callCount).to.equal(0);
    expect(debugStub.callCount).to.equal(0);
  };

  const resetLogCounts = () => {
    infoStub.reset();
    debugStub.reset();
  };

  resetLogCounts();
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
  const infoStub = sinon.stub(console, 'info');
  const debugStub = sinon.stub(console, 'debug');
  const expectNoLogsToConsole = () => {
    expect(infoStub.callCount).to.equal(0);
    expect(debugStub.callCount).to.equal(0);
  };

  const initLogCounts = () => ({
    info: 0,
    debug: 0,
    error: 0,
  });
  let logCounts: Record<LogLevel, number> = initLogCounts();
  const resetLogCounts = () => {
    logCounts = initLogCounts();
    infoStub.reset();
    debugStub.reset();
  };

  const logSink = {
    log: (level: LogLevel, _ctx: Context | undefined, ..._args: unknown[]) => {
      logCounts[level]++;
    },
  };
  resetLogCounts();
  let rep = await replicacheForTesting('logSinks-1', {
    logLevel: 'info',
    logSinks: [logSink],
  });
  await rep.query(() => 42);
  expect(logCounts.info).to.be.greaterThan(0);
  expect(logCounts.debug).to.equal(0);
  expectNoLogsToConsole();
  await rep.close();

  logCounts = initLogCounts();
  rep = await replicacheForTesting('logSinks-1', {
    logLevel: 'debug',
    logSinks: [logSink],
  });
  await rep.query(() => 42);
  expect(logCounts.info).to.be.greaterThan(0);
  expect(logCounts.debug).to.be.greaterThan(0);
  expectNoLogsToConsole();
  await rep.close();
});

test('logSinks length 3', async () => {
  const infoStub = sinon.stub(console, 'info');
  const debugStub = sinon.stub(console, 'debug');
  const expectNoLogsToConsole = () => {
    expect(infoStub.callCount).to.equal(0);
    expect(debugStub.callCount).to.equal(0);
  };

  const initLogCounts = () =>
    Array.from({length: 3}, () => ({
      info: 0,
      debug: 0,
      error: 0,
    }));
  let logCounts: Record<LogLevel, number>[] = initLogCounts();
  const resetLogCounts = () => {
    logCounts = initLogCounts();
    infoStub.reset();
    debugStub.reset();
  };

  const logSinks = Array.from({length: 3}, (_, i) => ({
    log: (level: LogLevel, _ctx: Context | undefined, ..._args: unknown[]) => {
      logCounts[i][level]++;
    },
  }));
  resetLogCounts();
  let rep = await replicacheForTesting('log-level', {
    logLevel: 'info',
    logSinks,
  });
  await rep.query(() => 42);
  for (const counts of logCounts) {
    expect(counts.info).to.be.greaterThan(0);
    expect(counts.debug).to.equal(0);
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
    expect(counts.info).to.be.greaterThan(0);
    expect(counts.info).to.be.greaterThan(0);
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
  expect(await rep.query(tx => tx.get('a'))).to.equal(42);
  await rep.close();

  // Open again and test that we lost the data
  rep = await replicacheForTesting('mem');
  expect(await rep.query(tx => tx.get('a'))).to.equal(undefined);
});

test('isEmpty', async () => {
  const rep = await replicacheForTesting('test-is-empty', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
      mut: async (tx: WriteTransaction) => {
        expect(await tx.isEmpty()).to.equal(false);

        await tx.del('c');
        expect(await tx.isEmpty()).to.equal(false);

        await tx.del('a');
        expect(await tx.isEmpty()).to.equal(true);

        await tx.set('d', 4);
        expect(await tx.isEmpty()).to.equal(false);
      },
    },
  });
  const {addData: add, del, mut} = rep.mutate;

  async function t(expected: boolean) {
    expect(await rep.query(tx => tx.isEmpty())).to.equal(expected);
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
        expect(tx.mutationID).to.equal(expectedMutationID++);
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

  const rep = await replicacheForTesting('onSync', {
    pullURL,
    pushURL,
    pushDelay: 5,
    mutators: {addData},
    enablePullAndPushInOpen: false,
  });
  const add = rep.mutate.addData;

  const onSync = sinon.fake();
  rep.onSync = onSync;

  expect(onSync.callCount).to.equal(0);

  const {clientID} = rep;
  fetchMock.postOnce(pullURL, makePullResponseV1(clientID, 2));
  await rep.pull();
  await tickAFewTimes(15);

  expect(onSync.callCount).to.equal(2);
  expect(onSync.getCall(0).args[0]).to.be.true;
  expect(onSync.getCall(1).args[0]).to.be.false;

  onSync.resetHistory();
  fetchMock.postOnce(pushURL, {});
  await add({a: 'a'});
  await tickAFewTimes();

  expect(onSync.callCount).to.equal(2);
  expect(onSync.getCall(0).args[0]).to.be.true;
  expect(onSync.getCall(1).args[0]).to.be.false;

  fetchMock.postOnce(pushURL, {});
  onSync.resetHistory();
  await add({b: 'b'});
  await tickAFewTimes();
  expect(onSync.callCount).to.equal(2);
  expect(onSync.getCall(0).args[0]).to.be.true;
  expect(onSync.getCall(1).args[0]).to.be.false;

  {
    // Try with reauth
    const consoleErrorStub = sinon.stub(console, 'error');
    fetchMock.postOnce(pushURL, {body: 'xxx', status: httpStatusUnauthorized});
    onSync.resetHistory();
    rep.getAuth = () => {
      // Next time it is going to be fine
      fetchMock.postOnce({url: pushURL, headers: {authorization: 'ok'}}, {});
      return 'ok';
    };

    await add({c: 'c'});

    await tickUntil(() => onSync.callCount >= 4);

    expectConsoleLogContextStub(
      rep.name,
      consoleErrorStub.firstCall,
      'Got error response doing push: 401: xxx',
      ['push', requestIDLogContextRegex],
    );

    expect(onSync.callCount).to.equal(4);
    expect(onSync.getCall(0).args[0]).to.be.true;
    expect(onSync.getCall(1).args[0]).to.be.false;
    expect(onSync.getCall(2).args[0]).to.be.true;
    expect(onSync.getCall(3).args[0]).to.be.false;
  }

  rep.onSync = null;
  onSync.resetHistory();
  fetchMock.postOnce(pushURL, {});
  expect(onSync.callCount).to.equal(0);
});

test('push timing', async () => {
  const pushURL = 'https://push.com/push';
  const pushDelay = 5;

  const rep = await replicacheForTesting('push-timing', {
    pushURL,
    pushDelay,
    mutators: {addData},
  });

  const onInvokePush = (getTestInstance(rep).onPushInvoked = sinon.fake());

  const add = rep.mutate.addData;

  fetchMock.post(pushURL, {});
  await add({a: 0});
  await tickAFewTimes();

  const pushCallCount = () => {
    const rv = onInvokePush.callCount;
    onInvokePush.resetHistory();
    return rv;
  };

  expect(pushCallCount()).to.equal(1);

  // This will schedule push in pushDelay ms
  await add({a: 1});
  await add({b: 2});
  await add({c: 3});
  await add({d: 4});

  expect(pushCallCount()).to.equal(0);

  await clock.tickAsync(pushDelay + 10);

  expect(pushCallCount()).to.equal(1);

  const p1 = add({e: 5});
  const p2 = add({f: 6});
  const p3 = add({g: 7});

  expect(pushCallCount()).to.equal(0);

  await tickAFewTimes();
  await p1;
  expect(pushCallCount()).to.equal(1);
  await tickAFewTimes();
  await p2;
  expect(pushCallCount()).to.equal(0);
  await tickAFewTimes();
  await p3;
  expect(pushCallCount()).to.equal(0);
});

test('push and pull concurrently', async () => {
  const pushURL = 'https://push.com/push';
  const pullURL = 'https://pull.com/pull';

  const rep = await replicacheForTesting('concurrently', {
    pullURL,
    pushURL,
    pushDelay: 10,
    mutators: {addData},
    enablePullAndPushInOpen: false,
  });

  const onBeginPull = (getTestInstance(rep).onBeginPull = sinon.fake());
  const commitSpy = sinon.spy(Write.prototype, 'commitWithDiffs');
  const onPushInvoked = (getTestInstance(rep).onPushInvoked = sinon.fake());

  function resetSpies() {
    onBeginPull.resetHistory();
    commitSpy.resetHistory();
    onPushInvoked.resetHistory();
  }

  const callCounts = () => {
    const rv = {
      beginPull: onBeginPull.callCount,
      commit: commitSpy.callCount,
      invokePush: onPushInvoked.callCount,
    };
    resetSpies();
    return rv;
  };

  const add = rep.mutate.addData;

  const requests: string[] = [];

  const {clientID} = rep;
  fetchMock.post(pushURL, () => {
    requests.push(pushURL);
    return {};
  });
  fetchMock.post(pullURL, () => {
    requests.push(pullURL);
    return makePullResponseV1(clientID, 0, [], null);
  });

  await add({a: 0});
  resetSpies();

  await add({b: 1});
  await rep.pull();

  await clock.tickAsync(10);

  // Only one push at a time but we want push and pull to be concurrent.
  expect(callCounts()).to.deep.equal({
    beginPull: 1,
    commit: 1,
    invokePush: 1,
  });

  await tickAFewTimes();

  expect(requests).to.deep.equal([pullURL, pushURL]);

  await tickAFewTimes();

  expect(requests).to.deep.equal([pullURL, pushURL]);

  expect(callCounts()).to.deep.equal({
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
  await tickAFewTimes();

  const req = await fetchMock.lastCall().request.json();
  expect(req.schemaVersion).to.deep.equal(schemaVersion);
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

  fetchMock.post(pushURL, {});
  await tickAFewTimes();

  const req = await fetchMock.lastCall().request.json();
  expect(req.schemaVersion).to.deep.equal(schemaVersion);
});

test('clientID', async () => {
  const re =
    /^[0-9:A-z]{8}-[0-9:A-z]{4}-4[0-9:A-z]{3}-[0-9:A-z]{4}-[0-9:A-z]{12}$/;

  let rep = await replicacheForTesting('clientID');
  const {clientID} = rep;
  expect(clientID).to.match(re);
  await rep.close();

  const rep2 = await replicacheForTesting('clientID2');
  const clientID2 = rep2.clientID;
  expect(clientID2).to.match(re);
  expect(clientID2).to.not.equal(clientID);

  rep = await replicacheForTesting('clientID');
  const clientID3 = rep.clientID;
  expect(clientID3).to.match(re);
  // With SDD we never reuse client IDs.
  expect(clientID3).to.not.equal(clientID);

  const rep4 = new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name: 'clientID4',
    pullInterval: null,
  });
  const clientID4 = rep4.clientID;
  expect(clientID4).to.match(re);
  await rep4.close();
});

test('profileID', async () => {
  const re = /^p.+/; // More specific re tested in IdbDatabase.test.ts.

  const rep = await replicacheForTesting('clientID');
  const profileID = await rep.profileID;
  expect(profileID).to.not.equal(rep.clientID);
  expect(profileID).to.match(re);
  await rep.close();

  const rep2 = await replicacheForTesting('clientID2');
  const profileID2 = await rep2.profileID;
  expect(profileID2).to.equal(profileID);

  const rep3 = new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name: 'clientID3',
  });
  const profileID3 = await rep3.profileID;
  expect(profileID3).to.equal(profileID);
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
  async function testPull(opt: {
    patch: PatchOperation[];
    expectedResult: JSONValue;
  }) {
    let pullDone = false;
    fetchMock.post(pullURL, () => {
      pullDone = true;
      return makePullResponseV1(clientID, lastMutationID++, opt.patch);
    });

    await rep.pull();

    await tickUntil(() => pullDone);
    await tickAFewTimes();

    const actualResult = await rep.query(tx =>
      tx.scan({indexName}).entries().toArray(),
    );
    expect(actualResult).to.deep.equal(opt.expectedResult);
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

async function populateDataUsingPull<
  // eslint-disable-next-line @typescript-eslint/ban-types
  MD extends MutatorDefs = {},
>(rep: ReplicacheTest<MD>, data: Record<string, ReadonlyJSONValue>) {
  const {clientID} = rep;
  fetchMock.postOnce(rep.pullURL, {
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
  await clock.tickAsync(22 * 1000);
  await tickAFewTimes(20, 100);

  await rep.persist();
}

async function tickUntilTimeIs(time: number, tick = 10) {
  while (Date.now() < time) {
    await clock.tickAsync(tick);
  }
}

test('pull mutate options', async () => {
  const pullURL = 'https://diff.com/pull';
  const rep = await replicacheForTesting('pull-mutate-options', {
    pullURL,
    pushURL: '',
    ...disableAllBackgroundProcesses,
    enablePullAndPushInOpen: false,
  });
  const {clientID} = rep;
  const log: number[] = [];

  fetchMock.post(pullURL, () => {
    log.push(Date.now());
    return makePullResponseV1(clientID, 0, [], '');
  });

  await tickUntilTimeIs(1000);

  while (Date.now() < 1150) {
    rep.pullIgnorePromise();
    await clock.tickAsync(10);
  }

  rep.requestOptions.minDelayMs = 500;

  while (Date.now() < 2000) {
    rep.pullIgnorePromise();
    await clock.tickAsync(100);
  }

  rep.requestOptions.minDelayMs = 25;

  while (Date.now() < 2500) {
    rep.pullIgnorePromise();
    await clock.tickAsync(5);
  }

  // the first one is often off by a few ms
  expect(log[0]).to.be.within(1000, 1060);
  expect(log.slice(1)).to.deep.equal([
    // 1000, checked above
    1030, 1060, 1090, 1120, 1150, 1180, 1680, 2180, 2205, 2230, 2255, 2280,
    2305, 2330, 2355, 2380, 2405, 2430, 2455, 2480,
  ]);
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

  const consoleDebugStub = sinon.stub(console, 'debug');

  fetchMock.post(pushURL, async () => {
    await sleep(10);
    return {throws: new Error('Simulate fetch error in push')};
  });

  expect(rep.online).to.equal(true);
  expect(log).to.deep.equal([]);

  await rep.mutate.addData({a: 0});

  await tickAFewTimes();

  expect(rep.online).to.equal(false);
  expect(
    consoleDebugStub
      .getCalls()
      .some(({args}) => args.join('\n').indexOf('Push threw') > -1),
  );
  expect(log).to.deep.equal([false]);

  consoleDebugStub.resetHistory();

  fetchMock.post(pushURL, 'ok');
  await rep.mutate.addData({a: 1});

  await tickAFewTimes(20);

  expect(
    !consoleDebugStub
      .getCalls()
      .some(({args}) => args.join('\n').indexOf('Push threw') > -1),
  );
  expect(rep.online).to.equal(true);
  expect(log).to.deep.equal([false, true]);
});

type LicenseKeyCheckTestCase = {
  licenseKey: string;
  enableLicensing?: boolean; // default true
  mockFetchParams: object | undefined;
  expectValid: boolean;
  expectDisable: boolean;
  expectFetchCalled: boolean;
  expectDisableAfter?: number;
};

// TODO(phritz) ick, export these urls from the licensing client.
const statusUrlMatcher = new RegExp(
  `${PROD_LICENSE_SERVER_URL}${LICENSE_STATUS_PATH.slice(1)}`,
);
const activeUrlMatcher = new RegExp(
  `${PROD_LICENSE_SERVER_URL}${LICENSE_ACTIVE_PATH.slice(1)}`,
);

async function licenseKeyCheckTest(tc: LicenseKeyCheckTestCase) {
  const consoleErrorStub = sinon.stub(console, 'error');
  const name = 'license-key-test';
  fetchMock.reset();
  fetchMock.post(activeUrlMatcher, 200);
  if (tc.expectFetchCalled) {
    fetchMock.postOnce(statusUrlMatcher, tc.mockFetchParams);
  }
  fetchMock.catch();

  const rep = await replicacheForTesting(
    name,
    tc.enableLicensing !== undefined
      ? {
          licenseKey: tc.licenseKey,
          enableLicensing: tc.enableLicensing,
        }
      : {licenseKey: tc.licenseKey},
  );

  expect(await rep.licenseValid()).to.equal(tc.expectValid);
  if (tc.expectDisable) {
    expect(rep.closed).to.be.true;
    expect(consoleErrorStub.lastCall.args[1]).to.match(/REPLICACHE DISABLED/);
  } else {
    expect(rep.closed).to.be.false;

    if (tc.expectDisableAfter !== undefined) {
      await clock.tickAsync(tc.expectDisableAfter);
      expect(rep.closed).to.be.true;
    }
  }
  if (!tc.expectValid) {
    expect(consoleErrorStub.getCall(0).args[1]).to.match(
      /REPLICACHE LICENSE NOT VALID/,
    );
  }
  expect(fetchMock.called(statusUrlMatcher)).to.equal(tc.expectFetchCalled);

  await rep.close();
}

test('empty licensing key is not valid and does not send status check', async () => {
  await licenseKeyCheckTest({
    licenseKey: '',
    mockFetchParams: undefined,
    expectValid: false,
    expectDisable: true,
    expectFetchCalled: false,
  });
});

test('test licensing key is valid and does not send status check', async () => {
  await licenseKeyCheckTest({
    licenseKey: TEST_LICENSE_KEY,
    mockFetchParams: undefined,
    expectValid: true,
    expectDisable: false,
    expectFetchCalled: false,
    expectDisableAfter: 5 * 60 * 1000,
  });
});

test('test when internal option enableLicensing is false any key is valid and does not send status check', async () => {
  await licenseKeyCheckTest({
    licenseKey: 'any-random-key',
    enableLicensing: false,
    mockFetchParams: undefined,
    expectValid: true,
    expectDisable: false,
    expectFetchCalled: false,
  });
});

test('licensing key is valid if check returns valid', async () => {
  await licenseKeyCheckTest({
    licenseKey: 'l123validkey',
    mockFetchParams: {
      body: {
        status: LicenseStatus.Valid,
        disable: false,
        pleaseUpdate: false,
      },
    },
    expectValid: true,
    expectDisable: false,
    expectFetchCalled: true,
  });
});

test('licensing key is not valid if check returns invalid', async () => {
  await licenseKeyCheckTest({
    licenseKey: 'l123keyreturnsINVALID',
    mockFetchParams: {
      body: {
        status: LicenseStatus.Invalid,
        disable: false,
        pleaseUpdate: false,
      },
    },
    expectValid: false,
    expectDisable: false,
    expectFetchCalled: true,
  });
});

test('Replicache is disabled if check returns disable', async () => {
  await licenseKeyCheckTest({
    licenseKey: 'l123keyreturnsINVALID',
    mockFetchParams: {
      body: {
        status: LicenseStatus.Invalid,
        disable: true,
        pleaseUpdate: false,
      },
    },
    expectValid: false,
    expectDisable: true,
    expectFetchCalled: true,
  });
});

test('licensing key is valid if check throws', async () => {
  await licenseKeyCheckTest({
    licenseKey: 'l123keythrows',
    mockFetchParams: {
      throws: new Error('kaboom (this is a fake error in a test)'),
    },
    expectValid: true,
    expectDisable: false,
    expectFetchCalled: true,
  });
});

test('licensing key is valid if check returns non-200', async () => {
  await licenseKeyCheckTest({
    licenseKey: 'lkeyreturns500',
    mockFetchParams: {
      status: 500,
    },
    expectValid: true,
    expectDisable: false,
    expectFetchCalled: true,
  });
});

type LicenseActiveTestCase = {
  licenseKey: string | undefined;
  enableLicensing?: boolean; // default true
  mockFetchParams: object | undefined;
  expectActive: boolean;
  expectFetchCalled: boolean;
};

async function licenseActiveTest(tc: LicenseActiveTestCase) {
  // Silence console.error
  sinon.stub(console, 'error');
  // TODO: assert we are getting the correct logs

  fetchMock.reset();
  fetchMock.post(
    statusUrlMatcher,
    '{"status": "VALID", "disable": false, "pleaseUpdate": false}',
  );
  if (tc.expectFetchCalled) {
    fetchMock.postOnce(activeUrlMatcher, tc.mockFetchParams);
  }
  fetchMock.catch();
  const rep = await replicacheForTesting(
    'license-active-test',
    tc.enableLicensing !== undefined
      ? {
          licenseKey: tc.licenseKey,
          enableLicensing: tc.enableLicensing,
        }
      : {licenseKey: tc.licenseKey},
  );
  const licenseActive = await rep.licenseActive();
  expect(licenseActive).to.equal(tc.expectActive);
  expect(fetchMock.called(activeUrlMatcher)).to.equal(tc.expectFetchCalled);
  if (tc.expectFetchCalled && fetchMock.called(activeUrlMatcher)) {
    const got = JSON.parse(fetchMock.lastCall(activeUrlMatcher)[1].body);
    const {licenseKey, profileID} = got;
    expect(licenseKey).to.equal(tc.licenseKey);
    expect(profileID).to.equal(await rep.profileID);
  }
  // TODO(phritz) Should we test that it gets called repeatedly?
  await rep.close();
}

test('no licensing key is not active and does not send active pings', async () => {
  await licenseActiveTest({
    licenseKey: undefined,
    mockFetchParams: undefined,
    expectActive: false,
    expectFetchCalled: false,
  });
});

test('test licensing key is not active and does not send active pings', async () => {
  await licenseActiveTest({
    licenseKey: TEST_LICENSE_KEY,
    mockFetchParams: undefined,
    expectActive: false,
    expectFetchCalled: false,
  });
});

test('test when internal option enableLicensing is false any licensing key is not active and does not send active pings', async () => {
  await licenseActiveTest({
    licenseKey: 'any-random-key',
    enableLicensing: false,
    mockFetchParams: undefined,
    expectActive: false,
    expectFetchCalled: false,
  });
});

test('a non-empty, non-test licensing key is active and does send active pings', async () => {
  await licenseActiveTest({
    licenseKey: 'l123validkey',
    mockFetchParams: {
      status: 200,
      body: '{}',
    },
    expectActive: true,
    expectFetchCalled: true,
  });
});

test('overlapping open/close', async () => {
  const pullInterval = 60_000;
  const name = 'overlapping-open-close';

  const rep = new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name,
    pullInterval,
  });
  const p = rep.close();

  const rep2 = new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name,
    pullInterval,
  });
  const p2 = rep2.close();

  const rep3 = new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name,
    pullInterval,
  });
  const p3 = rep3.close();

  await p;
  await p2;
  await p3;

  {
    const rep = new Replicache({
      licenseKey: TEST_LICENSE_KEY,
      name,
      pullInterval,
    });
    await rep.clientGroupID;
    const p = rep.close();
    const rep2 = new Replicache({
      licenseKey: TEST_LICENSE_KEY,
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
  await clock.tickAsync(2000);

  expect(store.readCount).to.be.greaterThan(0, 'readCount');
  expect(store.writeCount).to.be.greaterThan(0, 'writeCount');
  expect(store.closeCount).to.equal(0, 'closeCount');
  store.resetCounters();

  const b = await rep.query(tx => tx.has('foo'));
  expect(b).to.be.false;
  // When DD31 refresh has pulled enough data into the lazy store
  // to not have to read from the experiment-kv-store
  expect(store.readCount).to.equal(1, 'readCount');
  expect(store.writeCount).to.equal(0, 'writeCount');
  expect(store.closeCount).to.equal(0, 'closeCount');
  store.resetCounters();

  await rep.mutate.addData({foo: 'bar'});
  expect(store.readCount).to.equal(0, 'readCount');
  expect(store.writeCount).to.equal(0, 'writeCount');
  expect(store.closeCount).to.equal(0, 'closeCount');
  store.resetCounters();

  await rep.persist();

  expect(store.readCount).to.equal(1, 'readCount');
  expect(store.writeCount).to.equal(1, 'writeCount');
  expect(store.closeCount).to.equal(0, 'closeCount');
  store.resetCounters();

  await rep.close();
  expect(store.readCount).to.equal(0, 'readCount');
  expect(store.writeCount).to.equal(0, 'writeCount');
  expect(store.closeCount).to.equal(1, 'closeCount');
}

test('Experimental Create KV Store', async () => {
  let store: MemStoreWithCounters | undefined;

  const rep = await replicacheForTesting('experiment-kv-store', {
    experimentalCreateKVStore: name => {
      if (!store && name.includes('experiment-kv-store')) {
        store = new MemStoreWithCounters(name);
        return store;
      }

      return new MemStoreWithCounters(name);
    },
    mutators: {addData},
    ...disableAllBackgroundProcesses,
  });

  expect(store).instanceOf(MemStoreWithCounters);
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
    experimentalCreateKVStore: () => store,
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
  expect(err).instanceOf(Error);

  // Safari does not have requestIdleTimeout so it waits for a second.
  await clock.tickAsync(1000);

  const o = findPropertyValue(store.map(), 'mutatorName', 'mutArgs');
  expect(o).undefined;
});

test('client ID is set correctly on transactions', async () => {
  const rep = await replicacheForTesting(
    'client-id-is-set-correctly-on-transactions',
    {
      mutators: {
        expectClientID(tx: WriteTransaction, expectedClientID: ClientID) {
          expect(tx.clientID).to.equal(expectedClientID);
        },
      },
    },
  );

  const {clientID} = rep;

  await rep.query(tx => {
    expect(tx.clientID).to.equal(clientID);
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
    // eslint-disable-next-line require-await
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
  expect(pending).deep.equal([
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
  await tickAFewTimes();

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
  expect(val).equal('dog');

  // Check that mutation timestamp did not change
  await rep.push({now: true});
  expect(pending).deep.equal([
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

suite('check for client not found in visibilitychange', () => {
  const t = (
    visibilityState: DocumentVisibilityState,
    shouldBeCalled: boolean,
  ) => {
    test('visibilityState: ' + visibilityState, async () => {
      const consoleErrorStub = sinon.stub(console, 'error');
      const visibilityStateResolver = resolver<void>();
      sinon.stub(document, 'visibilityState').get(() => {
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

      consoleErrorStub.resetHistory();

      document.dispatchEvent(new Event('visibilitychange'));
      await visibilityStateResolver.promise;

      if (shouldBeCalled) {
        await onClientStateNotFound.resolver.promise;
        expect(onClientStateNotFound.called).true;
        expectLogContext(
          consoleErrorStub,
          0,
          rep,
          `Client state not found on client, clientID: ${clientID}`,
        );
      } else {
        expect(onClientStateNotFound.called).false;
      }

      await rep.close();
    });
  };

  t('hidden', false);
  t('visible', true);
});

test('scan in write transaction', async () => {
  let x = 0;
  const rep = await replicacheForTesting('scan-before-commit', {
    mutators: {
      async test(tx: WriteTransaction, v: number) {
        await tx.set('a', v);
        expect(await tx.scan().toArray()).to.deep.equal([v]);
        x++;
      },
    },
  });

  await rep.mutate.test(42);

  expect(x).to.equal(1);
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
  expect(log).to.deep.equal([
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
  expect(log).to.deep.equal([
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
  expect(values).to.deep.equal([1, 2, 3]);

  await rep.mutate.race();
  const v = await rep.query(tx => tx.get('d'));
  expect(v === 1 || v === 4).to.be.true;

  const v2 = await rep.query(tx => tx.get('a'));
  expect(v2).to.equal(4);
});

test('Invalid name', () => {
  expect(
    () => new ReplicacheTest({name: '', licenseKey: TEST_LICENSE_KEY}),
  ).to.throw('name is required and must be non-empty');
  expect(
    () =>
      new Replicache({
        name: 1 as unknown as string,
        licenseKey: TEST_LICENSE_KEY,
      }),
  ).to.throw('name is required and must be non-empty');

  expect(
    () =>
      new ReplicacheTest({
        name: true as unknown as string,
        licenseKey: TEST_LICENSE_KEY,
      }),
  ).to.throw(TypeError);
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

  expect(await set(undefined)).instanceOf(TypeError);

  // no error
  expect(await set({a: undefined})).equal(undefined);

  expect(await set([1, undefined, 2])).instanceOf(TypeError);

  // eslint-disable-next-line no-sparse-arrays
  expect(await set([1, , 2])).instanceOf(TypeError);
});
