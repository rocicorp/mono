/* eslint-disable @typescript-eslint/naming-convention */
import {expect, test} from '@jest/globals';
import {compareUTF8} from 'compare-utf8';
import type {ScanOptions} from 'replicache';
import {assert} from 'shared/asserts.js';
import type {ReadonlyJSONValue} from 'shared/json.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {EntryCache} from '../../src/storage/entry-cache.js';
import {ReplicacheTransaction} from '../../src/storage/replicache-transaction.js';
import {
  UserValue,
  userValueKey,
  userValueSchema,
} from '../../src/types/user-value.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('ReplicacheTransaction', async () => {
  const storage = new DurableStorage(
    await getMiniflareDurableObjectStorage(id),
  );

  const entryCache = new EntryCache(storage);
  const writeTx = new ReplicacheTransaction(entryCache, 'c1', 1, 1);

  expect(!(await writeTx.has('foo')));
  expect(await writeTx.get('foo')).toBeUndefined;
  expect(await writeTx.isEmpty()).toBe(true);

  await writeTx.put('foo', 'bar');
  expect(await writeTx.has('foo'));
  expect(await writeTx.get('foo')).toEqual('bar');
  expect(await writeTx.isEmpty()).toBe(false);

  // They don't overlap until one flushes and the other is reloaded.
  const writeTx2 = new ReplicacheTransaction(
    new EntryCache(storage),
    'c1',
    2,
    2,
  );
  expect(!(await writeTx2.has('foo')));
  expect(await writeTx2.get('foo')).toBeUndefined;

  expect(await writeTx.scan().toArray()).toEqual(['bar']);
  expect(await writeTx2.scan().toArray()).toEqual([]);

  // Go ahead and flush one
  await entryCache.flush();
  const writeTx3 = new ReplicacheTransaction(entryCache, 'c1', 3, 3);
  expect(await writeTx3.has('foo'));
  expect(await writeTx3.get('foo')).toEqual('bar');

  // Check the underlying storage gets written in the way we expect.
  const expected: UserValue = {
    deleted: false,
    value: 'bar',
    version: 1,
  };
  expect(await storage.get(userValueKey('foo'), userValueSchema)).toEqual(
    expected,
  );

  // delete has special return value
  expect(await writeTx3.del('foo'));
  expect(!(await writeTx3.del('bar')));
});

test('ReplicacheTransaction environment and reason', async () => {
  const storage = new DurableStorage(
    await getMiniflareDurableObjectStorage(id),
  );

  const entryCache = new EntryCache(storage);
  const tx = new ReplicacheTransaction(entryCache, 'c1', 1, 1);
  expect(tx.environment).toEqual('server');
  expect(tx.reason).toEqual('authoritative');
});

test('ReplicacheTransaction scan()', async () => {
  const cfStorage = await getMiniflareDurableObjectStorage(id);
  const durableStorage = new DurableStorage(cfStorage);
  let version = 1;
  let mutationID = 1;
  // add some non-user data to durable storage
  await durableStorage.put('internal-value-1', 'foo');
  await durableStorage.put('internal-value-2', null);

  function makeTx(): [ReplicacheTransaction, EntryCache] {
    const cache = new EntryCache(durableStorage);
    const tx = new ReplicacheTransaction(cache, 'name', mutationID, version);
    version++;
    mutationID++;
    return [tx, cache];
  }

  async function put(tx: ReplicacheTransaction, key: string) {
    await tx.put(key, key);
  }

  async function expectScan(
    tx: ReplicacheTransaction,
    opts: ScanOptions,
    expected: [string, string][],
  ) {
    const actual = await tx.scan(opts).entries().toArray();
    expect(actual).toEqual(expected);

    // ensure the keys are in order
    for (let i = 0; i < actual.length - 1; i++) {
      const a = actual[i][0];
      const b = actual[i + 1][0];
      expect(compareUTF8(a, b)).toBeLessThan(0);
    }
  }

  const existingKeys: string[] = [
    'item/3',
    'item/1',
    'item/2',
    'user/5',
    'user/4',
    'user/6',
  ];

  let [tx, cache] = makeTx();
  for (const k of existingKeys) {
    await put(tx, k);
  }
  await cache.flush();

  // scan with no pending changes
  [tx, cache] = makeTx();
  await expectScan(tx, {}, [
    ['item/1', 'item/1'],
    ['item/2', 'item/2'],
    ['item/3', 'item/3'],
    ['user/4', 'user/4'],
    ['user/5', 'user/5'],
    ['user/6', 'user/6'],
  ]);
  await expectScan(tx, {prefix: ''}, [
    ['item/1', 'item/1'],
    ['item/2', 'item/2'],
    ['item/3', 'item/3'],
    ['user/4', 'user/4'],
    ['user/5', 'user/5'],
    ['user/6', 'user/6'],
  ]);
  await expectScan(tx, {limit: 3}, [
    ['item/1', 'item/1'],
    ['item/2', 'item/2'],
    ['item/3', 'item/3'],
  ]);
  await expectScan(tx, {prefix: 'i'}, [
    ['item/1', 'item/1'],
    ['item/2', 'item/2'],
    ['item/3', 'item/3'],
  ]);
  await expectScan(tx, {limit: 2, prefix: 'user'}, [
    ['user/4', 'user/4'],
    ['user/5', 'user/5'],
  ]);
  await expectScan(tx, {limit: 2, prefix: 'user', start: {key: 'user/5'}}, [
    ['user/5', 'user/5'],
    ['user/6', 'user/6'],
  ]);
  await expectScan(
    tx,
    {limit: 2, prefix: 'user', start: {key: 'user/5', exclusive: true}},
    [['user/6', 'user/6']],
  );

  // pending put()s
  await put(tx, 'item/1.5');
  await put(tx, 'user/4.5');

  async function testScanForPuts(tx: ReplicacheTransaction) {
    await expectScan(tx, {}, [
      ['item/1', 'item/1'],
      ['item/1.5', 'item/1.5'],
      ['item/2', 'item/2'],
      ['item/3', 'item/3'],
      ['user/4', 'user/4'],
      ['user/4.5', 'user/4.5'],
      ['user/5', 'user/5'],
      ['user/6', 'user/6'],
    ]);
    await expectScan(tx, {prefix: ''}, [
      ['item/1', 'item/1'],
      ['item/1.5', 'item/1.5'],
      ['item/2', 'item/2'],
      ['item/3', 'item/3'],
      ['user/4', 'user/4'],
      ['user/4.5', 'user/4.5'],
      ['user/5', 'user/5'],
      ['user/6', 'user/6'],
    ]);
    await expectScan(tx, {limit: 3}, [
      ['item/1', 'item/1'],
      ['item/1.5', 'item/1.5'],
      ['item/2', 'item/2'],
    ]);
    await expectScan(tx, {prefix: 'user'}, [
      ['user/4', 'user/4'],
      ['user/4.5', 'user/4.5'],
      ['user/5', 'user/5'],
      ['user/6', 'user/6'],
    ]);
    await expectScan(tx, {prefix: 'user', limit: 3}, [
      ['user/4', 'user/4'],
      ['user/4.5', 'user/4.5'],
      ['user/5', 'user/5'],
    ]);
    await expectScan(tx, {limit: 3, start: {key: 'item/3'}}, [
      ['item/3', 'item/3'],
      ['user/4', 'user/4'],
      ['user/4.5', 'user/4.5'],
    ]);
    await expectScan(
      tx,
      {prefix: 'user', limit: 3, start: {key: 'item/3', exclusive: true}},
      [
        ['user/4', 'user/4'],
        ['user/4.5', 'user/4.5'],
        ['user/5', 'user/5'],
      ],
    );
  }
  await testScanForPuts(tx);

  // pending del()s
  await tx.del('item/2');
  await tx.del('user/4');
  await tx.del('user/4.5'); // deleting a pending put()
  await tx.del('user/6');

  async function testScanForDels(tx: ReplicacheTransaction) {
    await expectScan(tx, {}, [
      ['item/1', 'item/1'],
      ['item/1.5', 'item/1.5'],
      ['item/3', 'item/3'],
      ['user/5', 'user/5'],
    ]);
    await expectScan(tx, {prefix: ''}, [
      ['item/1', 'item/1'],
      ['item/1.5', 'item/1.5'],
      ['item/3', 'item/3'],
      ['user/5', 'user/5'],
    ]);
    await expectScan(tx, {limit: 3}, [
      ['item/1', 'item/1'],
      ['item/1.5', 'item/1.5'],
      ['item/3', 'item/3'],
    ]);
    await expectScan(tx, {prefix: 'user'}, [['user/5', 'user/5']]);
    await expectScan(tx, {prefix: 'user', limit: 3}, [['user/5', 'user/5']]);
    await expectScan(tx, {limit: 3, start: {key: 'item/1.2'}}, [
      ['item/1.5', 'item/1.5'],
      ['item/3', 'item/3'],
      ['user/5', 'user/5'],
    ]);
    await expectScan(
      tx,
      {limit: 3, start: {key: 'item/1.5', exclusive: true}},
      [
        ['item/3', 'item/3'],
        ['user/5', 'user/5'],
      ],
    );
  }
  await testScanForDels(tx);

  // flush pending changes and test against stored data
  await cache.flush();
  [tx, cache] = makeTx();
  await testScanForDels(tx);
});

test('put with non JSON value', async () => {
  const storage = new DurableStorage(
    await getMiniflareDurableObjectStorage(id),
  );

  const entryCache = new EntryCache(storage);
  const writeTx = new ReplicacheTransaction(entryCache, 'c1', 1, 1);

  let err;
  try {
    await writeTx.put('key', {a: Symbol()} as unknown as ReadonlyJSONValue);
  } catch (e) {
    err = e;
  }
  assert(err instanceof TypeError);
  expect(err.message).toBe('Not a JSON value at a. Got symbol');
});
