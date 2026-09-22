// This test file is loaded by worker.test.ts

import {LogContext} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../shared/src/json.ts';
import {asyncIterableToArray} from './async-iterable-to-array.ts';
import {newIDBStoreWithMemFallback} from './kv/idb-store-with-mem-fallback.ts';
import {dropDatabase} from './persist/collect-idb-databases.ts';
import {IDBDatabasesStore} from './persist/idb-databases-store.ts';
import {Replicache} from './replicache.ts';
import type {ReadTransaction, WriteTransaction} from './transactions.ts';

onmessage = async (e: MessageEvent) => {
  const {name} = e.data;
  try {
    await testGetHasScanOnEmptyDB(name);
    postMessage(undefined);
  } catch (ex) {
    postMessage(ex);
  }
};

async function testGetHasScanOnEmptyDB(name: string) {
  const rep = new Replicache({
    pushDelay: 60_000, // Large to prevent interfering
    name,
    mutators: {
      testMut: async (
        tx: WriteTransaction,
        args: {key: string; value: JSONValue},
      ) => {
        const {key, value} = args;
        await tx.set(key, value);
        assert((await tx.has(key)) === true, 'Expected key to exist after set');
        const v = await tx.get(key);
        assert(deepEqual(v, value), 'Expected get value to equal set value');

        assert((await tx.del(key)) === true, 'Expected del to return true');
        assert(
          (await tx.has(key)) === false,
          'Expected key to not exist after del',
        );
      },
    },
  });

  try {
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

    async function t(tx: ReadTransaction) {
      assert(
        (await tx.get('key')) === undefined,
        'Expected get to return undefined for missing key',
      );
      assert(
        (await tx.has('key')) === false,
        'Expected has to return false for missing key',
      );

      const scanItems = await asyncIterableToArray(tx.scan());
      assert(scanItems.length === 0, 'Expected scan items to be empty');
    }

    await rep.query(t);
  } finally {
    // Workers use the real origin-wide IndexedDB, bypassing vitest browser
    // mode's per-file storage isolation, so clean up here rather than in
    // worker.test.ts. dropDatabase removes both the database and its record
    // in the replicache-dbs-v0 registry.
    await rep.close();
    await dropDatabase(rep.idbName);
  }

  // Verify the registry record is gone; a record left here leaks into every
  // other browser test file's storage.
  const store = new IDBDatabasesStore(name =>
    newIDBStoreWithMemFallback(new LogContext(), name),
  );
  try {
    const dbs = await store.getDatabases();
    assert(
      !(rep.idbName in dbs),
      `Expected ${rep.idbName} to have been removed from the registry`,
    );
  } finally {
    await store.close();
  }
}
