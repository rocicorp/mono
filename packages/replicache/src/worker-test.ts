// This test file is loaded by worker.test.ts

import {assert} from '../../shared/src/asserts.js';
import {deepEqual, type JSONValue} from '../../shared/src/json.js';
import {asyncIterableToArray} from './async-iterable-to-array.js';
import {Replicache} from './replicache.js';
import type {ReadTransaction, WriteTransaction} from './transactions.js';

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
        assert((await tx.has(key)) === true);
        const v = await tx.get(key);
        assert(deepEqual(v, value));

        assert((await tx.del(key)) === true);
        assert((await tx.has(key)) === false);
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

  async function t(tx: ReadTransaction) {
    assert((await tx.get('key')) === undefined);
    assert((await tx.has('key')) === false);

    const scanItems = await asyncIterableToArray(tx.scan());
    assert(scanItems.length === 0);
  }

  await rep.query(t);
}
