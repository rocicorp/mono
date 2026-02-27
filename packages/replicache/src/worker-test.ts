// This test file is loaded by worker.test.ts

import {assert} from '../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../shared/src/json.ts';
import {asyncIterableToArray} from './async-iterable-to-array.ts';
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
}
