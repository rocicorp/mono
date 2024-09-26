// This test file is loaded by worker.test.ts

import {TEST_LICENSE_KEY} from '@rocicorp/licensing/out/client';
import {expect} from 'chai';
import type {JSONValue} from 'shared/src/json.js';
import {asyncIterableToArray} from './async-iterable-to-array.js';
import {ReplicacheTest, closeAllReps, reps} from './test-util.js';
import type {ReadTransaction, WriteTransaction} from './transactions.js';

onmessage = async (e: MessageEvent) => {
  const {name} = e.data;
  try {
    await testGetHasScanOnEmptyDB(name);
    postMessage(undefined);
    await closeAllReps();
  } catch (ex) {
    await closeAllReps();
    postMessage(ex);
  }
};

async function testGetHasScanOnEmptyDB(name: string) {
  const rep = new ReplicacheTest({
    pushDelay: 60_000, // Large to prevent interfering
    name,
    licenseKey: TEST_LICENSE_KEY,
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
  reps.add(rep);

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
    expect(await tx.get('key')).to.equal(undefined);
    expect(await tx.has('key')).to.be.false;

    const scanItems = await asyncIterableToArray(tx.scan());
    expect(scanItems).to.have.length(0);
  }

  await rep.query(t);
}
