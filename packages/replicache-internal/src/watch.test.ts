import {
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util';
import type {WriteTransaction} from './mod';
import type {JSONValue} from './json';
import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';

initReplicacheTesting();

async function addData(tx: WriteTransaction, data: {[key: string]: JSONValue}) {
  for (const [key, value] of Object.entries(data)) {
    await tx.put(key, value);
  }
}

test('watch', async () => {
  const rep = await replicacheForTesting('watch', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy);

  await rep.mutate.addData({a: 1, b: 2});

  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: 'a',
        newValue: 1,
      },
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.addData({a: 1, b: 2});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({a: 11});
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'change',
        key: 'a',
        newValue: 11,
        oldValue: 1,
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'del',
        key: 'b',
        oldValue: 2,
      },
    ],
  ]);

  unwatch();

  spy.resetHistory();
  await rep.mutate.addData({c: 6});
  expect(spy.callCount).to.equal(0);
});

test('watch with prefix', async () => {
  const rep = await replicacheForTesting('watch-with-prefix', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {prefix: 'b'});

  await rep.mutate.addData({a: 1, b: 2});

  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.addData({a: 1, b: 2});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({a: 11});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({b: 3, b1: 4, c: 5});
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'change',
        key: 'b',
        oldValue: 2,
        newValue: 3,
      },
      {
        op: 'add',
        key: 'b1',
        newValue: 4,
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'del',
        key: 'b',
        oldValue: 3,
      },
    ],
  ]);

  unwatch();

  spy.resetHistory();
  await rep.mutate.addData({b: 6});
  expect(spy.callCount).to.equal(0);
});

test('watch and initial callback with no data', async () => {
  const rep = await replicacheForTesting('watch', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {initialValuesInFirstDiff: true});
  await tickAFewTimes();
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([[]]);
  spy.resetHistory();

  unwatch();
});

test('watch and initial callback with data', async () => {
  const rep = await replicacheForTesting('watch', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  await rep.mutate.addData({a: 1, b: 2});

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {initialValuesInFirstDiff: true});
  await tickAFewTimes();
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: 'a',
        newValue: 1,
      },
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
  ]);

  spy.resetHistory();

  unwatch();
});

test('watch with prefix and initial callback no data', async () => {
  const rep = await replicacheForTesting('watch-with-prefix', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    prefix: 'b',
    initialValuesInFirstDiff: true,
  });

  await tickAFewTimes();

  // No data no callback.
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({a: 1, b: 2});

  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
  ]);

  unwatch();
});

test('watch with prefix and initial callback and data', async () => {
  const rep = await replicacheForTesting('watch-with-prefix', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  await rep.mutate.addData({a: 1, b: 2});

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    prefix: 'b',
    initialValuesInFirstDiff: true,
  });

  await tickAFewTimes();

  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
  ]);

  unwatch();
});
