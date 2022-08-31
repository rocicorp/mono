import {
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util';
import type {WriteTransaction} from './mod';
import type {JSONValue} from './json';
import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
import {WatchHash, WatchHashInitialRun} from './subscriptions';
import type {Hash} from './hash';

initReplicacheTesting();

async function addData(tx: WriteTransaction, data: {[key: string]: JSONValue}) {
  for (const [key, value] of Object.entries(data)) {
    await tx.put(key, value);
  }
}

function expectWatchCallbackArgs(
  args: unknown[],
  expectedDiff: unknown[],
  watchHash: WatchHash,
) {
  expect(args).to.deep.equal([expectedDiff, watchHash]);
}

function watchHashFromHash(hash: Hash | undefined): WatchHash {
  return hash ? (hash as unknown as WatchHash) : WatchHashInitialRun;
}

test('watch', async () => {
  const rep = await replicacheForTesting('watch', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy);

  await rep.mutate.addData({a: 1, b: 2});

  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
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
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.addData({a: 1, b: 2});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({a: 11});
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'change',
        key: 'a',
        newValue: 11,
        oldValue: 1,
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'del',
        key: 'b',
        oldValue: 2,
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

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
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.addData({a: 1, b: 2});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({a: 11});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({b: 3, b1: 4, c: 5});
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
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
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'del',
        key: 'b',
        oldValue: 3,
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

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
  expectWatchCallbackArgs(spy.lastCall.args, [], WatchHashInitialRun);
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
  expectWatchCallbackArgs(
    spy.lastCall.args,
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
    WatchHashInitialRun,
  );

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

  // Initial callback should always be called even with no data.
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(spy.lastCall.args, [], WatchHashInitialRun);
  spy.resetHistory();

  await rep.mutate.addData({a: 1, b: 2});

  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

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
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
    WatchHashInitialRun,
  );

  unwatch();
});

test('watch on index', async () => {
  const rep = await replicacheForTesting('watch-on-index', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
    indexes: {id1: {jsonPointer: '/id'}},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    indexName: 'id1',
  });

  await tickAFewTimes();

  await rep.mutate.addData({a: {id: 'aaa'}, b: {id: 'bbb'}});

  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: ['aaa', 'a'],
        newValue: {id: 'aaa'},
      },
      {
        op: 'add',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb'},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.addData({b: {id: 'bbb', more: 42}});
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'change',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb', more: 42},
        oldValue: {id: 'bbb'},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.del('a');
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'del',
        key: ['aaa', 'a'],
        oldValue: {id: 'aaa'},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  unwatch();
});

test('watch on index with prefix', async () => {
  const rep = await replicacheForTesting('watch-on-index-with-prefix', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
    indexes: {id1: {jsonPointer: '/id'}},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    indexName: 'id1',
    prefix: 'b',
  });

  await tickAFewTimes();

  await rep.mutate.addData({a: {id: 'aaa'}, b: {id: 'bbb'}});

  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb'},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.addData({b: {id: 'bbb', more: 42}});
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'change',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb', more: 42},
        oldValue: {id: 'bbb'},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.addData({a: {id: 'baa'}});
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: ['baa', 'a'],
        newValue: {id: 'baa'},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  spy.resetHistory();
  await rep.mutate.addData({c: {id: 'abaa'}});
  expect(spy.callCount).to.equal(0);

  spy.resetHistory();
  await rep.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'del',
        key: ['bbb', 'b'],
        oldValue: {id: 'bbb', more: 42},
      },
    ],
    watchHashFromHash(await rep.rootHash),
  );

  unwatch();
});

test('watch with index and initial callback with no data', async () => {
  const rep = await replicacheForTesting('watch-with-index-initial-no-data', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
    indexes: {id1: {jsonPointer: '/id'}},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    initialValuesInFirstDiff: true,
    indexName: 'id1',
  });
  await tickAFewTimes();

  // Initial callback should always be called even with no data.
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(spy.lastCall.args, [], WatchHashInitialRun);
  spy.resetHistory();

  unwatch();
});

test('watch and initial callback with data', async () => {
  const rep = await replicacheForTesting('watch-with-index-initial-and-data', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
    indexes: {id1: {jsonPointer: '/id'}},
  });

  await rep.mutate.addData({a: {id: 'aaa'}, b: {id: 'bbb'}});

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    initialValuesInFirstDiff: true,
    indexName: 'id1',
  });
  await tickAFewTimes();
  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: ['aaa', 'a'],
        newValue: {id: 'aaa'},
      },
      {
        op: 'add',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb'},
      },
    ],
    WatchHashInitialRun,
  );

  unwatch();
});

test('watch with index and prefix and initial callback and data', async () => {
  const rep = await replicacheForTesting('watch-with-index-and-prefix', {
    mutators: {addData, del: (tx, key) => tx.del(key)},
    indexes: {id1: {jsonPointer: '/id'}},
  });

  await rep.mutate.addData({a: {id: 'aaa'}, b: {id: 'bbb'}});

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    prefix: 'b',
    initialValuesInFirstDiff: true,
    indexName: 'id1',
  });

  await tickAFewTimes();

  expect(spy.callCount).to.equal(1);
  expectWatchCallbackArgs(
    spy.lastCall.args,
    [
      {
        op: 'add',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb'},
      },
    ],
    WatchHashInitialRun,
  );

  unwatch();
});
