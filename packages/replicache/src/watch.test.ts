import {
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.js';
import type {WriteTransaction} from './mod.js';
import type {JSONValue} from './json.js';
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
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
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
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
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
  const rep = await replicacheForTesting('watch-no-data', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
    ...disableAllBackgroundProcesses,
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
  const rep = await replicacheForTesting('watch-with-data', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
    ...disableAllBackgroundProcesses,
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
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    prefix: 'b',
    initialValuesInFirstDiff: true,
  });

  await tickAFewTimes();

  // Initial callback should always be called even with no data.
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([[]]);
  spy.resetHistory();

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
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
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

test('watch on index', async () => {
  const rep = await replicacheForTesting('watch-on-index', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
    indexes: {id1: {jsonPointer: '/id'}},
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    indexName: 'id1',
  });

  await tickAFewTimes();

  await rep.mutate.addData({a: {id: 'aaa'}, b: {id: 'bbb'}});

  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
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
  ]);

  spy.resetHistory();
  await rep.mutate.addData({b: {id: 'bbb', more: 42}});
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'change',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb', more: 42},
        oldValue: {id: 'bbb'},
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.del('a');
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'del',
        key: ['aaa', 'a'],
        oldValue: {id: 'aaa'},
      },
    ],
  ]);

  unwatch();
});

test('watch on index with prefix', async () => {
  const rep = await replicacheForTesting('watch-on-index-with-prefix', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
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
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb'},
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.addData({b: {id: 'bbb', more: 42}});
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'change',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb', more: 42},
        oldValue: {id: 'bbb'},
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.addData({a: {id: 'baa'}});
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: ['baa', 'a'],
        newValue: {id: 'baa'},
      },
    ],
  ]);

  spy.resetHistory();
  await rep.mutate.addData({c: {id: 'abaa'}});
  expect(spy.callCount).to.equal(0);

  spy.resetHistory();
  await rep.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'del',
        key: ['bbb', 'b'],
        oldValue: {id: 'bbb', more: 42},
      },
    ],
  ]);

  unwatch();
});

test('watch with index and initial callback with no data', async () => {
  const rep = await replicacheForTesting('watch-with-index-initial-no-data', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
    indexes: {id1: {jsonPointer: '/id'}},
    ...disableAllBackgroundProcesses,
  });

  const spy = sinon.spy();
  const unwatch = rep.experimentalWatch(spy, {
    initialValuesInFirstDiff: true,
    indexName: 'id1',
  });
  await tickAFewTimes();

  // Initial callback should always be called even with no data.
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([[]]);
  spy.resetHistory();

  unwatch();
});

test('watch and initial callback with data', async () => {
  const rep = await replicacheForTesting('watch-with-index-initial-and-data', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
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
  expect(spy.lastCall.args).to.deep.equal([
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
  ]);

  unwatch();
});

test('watch with index and prefix and initial callback and data', async () => {
  const rep = await replicacheForTesting('watch-with-index-and-prefix', {
    mutators: {
      addData,
      del: (tx: WriteTransaction, key: string) => tx.del(key),
    },
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
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: ['bbb', 'b'],
        newValue: {id: 'bbb'},
      },
    ],
  ]);

  unwatch();
});
