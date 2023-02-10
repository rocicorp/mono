import {expect} from '@esm-bundle/chai';
import {fakeHash, Hash} from '../hash';
import {computeRefCountUpdates, GarbageCollectionDelegate} from './gc';

function createGraph(args: {
  graph: Record<string, string[]>;
  heads: string[];
  allZeroRefCounts?: boolean;
}) {
  const {graph, heads, allZeroRefCounts} = args;
  const hashes = Object.fromEntries(
    Object.keys(graph).map(k => [k.toString(), fakeHash(k)]),
  );
  const refs = Object.fromEntries(
    Object.entries(graph).map(([k, refs]) => [
      hashes[k].toString(),
      refs.map(v => hashes[v]),
    ]),
  );

  const refCounts = Object.fromEntries(
    Object.keys(graph).map(k => [hashes[k].toString(), 0]),
  );

  if (!allZeroRefCounts) {
    const q = Array.from(new Set(heads));
    for (const k of q) {
      refCounts[hashes[k].toString()] = refCounts[hashes[k].toString()] + 1;
      q.push(...graph[k]);
    }
  }

  const delegate: GarbageCollectionDelegate = {
    getRefCount: async hash => refCounts[hash.toString()] || 0,
    getRefs: async hash => refs[hash.toString()] || [],
  };

  return {
    hashes,
    refCounts,
    refs,
    delegate,
  };
}

function expectRefCountUpdates(
  actual: Map<Hash, number>,
  expected: Record<string, number>,
) {
  const expectedAsMap = new Map(
    Object.entries(expected).map(([k, v]) => [fakeHash(k), v]),
  );
  expect(actual).to.deep.equal(expectedAsMap);
}

test('computeRefCountUpdates includes entry for every putChunk', async () => {
  //   R    C
  //  / \   |
  // A   B  D
  const {hashes, delegate} = createGraph({
    graph: {
      r: ['a', 'b'],
      a: [],
      b: [],
      c: ['d'],
      d: [],
    },
    heads: [],
    allZeroRefCounts: true,
  });

  const refCountUpdates = await computeRefCountUpdates(
    [{old: undefined, new: hashes['r']}],
    new Set(Object.values(hashes)),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    r: 1,
    a: 1,
    b: 1,
    c: 0,
    d: 0,
  });
});

test('computeRefCountUpdates for basic diamond pattern', async () => {
  // If we have a diamond structure we update the refcount for C twice.
  //
  //   R
  //  / \
  //  A  B
  //  \ /
  //   C

  const {hashes, delegate} = createGraph({
    graph: {
      r: ['a', 'b'],
      a: ['c'],
      b: ['c'],
      c: [],
    },
    heads: ['r'],
  });

  const eHash = fakeHash('e');
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['r'], new: eHash}],
    new Set([eHash]),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    r: 0,
    a: 0,
    b: 0,
    c: 0,
    e: 1,
  });
});

test('computeRefCountUpdates for a diamond pattern and a child', async () => {
  // If we have a diamond structure we update the refcount for C twice.
  //
  //   R
  //  / \
  //  A  B
  //  \ /
  //   C
  //   |
  //   D

  const {hashes, delegate} = createGraph({
    graph: {
      r: ['a', 'b'],
      a: ['c'],
      b: ['c'],
      c: ['d'],
      d: [],
    },
    heads: ['r'],
  });

  // Move test head from R to A
  //  A
  //  |
  //  C
  //  |
  //  D
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['r'], new: hashes['a']}],
    new Set(),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    r: 0,
    a: 1,
    b: 0,
    c: 1,
  });
});

test('computeRefCountUpdates for 3 incoming refs', async () => {
  // If we have a diamond structure we update the refcount for D three times.
  //
  //    R
  //  / | \
  //  A B C
  //  \ | /
  //    D

  const {hashes, delegate} = createGraph({
    graph: {
      r: ['a', 'b', 'c'],
      a: ['d'],
      b: ['d'],
      c: ['d'],
      d: [],
    },
    heads: ['r'],
  });

  const eHash = fakeHash('e');
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['r'], new: eHash}],
    new Set([eHash]),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    r: 0,
    a: 0,
    b: 0,
    c: 0,
    d: 0,
    e: 1,
  });
});

test('computeRefCountUpdates for 3 incoming refs bypassing one level', async () => {
  //    R
  //  / | \
  //  A B  |
  //  \ | /
  //    C

  const {hashes, delegate} = createGraph({
    graph: {
      r: ['a', 'b', 'c'],
      a: ['c'],
      b: ['c'],
      c: [],
    },
    heads: ['r'],
  });

  const eHash = fakeHash('e');
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['r'], new: eHash}],
    new Set([eHash]),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    r: 0,
    a: 0,
    b: 0,
    c: 0,
    e: 1,
  });
});

test('computeRefCountUpdates for heads updating to same hash should have no refcount updates', async () => {
  const {hashes, delegate} = createGraph({
    graph: {
      r: ['a'],
      a: [],
    },
    heads: ['r'],
  });

  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['r'], new: hashes['r']}],
    new Set(),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {});
});
