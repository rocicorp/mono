import {expect} from 'chai';
import {fakeHash, Hash} from '../hash.js';
import {computeRefCountUpdates, RefCountUpdatesDelegate} from './gc.js';

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

  const delegate: RefCountUpdatesDelegate = {
    getRefCount: hash => refCounts[hash.toString()] || 0,
    getRefs: hash => refs[hash.toString()] || [],
  };

  return {
    hashes,
    refCounts,
    refs,
    delegate,
  };
}

function createLazyDelegate(
  refCounts: Record<string, number>,
  refs: Record<string, readonly string[]>,
  counted: Set<string>,
): RefCountUpdatesDelegate {
  const refCountsHashes = Object.fromEntries(
    Object.keys(refCounts).map(k => [fakeHash(k), refCounts[k]]),
  );
  const refsHashes = Object.fromEntries(
    Object.keys(refs).map(k => [fakeHash(k), refs[k].map(w => fakeHash(w))]),
  );
  const countedHashes = new Set([...counted.values()].map(w => fakeHash(w)));
  return {
    getRefCount: hash => refCountsHashes[hash],
    getRefs: hash => refsHashes[hash],
    areRefsCounted: hash => countedHashes.has(hash),
  };
}

function expectRefCountUpdates(
  actual: Map<Hash, number>,
  expected: Record<string, number>,
) {
  const expectedHashes = Object.fromEntries(
    Object.entries(expected).map(([k, v]) => [fakeHash(k), v]),
  );
  expect(Object.fromEntries(actual)).to.deep.equal(expectedHashes);
}

test('computeRefCountUpdates includes entry for every putChunk', async () => {
  //   R    C
  //  / \   |
  // A   B  D
  const {hashes, delegate} = createGraph({
    graph: {
      '000': ['a', 'b'],
      'a': [],
      'b': [],
      'c': ['d'],
      'd': [],
    },
    heads: [],
    allZeroRefCounts: true,
  });

  const refCountUpdates = await computeRefCountUpdates(
    [{old: undefined, new: hashes['000']}],
    new Set(Object.values(hashes)),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    '000': 1,
    'a': 1,
    'b': 1,
    'c': 0,
    'd': 0,
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
      '000': ['a', 'b'],
      'a': ['c'],
      'b': ['c'],
      'c': [],
    },
    heads: ['000'],
  });

  const eHash = fakeHash('e');
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['000'], new: eHash}],
    new Set([eHash]),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    '000': 0,
    'a': 0,
    'b': 0,
    'c': 0,
    'e': 1,
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
      '000': ['a', 'b'],
      'a': ['c'],
      'b': ['c'],
      'c': ['d'],
      'd': [],
    },
    heads: ['000'],
  });

  // Move test head from R to A
  //  A
  //  |
  //  C
  //  |
  //  D
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['000'], new: hashes['a']}],
    new Set(),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    '000': 0,
    'a': 1,
    'b': 0,
    'c': 1,
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
      '000': ['a', 'b', 'c'],
      'a': ['d'],
      'b': ['d'],
      'c': ['d'],
      'd': [],
    },
    heads: ['000'],
  });

  const eHash = fakeHash('e');
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['000'], new: eHash}],
    new Set([eHash]),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    '000': 0,
    'a': 0,
    'b': 0,
    'c': 0,
    'd': 0,
    'e': 1,
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
      '000': ['a', 'b', 'c'],
      'a': ['c'],
      'b': ['c'],
      'c': [],
    },
    heads: ['000'],
  });

  const eHash = fakeHash('e');
  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['000'], new: eHash}],
    new Set([eHash]),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {
    '000': 0,
    'a': 0,
    'b': 0,
    'c': 0,
    'e': 1,
  });
});

test('computeRefCountUpdates with lazy delegate', async () => {
  //    0
  //  / | \
  //  A B C
  //  \ | / \
  //    D    E
  //    |    |
  //    F    AA
  //    |  /
  //    BB
  // Refs for 0, A, B are counted, ref for C, D, E, F, AA, and BB are not
  // Changing to the below, and putting 1, C, E, and F
  //
  //    1
  //    |
  //    0
  //  / | \
  //  A B C
  //  \ | / \
  //    D    E
  //    |    |
  //    F   AA
  //    | /
  //    BB

  const delegate1 = createLazyDelegate(
    {0: 1, a: 1, b: 1, c: 1, d: 2},
    {
      1: ['0'],
      0: ['a', 'b', 'c'],
      a: ['d'],
      b: ['d'],
      c: ['d', 'e'],
      e: ['aa'],
      f: ['bb'],
    },
    new Set(['0', 'a', 'b']),
  );

  const refCountUpdates = await computeRefCountUpdates(
    [{old: fakeHash('0'), new: fakeHash('1')}],
    new Set([fakeHash('1'), fakeHash('c'), fakeHash('e'), fakeHash('f')]),
    delegate1,
  );

  // Expect C and E refs to be counted.  C was already known to be reachable
  // (positive ref count) but not counted, E becomes reachable via C.  Expect
  // F's refs not to be counted, it was not known to be reachable
  // (since D's refs have not been counted), and is still not known
  // to be reachable after the write (D's refs remain uncounted).
  // While the counting of E's refs discovers that AA is reachable (giving
  // it a ref count of 1), AA's refs are not counted because they are not
  // returned by the delegate (and thus BB's ref count is still 0).
  expectRefCountUpdates(refCountUpdates, {
    '0': 1,
    '1': 1,
    'c': 1,
    'd': 3,
    'e': 1,
    'aa': 1,
    'f': 0,
  });
});

test('computeRefCountUpdates for heads updating to same hash should have no refcount updates', async () => {
  const {hashes, delegate} = createGraph({
    graph: {
      '000': ['a'],
      'a': [],
    },
    heads: ['000'],
  });

  const refCountUpdates = await computeRefCountUpdates(
    [{old: hashes['000'], new: hashes['000']}],
    new Set(),
    delegate,
  );
  expectRefCountUpdates(refCountUpdates, {});
});
