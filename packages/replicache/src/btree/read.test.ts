import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {getSizeOfValue} from '../../../shared/src/size-of-value.ts';
import {asyncIterableToArray} from '../async-iterable-to-array.ts';
import type {Chunk} from '../dag/chunk.ts';
import {TestStore} from '../dag/test-store.ts';
import {Read as DBRead} from '../db/read.ts';
import * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';
import {ReadTransactionImpl} from '../transactions.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import type {DataNodeImpl, Entry, InternalNodeImpl} from './node.ts';
import {BTreeRead} from './read.ts';
import {BTreeWrite} from './write.ts';

const formatVersion = FormatVersion.Latest;

test.each([
  [8192, 16384],
  [2, 3],
])('Unicode diff with node sizes %i/%i', async (min, max) => {
  const store = new TestStore();
  await withWrite(store, async dagWrite => {
    const previous = makeTree(dagWrite, min, max);
    for (let i = 0; i < 12; i++) {
      await previous.put(`ascii-${i}`, i);
    }
    await previous.put('\u{10000}', 'before');
    await previous.flush();

    const current = new BTreeWrite(
      dagWrite,
      formatVersion,
      previous.rootHash,
      min,
      max,
      () => 1,
      0,
    );
    await current.put('\ue000', 'added');
    await current.put('\u{10000}', 'after');
    await current.flush();

    expect(await current.get('\ue000')).toBe('added');
    expect(await current.get('\u{10000}')).toBe('after');
    expect(await asyncIterableToArray(current.diff(previous))).toEqual([
      {op: 'add', key: '\ue000', newValue: 'added'},
      {op: 'change', key: '\u{10000}', oldValue: 'before', newValue: 'after'},
    ]);
    expect(await asyncIterableToArray(previous.diff(current))).toEqual([
      {op: 'del', key: '\ue000', oldValue: 'added'},
      {op: 'change', key: '\u{10000}', oldValue: 'after', newValue: 'before'},
    ]);
  });
});

// Small min/max sizes force a deep, multi-level tree so the internal-node
// branch of scanForHash (the one that prefetches children) is exercised.
function makeTree(
  dagWrite: ConstructorParameters<typeof BTreeWrite>[0],
  min = 2,
  max = 3,
) {
  return new BTreeWrite(
    dagWrite,
    formatVersion,
    undefined,
    min,
    max,
    () => 1,
    0,
  );
}

async function drainKeys(
  iter: AsyncIterable<Entry<unknown>>,
): Promise<string[]> {
  const out: string[] = [];
  for await (const entry of iter) {
    out.push(entry[0]);
  }
  return out;
}

test('scan over a multi-level btree yields all entries in order', async () => {
  const dagStore = new TestStore();
  const keys = Array.from({length: 200}, (_, i) => String(i).padStart(4, '0'));

  await withWrite(dagStore, async dagWrite => {
    const map = makeTree(dagWrite);
    for (const k of keys) {
      await map.put(k, k);
    }
    await map.flush();

    const rootNode = await map.getNode(map.rootHash);
    expect(rootNode.level).toBeGreaterThan(0);

    const sorted = keys.toSorted();

    expect(await drainKeys(map.scan('', true))).toEqual(sorted);
    expect(await drainKeys(map.scan('0100', true))).toEqual(
      sorted.filter(k => k >= '0100'),
    );
    expect(await drainKeys(map.scan('01005', true))).toEqual(
      sorted.filter(k => k >= '01005'),
    );
    expect(await drainKeys(map.scan('9999', true))).toEqual([]);
  });
});

// Overrides getNode so a single chunk read can be made to fail, letting us
// probe the boundaries of the prefetch added to scanForHash.
class PoisonBTree extends BTreeWrite {
  poisonHash: Hash | undefined = undefined;
  poisonReads = 0;

  override getNode(hash: Hash): Promise<DataNodeImpl | InternalNodeImpl> {
    if (this.poisonHash !== undefined && hash === this.poisonHash) {
      this.poisonReads++;
      return Promise.reject(new Error('poisoned chunk'));
    }
    return super.getNode(hash);
  }
}

test('prefetch error on a node the scan reaches is not masked by the catch', async () => {
  const dagStore = new TestStore();
  const keys = Array.from({length: 200}, (_, i) => String(i).padStart(4, '0'));

  await withWrite(dagStore, async dagWrite => {
    const map = new PoisonBTree(
      dagWrite,
      formatVersion,
      undefined,
      2,
      3,
      () => 1,
      0,
    );
    for (const k of keys) {
      await map.put(k, k);
    }
    await map.flush();

    const root = await map.getNode(map.rootHash);
    expect(root.level).toBeGreaterThan(0);

    // Child 0 is reached first when scanning from the start. The prefetch
    // swallows its own read error, but the serial recursion must still throw.
    map.poisonHash = (root.entries[0] as Entry<Hash>)[1];
    await expect(drainKeys(map.scan('', true))).rejects.toThrow(
      'poisoned chunk',
    );
  });
});

test('prefetch respects fromKey and does not read children before the search index', async () => {
  const dagStore = new TestStore();
  const keys = Array.from({length: 200}, (_, i) => String(i).padStart(4, '0'));

  await withWrite(dagStore, async dagWrite => {
    const map = new PoisonBTree(
      dagWrite,
      formatVersion,
      undefined,
      2,
      3,
      () => 1,
      0,
    );
    for (const k of keys) {
      await map.put(k, k);
    }
    await map.flush();

    const root = await map.getNode(map.rootHash);
    expect(root.level).toBeGreaterThan(0);

    // Poison child 0 and scan starting past child 0's max key. slice(i) must
    // exclude it, so neither the prefetch nor the serial walk may read it.
    const firstChild = root.entries[0] as Entry<Hash>;
    const firstChildMaxKey = firstChild[0];
    map.poisonHash = firstChild[1];

    const fromKey = keys.at(-1) ?? '';
    expect(fromKey > firstChildMaxKey).toBe(true);

    const sorted = keys.toSorted();
    expect(await drainKeys(map.scan(fromKey, true))).toEqual(
      sorted.filter(k => k >= fromKey),
    );
  });
});

test('prefetch is opt-in', async () => {
  const dagStore = new TestStore();
  const keys = Array.from({length: 200}, (_, i) => String(i).padStart(4, '0'));

  await withWrite(dagStore, async dagWrite => {
    const map = new PoisonBTree(
      dagWrite,
      formatVersion,
      undefined,
      2,
      3,
      () => 1,
      0,
    );
    for (const k of keys) {
      await map.put(k, k);
    }
    await map.flush();

    const root = await map.getNode(map.rootHash);
    expect(root.level).toBeGreaterThan(0);
    expect(root.entries.length).toBeGreaterThan(1);

    map.poisonHash = (root.entries.at(-1) as Entry<Hash>)[1];

    const lazyScan = map.scan('', false);
    expect((await lazyScan.next()).done).toBe(false);
    expect(map.poisonReads).toBe(0);
    await lazyScan.return?.();

    const prefetchScan = map.scan('', true);
    expect((await prefetchScan.next()).done).toBe(false);
    expect(map.poisonReads).toBeGreaterThan(0);
    await prefetchScan.return?.();
  });
});

test('limit 1 scan reads only the root-to-leaf path', async () => {
  const dagStore = new TestStore();
  const value = 'x'.repeat(256);
  const entries = Array.from({length: 20_000}, (_, i) => [
    String(i).padStart(8, '0'),
    value,
  ]) as [string, string][];

  const rootHash = await withWrite(dagStore, async dagWrite => {
    const map = new BTreeWrite(dagWrite, formatVersion);
    await map.putMany(entries);
    const hash = await map.flush();
    await dagWrite.setHead('scan-test', hash);
    return hash;
  });

  const rootLevel = await withRead(dagStore, async dagRead => {
    const map = new BTreeRead(dagRead, formatVersion, rootHash);
    return (await map.getNode(rootHash)).level;
  });
  expect(rootLevel).toBeGreaterThan(1);

  await withRead(dagStore, async dagRead => {
    const chunksRead: Chunk[] = [];
    const mustGetChunk = dagRead.mustGetChunk.bind(dagRead);
    vi.spyOn(dagRead, 'mustGetChunk').mockImplementation(async hash => {
      const chunk = await mustGetChunk(hash);
      chunksRead.push(chunk);
      return chunk;
    });

    const map = new BTreeRead(dagRead, formatVersion, rootHash);
    const dbRead = new DBRead(dagRead, map, new Map());
    const tx = new ReadTransactionImpl('client-id', dbRead, new LogContext());

    expect(await tx.scan({limit: 1}).entries().toArray()).toEqual([entries[0]]);

    // A cold scan needs one chunk for each level, including the root and leaf.
    // Reading more means the B-tree eagerly loaded siblings that the limit
    // prevented the public scan from visiting.
    const bytesRead = chunksRead.reduce(
      (sum, chunk) => sum + getSizeOfValue(chunk.data),
      0,
    );
    expect(bytesRead).toBeLessThanOrEqual((rootLevel + 1) * 16 * 1024);
    expect(chunksRead).toHaveLength(rootLevel + 1);
  });
});

const visitKeys = Array.from({length: 200}, (_, i) =>
  String(i).padStart(4, '0'),
);

/** A multi-level tree of {@link visitKeys}, each key its own value. */
async function makeVisitTree<T extends BTreeWrite>(map: T): Promise<T> {
  for (const k of visitKeys) {
    await map.put(k, k);
  }
  return map;
}

function makePoisonTree(dagWrite: ConstructorParameters<typeof BTreeWrite>[0]) {
  return new PoisonBTree(dagWrite, formatVersion, undefined, 2, 3, () => 1, 0);
}

/** The keys `visit` passes to its visitor, up to and including `stopAt`. */
async function visitedKeys(
  map: BTreeRead,
  fromKey: string,
  prefetch: boolean,
  stopAt?: string,
): Promise<string[]> {
  const out: string[] = [];
  await map.visit(fromKey, prefetch, ([key]) => {
    out.push(key);
    return key !== stopAt;
  });
  return out;
}

test.each([
  ['unflushed, so its nodes are mutable', false],
  ['flushed', true],
])(
  'visit passes the same entries as scan, in order (%s)',
  async (_name, flush) => {
    await withWrite(new TestStore(), async dagWrite => {
      const map = await makeVisitTree(makeTree(dagWrite));
      if (flush) {
        await map.flush();
      }
      expect((await map.getNode(map.rootHash)).level).toBeGreaterThan(1);

      // Before the first key, on a key, between two keys, on the last key and
      // past it.
      for (const fromKey of ['', '0000', '0100', '01005', '0199', '9999']) {
        const expected = await asyncIterableToArray(map.scan(fromKey, false));
        for (const prefetch of [false, true]) {
          const visited: Entry<unknown>[] = [];
          await map.visit(fromKey, prefetch, entry => {
            visited.push(entry);
            return true;
          });
          expect(visited).toEqual(expected);
        }
      }
    });
  },
);

test('visit reads a tree through BTreeRead', async () => {
  const dagStore = new TestStore();
  const rootHash = await withWrite(dagStore, async dagWrite => {
    const map = await makeVisitTree(makeTree(dagWrite));
    const hash = await map.flush();
    await dagWrite.setHead('visit-test', hash);
    return hash;
  });

  await withRead(dagStore, async dagRead => {
    const map = new BTreeRead(dagRead, formatVersion, rootHash);
    expect(await visitedKeys(map, '', true)).toEqual(visitKeys);
    expect(await visitedKeys(map, '0150', false)).toEqual(
      visitKeys.filter(k => k >= '0150'),
    );
  });
});

test('visit calls nothing for an empty tree', async () => {
  await withRead(new TestStore(), async dagRead => {
    const map = new BTreeRead(dagRead, formatVersion);
    expect(await visitedKeys(map, '', true)).toEqual([]);
    expect(await visitedKeys(map, 'a', false)).toEqual([]);
  });
});

test('visit stops when the visitor returns false', async () => {
  await withWrite(new TestStore(), async dagWrite => {
    const map = await makeVisitTree(makePoisonTree(dagWrite));
    await map.flush();
    const root = await map.getNode(map.rootHash);
    expect(root.entries.length).toBeGreaterThan(1);

    // Stopping on the first entry of a leaf, in the middle of the tree and on
    // the last entry: none may let the walk run on into what follows.
    for (const fromKey of ['', '0050']) {
      for (const stopAt of [fromKey || '0000', '0100', '0101', '0199']) {
        for (const prefetch of [false, true]) {
          expect(await visitedKeys(map, fromKey, prefetch, stopAt)).toEqual(
            visitKeys.filter(k => k >= fromKey && k <= stopAt),
          );
        }
      }
    }

    // Without prefetch, a visit that stops in the first subtree never reads
    // the last one.
    map.poisonHash = (root.entries.at(-1) as Entry<Hash>)[1];
    expect(await visitedKeys(map, '', false, '0000')).toEqual(['0000']);
    expect(map.poisonReads).toBe(0);

    // With it, it has read the last one already, and the failure of a read
    // the visit never needed is not reported.
    expect(await visitedKeys(map, '', true, '0000')).toEqual(['0000']);
    expect(map.poisonReads).toBeGreaterThan(0);
  });
});

test('visit reports a failed read of a node it reaches, with or without prefetch', async () => {
  await withWrite(new TestStore(), async dagWrite => {
    const map = await makeVisitTree(makePoisonTree(dagWrite));
    await map.flush();
    const root = await map.getNode(map.rootHash);

    map.poisonHash = (root.entries[0] as Entry<Hash>)[1];
    for (const prefetch of [false, true]) {
      await expect(visitedKeys(map, '', prefetch)).rejects.toThrow(
        'poisoned chunk',
      );
    }
  });
});

test('visit does not read subtrees before fromKey', async () => {
  await withWrite(new TestStore(), async dagWrite => {
    const map = await makeVisitTree(makePoisonTree(dagWrite));
    await map.flush();
    const root = await map.getNode(map.rootHash);

    const firstChild = root.entries[0] as Entry<Hash>;
    map.poisonHash = firstChild[1];
    const fromKey = visitKeys.at(-1) ?? '';
    expect(fromKey > firstChild[0]).toBe(true);

    for (const prefetch of [false, true]) {
      expect(await visitedKeys(map, fromKey, prefetch)).toEqual([fromKey]);
    }
    expect(map.poisonReads).toBe(0);
  });
});
