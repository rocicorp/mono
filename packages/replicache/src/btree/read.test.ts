import {expect, test} from 'vitest';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';
import {withWrite} from '../with-transactions.ts';
import type {DataNodeImpl, Entry, InternalNodeImpl} from './node.ts';
import {BTreeWrite} from './write.ts';

const formatVersion = FormatVersion.Latest;

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

    expect(await drainKeys(map.scan(''))).toEqual(sorted);
    expect(await drainKeys(map.scan('0100'))).toEqual(
      sorted.filter(k => k >= '0100'),
    );
    expect(await drainKeys(map.scan('01005'))).toEqual(
      sorted.filter(k => k >= '01005'),
    );
    expect(await drainKeys(map.scan('9999'))).toEqual([]);
  });
});

// Overrides getNode so a single chunk read can be made to fail, letting us
// probe the boundaries of the prefetch added to scanForHash.
class PoisonBTree extends BTreeWrite {
  poisonHash: Hash | undefined = undefined;

  override getNode(hash: Hash): Promise<DataNodeImpl | InternalNodeImpl> {
    if (this.poisonHash !== undefined && hash === this.poisonHash) {
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
    await expect(drainKeys(map.scan(''))).rejects.toThrow('poisoned chunk');
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
    expect(await drainKeys(map.scan(fromKey))).toEqual(
      sorted.filter(k => k >= fromKey),
    );
  });
});
