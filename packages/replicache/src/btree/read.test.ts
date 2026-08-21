import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {getSizeOfValue} from '../../../shared/src/size-of-value.ts';
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

    expect(await drainKeys(map.scan('', {prefetch: true}))).toEqual(sorted);
    expect(await drainKeys(map.scan('0100', {prefetch: true}))).toEqual(
      sorted.filter(k => k >= '0100'),
    );
    expect(await drainKeys(map.scan('01005', {prefetch: true}))).toEqual(
      sorted.filter(k => k >= '01005'),
    );
    expect(await drainKeys(map.scan('9999', {prefetch: true}))).toEqual([]);
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
    await expect(drainKeys(map.scan('', {prefetch: true}))).rejects.toThrow(
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
    expect(await drainKeys(map.scan(fromKey, {prefetch: true}))).toEqual(
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

    const lazyScan = map.scan('');
    expect((await lazyScan.next()).done).toBe(false);
    expect(map.poisonReads).toBe(0);
    await lazyScan.return?.();

    const prefetchScan = map.scan('', {prefetch: true});
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
