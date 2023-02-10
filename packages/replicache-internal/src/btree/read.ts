import {getSizeOfValue} from '../json';
import type * as dag from '../dag/mod';
import {Hash, emptyHash} from '../hash';
import {
  DataNodeImpl,
  InternalNodeImpl,
  Entry,
  emptyDataNodeImpl,
  newNodeImpl,
  findLeaf,
  NODE_LEVEL,
  NODE_ENTRIES,
  isInternalNode,
  DataNode,
  InternalNode,
  InternalDiff,
  binarySearch,
  binarySearchFound,
  InternalDiffOperation,
  ValueEntry,
  HashEntry,
  ValueEntries,
  internalizeBTreeNode,
} from './node';
import {
  computeSplices,
  SPLICE_ADDED,
  SPLICE_AT,
  SPLICE_FROM,
  SPLICE_REMOVED,
} from './splice';
import {deepEqual, InternalValue} from '../internal-value.js';

/**
 * The size of the header of a node. (If we had compile time
 * constants we would have used that).
 *
 * There is a test ensuring this is correct.
 */
export const NODE_HEADER_SIZE = 11;

export class BTreeRead implements AsyncIterable<ValueEntry> {
  rootHash: Hash;
  protected readonly _dagRead: dag.Read;
  private readonly _cache: Map<Hash, DataNodeImpl | InternalNodeImpl> =
    new Map();

  readonly getEntrySize: <T>(e: Entry<T>) => number;
  readonly chunkHeaderSize: number;

  constructor(
    dagRead: dag.Read,
    root: Hash = emptyHash,
    getEntrySize: <T>(e: Entry<T>) => number = getSizeOfValue,
    chunkHeaderSize = NODE_HEADER_SIZE,
  ) {
    this.rootHash = root;
    this._dagRead = dagRead;
    this.getEntrySize = getEntrySize;
    this.chunkHeaderSize = chunkHeaderSize;
  }

  async getNode(hash: Hash): Promise<DataNodeImpl | InternalNodeImpl> {
    if (hash === emptyHash) {
      return emptyDataNodeImpl;
    }

    const cached = this._cache.get(hash);
    if (cached) {
      return cached;
    }

    const {data} = await this._dagRead.mustGetChunk(hash);
    internalizeBTreeNode(data);
    const impl = newNodeImpl(
      // We enforce that we do not mutate this at runtime by first checking the
      // hash.
      data[NODE_ENTRIES] as Entry<InternalValue>[],
      hash,
      data[NODE_LEVEL],
      false,
    );
    this._cache.set(hash, impl);
    return impl;
  }

  async get(key: string): Promise<InternalValue | undefined> {
    const leaf = await findLeaf(key, this.rootHash, this, this.rootHash);
    const index = binarySearch(key, leaf.entries);
    if (!binarySearchFound(index, leaf.entries, key)) {
      return undefined;
    }
    return leaf.entries[index][1];
  }

  async has(key: string): Promise<boolean> {
    const leaf = await findLeaf(key, this.rootHash, this, this.rootHash);
    const index = binarySearch(key, leaf.entries);
    return binarySearchFound(index, leaf.entries, key);
  }

  async isEmpty(): Promise<boolean> {
    const {rootHash} = this;
    const node = await this.getNode(this.rootHash);
    // The root hash has changed, so the tree has been modified.
    if (this.rootHash !== rootHash) {
      return this.isEmpty();
    }
    return node.entries.length === 0;
  }

  // We don't do any encoding of the key in the map, so we have no way of
  // determining from an entry.key alone whether it is a regular key or an
  // encoded IndexKey in an index map. Without encoding regular map keys the
  // caller has to deal with encoding and decoding the keys for the index map.
  scan(fromKey: string): AsyncIterableIterator<ValueEntry> {
    return scanForHash(
      this.rootHash,
      () => this.rootHash,
      this.rootHash,
      fromKey,
      async (hash: Hash) => {
        const cached = await this.getNode(hash);
        if (cached) {
          return cached.toChunkData();
        }
        const {data} = await this._dagRead.mustGetChunk(hash);
        internalizeBTreeNode(data);
        return data;
      },
    );
  }

  async *keys(): AsyncIterableIterator<string> {
    const node = await this.getNode(this.rootHash);
    yield* node.keys(this);
  }

  async *entries(): AsyncIterableIterator<ValueEntry> {
    const node = await this.getNode(this.rootHash);
    yield* node.entriesIter(this);
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<ValueEntry> {
    return this.entries();
  }

  async *diff(last: BTreeRead): AsyncIterableIterator<InternalDiffOperation> {
    const [currentNode, lastNode] = await Promise.all([
      this.getNode(this.rootHash),
      last.getNode(last.rootHash),
    ]);
    yield* diffNodes(lastNode, currentNode, last, this);
  }
}

async function* diffNodes(
  last: InternalNodeImpl | DataNodeImpl,
  current: InternalNodeImpl | DataNodeImpl,
  lastTree: BTreeRead,
  currentTree: BTreeRead,
): AsyncIterableIterator<InternalDiffOperation> {
  if (last.level > current.level) {
    // merge all of last's children into a new node
    // We know last is an internal node because level > 0.
    const lastChild = (await (last as InternalNodeImpl).getCompositeChildren(
      0,
      last.entries.length,
      lastTree,
    )) as InternalNodeImpl;
    yield* diffNodes(lastChild, current, lastTree, currentTree);
    return;
  }

  if (current.level > last.level) {
    // We know current is an internal node because level > 0.
    const currentChild = (await (
      current as InternalNodeImpl
    ).getCompositeChildren(
      0,
      current.entries.length,
      currentTree,
    )) as InternalNodeImpl;
    yield* diffNodes(last, currentChild, lastTree, currentTree);
    return;
  }

  if (last.level === 0 && current.level === 0) {
    yield* diffEntries(
      (last as DataNodeImpl).entries,
      (current as DataNodeImpl).entries,
    );
    return;
  }

  // Now we have two internal nodes with the same level. We compute the diff as
  // splices for the internal node entries. We then flatten these and call diff
  // recursively.
  const initialSplices = computeSplices(
    (last as InternalNodeImpl).entries,
    (current as InternalNodeImpl).entries,
  );
  for (const splice of initialSplices) {
    const [lastChild, currentChild] = await Promise.all([
      (last as InternalNodeImpl).getCompositeChildren(
        splice[SPLICE_AT],
        splice[SPLICE_REMOVED],
        lastTree,
      ),
      (current as InternalNodeImpl).getCompositeChildren(
        splice[SPLICE_FROM],
        splice[SPLICE_ADDED],
        currentTree,
      ),
    ]);
    yield* diffNodes(lastChild, currentChild, lastTree, currentTree);
  }
}

function* diffEntries(
  lastEntries: ValueEntries,
  currentEntries: ValueEntries,
): IterableIterator<InternalDiffOperation> {
  const lastLength = lastEntries.length;
  const currentLength = currentEntries.length;
  let i = 0;
  let j = 0;
  while (i < lastLength && j < currentLength) {
    const lastKey = lastEntries[i][0];
    const currentKey = currentEntries[j][0];
    if (lastKey === currentKey) {
      if (!deepEqual(lastEntries[i][1], currentEntries[j][1])) {
        yield {
          op: 'change',
          key: lastKey,
          oldValue: lastEntries[i][1],
          newValue: currentEntries[j][1],
        };
      }
      i++;
      j++;
    } else if (lastKey < currentKey) {
      yield {
        op: 'del',
        key: lastKey,
        oldValue: lastEntries[i][1],
      };
      i++;
    } else {
      yield {
        op: 'add',
        key: currentKey,
        newValue: currentEntries[j][1],
      };
      j++;
    }
  }
  for (; i < lastLength; i++) {
    yield {
      op: 'del',
      key: lastEntries[i][0],
      oldValue: lastEntries[i][1],
    };
  }
  for (; j < currentLength; j++) {
    yield {
      op: 'add',
      key: currentEntries[j][0],
      newValue: currentEntries[j][1],
    };
  }
}

type ReadNode = (hash: Hash) => Promise<InternalNode | DataNode>;

export async function* scanForHash(
  expectedRootHash: Hash,
  getRootHash: () => Hash,
  hash: Hash,
  fromKey: string,
  readNode: ReadNode,
): AsyncIterableIterator<ValueEntry> {
  if (hash === emptyHash) {
    return;
  }

  const data = await readNode(hash);
  // Clone to ensure we are not affected by mutated entry arrays.
  const entries: readonly Entry<unknown>[] = [...data[NODE_ENTRIES]];
  let i = 0;
  if (fromKey) {
    i = binarySearch(fromKey, entries);
  }

  if (isInternalNode(data)) {
    for (; i < entries.length; i++) {
      yield* scanForHash(
        expectedRootHash,
        getRootHash,
        (entries[i] as HashEntry)[1],
        fromKey,
        readNode,
      );
      fromKey = '';
    }
  } else {
    for (; i < entries.length; i++) {
      const rootHash = getRootHash();
      // If rootHash changed then we start a new iterator from the key.
      if (expectedRootHash !== rootHash) {
        yield* scanForHash(
          rootHash,
          getRootHash,
          rootHash,
          entries[i][0],
          readNode,
        );
        return;
      }
      yield entries[i] as ValueEntry;
    }
  }
}

export async function allEntriesAsDiff(
  map: BTreeRead,
  op: 'add' | 'del',
): Promise<InternalDiff> {
  const diff: InternalDiffOperation[] = [];
  const make: (entry: ValueEntry) => InternalDiffOperation =
    op === 'add'
      ? entry => {
          return {
            op: 'add',
            key: entry[0],
            newValue: entry[1],
          };
        }
      : entry => {
          return {
            op: 'del',
            key: entry[0],
            oldValue: entry[1],
          };
        };

  for await (const entry of map.entries()) {
    diff.push(make(entry));
  }
  return diff;
}
