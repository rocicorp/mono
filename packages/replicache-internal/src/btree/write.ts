import {RWLock} from '@rocicorp/lock';
import type {ReadonlyJSONValue} from '../json';
import type * as dag from '../dag/mod';
import {Hash, emptyHash, newTempHash} from '../hash';
import {BTreeRead} from './read';
import {
  DataNodeImpl,
  InternalNodeImpl,
  Entry,
  newNodeImpl,
  partition,
  ReadonlyEntry,
  emptyDataNode,
  isDataNodeImpl,
  InternalDiffOperation,
} from './node';
import type {CreateChunk} from '../dag/chunk';
import {assert} from '../asserts';

export class BTreeWrite extends BTreeRead {
  /**
   * This rw lock is used to ensure we do not mutate the btree in parallel. It
   * would be a problem if we didn't have the lock in cases like this:
   *
   * ```ts
   * const p1 = tree.put('a', 0);
   * const p2 = tree.put('b', 1);
   * await p1;
   * await p2;
   * ```
   *
   * because both `p1` and `p2` would start from the old root hash but a put
   * changes the root hash so the two concurrent puts would lead to only one of
   * them actually working, and it is not deterministic which one would finish
   * last.
   */
  private readonly _rwLock = new RWLock();
  private readonly _modified: Map<Hash, DataNodeImpl | InternalNodeImpl> =
    new Map();

  protected declare _dagRead: dag.Write;

  readonly minSize: number;
  readonly maxSize: number;

  constructor(
    dagWrite: dag.Write,
    root: Hash = emptyHash,
    minSize = 8 * 1024,
    maxSize = 16 * 1024,
    getEntrySize?: <T>(e: Entry<T>) => number,
    chunkHeaderSize?: number,
  ) {
    super(dagWrite, root, getEntrySize, chunkHeaderSize);
    this.minSize = minSize;
    this.maxSize = maxSize;
  }

  async getNode(hash: Hash): Promise<DataNodeImpl | InternalNodeImpl> {
    const node = this._modified.get(hash);
    if (node) {
      return node;
    }
    return super.getNode(hash);
  }

  private _addToModified(node: DataNodeImpl | InternalNodeImpl): void {
    assert(node.isMutable);
    this._modified.set(node.hash, node);
  }

  updateNode(node: DataNodeImpl | InternalNodeImpl): void {
    assert(node.isMutable);
    this._modified.delete(node.hash);
    node.hash = newTempHash();
    this._addToModified(node);
  }

  newInternalNodeImpl(
    entries: Array<Entry<Hash>>,
    level: number,
  ): InternalNodeImpl {
    const n = new InternalNodeImpl(entries, newTempHash(), level, true);
    this._addToModified(n);
    return n;
  }

  newDataNodeImpl(entries: Entry<ReadonlyJSONValue>[]): DataNodeImpl {
    const n = new DataNodeImpl(entries, newTempHash(), true);
    this._addToModified(n);
    return n;
  }

  newNodeImpl(entries: Entry<ReadonlyJSONValue>[], level: number): DataNodeImpl;
  newNodeImpl(entries: Entry<Hash>[], level: number): InternalNodeImpl;
  newNodeImpl(
    entries: Entry<Hash>[] | Entry<ReadonlyJSONValue>[],
    level: number,
  ): InternalNodeImpl | DataNodeImpl;
  newNodeImpl(
    entries: Entry<Hash>[] | Entry<ReadonlyJSONValue>[],
    level: number,
  ): InternalNodeImpl | DataNodeImpl {
    const n = newNodeImpl(entries, newTempHash(), level, true);
    this._addToModified(n);
    return n;
  }

  childNodeSize(node: InternalNodeImpl | DataNodeImpl): number {
    let sum = this.chunkHeaderSize;
    for (const entry of node.entries) {
      sum += this.getEntrySize(entry);
    }
    return sum;
  }

  // Override BTree to wrap all of these in a read lock
  override get(key: string): Promise<ReadonlyJSONValue | undefined> {
    return this._rwLock.withRead(() => super.get(key));
  }

  override has(key: string): Promise<boolean> {
    return this._rwLock.withRead(() => super.has(key));
  }

  override isEmpty(): Promise<boolean> {
    return this._rwLock.withRead(() => super.isEmpty());
  }

  override async *scan(
    fromKey: string,
  ): AsyncIterableIterator<ReadonlyEntry<ReadonlyJSONValue>> {
    yield* runRead(this._rwLock, super.scan(fromKey));
  }

  override async *keys(): AsyncIterableIterator<string> {
    yield* runRead(this._rwLock, super.keys());
  }

  async *entries(): AsyncIterableIterator<ReadonlyEntry<ReadonlyJSONValue>> {
    yield* runRead(this._rwLock, super.entries());
  }

  override async *diff(
    last: BTreeRead,
  ): AsyncIterableIterator<InternalDiffOperation> {
    yield* runRead(this._rwLock, super.diff(last));
  }

  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    return this._rwLock.withWrite(async () => {
      const oldRootNode = await this.getNode(this.rootHash);
      const rootNode = await oldRootNode.set(key, value, this);

      // We do the rebalancing in the parent so we need to do it here as well.
      if (this.childNodeSize(rootNode) > this.maxSize) {
        const headerSize = this.chunkHeaderSize;
        const partitions = partition(
          rootNode.entries,
          this.getEntrySize,
          this.minSize - headerSize,
          this.maxSize - headerSize,
        );
        const {level} = rootNode;
        const entries: Entry<Hash>[] = partitions.map(entries => {
          const node = this.newNodeImpl(entries, level);
          return [node.maxKey(), node.hash];
        });
        const newRoot = this.newInternalNodeImpl(entries, level + 1);
        this.rootHash = newRoot.hash;
        return;
      }

      this.rootHash = rootNode.hash;
    });
  }

  del(key: string): Promise<boolean> {
    return this._rwLock.withWrite(async () => {
      const oldRootNode = await this.getNode(this.rootHash);
      const newRootNode = await oldRootNode.del(key, this);

      // No need to rebalance here since if root gets too small there is nothing
      // we can do about that.
      const found = this.rootHash !== newRootNode.hash;
      if (found) {
        // Flatten one layer.
        if (newRootNode.level > 0 && newRootNode.entries.length === 1) {
          this.rootHash = (newRootNode as InternalNodeImpl).entries[0][1];
        } else {
          this.rootHash = newRootNode.hash;
        }
      }

      return found;
    });
  }

  clear(): Promise<void> {
    return this._rwLock.withWrite(async () => {
      this._modified.clear();
      this.rootHash = emptyHash;
    });
  }

  flush(): Promise<Hash> {
    const walk = (
      hash: Hash,
      newChunks: dag.Chunk[],
      createChunk: CreateChunk,
    ): Hash => {
      const node = this._modified.get(hash);
      if (node === undefined) {
        // Not modified, use the original.
        return hash;
      }
      if (isDataNodeImpl(node)) {
        const chunk = createChunk(node.toChunkData(), []);
        newChunks.push(chunk);
        return chunk.hash;
      }
      const refs: Hash[] = [];

      for (const entry of node.entries) {
        const childHash = entry[1];
        const newChildHash = walk(childHash, newChunks, createChunk);
        if (newChildHash !== childHash) {
          // MUTATES the node!
          entry[1] = newChildHash;
        }
        refs.push(newChildHash);
      }
      const chunk = createChunk(node.toChunkData(), refs);
      newChunks.push(chunk);
      return chunk.hash;
    };

    return this._rwLock.withWrite(async () => {
      const dagWrite = this._dagRead;

      if (this.rootHash === emptyHash) {
        // Write a chunk for the empty tree.
        const chunk = dagWrite.createChunk(emptyDataNode, []);
        await dagWrite.putChunk(chunk);
        return chunk.hash;
      }

      const newChunks: dag.Chunk[] = [];

      const newRoot = walk(this.rootHash, newChunks, dagWrite.createChunk);

      await Promise.all(newChunks.map(chunk => dagWrite.putChunk(chunk)));

      this._modified.clear();

      this.rootHash = newRoot;
      return newRoot;
    });
  }
}

async function* runRead<T>(
  lock: RWLock,
  ai: AsyncIterableIterator<T>,
): AsyncIterableIterator<T> {
  const release = await lock.read();
  try {
    yield* ai;
  } finally {
    release();
  }
}
