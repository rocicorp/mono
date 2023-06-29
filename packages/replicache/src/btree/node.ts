import {compareUTF8} from 'compare-utf8';
import {
  assert,
  assertArray,
  assertNumber,
  assertString,
} from 'shared/src/asserts.js';
import {binarySearch as binarySearchWithFunc} from '../binary-search.js';
import {skipBTreeNodeAsserts} from '../config.js';
import type {IndexKey} from '../db/index.js';
import {FormatVersion} from '../format-version.js';
import {Hash, emptyHash, newUUIDHash} from '../hash.js';
import {joinIterables} from '../iterables.js';
import {
  FrozenJSONValue,
  FrozenTag,
  JSONValue,
  ReadonlyJSONValue,
  assertDeepFrozen,
  assertJSONValue,
  deepFreeze,
} from '../json.js';
import type {BTreeRead} from './read.js';
import type {BTreeWrite} from './write.js';

export type Entry<V> = readonly [key: string, value: V, sizeOfEntry: number];

export const NODE_LEVEL = 0;
export const NODE_ENTRIES = 1;

/**
 * The type of B+Tree node chunk data
 */
type BaseNode<V> = FrozenTag<
  readonly [level: number, entries: ReadonlyArray<Entry<V>>]
>;
export type InternalNode = BaseNode<Hash>;

export type DataNode = BaseNode<FrozenJSONValue>;

export function makeNodeChunkData<V>(
  level: number,
  entries: ReadonlyArray<Entry<V>>,
  formatVersion: FormatVersion,
): BaseNode<V> {
  return deepFreeze([
    level,
    (formatVersion >= FormatVersion.V7
      ? entries
      : entries.map(e => e.slice(0, 2))) as readonly ReadonlyJSONValue[],
  ]) as BaseNode<V>;
}

export type Node = DataNode | InternalNode;

export function getRefs(node: Node): ReadonlyArray<Hash> {
  return isInternalNode(node) ? node[NODE_ENTRIES].map(e => e[1]) : [];
}

/**
 * Describes the changes that happened to Replicache after a
 * {@link WriteTransaction} was committed.
 *
 * @experimental This type is experimental and may change in the future.
 */
export type Diff = IndexDiff | NoIndexDiff;

/**
 * @experimental This type is experimental and may change in the future.
 */
export type IndexDiff = readonly DiffOperation<IndexKey>[];

/**
 * @experimental This type is experimental and may change in the future.
 */
export type NoIndexDiff = readonly DiffOperation<string>[];

/**
 * InternalDiff uses string keys even for the secondary index maps.
 */
export type InternalDiff = readonly InternalDiffOperation[];

export type DiffOperationAdd<Key, Value = ReadonlyJSONValue> = {
  readonly op: 'add';
  readonly key: Key;
  readonly newValue: Value;
};

export type DiffOperationDel<Key, Value = ReadonlyJSONValue> = {
  readonly op: 'del';
  readonly key: Key;
  readonly oldValue: Value;
};

export type DiffOperationChange<Key, Value = ReadonlyJSONValue> = {
  readonly op: 'change';
  readonly key: Key;
  readonly oldValue: Value;
  readonly newValue: Value;
};

/**
 * The individual parts describing the changes that happened to the Replicache
 * data. There are three different kinds of operations:
 * - `add`: A new entry was added.
 * - `del`: An entry was deleted.
 * - `change`: An entry was changed.
 *
 * @experimental This type is experimental and may change in the future.
 */
export type DiffOperation<Key> =
  | DiffOperationAdd<Key>
  | DiffOperationDel<Key>
  | DiffOperationChange<Key>;

// Duplicated with DiffOperation to make the docs less confusing.
export type InternalDiffOperation<Key = string, Value = FrozenJSONValue> =
  | DiffOperationAdd<Key, Value>
  | DiffOperationDel<Key, Value>
  | DiffOperationChange<Key, Value>;

/**
 * Finds the leaf where a key is (if present) or where it should go if not
 * present.
 */
export async function findLeaf(
  key: string,
  hash: Hash,
  source: BTreeRead,
  expectedRootHash: Hash,
): Promise<DataNodeImpl> {
  const node = await source.getNode(hash);
  // The root changed. Try again
  if (expectedRootHash !== source.rootHash) {
    return findLeaf(key, source.rootHash, source, source.rootHash);
  }
  if (isDataNodeImpl(node)) {
    return node;
  }
  const {entries} = node;
  let i = binarySearch(key, entries);
  if (i === entries.length) {
    i--;
  }
  const entry = entries[i];
  return findLeaf(key, entry[1], source, expectedRootHash);
}

type BinarySearchEntries = readonly Entry<unknown>[];

/**
 * Does a binary search over entries
 *
 * If the key found then the return value is the index it was found at.
 *
 * If the key was *not* found then the return value is the index where it should
 * be inserted at
 */
export function binarySearch(
  key: string,
  entries: BinarySearchEntries,
): number {
  return binarySearchWithFunc(entries.length, i =>
    compareUTF8(key, entries[i][0]),
  );
}

export function binarySearchFound(
  i: number,
  entries: BinarySearchEntries,
  key: string,
): boolean {
  return i !== entries.length && entries[i][0] === key;
}

export function parseBTreeNode(
  v: unknown,
  formatVersion: FormatVersion,
  getSizeOfEntry: <K, V>(key: K, value: V) => number,
): InternalNode | DataNode {
  if (skipBTreeNodeAsserts && formatVersion >= FormatVersion.V7) {
    return v as InternalNode | DataNode;
  }

  assertArray(v);
  assertDeepFrozen(v);
  // Be relaxed about what we accept.
  assert(v.length >= 2);
  const [level, entries] = v;
  assertNumber(level);
  assertArray(entries);

  const f = level > 0 ? assertString : assertJSONValue;

  // For V7 we do not need to change the entries. Just assert that they are correct.
  if (formatVersion >= FormatVersion.V7) {
    for (const e of entries) {
      assertEntry(e, f);
    }
    return v as unknown as InternalNode | DataNode;
  }

  const newEntries = entries.map(e => convertNonV7Entry(e, f, getSizeOfEntry));
  return [level, newEntries] as unknown as InternalNode | DataNode;
}

function assertEntry(
  entry: unknown,
  f:
    | ((v: unknown) => asserts v is Hash)
    | ((v: unknown) => asserts v is JSONValue),
): asserts entry is Entry<Hash | JSONValue> {
  assertArray(entry);
  // Be relaxed about what we accept.
  assert(entry.length >= 3);
  assertString(entry[0]);
  f(entry[1]);
  assertNumber(entry[2]);
}

/**
 * Converts an entry that was from a format version before V7 to the format
 * wanted by V7.
 */
function convertNonV7Entry(
  entry: unknown,
  f:
    | ((v: unknown) => asserts v is Hash)
    | ((v: unknown) => asserts v is JSONValue),
  getSizeOfEntry: <K, V>(key: K, value: V) => number,
): Entry<Hash | JSONValue> {
  assertArray(entry);
  assert(entry.length >= 2);
  assertString(entry[0]);
  f(entry[1]);
  const entrySize = getSizeOfEntry(entry[0], entry[1]);
  return [entry[0], entry[1], entrySize] as Entry<Hash | JSONValue>;
}

export function isInternalNode(node: Node): node is InternalNode {
  return node[NODE_LEVEL] > 0;
}

abstract class NodeImpl<Value> {
  entries: Array<Entry<Value>>;
  hash: Hash;
  abstract readonly level: number;
  readonly isMutable: boolean;

  private _childNodeSize = -1;

  constructor(entries: Array<Entry<Value>>, hash: Hash, isMutable: boolean) {
    this.entries = entries;
    this.hash = hash;
    this.isMutable = isMutable;
  }

  abstract set(
    key: string,
    value: FrozenJSONValue,
    entrySize: number,
    tree: BTreeWrite,
  ): Promise<NodeImpl<Value>>;

  abstract del(
    key: string,
    tree: BTreeWrite,
  ): Promise<NodeImpl<Value> | DataNodeImpl>;

  maxKey(): string {
    return this.entries[this.entries.length - 1][0];
  }

  getChildNodeSize(tree: BTreeRead): number {
    if (this._childNodeSize !== -1) {
      return this._childNodeSize;
    }

    let sum = tree.chunkHeaderSize;
    for (const entry of this.entries) {
      sum += entry[2];
    }
    return (this._childNodeSize = sum);
  }

  protected _updateNode(tree: BTreeWrite) {
    this._childNodeSize = -1;
    tree.updateNode(
      this as NodeImpl<unknown> as DataNodeImpl | InternalNodeImpl,
    );
  }
}

export function toChunkData<V>(
  node: NodeImpl<V>,
  formatVersion: FormatVersion,
): BaseNode<V> {
  return makeNodeChunkData(node.level, node.entries, formatVersion);
}

export class DataNodeImpl extends NodeImpl<FrozenJSONValue> {
  readonly level = 0;

  set(
    key: string,
    value: FrozenJSONValue,
    entrySize: number,
    tree: BTreeWrite,
  ): Promise<DataNodeImpl> {
    let deleteCount: number;
    const i = binarySearch(key, this.entries);
    if (!binarySearchFound(i, this.entries, key)) {
      // Not found, insert.
      deleteCount = 0;
    } else {
      deleteCount = 1;
    }

    return Promise.resolve(
      this._splice(tree, i, deleteCount, [key, value, entrySize]),
    );
  }

  private _splice(
    tree: BTreeWrite,
    start: number,
    deleteCount: number,
    ...items: Entry<FrozenJSONValue>[]
  ): DataNodeImpl {
    if (this.isMutable) {
      this.entries.splice(start, deleteCount, ...items);
      this._updateNode(tree);
      return this;
    }

    const entries = readonlySplice(this.entries, start, deleteCount, ...items);
    return tree.newDataNodeImpl(entries);
  }

  del(key: string, tree: BTreeWrite): Promise<DataNodeImpl> {
    const i = binarySearch(key, this.entries);
    if (!binarySearchFound(i, this.entries, key)) {
      // Not found. Return this without changes.
      return Promise.resolve(this);
    }

    // Found. Create new node or mutate existing one.
    return Promise.resolve(this._splice(tree, i, 1));
  }

  async *keys(_tree: BTreeRead): AsyncGenerator<string, void> {
    for (const entry of this.entries) {
      yield entry[0];
    }
  }

  async *entriesIter(
    _tree: BTreeRead,
  ): AsyncGenerator<Entry<FrozenJSONValue>, void> {
    for (const entry of this.entries) {
      yield entry;
    }
  }
}

function readonlySplice<T>(
  array: ReadonlyArray<T>,
  start: number,
  deleteCount: number,
  ...items: T[]
): T[] {
  const arr = array.slice(0, start);
  for (let i = 0; i < items.length; i++) {
    arr.push(items[i]);
  }
  for (let i = start + deleteCount; i < array.length; i++) {
    arr.push(array[i]);
  }
  return arr;
}

export class InternalNodeImpl extends NodeImpl<Hash> {
  readonly level: number;

  constructor(
    entries: Array<Entry<Hash>>,
    hash: Hash,
    level: number,
    isMutable: boolean,
  ) {
    super(entries, hash, isMutable);
    this.level = level;
  }

  async set(
    key: string,
    value: FrozenJSONValue,
    entrySize: number,
    tree: BTreeWrite,
  ): Promise<InternalNodeImpl> {
    let i = binarySearch(key, this.entries);
    if (i === this.entries.length) {
      // We are going to insert into last (right most) leaf.
      i--;
    }

    const childHash = this.entries[i][1];
    const oldChildNode = await tree.getNode(childHash);

    const childNode = await oldChildNode.set(key, value, entrySize, tree);

    const childNodeSize = childNode.getChildNodeSize(tree);
    if (childNodeSize > tree.maxSize || childNodeSize < tree.minSize) {
      return this._mergeAndPartition(tree, i, childNode);
    }

    const newEntry = createNewInternalEntryForNode(
      childNode,
      tree.getEntrySize,
    );
    return this._replaceChild(tree, i, newEntry);
  }

  /**
   * This merges the child node entries with previous or next sibling and then
   * partitions the merged entries.
   */
  private async _mergeAndPartition(
    tree: BTreeWrite,
    i: number,
    childNode: DataNodeImpl | InternalNodeImpl,
  ): Promise<InternalNodeImpl> {
    const level = this.level - 1;
    const thisEntries = this.entries;

    type IterableHashEntries = Iterable<Entry<Hash>>;

    let values: IterableHashEntries;
    let startIndex: number;
    let removeCount: number;
    if (i > 0) {
      const hash = thisEntries[i - 1][1];
      const previousSibling = await tree.getNode(hash);
      values = joinIterables(
        previousSibling.entries as IterableHashEntries,
        childNode.entries as IterableHashEntries,
      );
      startIndex = i - 1;
      removeCount = 2;
    } else if (i < thisEntries.length - 1) {
      const hash = thisEntries[i + 1][1];
      const nextSibling = await tree.getNode(hash);
      values = joinIterables(
        childNode.entries as IterableHashEntries,
        nextSibling.entries as IterableHashEntries,
      );
      startIndex = i;
      removeCount = 2;
    } else {
      values = childNode.entries as IterableHashEntries;
      startIndex = i;
      removeCount = 1;
    }

    const partitions = partition(
      values,
      value => value[2],
      tree.minSize - tree.chunkHeaderSize,
      tree.maxSize - tree.chunkHeaderSize,
    );

    // TODO: There are cases where we can reuse the old nodes. Creating new ones
    // means more memory churn but also more writes to the underlying KV store.
    const newEntries: Entry<Hash>[] = [];
    for (const entries of partitions) {
      const node = tree.newNodeImpl(entries, level);
      const newHashEntry = createNewInternalEntryForNode(
        node,
        tree.getEntrySize,
      );
      newEntries.push(newHashEntry);
    }

    if (this.isMutable) {
      this.entries.splice(startIndex, removeCount, ...newEntries);
      this._updateNode(tree);
      return this;
    }

    const entries = readonlySplice(
      thisEntries,
      startIndex,
      removeCount,
      ...newEntries,
    );

    return tree.newInternalNodeImpl(entries, this.level);
  }

  private _replaceChild(
    tree: BTreeWrite,
    index: number,
    newEntry: Entry<Hash>,
  ): InternalNodeImpl {
    if (this.isMutable) {
      this.entries.splice(index, 1, newEntry);
      this._updateNode(tree);
      return this;
    }
    const entries = readonlySplice(this.entries, index, 1, newEntry);
    return tree.newInternalNodeImpl(entries, this.level);
  }

  async del(
    key: string,
    tree: BTreeWrite,
  ): Promise<InternalNodeImpl | DataNodeImpl> {
    const i = binarySearch(key, this.entries);
    if (i === this.entries.length) {
      // Key is larger than maxKey of rightmost entry so it is not present.
      return this;
    }

    const childHash = this.entries[i][1];
    const oldChildNode = await tree.getNode(childHash);
    const oldHash = oldChildNode.hash;

    const childNode = await oldChildNode.del(key, tree);
    if (childNode.hash === oldHash) {
      // Not changed so not found.
      return this;
    }

    if (childNode.entries.length === 0) {
      // Subtree is now empty. Remove internal node.
      const entries = readonlySplice(this.entries, i, 1);
      return tree.newInternalNodeImpl(entries, this.level);
    }

    if (i === 0 && this.entries.length === 1) {
      // There was only one node at this level and it was removed. We can return
      // the modified subtree.
      return childNode;
    }

    // The child node is still a good size.
    if (childNode.getChildNodeSize(tree) > tree.minSize) {
      // No merging needed.
      const entry = createNewInternalEntryForNode(childNode, tree.getEntrySize);
      return this._replaceChild(tree, i, entry);
    }

    // Child node size is too small.
    return this._mergeAndPartition(tree, i, childNode);
  }

  async *keys(tree: BTreeRead): AsyncGenerator<string, void> {
    for (const entry of this.entries) {
      const childNode = await tree.getNode(entry[1]);
      yield* childNode.keys(tree);
    }
  }

  async *entriesIter(
    tree: BTreeRead,
  ): AsyncGenerator<Entry<FrozenJSONValue>, void> {
    for (const entry of this.entries) {
      const childNode = await tree.getNode(entry[1]);
      yield* childNode.entriesIter(tree);
    }
  }

  getChildren(
    start: number,
    length: number,
    tree: BTreeRead,
  ): Promise<Array<InternalNodeImpl | DataNodeImpl>> {
    const ps: Promise<DataNodeImpl | InternalNodeImpl>[] = [];
    for (let i = start; i < length && i < this.entries.length; i++) {
      ps.push(tree.getNode(this.entries[i][1]));
    }
    return Promise.all(ps);
  }

  async getCompositeChildren(
    start: number,
    length: number,
    tree: BTreeRead,
  ): Promise<InternalNodeImpl | DataNodeImpl> {
    const {level} = this;

    if (length === 0) {
      return new InternalNodeImpl([], newUUIDHash(), level - 1, true);
    }

    const output = await this.getChildren(start, start + length, tree);

    if (level > 1) {
      const entries: Entry<Hash>[] = [];
      for (const child of output as InternalNodeImpl[]) {
        entries.push(...child.entries);
      }
      return new InternalNodeImpl(entries, newUUIDHash(), level - 1, true);
    }

    assert(level === 1);
    const entries: Entry<FrozenJSONValue>[] = [];
    for (const child of output as DataNodeImpl[]) {
      entries.push(...child.entries);
    }
    return new DataNodeImpl(entries, newUUIDHash(), true);
  }
}

export function newNodeImpl(
  entries: Array<Entry<FrozenJSONValue>>,
  hash: Hash,
  level: number,
  isMutable: boolean,
): DataNodeImpl;
export function newNodeImpl(
  entries: Array<Entry<Hash>>,
  hash: Hash,
  level: number,
  isMutable: boolean,
): InternalNodeImpl;
export function newNodeImpl(
  entries: Array<Entry<FrozenJSONValue>> | Array<Entry<Hash>>,
  hash: Hash,
  level: number,
  isMutable: boolean,
): DataNodeImpl | InternalNodeImpl;
export function newNodeImpl(
  entries: Array<Entry<FrozenJSONValue>> | Array<Entry<Hash>>,
  hash: Hash,
  level: number,
  isMutable: boolean,
): DataNodeImpl | InternalNodeImpl {
  if (level === 0) {
    return new DataNodeImpl(
      entries as Entry<FrozenJSONValue>[],
      hash,
      isMutable,
    );
  }
  return new InternalNodeImpl(entries as Entry<Hash>[], hash, level, isMutable);
}

export function isDataNodeImpl(
  node: DataNodeImpl | InternalNodeImpl,
): node is DataNodeImpl {
  return node.level === 0;
}

export function partition<T>(
  values: Iterable<T>,
  // This is the size of each Entry
  getSizeOfEntry: (v: T) => number,
  min: number,
  max: number,
): T[][] {
  const partitions: T[][] = [];
  const sizes: number[] = [];
  let sum = 0;
  let accum: T[] = [];
  for (const value of values) {
    const size = getSizeOfEntry(value);
    if (size >= max) {
      if (accum.length > 0) {
        partitions.push(accum);
        sizes.push(sum);
      }
      partitions.push([value]);
      sizes.push(size);
      sum = 0;
      accum = [];
    } else if (sum + size >= min) {
      accum.push(value);
      partitions.push(accum);
      sizes.push(sum + size);
      sum = 0;
      accum = [];
    } else {
      sum += size;
      accum.push(value);
    }
  }

  if (sum > 0) {
    if (sizes.length > 0 && sum + sizes[sizes.length - 1] <= max) {
      partitions[partitions.length - 1].push(...accum);
    } else {
      partitions.push(accum);
    }
  }

  return partitions;
}

export const emptyDataNode = makeNodeChunkData<ReadonlyJSONValue>(
  0,
  [],
  FormatVersion.Latest,
);
export const emptyDataNodeImpl = new DataNodeImpl([], emptyHash, false);

export function createNewInternalEntryForNode(
  node: NodeImpl<unknown>,
  getSizeOfEntry: <K, V>(k: K, v: V) => number,
): [string, Hash, number] {
  const key = node.maxKey();
  const value = node.hash;
  const size = getSizeOfEntry(key, value);
  return [key, value, size];
}
