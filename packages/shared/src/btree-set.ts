import {assert} from './asserts.ts';

const MAX_NODE_SIZE = 32;

type Comparator<K> = (a: K, b: K) => number;
export class BTreeSet<K> {
  #root: BNode<K> = emptyLeaf as BNode<K>;
  size: number = 0;

  readonly comparator: Comparator<K>;

  /**
   * Creates a BTreeSet.
   * If entries is provided, it MUST be sorted according to the comparator
   * and contain no duplicates. This is O(n) and much faster than adding
   * keys one by one.
   *
   * @param comparator The comparator function for ordering keys
   * @param entries Optional iterable of keys in sorted order (ascending)
   */
  constructor(comparator: Comparator<K>, entries?: Iterable<K>) {
    this.comparator = comparator;
    if (entries) {
      this.#buildFromSorted(entries);
    }
  }

  /** Releases the tree so that its size is 0. */
  clear() {
    this.#root = emptyLeaf as BNode<K>;
    this.size = 0;
  }

  clone() {
    this.#root.isShared = true;
    const ret = new BTreeSet<K>(this.comparator);
    ret.#root = this.#root;
    ret.size = this.size;
    return ret;
  }

  get(key: K): K | undefined {
    return this.#root.get(key, this);
  }

  add(key: K): this {
    if (this.#root.isShared) this.#root = this.#root.clone();
    const result = this.#root.set(key, this);
    if (result === null) return this;
    // Root node has split, so create a new root node.
    this.#root = new BNodeInternal<K>([this.#root, result]);
    return this;
  }

  /**
   * Returns true if the key exists in the B+ tree, false if not.
   * Use get() for best performance; use has() if you need to
   * distinguish between "undefined value" and "key not present".
   * @param key Key to detect
   * @description Computational complexity: O(log size)
   */
  has(key: K): boolean {
    return this.#root.has(key, this);
  }

  /**
   * Removes a single key-value pair from the B+ tree.
   * @param key Key to find
   * @returns true if a pair was found and removed, false otherwise.
   * @description Computational complexity: O(log size)
   */
  delete(key: K): boolean {
    return this.#delete(key);
  }

  #delete(key: K): boolean {
    let root = this.#root;
    if (root.isShared) {
      this.#root = root = root.clone();
    }
    try {
      return root.delete(key, this);
    } finally {
      let isShared;
      while (root.keys.length <= 1 && root.isInternal()) {
        isShared ||= root.isShared;
        this.#root = root =
          root.keys.length === 0 ? emptyLeaf : root.children[0];
      }
      // If any ancestor of the new root was shared, the new root must also be shared
      if (isShared) {
        root.isShared = true;
      }
    }
  }

  keys(): IterableIterator<K> {
    return valuesFrom(this.#root, this.comparator, undefined, true);
  }

  values(): IterableIterator<K> {
    return valuesFrom(this.#root, this.comparator, undefined, true);
  }

  valuesFrom(lowestKey?: K, inclusive: boolean = true): IterableIterator<K> {
    return valuesFrom(this.#root, this.comparator, lowestKey, inclusive);
  }

  valuesReversed(): IterableIterator<K> {
    return valuesFromReversed(
      this.#maxKey(),
      this.#root,
      this.comparator,
      undefined,
      true,
    );
  }

  valuesFromReversed(
    highestKey?: K,
    inclusive: boolean = true,
  ): IterableIterator<K> {
    return valuesFromReversed(
      this.#maxKey(),
      this.#root,
      this.comparator,
      highestKey,
      inclusive,
    );
  }

  /** Gets the highest key in the tree. Complexity: O(1) */
  #maxKey(): K | undefined {
    return this.#root.maxKey();
  }

  /**
   * Builds a BTree from a sorted iterable of keys.
   * Asserts that keys are in sorted order. This is O(n).
   */
  #buildFromSorted(entries: Iterable<K>): void {
    const comparator = this.comparator;

    // Build leaf nodes from sorted keys
    const leaves: BNode<K>[] = [];
    let keys: K[] = [];
    let size = 0;
    let prev: K | undefined;
    let hasPrev = false;

    for (const key of entries) {
      if (hasPrev) {
        assert(comparator(prev as K, key) < 0, 'Keys must be in sorted order');
      }
      prev = key;
      hasPrev = true;
      size++;

      keys.push(key);
      if (keys.length === MAX_NODE_SIZE) {
        leaves.push(new BNode<K>(keys));
        keys = [];
      }
    }

    // Handle remaining keys
    if (keys.length > 0) {
      leaves.push(new BNode<K>(keys));
    }

    if (leaves.length === 0) {
      return;
    }

    // Build tree bottom-up
    let currentLevel: BNode<K>[] = leaves;
    while (currentLevel.length > 1) {
      const nextLevel: BNodeInternal<K>[] = [];
      for (let i = 0; i < currentLevel.length; i += MAX_NODE_SIZE) {
        const end = Math.min(i + MAX_NODE_SIZE, currentLevel.length);
        const children = currentLevel.slice(i, end);
        nextLevel.push(new BNodeInternal<K>(children));
      }
      currentLevel = nextLevel;
    }

    this.#root = currentLevel[0];
    this.size = size;
  }

  [Symbol.iterator](): IterableIterator<K> {
    return this.keys();
  }
}

function valuesFrom<K>(
  root: BNode<K>,
  comparator: Comparator<K>,
  lowestKey: K | undefined,
  inclusive: boolean,
): IterableIterator<K> {
  const info = findPath(lowestKey, root, comparator);
  if (info === undefined) {
    return iterator<K>(() => ({done: true, value: undefined}));
  }

  let [nodeQueue, nodeIndex, leaf] = info;
  let i =
    lowestKey === undefined
      ? -1
      : indexOf(lowestKey, leaf.keys, 0, comparator) - 1;

  if (
    !inclusive &&
    i < leaf.keys.length &&
    // +1 because we did -1 above.
    comparator(leaf.keys[i + 1], lowestKey!) === 0
  ) {
    i++;
  }

  return iterator<K>(() => {
    for (;;) {
      if (++i < leaf.keys.length) {
        return {done: false, value: leaf.keys[i]};
      }

      let level = -1;
      for (;;) {
        if (++level >= nodeQueue.length) {
          return {done: true, value: undefined};
        }
        if (++nodeIndex[level] < nodeQueue[level].length) {
          break;
        }
      }
      for (; level > 0; level--) {
        nodeQueue[level - 1] = (
          nodeQueue[level][nodeIndex[level]] as BNodeInternal<K>
        ).children;
        nodeIndex[level - 1] = 0;
      }
      leaf = nodeQueue[0][nodeIndex[0]];
      i = -1;
    }
  });
}

function valuesFromReversed<K>(
  maxKey: K | undefined,
  root: BNode<K>,
  comparator: Comparator<K>,
  highestKey: K | undefined,
  inclusive: boolean,
): IterableIterator<K> {
  if (highestKey === undefined) {
    highestKey = maxKey;
    if (highestKey === undefined)
      return iterator<K>(() => ({done: true, value: undefined})); // collection is empty
  }
  let [nodeQueue, nodeIndex, leaf] =
    findPath(highestKey, root, comparator) ||
    findPath(maxKey, root, comparator)!;
  assert(!nodeQueue[0] || leaf === nodeQueue[0][nodeIndex[0]]);
  let i = indexOf(highestKey, leaf.keys, 0, comparator);
  if (
    inclusive &&
    i < leaf.keys.length &&
    comparator(leaf.keys[i], highestKey) <= 0
  ) {
    i++;
  }

  return iterator<K>(() => {
    for (;;) {
      if (--i >= 0) {
        return {done: false, value: leaf.keys[i]};
      }

      let level;
      // Advance to the next leaf node
      for (level = -1; ; ) {
        if (++level >= nodeQueue.length) {
          return {done: true, value: undefined};
        }
        if (--nodeIndex[level] >= 0) {
          break;
        }
      }
      for (; level > 0; level--) {
        nodeQueue[level - 1] = (
          nodeQueue[level][nodeIndex[level]] as BNodeInternal<K>
        ).children;
        nodeIndex[level - 1] = nodeQueue[level - 1].length - 1;
      }
      leaf = nodeQueue[0][nodeIndex[0]];
      i = leaf.keys.length;
    }
  });
}

function findPath<K>(
  key: K | undefined,
  root: BNode<K>,
  comparator: Comparator<K>,
): [nodeQueue: BNode<K>[][], nodeIndex: number[], leaf: BNode<K>] | undefined {
  let nextNode = root;
  const nodeQueue: BNode<K>[][] = [];
  const nodeIndex: number[] = [];

  if (nextNode.isInternal()) {
    for (let d = 0; nextNode.isInternal(); d++) {
      nodeQueue[d] = nextNode.children;
      nodeIndex[d] =
        key === undefined ? 0 : indexOf(key, nextNode.keys, 0, comparator);
      if (nodeIndex[d] >= nodeQueue[d].length) return; // first key > maxKey()
      nextNode = nodeQueue[d][nodeIndex[d]];
    }
    nodeQueue.reverse();
    nodeIndex.reverse();
  }
  return [nodeQueue, nodeIndex, nextNode];
}

function iterator<T>(next: () => IteratorResult<T>): IterableIterator<T> {
  return {
    next,
    [Symbol.iterator]() {
      return this;
    },
  };
}

/** Leaf node / base class. **************************************************/
class BNode<K> {
  // If this is an internal node, _keys[i] is the highest key in children[i].
  keys: K[];
  // True if this node might be within multiple `BTree`s (or have multiple parents).
  // If so, it must be cloned before being mutated to avoid changing an unrelated tree.
  // This is transitive: if it's true, children are also shared even if `isShared!=true`
  // in those children. (Certain operations will propagate isShared=true to children.)
  isShared: true | undefined;

  constructor(keys: K[]) {
    this.keys = keys;
    this.isShared = undefined;
  }

  isInternal(): this is BNodeInternal<K> {
    return false;
  }

  maxKey() {
    return this.keys[this.keys.length - 1];
  }

  minKey(): K | undefined {
    return this.keys[0];
  }

  clone(): BNode<K> {
    return new BNode<K>(this.keys.slice(0));
  }

  get(key: K, tree: BTreeSet<K>): K | undefined {
    const i = indexOf(key, this.keys, -1, tree.comparator);
    return i < 0 ? undefined : this.keys[i];
  }

  has(key: K, tree: BTreeSet<K>): boolean {
    const i = indexOf(key, this.keys, -1, tree.comparator);
    return i >= 0 && i < this.keys.length;
  }

  set(key: K, tree: BTreeSet<K>): null | BNode<K> {
    let i = indexOf(key, this.keys, -1, tree.comparator);
    if (i < 0) {
      // key does not exist yet
      i = ~i;
      tree.size++;

      if (this.keys.length < MAX_NODE_SIZE) {
        this.keys.splice(i, 0, key);
        return null;
      }
      // This leaf node is full and must split
      const newRightSibling = this.splitOffRightSide();
      // oxlint-disable-next-line @typescript-eslint/no-this-alias
      let target: BNode<K> = this;
      if (i > this.keys.length) {
        i -= this.keys.length;
        target = newRightSibling;
      }
      // target.#insertInLeaf(i, key);
      target.keys.splice(i, 0, key);

      return newRightSibling;
    }

    // usually this is a no-op, but some users may wish to edit the key
    this.keys[i] = key;
    return null;
  }

  takeFromRight(rhs: BNode<K>) {
    this.keys.push(rhs.keys.shift()!);
  }

  takeFromLeft(lhs: BNode<K>) {
    this.keys.unshift(lhs.keys.pop()!);
  }

  splitOffRightSide(): BNode<K> {
    const half = this.keys.length >> 1;
    const keys = this.keys.splice(half);
    return new BNode<K>(keys);
  }

  delete(key: K, tree: BTreeSet<K>): boolean {
    const cmp = tree.comparator;
    const iLow = indexOf(key, this.keys, -1, cmp);
    const iHigh = iLow + 1;

    if (iLow < 0) {
      return false;
    }

    const {keys} = this;
    for (let i = iLow; i < iHigh; i++) {
      const key = keys[i];

      if (key !== keys[i] || this.isShared === true) {
        throw new Error('BTree illegally changed or cloned in delete');
      }

      this.keys.splice(i, 1);
      tree.size--;
      return true;
    }

    return false;
  }

  mergeSibling(rhs: BNode<K>, _: number) {
    this.keys.push(...rhs.keys);
  }
}

/** Internal node (non-leaf node) ********************************************/
class BNodeInternal<K> extends BNode<K> {
  // Note: conventionally B+ trees have one fewer key than the number of
  // children, but I find it easier to keep the array lengths equal: each
  // keys[i] caches the value of children[i].maxKey().
  children: BNode<K>[];

  /**
   * This does not mark `children` as shared, so it is the responsibility of the caller
   * to ensure children are either marked shared, or aren't included in another tree.
   */
  constructor(children: BNode<K>[], keys?: K[]) {
    if (!keys) {
      keys = [];
      for (let i = 0; i < children.length; i++) {
        keys[i] = children[i].maxKey();
      }
    }
    super(keys);
    this.children = children;
  }

  isInternal(): this is BNodeInternal<K> {
    return true;
  }

  clone(): BNode<K> {
    const children = this.children.slice(0);
    for (let i = 0; i < children.length; i++) {
      children[i].isShared = true;
    }
    return new BNodeInternal<K>(children, this.keys.slice(0));
  }

  minKey() {
    return this.children[0].minKey();
  }

  get(key: K, tree: BTreeSet<K>): K | undefined {
    const i = indexOf(key, this.keys, 0, tree.comparator);
    const {children} = this;
    return i < children.length ? children[i].get(key, tree) : undefined;
  }

  has(key: K, tree: BTreeSet<K>): boolean {
    const i = indexOf(key, this.keys, 0, tree.comparator);
    const {children} = this;
    return i < children.length ? children[i].has(key, tree) : false;
  }

  set(key: K, tree: BTreeSet<K>): null | BNode<K> {
    const c = this.children;
    const cmp = tree.comparator;
    let i = Math.min(indexOf(key, this.keys, 0, cmp), c.length - 1);
    let child = c[i];

    if (child.isShared) {
      c[i] = child = child.clone();
    }
    if (child.keys.length >= MAX_NODE_SIZE) {
      // child is full; inserting anything else will cause a split.
      // Shifting an item to the left or right sibling may avoid a split.
      // We can do a shift if the adjacent node is not full and if the
      // current key can still be placed in the same node after the shift.
      let other: BNode<K>;
      if (
        i > 0 &&
        (other = c[i - 1]).keys.length < MAX_NODE_SIZE &&
        cmp(child.keys[0], key) < 0
      ) {
        if (other.isShared) {
          c[i - 1] = other = other.clone();
        }
        other.takeFromRight(child);
        this.keys[i - 1] = other.maxKey();
      } else if (
        (other = c[i + 1]) !== undefined &&
        other.keys.length < MAX_NODE_SIZE &&
        cmp(child.maxKey(), key) < 0
      ) {
        if (other.isShared) c[i + 1] = other = other.clone();
        other.takeFromLeft(child);
        this.keys[i] = c[i].maxKey();
      }
    }

    const result = child.set(key, tree);
    this.keys[i] = child.maxKey();
    if (result === null) return null;

    // The child has split and `result` is a new right child... does it fit?
    if (this.keys.length < MAX_NODE_SIZE) {
      // yes
      this.insert(i + 1, result);
      return null;
    }
    // no, we must split also
    const newRightSibling = this.splitOffRightSide();
    // oxlint-disable-next-line @typescript-eslint/no-this-alias
    let target: BNodeInternal<K> = this;
    if (cmp(result.maxKey(), this.maxKey()) > 0) {
      target = newRightSibling;
      i -= this.keys.length;
    }
    target.insert(i + 1, result);
    return newRightSibling;
  }

  /**
   * Inserts `child` at index `i`.
   * This does not mark `child` as shared, so it is the responsibility of the caller
   * to ensure that either child is marked shared, or it is not included in another tree.
   */
  insert(i: number, child: BNode<K>) {
    this.children.splice(i, 0, child);
    this.keys.splice(i, 0, child.maxKey());
  }

  /**
   * Split this node.
   * Modifies this to remove the second half of the items, returning a separate node containing them.
   */
  splitOffRightSide() {
    const half = this.children.length >> 1;
    return new BNodeInternal<K>(
      this.children.splice(half),
      this.keys.splice(half),
    );
  }

  takeFromRight(rhs: BNode<K>) {
    this.keys.push(rhs.keys.shift()!);
    this.children.push((rhs as BNodeInternal<K>).children.shift()!);
  }

  takeFromLeft(lhs: BNode<K>) {
    this.keys.unshift(lhs.keys.pop()!);
    this.children.unshift((lhs as BNodeInternal<K>).children.pop()!);
  }

  delete(key: K, tree: BTreeSet<K>): boolean {
    const cmp = tree.comparator;
    const {keys} = this;
    const {children} = this;
    let iLow = indexOf(key, this.keys, 0, cmp);
    let i = iLow;
    const iHigh = Math.min(iLow, keys.length - 1);
    if (i <= iHigh) {
      try {
        if (children[i].isShared) {
          children[i] = children[i].clone();
        }
        const result = children[i].delete(key, tree);
        // Note: if children[i] is empty then keys[i]=undefined.
        //       This is an invalid state, but it is fixed below.
        keys[i] = children[i].maxKey();
        return result;
      } finally {
        // Deletions may have occurred, so look for opportunities to merge nodes.
        const half = MAX_NODE_SIZE >> 1;
        if (iLow > 0) iLow--;
        for (i = iHigh; i >= iLow; i--) {
          if (children[i].keys.length <= half) {
            if (children[i].keys.length !== 0) {
              this.tryMerge(i, MAX_NODE_SIZE);
            } else {
              // child is empty! delete it!
              keys.splice(i, 1);
              children.splice(i, 1);
            }
          }
        }
      }
    }
    return false;
  }

  /** Merges child i with child i+1 if their combined size is not too large */
  tryMerge(i: number, maxSize: number): boolean {
    const {children} = this;
    if (i >= 0 && i + 1 < children.length) {
      if (children[i].keys.length + children[i + 1].keys.length <= maxSize) {
        if (children[i].isShared)
          // cloned already UNLESS i is outside scan range
          children[i] = children[i].clone();
        children[i].mergeSibling(children[i + 1], maxSize);
        children.splice(i + 1, 1);
        this.keys.splice(i + 1, 1);
        this.keys[i] = children[i].maxKey();
        return true;
      }
    }
    return false;
  }

  /**
   * Move children from `rhs` into this.
   * `rhs` must be part of this tree, and be removed from it after this call
   * (otherwise isShared for its children could be incorrect).
   */
  mergeSibling(rhs: BNode<K>, maxNodeSize: number) {
    // assert !this.isShared;
    const oldLength = this.keys.length;
    this.keys.push(...rhs.keys);
    const rhsChildren = (rhs as unknown as BNodeInternal<K>).children;
    this.children.push(...rhsChildren);

    if (rhs.isShared && !this.isShared) {
      // All children of a shared node are implicitly shared, and since their new
      // parent is not shared, they must now be explicitly marked as shared.
      for (let i = 0; i < rhsChildren.length; i++) {
        rhsChildren[i].isShared = true;
      }
    }

    // If our children are themselves almost empty due to a mass-delete,
    // they may need to be merged too (but only the oldLength-1 and its
    // right sibling should need this).
    this.tryMerge(oldLength - 1, maxNodeSize);
  }
}

// If key not found, returns i^failXor where i is the insertion index.
// Callers that don't care whether there was a match will set failXor=0.
function indexOf<K>(
  key: K,
  keys: K[],
  failXor: number,
  comparator: Comparator<K>,
): number {
  let lo = 0;
  let hi = keys.length;
  let mid = hi >> 1;
  while (lo < hi) {
    const c = comparator(keys[mid], key);
    if (c < 0) {
      lo = mid + 1;
    } else if (c > 0) {
      // key < keys[mid]
      hi = mid;
    } else if (c === 0) {
      return mid;
    } else {
      // c is NaN or otherwise invalid
      if (key === key) {
        // at least the search key is not NaN
        return keys.length;
      }
      throw new Error('NaN was used as a key');
    }
    mid = (lo + hi) >> 1;
  }
  return mid ^ failXor;
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
const emptyLeaf = new BNode<any>([]);
emptyLeaf.isShared = true;
