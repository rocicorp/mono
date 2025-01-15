export type Comparator<T> = (l: T, r: T) => number;

class Node<T> {
  size: number = 1;
  value: T;
  priority: number;
  left: Node<T> | null;
  right: Node<T> | null;

  constructor(
    value: T,
    priority: number,
    left: Node<T> | null,
    right: Node<T> | null,
  ) {
    this.value = value;
    this.priority = priority;
    this.left = left;
    this.right = right;
  }
}

export type {Node};

export class Treap<T> {
  readonly #comparator: Comparator<T>;
  #root: Node<T> | null = null;

  constructor(comparator: Comparator<T>) {
    this.#comparator = comparator;
  }

  get size(): number {
    return this.#root?.size ?? 0;
  }

  get root(): Node<T> | null {
    return this.#root;
  }

  add(value: T): Treap<T> {
    const priority = Math.random(); // Random priority
    this.#root = insert(this.#root, value, priority, this.#comparator);
    return this;
  }

  delete(value: T): Treap<T> {
    this.#root = remove(this.#root, value, this.#comparator);
    return this;
  }

  clear(): Treap<T> {
    this.#root = null;
    return this;
  }

  has(value: T): boolean {
    return has(this.#root, value, this.#comparator);
  }

  get(value: T): T | undefined {
    let currentNode = this.#root;

    while (currentNode) {
      const cmp = this.#comparator(value, currentNode.value);

      if (cmp === 0) {
        return currentNode.value;
      }
      if (cmp < 0) {
        currentNode = currentNode.left;
      } else {
        currentNode = currentNode.right;
      }
    }

    return undefined;
  }

  [Symbol.iterator](): IterableIterator<T> {
    return values(this.#root);
  }

  values(): IterableIterator<T> {
    return values(this.#root);
  }

  keys(): IterableIterator<T> {
    return values(this.#root);
  }

  valuesReversed(): IterableIterator<T> {
    return valuesReversed(this.#root);
  }

  valuesFrom(value?: T): IterableIterator<T> {
    if (value === undefined) {
      return values(this.#root);
    }
    return valuesFrom(this.#root, value, this.#comparator);
  }

  valuesFromReversed(value?: T): IterableIterator<T> {
    if (value === undefined) {
      return valuesReversed(this.#root);
    }
    return valuesFromReversed(this.#root, value, this.#comparator);
  }
}

function* values<T>(node: Node<T> | null): IterableIterator<T> {
  if (node) {
    yield* values(node.left);
    yield node.value;
    yield* values(node.right);
  }
}

function* valuesFrom<T>(
  node: Node<T> | null,
  startAt: T,
  comparator: Comparator<T>,
): IterableIterator<T> {
  if (!node) {
    return;
  }
  const cmp = comparator(startAt, node.value);
  if (cmp < 0) {
    yield* valuesFrom(node.left, startAt, comparator);
  }
  if (cmp <= 0) {
    yield node.value;
  }
  yield* valuesFrom(node.right, startAt, comparator);
}

function* valuesReversed<T>(node: Node<T> | null): IterableIterator<T> {
  if (node) {
    yield* valuesReversed(node.right);
    yield node.value;
    yield* valuesReversed(node.left);
  }
}

function* valuesFromReversed<T>(
  node: Node<T> | null,
  startAt: T,
  comparator: Comparator<T>,
): IterableIterator<T> {
  if (!node) {
    return;
  }
  const cmp = comparator(startAt, node.value);
  if (cmp > 0) {
    yield* valuesFromReversed(node.right, startAt, comparator);
  }
  if (cmp >= 0) {
    yield node.value;
  }
  yield* valuesFromReversed(node.left, startAt, comparator);
}

function findMin<T>(node: Node<T>): Node<T> {
  while (node.left) {
    node = node.left;
  }
  return node;
}

function rotateLeft<T>(node: Node<T>): Node<T> {
  const newNode = node.right!;
  node.right = newNode.left;
  newNode.left = node;

  newNode.size = node.size;
  node.size =
    1 + (node.left ? node.left.size : 0) + (node.right ? node.right.size : 0);

  return newNode;
}

function rotateRight<T>(node: Node<T>): Node<T> {
  const newNode = node.left!;
  node.left = newNode.right;
  newNode.right = node;

  newNode.size = node.size;
  node.size =
    1 + (node.left ? node.left.size : 0) + (node.right ? node.right.size : 0);

  return newNode;
}

function balance<T>(node: Node<T>): Node<T> {
  if (node.right && node.right.priority < node.priority) {
    node = rotateLeft(node);
  }
  if (node.left && node.left.priority < node.priority) {
    node = rotateRight(node);
  }
  return node;
}

function removeMin<T>(node: Node<T>): Node<T> | null {
  if (!node.left) {
    return node.right;
  }
  const newNode = node;
  newNode.size = node.size - 1;
  newNode.left = removeMin(node.left);
  newNode.right = node.right;
  newNode.size =
    1 +
    (newNode.left ? newNode.left.size : 0) +
    (newNode.right ? newNode.right.size : 0); // Recalculate the size.
  return balance(newNode);
}

function has<T>(
  node: Node<T> | null,
  value: T,
  comparator: Comparator<T>,
): boolean {
  if (!node) {
    return false;
  }

  const cmp = comparator(value, node.value);
  if (cmp === 0) {
    return true;
  }
  if (cmp < 0) {
    return has(node.left, value, comparator);
  }
  return has(node.right, value, comparator);
}

function insert<T>(
  node: Node<T> | null,
  value: T,
  priority: number,
  comparator: Comparator<T>,
): Node<T> {
  if (!node) {
    return new Node(value, priority, null, null);
  }

  const cmp = comparator(value, node.value);
  const newNode = node;
  newNode.left = node.left;
  newNode.right = node.right;
  newNode.size = node.size + 1; // Increment the size since we're inserting.

  if (cmp < 0) {
    newNode.left = insert(newNode.left, value, priority, comparator);
  } else if (cmp > 0) {
    newNode.right = insert(newNode.right, value, priority, comparator);
  } else {
    newNode.value = value; // Duplicate insertion, just overwrite.
  }

  newNode.size = (newNode.left?.size ?? 0) + (newNode.right?.size ?? 0) + 1;
  return balance(newNode); // Balance the node after insertion.
}

function remove<T>(
  node: Node<T> | null,
  value: T,
  comparator: Comparator<T>,
): Node<T> | null {
  if (!node) {
    return null;
  }

  const newNode = node;

  const cmp = comparator(value, newNode.value);
  if (cmp < 0) {
    newNode.left = remove(newNode.left, value, comparator);
  } else if (cmp > 0) {
    newNode.right = remove(newNode.right, value, comparator);
  } else {
    if (!newNode.left) {
      return newNode.right;
    }
    if (!newNode.right) {
      return newNode.left;
    }

    const minRightNode = findMin(newNode.right);
    newNode.value = minRightNode.value; // Update the value with the min value from the right subtree.
    newNode.right = removeMin(newNode.right);
  }

  newNode.size =
    1 +
    (newNode.left ? newNode.left.size : 0) +
    (newNode.right ? newNode.right.size : 0); // Recalculate the size.
  return balance(newNode);
}
