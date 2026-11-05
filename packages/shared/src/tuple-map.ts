import {getOrInsertComputed} from './map.ts';

/** The value for the key that ends at this node, and the longer keys. */
type Node<V> = [value: V | undefined, children: Map<unknown, Node<V>>];

const newNode = <V>(): Node<V> => [undefined, new Map()];

/**
 * A map whose keys are tuples. Two keys are the same if they have the same
 * length and their elements are the same, as a {@link Map} compares keys. No
 * key is serialized, so the elements can be objects, compared by identity.
 *
 * Keys of different lengths can be mixed: a key and a prefix of it are
 * different keys.
 *
 * The values cannot be `undefined`, which is how a missing entry is told
 * apart.
 */
export class TupleMap<K extends readonly unknown[], V extends {} | null> {
  readonly #root: Node<V> = newNode();

  get(key: K): V | undefined {
    let node: Node<V> | undefined = this.#root;
    for (const element of key) {
      node = node[1].get(element);
      if (node === undefined) {
        return undefined;
      }
    }
    return node[0];
  }

  set(key: K, value: V): this {
    this.#node(key)[0] = value;
    return this;
  }

  /**
   * Returns the value for {@link key}. If there is none, calls
   * {@link compute} with the key, inserts the result and returns it.
   */
  getOrInsertComputed(key: K, compute: (key: K) => V): V {
    const node = this.#node(key);
    let value = node[0];
    if (value === undefined) {
      value = node[0] = compute(key);
    }
    return value;
  }

  #node(key: K): Node<V> {
    let node = this.#root;
    for (const element of key) {
      node = getOrInsertComputed(node[1], element, newNode<V>);
    }
    return node;
  }
}
