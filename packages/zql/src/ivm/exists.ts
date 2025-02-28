import {areEqual} from '../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import {type Change} from './change.ts';
import {
  drainStreams,
  normalizeUndefined,
  type Node,
  type NormalizedValue,
} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {first} from './stream.ts';

type SizeStorageKey = `row/${string}/${string}`;
type CacheStorageKey = `row/${string}`;

interface ExistsStorage {
  get(key: SizeStorageKey | CacheStorageKey): number | undefined;
  set(key: SizeStorageKey | CacheStorageKey, value: number): void;
  del(key: SizeStorageKey | CacheStorageKey): void;
  scan({prefix}: {prefix: `${CacheStorageKey}/`}): Iterable<[string, number]>;
}

/**
 * The Exists operator filters data based on whether or not a relationship is
 * non-empty.
 */
export class Exists implements Operator {
  readonly #input: Input;
  readonly #relationshipName: string;
  readonly #storage: ExistsStorage;
  readonly #not: boolean;
  readonly #parentJoinKey: CompoundKey;
  readonly #skipCache: boolean;

  #output: Output = throwOutput;

  constructor(
    input: Input,
    storage: Storage,
    relationshipName: string,
    parentJoinKey: CompoundKey,
    type: 'EXISTS' | 'NOT EXISTS',
  ) {
    this.#input = input;
    this.#relationshipName = relationshipName;
    this.#input.setOutput(this);
    this.#storage = storage as ExistsStorage;
    assert(this.#input.getSchema().relationships[relationshipName]);
    this.#not = type === 'NOT EXISTS';
    this.#parentJoinKey = parentJoinKey;

    // If the parentJoinKey is the primary key, no sense in caching.
    this.#skipCache = areEqual(
      parentJoinKey,
      this.#input.getSchema().primaryKey,
    );
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest) {
    for (const node of this.#input.fetch(req)) {
      if (this.#filter(node)) {
        yield node;
      }
    }
  }

  *cleanup(req: FetchRequest) {
    for (const node of this.#input.cleanup(req)) {
      if (this.#filter(node)) {
        yield node;
      } else {
        drainStreams(node);
      }
      this.#delSize(node);
    }
  }

  push(change: Change) {
    switch (change.type) {
      // add, remove and edit cannot change the size of the
      // this.#relationshipName relationship, so simply #pushWithFilter
      case 'add':
      case 'edit': {
        this.#pushWithFilter(change);
        return;
      }
      case 'remove': {
        const size = this.#getSize(change.node);
        // If size is undefined, this operator has not output
        // this row before and so it is unnecessary to output a remove for
        // it.  Which is fortunate, since #fetchSize would
        // not be able to fetch a Node for this change since it is
        // removed from the source.
        if (size === undefined) {
          return;
        }

        this.#pushWithFilter(change, size);
        this.#delSize(change.node);
        return;
      }
      case 'child':
        // Only add and remove child changes for the
        // this.#relationshipName relationship, can change the size
        // of the this.#relationshipName relationship, for other
        // child changes simply #pushWithFilter
        if (
          change.child.relationshipName !== this.#relationshipName ||
          change.child.change.type === 'edit' ||
          change.child.change.type === 'child'
        ) {
          this.#pushWithFilter(change);
          return;
        }
        switch (change.child.change.type) {
          case 'add': {
            let size = this.#getSize(change.node);
            if (size !== undefined) {
              size++;
              this.#setCachedSize(change.node, size);
              this.#setSize(change.node, size);
            } else {
              size = this.#fetchSize(change.node);
            }
            if (size === 1) {
              if (this.#not) {
                // Since the add child change currently being processed is not
                // pushed to output, the added child needs to be excluded from
                // the remove being pushed to output (since the child has
                // never been added to the output).
                this.#output.push({
                  type: 'remove',
                  node: {
                    row: change.node.row,
                    relationships: {
                      ...change.node.relationships,
                      [this.#relationshipName]: () => [],
                    },
                  },
                });
              } else {
                this.#output.push({
                  type: 'add',
                  node: change.node,
                });
              }
            } else {
              this.#pushWithFilter(change, size);
            }
            return;
          }
          case 'remove': {
            let size = this.#getSize(change.node);
            if (size !== undefined) {
              // Work around for issue https://bugs.rocicorp.dev/issue/3204
              // assert(size > 0);
              if (size === 0) {
                return;
              }
              size--;
              this.#setCachedSize(change.node, size);
              this.#setSize(change.node, size);
            } else {
              size = this.#fetchSize(change.node);
            }
            if (size === 0) {
              if (this.#not) {
                this.#output.push({
                  type: 'add',
                  node: change.node,
                });
              } else {
                // Since the remove child change currently being processed is
                // not pushed to output, the removed child needs to be added to
                // the remove being pushed to output.
                this.#output.push({
                  type: 'remove',
                  node: {
                    row: change.node.row,
                    relationships: {
                      ...change.node.relationships,
                      [this.#relationshipName]: () => [
                        change.child.change.node,
                      ],
                    },
                  },
                });
              }
            } else {
              this.#pushWithFilter(change, size);
            }
            return;
          }
        }
        return;
      default:
        unreachable(change);
    }
  }

  /**
   * Returns whether or not the node's this.#relationshipName
   * relationship passes the exist/not exists filter condition.
   * If the optional `size` is passed it is used.
   * Otherwise, if there is a stored size for the row it is used.
   * Otherwise the size is computed by streaming the node's
   * relationship with this.#relationshipName (this computed size is also
   * stored).
   */
  #filter(node: Node, size?: number): boolean {
    const exists = (size ?? this.#getOrFetchSize(node)) > 0;
    return this.#not ? !exists : exists;
  }

  /**
   * Pushes a change if this.#filter is true for its row.
   */
  #pushWithFilter(change: Change, size?: number): void {
    if (this.#filter(change.node, size)) {
      this.#output.push(change);
    }
  }

  #getSize(node: Node): number | undefined {
    return this.#storage.get(this.#makeSizeStorageKey(node));
  }

  #setSize(node: Node, size: number) {
    this.#storage.set(this.#makeSizeStorageKey(node), size);
  }

  #setCachedSize(node: Node, size: number) {
    if (this.#skipCache) {
      return;
    }

    this.#storage.set(this.#makeCacheStorageKey(node), size);
  }

  #getCachedSize(node: Node): number | undefined {
    if (this.#skipCache) {
      return undefined;
    }

    return this.#storage.get(this.#makeCacheStorageKey(node));
  }

  #delSize(node: Node) {
    this.#storage.del(this.#makeSizeStorageKey(node));
    if (!this.#skipCache) {
      const cacheKey = this.#makeCacheStorageKey(node);
      if (first(this.#storage.scan({prefix: `${cacheKey}/`})) === undefined) {
        this.#storage.del(cacheKey);
      }
    }
  }

  #getOrFetchSize(node: Node): number {
    const size = this.#getSize(node);
    if (size !== undefined) {
      return size;
    }
    return this.#fetchSize(node);
  }

  #fetchSize(node: Node) {
    const cachedSize = this.#getCachedSize(node);
    if (cachedSize !== undefined) {
      this.#setSize(node, cachedSize);
      return cachedSize;
    }

    const relationship = node.relationships[this.#relationshipName];
    assert(relationship);
    let size = 0;
    for (const _relatedNode of relationship()) {
      size++;
    }

    this.#setCachedSize(node, size);
    this.#setSize(node, size);
    return size;
  }

  #makeCacheStorageKey(node: Node): CacheStorageKey {
    return `row/${JSON.stringify(
      this.#getKeyValues(node, this.#parentJoinKey),
    )}`;
  }

  #makeSizeStorageKey(node: Node): SizeStorageKey {
    return `row/${
      this.#skipCache
        ? ''
        : JSON.stringify(this.#getKeyValues(node, this.#parentJoinKey))
    }/${JSON.stringify(
      this.#getKeyValues(node, this.#input.getSchema().primaryKey),
    )}`;
  }

  #getKeyValues(node: Node, def: CompoundKey): NormalizedValue[] {
    const values: NormalizedValue[] = [];
    for (const key of def) {
      values.push(normalizeUndefined(node.row[key]));
    }
    return values;
  }
}
