import {areEqual} from '../../../shared/src/arrays.js';
import {assert, unreachable} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import type {CompoundKey} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import {rowForChange, type Change} from './change.js';
import {normalizeUndefined, type NormalizedValue} from './data.js';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
  type Storage,
} from './operator.js';
import type {SourceSchema} from './schema.js';
import {first} from './stream.js';

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
      if (this.#filter(node.row)) {
        yield node;
      }
    }
  }

  *cleanup(req: FetchRequest) {
    for (const node of this.#input.cleanup(req)) {
      if (this.#filter(node.row)) {
        yield node;
      }
      this.#delSize(node.row);
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
        const size = this.#getSize(change.node.row);
        // If size is undefined, this operator has not output
        // this row before and so it is unnecessary to output a remove for
        // it.  Which is fortunate, since #fetchSize/#fetchNodeForRow would
        // not be able to fetch a Node for this change since it is
        // removed from the source.
        if (size === undefined) {
          return;
        }

        this.#pushWithFilter(change, size);
        this.#delSize(change.node.row);
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
            let size = this.#getSize(change.row);
            if (size !== undefined) {
              size++;
              this.#setCachedSize(change.row, size);
              this.#setSize(change.row, size);
            } else {
              size = this.#fetchSize(change.row);
            }
            if (size === 1) {
              const type = this.#not ? 'remove' : 'add';
              // The node for the remove pushed below will contain the child
              // added by this change in its
              // relationships[this.#relationshipName],
              // so this child add needs to be sent first.  This balance
              // is important for outputs doing ref counting,
              if (type === 'remove') {
                this.#output.push(change);
              }
              this.#output.push({
                type,
                node: this.#fetchNodeForRow(change.row),
              });
            } else {
              this.#pushWithFilter(change, size);
            }
            return;
          }
          case 'remove': {
            let size = this.#getSize(change.row);
            if (size !== undefined) {
              // Work around for issue https://bugs.rocicorp.dev/issue/3204
              // assert(size > 0);
              if (size === 0) {
                return;
              }
              size--;
              this.#setCachedSize(change.row, size);
              this.#setSize(change.row, size);
            } else {
              size = this.#fetchSize(change.row);
            }
            if (size === 0) {
              const type = this.#not ? 'add' : 'remove';
              // The node for the remove pushed below will not contain the child
              // removed by this change in its
              // relationships[this.#relationshipName],
              // so this child remove needs to be sent.
              if (type === 'remove') {
                this.#output.push(change);
              }
              this.#output.push({
                type,
                node: this.#fetchNodeForRow(change.row),
              });
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
   * Returns whether or not the change's row's this.#relationshipName
   * relationship passes the exist/not exists filter condition.
   * If the optional `size` is passed it is used.
   * Otherwise, if there is a stored size for the row it is used.
   * Otherwise the size is computed by fetching a node for the row from
   * this.#input (this computed size is also stored).
   */
  #filter(row: Row, size?: number): boolean {
    const exists = (size ?? this.#getOrFetchSize(row)) > 0;
    return this.#not ? !exists : exists;
  }

  /**
   * Pushes a change if this.#filter is true for its row.
   */
  #pushWithFilter(change: Change, size?: number): void {
    const row = rowForChange(change);
    if (this.#filter(row, size)) {
      this.#output.push(change);
    }
  }

  #getSize(row: Row): number | undefined {
    return this.#storage.get(this.#makeSizeStorageKey(row));
  }

  #setSize(row: Row, size: number) {
    this.#storage.set(this.#makeSizeStorageKey(row), size);
  }

  #setCachedSize(row: Row, size: number) {
    if (this.#skipCache) {
      return;
    }

    this.#storage.set(this.#makeCacheStorageKey(row), size);
  }

  #getCachedSize(row: Row): number | undefined {
    if (this.#skipCache) {
      return undefined;
    }

    return this.#storage.get(this.#makeCacheStorageKey(row));
  }

  #delSize(row: Row) {
    this.#storage.del(this.#makeSizeStorageKey(row));
    if (!this.#skipCache) {
      const cacheKey = this.#makeCacheStorageKey(row);
      if (first(this.#storage.scan({prefix: `${cacheKey}/`})) === undefined) {
        this.#storage.del(cacheKey);
      }
    }
  }

  #getOrFetchSize(row: Row): number {
    const size = this.#getSize(row);
    if (size !== undefined) {
      return size;
    }
    // We fetch this node so we can consume the relationship to
    // determine its size (we can't consume the relationship of
    // the node we are going to push or return via fetch to
    // our output, because the relationships are one time use streams).
    return this.#fetchSize(row);
  }

  #fetchSize(row: Row) {
    const cachedSize = this.#getCachedSize(row);
    if (cachedSize !== undefined) {
      this.#setSize(row, cachedSize);
      return cachedSize;
    }

    const relationship =
      this.#fetchNodeForRow(row).relationships[this.#relationshipName];
    assert(relationship);
    let size = 0;
    for (const _relatedNode of relationship) {
      size++;
    }

    this.#setCachedSize(row, size);
    this.#setSize(row, size);
    return size;
  }

  #fetchNodeForRow(row: Row) {
    const fetched = must(
      first(
        this.#input.fetch({
          start: {row, basis: 'at'},
        }),
      ),
    );
    assert(
      this.getSchema().compareRows(row, fetched.row) === 0,
      () =>
        `fetchNodeForRow returned unexpected row, expected ${JSON.stringify(
          row,
        )}, received ${JSON.stringify(fetched.row)}`,
    );
    return fetched;
  }

  #makeCacheStorageKey(row: Row): CacheStorageKey {
    return `row/${JSON.stringify(
      this.#getKeyValues(row, this.#parentJoinKey),
    )}`;
  }

  #makeSizeStorageKey(row: Row): SizeStorageKey {
    return `row/${
      this.#skipCache
        ? ''
        : JSON.stringify(this.#getKeyValues(row, this.#parentJoinKey))
    }/${JSON.stringify(
      this.#getKeyValues(row, this.#input.getSchema().primaryKey),
    )}`;
  }

  #getKeyValues(row: Row, def: CompoundKey): NormalizedValue[] {
    const values: NormalizedValue[] = [];
    for (const key of def) {
      values.push(normalizeUndefined(row[key]));
    }
    return values;
  }
}
