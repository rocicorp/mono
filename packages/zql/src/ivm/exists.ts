import {assert, unreachable} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import {rowForChange, type Change} from './change.js';
import {normalizeUndefined, type NormalizedValue} from './data.js';
import type {
  FetchRequest,
  Input,
  Operator,
  Output,
  Storage,
} from './operator.js';
import type {SourceSchema} from './schema.js';
import {first} from './stream.js';

interface ExistsStorage {
  get(key: string): number | undefined;
  set(key: string, value: number): void;
  del(key: string): void;
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

  #output: Output | undefined;

  constructor(
    input: Input,
    storage: Storage,
    relationshipName: string,
    type: 'EXISTS' | 'NOT EXISTS',
  ) {
    this.#input = input;
    this.#relationshipName = relationshipName;
    this.#input.setOutput(this);
    this.#storage = storage as ExistsStorage;
    assert(this.#input.getSchema().relationships[relationshipName]);
    this.#not = type === 'NOT EXISTS';
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
    assert(this.#output, 'Output not set');

    switch (change.type) {
      // add, remove and edit cannot change the size of the
      // this.#relationshipName relationship, so simply #pushWithFilter
      case 'add':
      case 'edit': {
        this.#pushWithFilter(change);
        return;
      }
      case 'remove': {
        this.#pushWithFilter(change);
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
              this.#setSize(change.row, size);
            } else {
              size = this.#fetchSize(change.row);
            }
            if (size === 1) {
              this.#output.push({
                type: this.#not ? 'remove' : 'add',
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
              assert(size > 0);
              size--;
              this.#setSize(change.row, size);
            } else {
              size = this.#fetchSize(change.row);
            }
            if (size === 0) {
              this.#output.push({
                type: this.#not ? 'add' : 'remove',
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
      must(this.#output).push(change);
    }
  }

  #getSize(row: Row): number | undefined {
    return this.#storage.get(this.#makeSizeStorageKey(row));
  }

  #setSize(row: Row, size: number) {
    this.#storage.set(this.#makeSizeStorageKey(row), size);
  }

  #delSize(row: Row) {
    this.#storage.del(this.#makeSizeStorageKey(row));
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
    const relationship =
      this.#fetchNodeForRow(row).relationships[this.#relationshipName];
    assert(relationship);
    let size = 0;
    for (const _relatedNode of relationship) {
      size++;
    }
    this.#setSize(row, size);
    return size;
  }

  #fetchNodeForRow(row: Row) {
    return must(
      first(
        this.#input.fetch({
          start: {row, basis: 'at'},
        }),
      ),
    );
  }

  #makeSizeStorageKey(row: Row) {
    const primaryKey: NormalizedValue[] = [];
    for (const key of this.#input.getSchema().primaryKey) {
      primaryKey.push(normalizeUndefined(row[key]));
    }
    return JSON.stringify(['size', primaryKey]);
  }
}
